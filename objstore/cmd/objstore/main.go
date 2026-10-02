// Command objstore serves an objstore store on its own, outside a suite: for
// a reproducer script that needs an S3 endpoint whose faults it injects,
// without the suite that found the bug.
//
// Usage:
//
//	objstore [-addr HOST:PORT] [-bucket NAME]... [-history FILE] [-unique-etags]
//
// It prints "endpoint: http://HOST:PORT" once it serves, and on SIGINT or
// SIGTERM writes the history of every request to -history, if set, and
// exits. Besides S3, it serves a control API under /_objstore/, which no
// S3 bucket name can collide with:
//
//	POST   /_objstore/rules      add a rule (objstore.Rule as JSON; "delay" as "1s"); answers {"id": N}
//	DELETE /_objstore/rules/N    remove rule N
//	DELETE /_objstore/rules      remove every rule
//	GET    /_objstore/history    the history so far, one JSON object per line
//
// For example, to fail one client's puts to the WAL after they take effect:
//
//	curl -X POST localhost:9000/_objstore/rules \
//	  -d '{"Clients":["w1"],"Ops":["put"],"Prefix":"db/wal/","Prob":1,"Action":"fail-after"}'
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dotnwat/torx/objstore"
)

type buckets []string

func (b *buckets) String() string     { return strings.Join(*b, ",") }
func (b *buckets) Set(v string) error { *b = append(*b, v); return nil }

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "address to serve on")
	history := flag.String("history", "", "file to write the request history to on exit")
	unique := flag.Bool("unique-etags", false, "give every write an ETag of its own, rather than its content's MD5")
	quiet := flag.Bool("quiet", false, "do not log the faults injected")
	var bs buckets
	flag.Var(&bs, "bucket", "a bucket to create (repeatable)")
	flag.Parse()

	opts := objstore.Options{UniqueETags: *unique}
	if !*quiet {
		opts.Logf = log.Printf
	}
	s := objstore.New(opts)
	for _, b := range bs {
		s.CreateBucket(b)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_objstore/rules", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			objstore.Rule
			Delay string
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rule := in.Rule
		if in.Delay != "" {
			d, err := time.ParseDuration(in.Delay)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			rule.Delay = d
		}
		id := s.AddRule(rule)
		log.Printf("objstore: rule %d: %+v", id, rule)
		_, _ = fmt.Fprintf(w, "{\"id\": %d}\n", id)
	})
	mux.HandleFunc("DELETE /_objstore/rules/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.RemoveRule(id)
		log.Printf("objstore: removed rule %d", id)
	})
	mux.HandleFunc("DELETE /_objstore/rules", func(w http.ResponseWriter, r *http.Request) {
		s.ClearRules()
		log.Printf("objstore: removed every rule")
	})
	mux.HandleFunc("GET /_objstore/history", func(w http.ResponseWriter, r *http.Request) {
		_ = s.WriteHistory(w)
	})
	mux.Handle("/", s)

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 30 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	fmt.Printf("endpoint: http://%s\n", *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errc:
		log.Fatal(err)
	case <-sig:
	}
	_ = srv.Close()
	if *history != "" {
		f, err := os.Create(*history)
		if err != nil {
			log.Fatal(err)
		}
		if err := s.WriteHistory(f); err != nil {
			log.Fatal(err)
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
