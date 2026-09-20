//go:build unix

// Command kvd is the system under test for the torx tutorial: a small HTTP
// key-value server, and the load generator that exercises it.
//
//	kvd serve --listen ADDR --port PORT --dir DIR
//	kvd load --addr HOST:PORT [--clients N] [--seconds S] [--keys K] [--prefix P]
//	kvd version
//
// serve keeps every write in an append-only log under DIR and replays it
// before it opens its port, so a restart -- even after a kill -- serves what
// was written; answers 200 on /readyz once it is listening; and exits 0 on
// SIGTERM once the requests in flight have finished, closing the connections
// that carried none. load runs N clients that
// each write a key and read it back for S seconds, and prints one JSON
// summary; each client checks it reads back what it wrote, so two load
// generators against one server must be given distinct prefixes. version
// prints the version, which a suite can use as a preflight that the binary
// is on a node's PATH.
//
// The API is four routes: PUT /kv/{key} with the value as the body (204),
// GET /kv/{key} (200 with the value, or 404), GET /readyz, and GET /stats
// (a JSON count of keys, puts, and gets).
//
// kvd knows nothing about torx. The tutorial's suites drive it the way they
// would drive any server: by name from the node's PATH, through its command
// line and its HTTP API. The client package beside it is theirs, not kvd's.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const version = "kvd 1.0"

func main() {
	// Everything kvd says goes to stdout, which the tutorial's service
	// captures into the results tree.
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("kvd: ")
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	code := 0
	switch os.Args[1] {
	case "serve":
		code = serveMain(os.Args[2:])
	case "load":
		code = loadMain(os.Args[2:])
	case "version":
		fmt.Println(version)
	default:
		usage()
		code = 2
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvd serve|load|version [flags]")
}

// ---- serve ----

// serveMain runs the server until SIGTERM or SIGINT, and exits 0 on a clean
// stop so that a suite asserting on the exit status sees one.
func serveMain(args []string) int {
	fs := flag.NewFlagSet("kvd serve", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1", "address to bind")
	port := fs.Int("port", 8080, "TCP port to listen on")
	dir := fs.String("dir", "", "data directory (required); the log lives at DIR/kv.log")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "kvd serve: --dir is required")
		return 2
	}

	st, err := openStore(filepath.Join(*dir, "kv.log"))
	if err != nil {
		log.Print(err)
		return 1
	}
	defer func() { _ = st.Close() }()
	log.Printf("replayed %d entries from %s", st.Len(), st.path)

	ln, err := net.Listen("tcp", net.JoinHostPort(*listen, strconv.Itoa(*port)))
	if err != nil {
		log.Print(err)
		return 1
	}
	log.Printf("listening on %s", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	srv, conns := newServer(st)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		log.Print(err)
		return 1
	case <-ctx.Done():
	}
	log.Printf("shutting down with %s", conns.summary())
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Printf("shutdown: %v; still open: %s", err, conns.summary())
		return 1
	}
	log.Print("stopped")
	return 0
}

// shutdownTimeout bounds how long a graceful shutdown waits for requests in
// flight before the server gives up and exits non-zero.
const shutdownTimeout = 5 * time.Second

// newServer builds the HTTP server over st, with its connections tracked.
//
// A graceful Shutdown finishes the requests in flight and closes the idle
// connections. A connection that was accepted but never sent a request is
// neither: net/http gives it five seconds to speak before treating it as
// idle, which from outside is a server that takes five seconds to stop.
// Such connections are common -- a browser pre-connects, and an HTTP
// client library can leave a pooled connection it never used -- and nothing
// is in flight on them, so they are closed as soon as Shutdown has closed
// the listener.
func newServer(st *store) (*http.Server, *connTracker) {
	conns := &connTracker{conns: map[net.Conn]connInfo{}}
	srv := &http.Server{Handler: st.handler(), ConnState: conns.onState}
	srv.RegisterOnShutdown(conns.closeNew)
	return srv, conns
}

// connInfo is a connection's last state and when it entered it.
type connInfo struct {
	state http.ConnState
	since time.Time
}

// connTracker follows every connection's state, for the shutdown log.
type connTracker struct {
	mu    sync.Mutex
	conns map[net.Conn]connInfo
}

func (t *connTracker) onState(c net.Conn, st http.ConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch st {
	case http.StateClosed, http.StateHijacked:
		delete(t.conns, c)
	default:
		t.conns[c] = connInfo{state: st, since: time.Now()}
	}
}

// closeNew closes every connection that has not sent a request. The server
// notices the close on its next read and drops the connection from its own
// accounting.
func (t *connTracker) closeNew() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for c, info := range t.conns {
		if info.state == http.StateNew {
			_ = c.Close()
		}
	}
}

// summary describes the open connections: how many, and each one that is
// not idle with its peer, state, and how long it has been in that state.
func (t *connTracker) summary() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var parts []string
	for c, info := range t.conns {
		if info.state == http.StateIdle {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %s for %s", c.RemoteAddr(), info.state, time.Since(info.since).Round(time.Millisecond)))
	}
	slices.Sort(parts)
	if len(parts) == 0 {
		return fmt.Sprintf("%d connection(s), all idle", len(t.conns))
	}
	return fmt.Sprintf("%d connection(s), not idle: %s", len(t.conns), strings.Join(parts, "; "))
}

// entry is one line of the log: a key and the value written to it.
type entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// store is the key-value map and the append-only log that makes it durable.
// A write goes to the log before the map, and the response only goes out
// once the write has reached the kernel -- which is what killing the process
// cannot undo. (A power loss could; a real store would fsync.)
type store struct {
	path string

	mu   sync.Mutex
	data map[string]string
	log  *os.File
	puts int
	gets int
}

// openStore opens or creates the log at path and replays it.
func openStore(path string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st := &store{path: path, data: map[string]string{}, log: f}
	if err := st.replay(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("replay %s: %w", path, err)
	}
	return st, nil
}

// replay applies the log's entries in order. A kill can cut a write short,
// leaving a torn last line; that write was never acknowledged, so the line is
// dropped and the log truncated to the last complete entry before anything is
// appended after it. A torn line anywhere else is corruption, and an error.
func (s *store) replay() error {
	data, err := io.ReadAll(s.log)
	if err != nil {
		return err
	}
	valid := 0
	for len(data) > valid {
		rest := data[valid:]
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break // a torn tail: the bytes after the last newline
		}
		var e entry
		if err := json.Unmarshal(rest[:i], &e); err != nil {
			return fmt.Errorf("corrupt entry at byte %d: %w", valid, err)
		}
		s.data[e.Key] = e.Value
		valid += i + 1
	}
	if valid < len(data) {
		log.Printf("dropping a torn entry of %d bytes at the end of %s", len(data)-valid, s.path)
		if err := s.log.Truncate(int64(valid)); err != nil {
			return err
		}
	}
	_, err = s.log.Seek(int64(valid), io.SeekStart)
	return err
}

// Len is the number of keys.
func (s *store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// Put records a write in the log and then the map.
func (s *store) Put(key, value string) error {
	line, err := json.Marshal(entry{Key: key, Value: value})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.log.Write(append(line, '\n')); err != nil {
		return err
	}
	s.data[key] = value
	s.puts++
	return nil
}

// Get returns the value at key.
func (s *store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	v, ok := s.data[key]
	return v, ok
}

// Close closes the log.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

// stats is the body of GET /stats.
type stats struct {
	Keys int `json:"keys"`
	Puts int `json:"puts"`
	Gets int `json:"gets"`
}

// maxValue bounds a value's size, so a client cannot exhaust the server.
const maxValue = 1 << 20

func (s *store) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		st := stats{Keys: len(s.data), Puts: s.puts, Gets: s.gets}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValue))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if err := s.Put(r.PathValue("key"), string(value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, ok := s.Get(r.PathValue("key"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, v)
	})
	return mux
}

// ---- load ----

// report is the JSON summary load prints: what it ran and what it measured.
// Latencies are per request; an op is one PUT or one GET.
type report struct {
	Clients   int     `json:"clients"`
	Seconds   float64 `json:"seconds"`
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Ms     float64 `json:"p50_ms"`
	P99Ms     float64 `json:"p99_ms"`
	Errors    int     `json:"errors"`
}

// loadMain runs the clients, prints the report, and exits 1 if any request
// failed or returned the wrong value -- the report is printed either way, so
// a caller can read it before deciding what an error means.
func loadMain(args []string) int {
	fs := flag.NewFlagSet("kvd load", flag.ContinueOnError)
	addr := fs.String("addr", "", "host:port of the server (required)")
	clients := fs.Int("clients", 1, "concurrent clients")
	seconds := fs.Float64("seconds", 2, "how long to run")
	keys := fs.Int("keys", 1000, "size of each client's key space")
	prefix := fs.String("prefix", "", "prefix for every key, to keep concurrent load generators apart")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *addr == "" || *clients < 1 || *seconds <= 0 || *keys < 1 {
		fmt.Fprintln(os.Stderr, "kvd load: --addr is required; --clients, --seconds and --keys must be positive")
		return 2
	}

	base := "http://" + *addr
	hc := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: *clients},
		Timeout:   10 * time.Second,
	}
	deadline := time.Now().Add(time.Duration(*seconds * float64(time.Second)))
	results := make([]clientResult, *clients)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() { results[i] = runClient(hc, base, *prefix, i, *keys, deadline) })
	}
	wg.Wait()

	rep := report{Clients: *clients, Seconds: *seconds}
	var lat []time.Duration
	for _, r := range results {
		rep.Ops += len(r.latencies)
		rep.Errors += r.errors
		lat = append(lat, r.latencies...)
	}
	slices.Sort(lat)
	rep.OpsPerSec = float64(rep.Ops) / *seconds
	rep.P50Ms = percentile(lat, 0.50)
	rep.P99Ms = percentile(lat, 0.99)
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(rep); err != nil {
		return 1
	}
	if rep.Errors > 0 {
		return 1
	}
	return 0
}

// clientResult is what one client measured.
type clientResult struct {
	latencies []time.Duration
	errors    int
}

// runClient writes and reads back random keys until the deadline. Each
// client works its own key space -- and each load generator its own, by the
// prefix -- so that the read-back check is not confused by another client's
// write. The first error is reported on stderr, so a run
// against a server that is not there says so once instead of a thousand
// times, and a client that cannot get through backs off rather than spinning.
func runClient(hc *http.Client, base, prefix string, id, keys int, deadline time.Time) clientResult {
	var res clientResult
	reported := false
	fail := func(err error) {
		res.errors++
		if !reported {
			fmt.Fprintf(os.Stderr, "kvd load: client %d: %v\n", id, err)
			reported = true
		}
		time.Sleep(10 * time.Millisecond)
	}
	for time.Now().Before(deadline) {
		key := fmt.Sprintf("%sc%d-k%d", prefix, id, randInt(keys))
		value := randHex()

		start := time.Now()
		if err := put(hc, base, key, value); err != nil {
			fail(err)
			continue
		}
		res.latencies = append(res.latencies, time.Since(start))

		start = time.Now()
		got, err := get(hc, base, key)
		if err != nil {
			fail(err)
			continue
		}
		res.latencies = append(res.latencies, time.Since(start))
		if got != value {
			fail(fmt.Errorf("read back %q for %s, wrote %q", got, key, value))
		}
	}
	return res
}

func put(hc *http.Client, base, key, value string) error {
	req, err := http.NewRequest(http.MethodPut, base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %s: %s: %s", key, resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func get(hc *http.Client, base, key string) (string, error) {
	resp, err := hc.Get(base + "/kv/" + url.PathEscape(key))
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: %s: %s", key, resp.Status, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// percentile returns the p-th percentile of sorted latencies in milliseconds,
// or 0 for none.
func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := min(int(float64(len(sorted))*p), len(sorted)-1)
	return float64(sorted[i]) / float64(time.Millisecond)
}

func randInt(n int) int {
	var b [8]byte
	_, _ = rand.Read(b[:])
	v := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	return v % n
}

func randHex() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
