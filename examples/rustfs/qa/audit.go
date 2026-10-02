//go:build unix

package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rustfs/qa/rustfs"
)

// auditor watches what each drive holds of each key while the job runs:
// every period it decodes every drive's xl.meta of every key, with
// RustFS's own decoder (crates/filemeta/examples/dump_versions, built from
// the RustFS tree under test), and remembers the versions each lists. A
// version a drive held and then does not is a copy lost: the end-of-run
// checks see only that a version is gone, and this says from which drive,
// and when, to find in the servers' logs.
type auditor struct {
	fs     *rustfs.Service
	h      *History
	bucket string
	keys   []string
	dump   string // the path of dump_versions on the nodes
	period time.Duration

	mu sync.Mutex
	// held is, by drive and key, each version's first and last sample
	// that held it, and lost the versions whose copies vanished.
	held map[string]map[string]*span
	lost []copyLost
	// failed counts samples that could not be read.
	failed int
}

// span is when a drive held a version: the first and the last sample that
// listed it.
type span struct{ first, last time.Duration }

// copyLost is a version a drive held, and then did not.
type copyLost struct {
	Drive    string        `json:"drive"`
	Key      string        `json:"key"`
	Version  string        `json:"version"`
	HeldFrom time.Duration `json:"held_from"`
	HeldTo   time.Duration `json:"held_to"`
	GoneAt   time.Duration `json:"gone_at"`
}

var versionIDPattern = regexp.MustCompile(`version_id=Some\(([0-9a-f-]+)\)`)

func newAuditor(fs *rustfs.Service, h *History, bucket string, keys []string, dump string) *auditor {
	return &auditor{fs: fs, h: h, bucket: bucket, keys: keys, dump: dump, period: time.Second,
		held: map[string]map[string]*span{}}
}

// run samples every period until ctx ends, and once more after.
func (a *auditor) run(ctx context.Context) {
	t := time.NewTicker(a.period)
	defer t.Stop()
	for {
		a.sample(context.WithoutCancel(ctx))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Sample decodes every drive's xl.meta of every key, and notes what each
// holds and what each no longer does.
func (a *auditor) sample(ctx context.Context) {
	var wg sync.WaitGroup
	for _, n := range a.fs.Nodes() {
		for d := range a.fs.Drives() {
			wg.Go(func() { a.sampleDrive(ctx, n, d) })
		}
	}
	wg.Wait()
}

func (a *auditor) sampleDrive(ctx context.Context, n *torx.Node, d int) {
	name := drive{n, d}.String()
	// One command a drive: decode each key's xl.meta, each preceded by a
	// line naming the key; a key with no xl.meta prints nothing.
	var script strings.Builder
	script.WriteString(`dump=$1; shift; for f; do k=$(basename "$(dirname "$f")"); [ -f "$f" ] || continue; echo "key $k"; "$dump" "$f" 2>/dev/null || echo "unreadable"; done`)
	args := []string{"-c", script.String(), "sh", a.dump}
	for _, k := range a.keys {
		args = append(args, filepath.Join(a.fs.Drive(n, d), a.bucket, k, "xl.meta"))
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	at := a.h.Now()
	res, err := n.Exec(sctx, torx.Command("sh", args...))
	if err != nil || res.ExitCode != 0 {
		a.mu.Lock()
		a.failed++
		a.mu.Unlock()
		return
	}
	now := map[string]map[string]bool{} // key -> versions listed
	skip := map[string]bool{}           // keys whose xl.meta could not be read: no verdict
	key := ""
	for line := range bytes.Lines(res.Stdout) {
		s := strings.TrimSpace(string(line))
		switch {
		case strings.HasPrefix(s, "key "):
			key = strings.TrimPrefix(s, "key ")
			now[key] = map[string]bool{}
		case s == "unreadable":
			skip[key] = true
		default:
			if m := versionIDPattern.FindStringSubmatch(s); m != nil && key != "" {
				now[key][m[1]] = true
			}
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	held := a.held[name]
	if held == nil {
		held = map[string]*span{}
		a.held[name] = held
	}
	for k, vs := range now {
		if skip[k] {
			continue
		}
		for v := range vs {
			id := k + "/" + v
			if sp := held[id]; sp != nil {
				sp.last = at
			} else {
				held[id] = &span{at, at}
			}
		}
	}
	for id, sp := range held {
		k, v, _ := strings.Cut(id, "/")
		vs, sampled := now[k]
		if !sampled || skip[k] || vs[v] || sp.last == at {
			continue
		}
		a.lost = append(a.lost, copyLost{Drive: name, Key: k, Version: v, HeldFrom: sp.first, HeldTo: sp.last, GoneAt: at})
		delete(held, id)
	}
}

// anomalies reports the copies lost of versions whose writes were
// acknowledged: a drive may drop a version a failed write left, and heal
// may drop residue, but a copy of an acknowledged version that vanishes is
// one of write quorum's copies gone.
func (a *auditor) anomalies(history []Op) []anomaly {
	acked := map[string]Op{}
	for _, op := range history {
		if op.Process != "nemesis" && isWrite(op.F) && op.Outcome == Ok && op.Version != "" {
			acked[op.Version] = op
		}
	}
	// The nemesis empties drives on purpose: a copy lost to that is not
	// RustFS's doing.
	wiped := func(l copyLost) bool {
		return slices.ContainsFunc(history, func(op Op) bool {
			return op.Process == "nemesis" && (op.F == "wipe-drive" || op.F == "replace-drive") &&
				op.Node == l.Drive && op.Start <= l.GoneAt && op.End >= l.HeldTo
		})
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []anomaly
	for _, l := range a.lost {
		w, ok := acked[l.Version]
		if !ok || wiped(l) {
			continue
		}
		out = append(out, anomaly{Kind: "copy-lost", Key: l.Key, Node: l.Drive, Process: w.Process, At: l.GoneAt,
			Detail: fmt.Sprintf("%s held version %s of %s, which %s's %s at %v-%v made and was acknowledged, from %v to %v, and not at %v",
				l.Drive, l.Version, l.Key, w.Process, describe(w), w.Start.Round(time.Millisecond), w.End.Round(time.Millisecond),
				l.HeldFrom.Round(time.Millisecond), l.HeldTo.Round(time.Millisecond), l.GoneAt.Round(time.Millisecond)),
			Ops: []Op{w}})
	}
	slices.SortFunc(out, func(x, y anomaly) int { return int(x.At - y.At) })
	return out
}
