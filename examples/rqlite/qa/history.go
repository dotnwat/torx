//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// Outcome is how an operation ended, as a checker must read it. Ok and Fail
// are definite: the operation took effect, or it certainly did not. Info is
// indeterminate: the request was sent and no answer says which, so the
// operation may have taken effect or not -- a write whose leader was lost
// while it was committing, a request that timed out.
type Outcome string

const (
	Ok   Outcome = "ok"
	Fail Outcome = "fail"
	Info Outcome = "info"
)

// Op is one completed operation in a history: who issued it, what it was,
// when it began and ended relative to the start of the history, and how it
// ended. A write carries the value it wrote; a read carries the values it
// saw. The nemesis records its faults as operations too, so a history reads
// as one timeline of what the clients did and what was done to the cluster.
type Op struct {
	Process string        `json:"process"` // "client-3", "nemesis", "final"
	F       string        `json:"f"`       // "add", "read"; for the nemesis, the fault
	Mode    string        `json:"mode,omitempty"`
	Node    string        `json:"node,omitempty"`
	Value   int64         `json:"value,omitempty"`
	Values  []int64       `json:"values,omitempty"`
	Lower   int64         `json:"lower,omitempty"` // a read saw every value above Lower that was there
	Start   time.Duration `json:"start"`
	End     time.Duration `json:"end"`
	Outcome Outcome       `json:"outcome"`
	Err     string        `json:"error,omitempty"`
}

// History records operations from many goroutines.
type History struct {
	t0  time.Time
	mu  sync.Mutex
	ops []Op
}

// NewHistory starts a history now.
func NewHistory() *History {
	return &History{t0: time.Now()}
}

// Now is the time since the history started.
func (h *History) Now() time.Duration { return time.Since(h.t0) }

// Add records a completed operation.
func (h *History) Add(op Op) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// Ops returns the operations recorded so far, ordered by start.
func (h *History) Ops() []Op {
	h.mu.Lock()
	ops := slices.Clone(h.ops)
	h.mu.Unlock()
	slices.SortStableFunc(ops, func(a, b Op) int { return int(a.Start - b.Start) })
	return ops
}

// NDJSON renders the history one operation per line.
func (h *History) NDJSON() []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, op := range h.Ops() {
		_ = enc.Encode(op)
	}
	return b.Bytes()
}

// classify reads the error of a write as a checker must: Fail only when the
// write certainly did not happen, Info otherwise. A request the node refused
// to connect for never arrived, and a node that answered that it knows no
// leader did not submit the write to one. Every other failure -- a timeout, a
// connection dropped mid-request, an error from Raft such as leadership lost
// while committing -- may have come after the write was committed.
func classify(err error) Outcome {
	if err == nil {
		return Ok
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return Fail
	}
	if se, ok := errors.AsType[*rqlite.StatusError](err); ok {
		if se.Code == 503 && strings.Contains(se.Text, "leader not found") {
			return Fail
		}
	}
	return Info
}
