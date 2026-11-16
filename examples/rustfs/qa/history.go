//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/rustfs/qa/s3"
)

// Outcome is how an operation ended, as a checker must read it. Ok and Fail
// are definite: the server answered that the operation took effect, or that
// it did not -- a precondition that did not hold, a key that holds nothing.
// Info is indeterminate: no answer says which, so the operation may have
// taken effect or not -- a request that timed out, a connection dropped, an
// internal error.
type Outcome string

const (
	Ok   Outcome = "ok"
	Fail Outcome = "fail"
	Info Outcome = "info"
)

// Op is one completed operation in a history: who issued it, to which
// server, what it was, when it began and ended relative to the start of the
// history, and how it ended. Values are named by the ids the workload gives
// them; "" is no object. The nemesis records its faults as operations too,
// so a history reads as one timeline of what the clients did and what was
// done to the cluster.
type Op struct {
	Process string `json:"process"` // "client-3", "nemesis", "final"
	// F is the operation: put, put-if-absent, put-if-match, multipart, get,
	// head, delete, delete-if-match, list; for the nemesis, the fault.
	F    string `json:"f"`
	Node string `json:"node,omitempty"`
	Key  string `json:"key,omitempty"`
	// Value is the value a write wrote, or a read saw.
	Value string `json:"value,omitempty"`
	// Expect is the value whose ETag a put-if-match or delete-if-match
	// sent as its precondition.
	Expect string `json:"expect,omitempty"`
	Size   int    `json:"size,omitempty"`
	// Listed is what a list saw: key -> value.
	Listed map[string]string `json:"listed,omitempty"`
	// Version is the version a write made, in a bucket with versioning:
	// an object's, or a delete marker's.
	Version string `json:"version,omitempty"`
	// Versions is what a list of versions saw, each key's newest first.
	Versions []VersionEntry `json:"versions,omitempty"`
	Start    time.Duration  `json:"start"`
	End      time.Duration  `json:"end"`
	Outcome  Outcome        `json:"outcome"`
	// Code is the S3 error code of a failure, and Status its HTTP status.
	Code   string `json:"code,omitempty"`
	Status int    `json:"status,omitempty"`
	Err    string `json:"error,omitempty"`
}

// VersionEntry is a version in a list of versions: an object's, naming
// its value, or a delete marker.
type VersionEntry struct {
	Key          string `json:"key"`
	Version      string `json:"version"`
	Value        string `json:"value,omitempty"`
	DeleteMarker bool   `json:"delete_marker,omitempty"`
	Modified     string `json:"modified,omitempty"`
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

// Start is when the history started, the zero of its times: add an op's
// times to it to find the op in the servers' logs.
func (h *History) Start() time.Time { return h.t0 }

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

// classify reads the error of an operation as a checker must: Fail only
// when the operation certainly did not take effect, Info otherwise. A
// request the server refused to connect for never arrived, and a 4xx answer
// is the server saying it did not act; a 5xx answer, a timeout, or a
// connection that dropped mid-request may come after the server acted.
func classify(err error) Outcome {
	if err == nil {
		return Ok
	}
	if notReady(err) {
		return Fail
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return Fail
	}
	if st := s3.StatusOf(err); st >= 400 && st < 500 && st != 408 && st != 409 && st != 429 {
		return Fail
	}
	return Info
}

// notReady reports whether err is a server's refusal to serve until it has
// started: RustFS's readiness gate answers every request before it is ready
// with a 503 that names what it waits for in a header, before the request
// reaches anything that could act on it.
func notReady(err error) bool {
	e, ok := errors.AsType[*s3.Error](err)
	return ok && e.Status == 503 && e.Header.Get("x-rustfs-readiness-pending") != ""
}

// record fills op's outcome from err.
func record(op *Op, err error) {
	op.Outcome = classify(err)
	if err == nil {
		return
	}
	op.Err = err.Error()
	if e, ok := errors.AsType[*s3.Error](err); ok {
		op.Code, op.Status, op.Err = e.Code, e.Status, e.Message
		if notReady(err) {
			op.Code = "NotReady"
		}
	}
}
