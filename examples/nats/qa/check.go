//go:build unix

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/dotnwat/torx/linearize"
	"github.com/dotnwat/torx/logcheck"
)

// anomaly is something the checks found: a kind, what it is, and the
// operations that show it, as indices into the history.
type anomaly struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
	Ops    []int  `json:"ops,omitempty"`
}

func (a anomaly) String() string { return a.Kind + ": " + a.Detail }

// msg is a message as the final read found it.
type msg struct {
	Seq     uint64 `json:"seq"`
	Subject string `json:"subj"`
	Value   string `json:"value"`
	// Val and Incr are a counter's value and the increment that made it.
	Val  string `json:"val,omitempty"`
	Incr string `json:"incr,omitempty"`
	// Scheduler is the schedule a scheduled message fired from, and Time
	// when the stream took it.
	Scheduler string    `json:"scheduler,omitempty"`
	Time      time.Time `json:"time,omitzero"`
}

// final is a stream's messages as they stand once every fault is healed:
// the ground truth every acked write and every read is held to.
type final struct {
	stream string
	msgs   []msg
	bySeq  map[uint64]msg
	// byValue are the sequences each value is at.
	byValue map[string][]uint64
	// bySubject are each subject's sequences, in order.
	bySubject map[string][]uint64
}

func newFinal(stream string, msgs []msg) *final {
	f := &final{stream: stream, msgs: msgs, bySeq: map[uint64]msg{}, byValue: map[string][]uint64{}, bySubject: map[string][]uint64{}}
	for _, m := range msgs {
		f.bySeq[m.Seq] = m
		f.byValue[m.Value] = append(f.byValue[m.Value], m.Seq)
		f.bySubject[m.Subject] = append(f.bySubject[m.Subject], m.Seq)
	}
	for _, seqs := range f.bySubject {
		slices.Sort(seqs)
	}
	return f
}

// entries is the final stream as logcheck takes a log: each message an
// entry at its sequence, filed under its subject.
func (f *final) entries() []logcheck.Entry {
	out := make([]logcheck.Entry, 0, len(f.msgs))
	for _, m := range f.msgs {
		out = append(out, logcheck.Entry{Offset: m.Seq, Key: m.Subject, Value: m.Value})
	}
	return out
}

// codeDuplicateInProcess is JetStream's answer to a message whose id is
// that of one being proposed: a refusal of a resend that says nothing of
// the first send.
const codeDuplicateInProcess = 10158

// appends are the writes to stream in hist as logcheck takes them: one
// append per value, its attempts each send of it, each batch's messages
// appends of their own. A write conditional on its subject's last sequence
// is conditional on its key's last offset; an append to the log, on the
// log's.
func appends(hist []op, stream string) []logcheck.Append {
	byValue := map[string]int{}
	var out []logcheck.Append
	for _, o := range expand(hist) {
		if o.Stream != stream || o.Value == "" {
			continue
		}
		i, ok := byValue[o.Value]
		if !ok {
			i = len(out)
			byValue[o.Value] = i
			a := logcheck.Append{Value: o.Value, Key: o.Subject, Idempotent: o.MsgID != ""}
			if o.Expect != nil {
				e := *o.Expect
				if o.Kind == "append" {
					a.AfterLog = &e
				} else {
					a.AfterKey = &e
				}
			}
			out = append(out, a)
		}
		t := logcheck.Attempt{Op: o.Index, Call: o.Call, Return: o.Return, Offset: o.Seq}
		switch {
		case o.Out == nats.OK:
			t.Outcome = logcheck.OK
		case o.Out == nats.Fail && o.Code != codeDuplicateInProcess:
			t.Outcome = logcheck.Failed
		default:
			t.Outcome = logcheck.Unknown
		}
		out[i].Attempts = append(out[i].Attempts, t)
	}
	return out
}

// expand turns each batch in hist into its messages, each a write of its
// own that shares the batch's index, times, and outcome: the first carries
// the batch's condition, and each is acked at its place before the last's.
func expand(hist []op) []op {
	var out []op
	for _, o := range hist {
		if o.Kind != "batch" {
			out = append(out, o)
			continue
		}
		n := len(o.Values)
		for i := range n {
			m := o
			m.Values, m.Subjects = nil, nil
			m.Value, m.Subject = o.Values[i], o.Subjects[i]
			if i > 0 {
				m.Expect = nil
			}
			if o.Out == nats.OK {
				m.Seq = o.Seq - uint64(n-1-i)
			}
			out = append(out, m)
		}
	}
	return out
}

// fromLogcheck turns what logcheck found into anomalies, each with the
// operations that show it.
func fromLogcheck(stream string, hist []op, found []logcheck.Anomaly) []anomaly {
	var out []anomaly
	for _, a := range found {
		var b strings.Builder
		fmt.Fprintf(&b, "%s: %s", stream, a.Detail)
		for _, i := range a.Ops[:min(len(a.Ops), 12)] {
			if i >= 0 && i < len(hist) {
				fmt.Fprintf(&b, "\n  %s", hist[i])
			}
		}
		out = append(out, anomaly{Kind: a.Kind, Detail: b.String(), Ops: a.Ops})
	}
	return out
}

// checkStream holds stream's writes in hist to its final contents f, as
// logcheck.Check does.
func checkStream(hist []op, stream string, f *final) []anomaly {
	return fromLogcheck(stream, hist, logcheck.Check(appends(hist, stream), f.entries()))
}

// checkReads checks the reads of kinds in hist of each subject's last
// message for linearizability, against the order the final stream f put
// the subject's messages in, as logcheck.CheckReads does. A write refused
// because its subject's last sequence was not the one it named, which the
// refusal names, is a read of that sequence. It returns what it found, and
// the subjects whose check could not finish.
func checkReads(ctx context.Context, hist []op, f *final, kinds map[string]bool, budget time.Duration) ([]anomaly, []string) {
	var reads []logcheck.Read
	for _, o := range hist {
		if o.Stream != f.stream {
			continue
		}
		switch {
		case kinds[o.Kind] && o.Out == nats.OK:
			r := logcheck.Read{Op: o.Index, Key: o.Subject, Call: o.Call, Return: o.Return}
			if o.Found {
				r.Offset, r.Value = o.Seq, o.Read
			}
			reads = append(reads, r)
		case kinds["get"] && o.Kind == "cas" && o.Out == nats.Fail && o.Code == nats.ErrCodeWrongLastSeq:
			// The leader refused it on the last sequence it holds. (It
			// refuses with ErrCodeWrongLastSeqConst, naming none, while
			// another write to the subject is in flight, which may yet
			// fail: that says nothing of the subject.)
			seq, ok := lastSeqIn(o.Err)
			if !ok {
				continue
			}
			r := logcheck.Read{Op: o.Index, Key: o.Subject, Call: o.Call, Return: o.Return, Offset: seq}
			if seq != 0 {
				r.Value = "?"
				if m, ok := f.bySeq[seq]; ok && m.Subject == o.Subject {
					r.Value = m.Value
				}
			}
			reads = append(reads, r)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	found, unknown := logcheck.CheckReads(ctx, appends(hist, f.stream), f.entries(), reads, linearize.Limits{Configurations: 5_000_000})
	return fromLogcheck(f.stream, hist, found), unknown
}
