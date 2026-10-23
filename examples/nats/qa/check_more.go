//go:build unix

package main

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/dotnwat/torx/nemesis"
)

// checkBatches holds each atomic batch to the final stream: the stream
// holds all of its messages or none, and all of them at consecutive
// sequences, in the batch's order, ending at the one its commit was acked
// at.
func checkBatches(hist []op, f *final) []anomaly {
	var bad []anomaly
	for _, o := range hist {
		if o.Kind != "batch" {
			continue
		}
		var seqs []uint64
		for _, v := range o.Values {
			if s := f.byValue[v]; len(s) > 0 {
				seqs = append(seqs, s[0])
			}
		}
		switch {
		case len(seqs) == 0:
			if o.Out == nats.OK {
				// The per-message checks report the loss.
				continue
			}
		case len(seqs) < len(o.Values):
			bad = append(bad, anomaly{Kind: "batch-partial", Detail: fmt.Sprintf("%s holds %d of the batch's %d messages, at %v: %s", f.stream, len(seqs), len(o.Values), seqs, o), Ops: []int{o.Index}})
		default:
			for i := 1; i < len(seqs); i++ {
				if seqs[i] != seqs[i-1]+1 {
					bad = append(bad, anomaly{Kind: "batch-interleaved", Detail: fmt.Sprintf("%s holds the batch's messages at %v, not one after another: %s", f.stream, seqs, o), Ops: []int{o.Index}})
					break
				}
			}
			if o.Out == nats.OK && o.Count != len(o.Values) {
				bad = append(bad, anomaly{Kind: "batch-ack", Detail: fmt.Sprintf("the batch of %d was acked as %d: %s", len(o.Values), o.Count, o), Ops: []int{o.Index}})
			}
		}
	}
	return bad
}

// checkCounters holds the counters to their arithmetic: each message of a
// counter holds the value of the one before it plus its own increment; an
// acked increment's value is the one its message holds; and a read's value
// is the one the message it read holds.
func checkCounters(hist []op, f *final) []anomaly {
	var bad []anomaly
	for subject, seqs := range f.bySubject {
		var val int64
		for _, seq := range seqs {
			m := f.bySeq[seq]
			incr, err1 := strconv.ParseInt(m.Incr, 10, 64)
			got, err2 := strconv.ParseInt(m.Val, 10, 64)
			if err1 != nil || err2 != nil {
				bad = append(bad, anomaly{Kind: "counter-broken", Detail: fmt.Sprintf("%s at %d holds increment %q and value %q", subject, seq, m.Incr, m.Val)})
				break
			}
			if got != val+incr {
				bad = append(bad, anomaly{Kind: "counter-arith", Detail: fmt.Sprintf("%s at %d holds %d, after %d and an increment of %+d (%s)", subject, seq, got, val, incr, m.Value)})
			}
			val = got
		}
	}
	for _, o := range hist {
		if o.Stream != ctrStream || o.Out != nats.OK {
			continue
		}
		switch {
		case o.Kind == "incr" && !o.Dup:
			if m, ok := f.bySeq[o.Seq]; ok && m.Value == o.Value && m.Val != o.Val {
				bad = append(bad, anomaly{Kind: "counter-ack", Detail: fmt.Sprintf("an increment was acked with value %s, and its message at %d holds %s: %s", o.Val, o.Seq, m.Val, o), Ops: []int{o.Index}})
			}
		case o.Kind == "cget" && o.Found:
			if m, ok := f.bySeq[o.Seq]; ok && m.Val != o.Val {
				bad = append(bad, anomaly{Kind: "counter-read", Detail: fmt.Sprintf("a read of %d found value %s, and the message there holds %s: %s", o.Seq, o.Val, m.Val, o), Ops: []int{o.Index}})
			}
		}
	}
	return bad
}

// silencing are the faults that can leave a leader deposed without its
// knowing: it stops hearing from its peers, or they from it.
var silencing = map[string]bool{
	"pause": true, "freeze": true, "partition-one": true, "partition-halves": true, "partition-ring": true,
}

// staleAfter is how long after a silencing fault heals a deposed leader
// may still answer reads: until it hears of the term that deposed it.
const staleAfter = 10 * time.Second

// staleReads reclassifies, when a silencing fault was held while the
// operation was in flight or had healed less than staleAfter before: a
// leader read that broke linearizability as a stale read, and a write the
// stream holds though it was refused as a deposed leader's refusal.
func (j *chaosJob) staleReads(hist []op, events []nemesis.Event, bad []anomaly) []anomaly {
	var windows []nemesis.Window
	for _, w := range nemesis.Windows(events) {
		if silencing[w.Fault] {
			windows = append(windows, w)
		}
	}
	as := map[string]string{"nonlinearizable-read": "stale-read", "failed-visible": "deposed-refusal"}
	for i, a := range bad {
		kind, ok := as[a.Kind]
		if !ok || len(a.Ops) == 0 {
			continue
		}
		o := hist[a.Ops[0]]
		call, ret := j.start.Add(time.Duration(o.Call)), j.start.Add(time.Duration(o.Return))
		if slices.ContainsFunc(windows, func(w nemesis.Window) bool { return w.Overlaps(call, ret, staleAfter) }) {
			bad[i].Kind = kind
		}
	}
	return bad
}
