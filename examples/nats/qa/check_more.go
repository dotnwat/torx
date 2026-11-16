//go:build unix

package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
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

// staleReads reclassifies anomalies a fault explains, by the fault held
// while the operation was in flight or that had healed less than
// staleAfter before. A full disk explains a write that broke its condition,
// a leader read that broke linearizability, and a refused write stored: a
// leader whose flush failed answered from a store that had dropped
// messages. Failing that, a silencing fault explains a leader read that
// broke linearizability, a stale read, and a refused write stored, a
// deposed leader's refusal.
func (j *chaosJob) staleReads(hist []op, events []nemesis.Event, bad []anomaly) []anomaly {
	var silenced, full []nemesis.Window
	for _, w := range nemesis.Windows(events) {
		switch {
		case silencing[w.Fault]:
			silenced = append(silenced, w)
		case w.Fault == "disk-full":
			full = append(full, w)
		}
	}
	during := func(ws []nemesis.Window, o op) bool {
		call, ret := j.start.Add(time.Duration(o.Call)), j.start.Add(time.Duration(o.Return))
		if o.Return == 0 {
			ret = call.Add(time.Minute)
		}
		return slices.ContainsFunc(ws, func(w nemesis.Window) bool { return w.Overlaps(call, ret, staleAfter) })
	}
	bySilence := map[string]string{"nonlinearizable-read": "stale-read", "failed-visible": "deposed-refusal"}
	byDisk := map[string]bool{"condition": true, "nonlinearizable-read": true, "failed-visible": true}
	for i, a := range bad {
		if len(a.Ops) == 0 {
			continue
		}
		o := hist[a.Ops[0]]
		if byDisk[a.Kind] && during(full, o) {
			bad[i].Kind = "flush-error"
		} else if kind, ok := bySilence[a.Kind]; ok && during(silenced, o) {
			bad[i].Kind = kind
		}
	}
	return bad
}

// scheduleEarly is how much before its time a scheduled message may fire:
// the stream stamps it with its own clock, and the job reads its time in
// whole seconds.
const scheduleEarly = time.Second

// checkSchedules holds the scheduled messages to the final schedule
// stream: a schedule the stream took fired once, onto its target, no
// sooner than its time, and was purged; one it refused never fired; and
// nothing fired that no client scheduled.
func checkSchedules(hist []op, f *final) []anomaly {
	var bad []anomaly
	type schedule struct {
		ops             []op
		at              int64
		stored, refused bool
	}
	scheds := map[string]*schedule{}
	var order []string
	for _, o := range hist {
		if o.Stream != schedStream || o.Kind != "schedule" {
			continue
		}
		s := scheds[o.Value]
		if s == nil {
			s = &schedule{at: o.At, refused: true}
			scheds[o.Value] = s
			order = append(order, o.Value)
		}
		s.ops = append(s.ops, o)
		if o.Out == nats.OK {
			s.stored = true
		}
		if o.Out != nats.Fail || o.Code == codeDuplicateInProcess {
			s.refused = false
		}
	}
	fired := map[string][]msg{}
	for _, m := range f.msgs {
		if v, ok := strings.CutPrefix(m.Subject, "sched.t."); ok {
			fired[v] = append(fired[v], m)
			if scheds[v] == nil || m.Scheduler != "sched.s."+v {
				bad = append(bad, anomaly{Kind: "schedule-phantom", Detail: fmt.Sprintf("%s holds %s at %d from %q, which no client scheduled there", f.stream, m.Subject, m.Seq, m.Scheduler)})
			}
		}
	}
	for _, v := range order {
		s := scheds[v]
		fs := fired[v]
		ops := attemptsString(s.ops)
		var idx []int
		for _, o := range s.ops {
			idx = append(idx, o.Index)
		}
		switch {
		case len(fs) > 1:
			bad = append(bad, anomaly{Kind: "schedule-duplicate", Detail: fmt.Sprintf("%s fired %d times, at %v: %s", v, len(fs), seqsOf(fs), ops), Ops: idx})
		case len(fs) == 0 && s.stored:
			bad = append(bad, anomaly{Kind: "schedule-missed", Detail: fmt.Sprintf("%s, acked, never fired (due %s): %s", v, time.Unix(s.at, 0).UTC().Format(time.RFC3339), ops), Ops: idx})
		case len(fs) > 0 && s.refused:
			bad = append(bad, anomaly{Kind: "schedule-refused-fired", Detail: fmt.Sprintf("%s fired at %v though every attempt was refused: %s", v, seqsOf(fs), ops), Ops: idx})
		}
		for _, m := range fs {
			if due := time.Unix(s.at, 0); !m.Time.IsZero() && m.Time.Before(due.Add(-scheduleEarly)) {
				bad = append(bad, anomaly{Kind: "schedule-early", Detail: fmt.Sprintf("%s fired at %s, due %s: %s", v, m.Time.UTC().Format(time.RFC3339Nano), due.UTC().Format(time.RFC3339), ops), Ops: idx})
			}
		}
		if len(fs) > 0 && len(f.bySubject["sched.s."+v]) > 0 {
			bad = append(bad, anomaly{Kind: "schedule-not-purged", Detail: fmt.Sprintf("%s fired, and its schedule is still at %v", v, f.bySubject["sched.s."+v]), Ops: idx})
		}
	}
	return bad
}

func seqsOf(ms []msg) []uint64 {
	var out []uint64
	for _, m := range ms {
		out = append(out, m.Seq)
	}
	return out
}

func attemptsString(as []op) string {
	var out []string
	for _, a := range as {
		out = append(out, a.String())
	}
	return strings.Join(out, "; ")
}
