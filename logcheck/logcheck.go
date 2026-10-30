// Package logcheck checks a history of appends to a log whose server gives
// each entry an offset of its own -- a stream's sequence, a partition's
// offset, a queue's position -- against the log as it stands once every
// fault is healed.
//
// A client appends a value unique to the append, perhaps in several
// attempts -- resending one whose answer was lost -- and an attempt the
// log acked says where it put the value. The final log is the ground truth
// the history is held to, and Check reports what it contradicts:
//
//   - lost: an acked append the log does not hold where its ack said;
//   - ack-mismatch: an ack whose offset the log holds another value at;
//   - phantom: a value in the log no client appended;
//   - duplicate: a value the log holds more often than its attempts
//     could have put it there, once for an idempotent append;
//   - failed-visible: an append every attempt of which the log refused,
//     which it holds anyway;
//   - condition: a conditional append the log holds after an entry other
//     than the one it was conditional on;
//   - realtime-order: an append the log holds before one that was acked
//     before it was sent. A log is one sequence, which orders its appends as
//     they happened;
//   - collision: two entries at one offset, which a log cannot hold. The
//     checks go on with the first.
//
// Offsets start at 1: 0 stands for no entry, in a read that found none and
// in a condition on there being none. A log whose offsets start at 0 -- a
// Kafka partition's -- is checked shifted up by one.
//
// Appends filed under keys -- a subject, a message key -- make the log a
// map of registers too, each key's value its last entry. CheckReads checks
// reads of a key's last entry for linearizability, against the order the
// log put the key's entries in, which settles every write's place: the
// only search is for where each read fits.
package logcheck

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/dotnwat/torx/linearize"
)

// Outcome is what came of an attempt.
type Outcome int

const (
	// OK: the log acked the append, at the attempt's Offset.
	OK Outcome = iota
	// Failed: the log refused the append; it did not take effect.
	Failed
	// Unknown: no answer came, or one that does not say; the append may
	// have taken effect, at any time after the attempt's call.
	Unknown
)

// Attempt is one try at an append.
type Attempt struct {
	// Op names the attempt in reports: its index in the job's history, say.
	Op int
	// Call and Return are when the attempt was sent and when its answer
	// came, on one clock; Return is unused for an Unknown attempt.
	Call, Return int64
	Outcome      Outcome
	// Offset is where an OK attempt's ack put the value.
	Offset uint64
}

// Append is one logical append: a value unique to it, sent in one or more
// attempts.
type Append struct {
	Value string
	// Key is what the entry is filed under -- a subject, a message key --
	// or "".
	Key      string
	Attempts []Attempt
	// Idempotent says the attempts carried one idempotency key, which the
	// log deduplicates by: the log holds the value once at most. Otherwise
	// each attempt may have put it there once.
	Idempotent bool
	// AfterKey, if set, says the append was conditional on the last entry
	// under its key being at offset *AfterKey (0: none); AfterLog, on the
	// log's last entry being.
	AfterKey, AfterLog *uint64
}

// Entry is an entry of the log as it stands once every fault is healed.
type Entry struct {
	// Offset is the entry's place in the log, 1 or more; an entry at 0 is a
	// programming error and panics.
	Offset uint64
	Key    string
	Value  string
}

// Anomaly is something a check found.
type Anomaly struct {
	// Kind is the anomaly: "lost", "ack-mismatch", "phantom",
	// "duplicate", "failed-visible", "condition", "realtime-order",
	// "collision", "dirty-read", "nonlinearizable-read".
	Kind string
	// Value is the value it concerns, if one; Key, the key.
	Value, Key string
	// Ops are the attempts and reads that show it, by Op.
	Ops    []int
	Detail string
}

func (a Anomaly) String() string { return a.Kind + ": " + a.Detail }

// log is the final log, indexed.
type log struct {
	byOffset map[uint64]Entry
	byValue  map[string][]uint64
	byKey    map[string][]uint64
	offsets  []uint64
	// collisions are the entries at an offset another entry already had,
	// which are left out of the index.
	collisions []Anomaly
}

func index(entries []Entry) *log {
	l := &log{byOffset: map[uint64]Entry{}, byValue: map[string][]uint64{}, byKey: map[string][]uint64{}}
	for _, e := range entries {
		if e.Offset == 0 {
			panic(fmt.Sprintf("logcheck: the log holds %s (key %q) at offset 0; offsets start at 1", e.Value, e.Key))
		}
		if first, ok := l.byOffset[e.Offset]; ok {
			l.collisions = append(l.collisions, Anomaly{Kind: "collision", Value: e.Value, Key: e.Key,
				Detail: fmt.Sprintf("the log holds %s (key %q) at %d, where it already holds %s (key %q)", e.Value, e.Key, e.Offset, first.Value, first.Key)})
			continue
		}
		l.byOffset[e.Offset] = e
		l.byValue[e.Value] = append(l.byValue[e.Value], e.Offset)
		l.byKey[e.Key] = append(l.byKey[e.Key], e.Offset)
	}
	for _, offs := range l.byValue {
		slices.Sort(offs)
	}
	for _, offs := range l.byKey {
		slices.Sort(offs)
	}
	l.offsets = slices.Sorted(maps.Keys(l.byOffset))
	return l
}

// before is the offset of the last entry before off in offs, which are
// sorted, or 0.
func before(offs []uint64, off uint64) uint64 {
	i, _ := slices.BinarySearch(offs, off)
	if i == 0 {
		return 0
	}
	return offs[i-1]
}

// timing is when an append was called and acked: its first attempt's call,
// and its first OK attempt's return and the offset that ack named, or
// linearize.Pending and 0 if none was OK.
func timing(a Append) (call, ret int64, off uint64) {
	call, ret = a.Attempts[0].Call, linearize.Pending
	for _, t := range a.Attempts {
		call = min(call, t.Call)
		if t.Outcome == OK && t.Return < ret {
			ret, off = t.Return, t.Offset
		}
	}
	return call, ret, off
}

func ops(a Append) []int {
	var out []int
	for _, t := range a.Attempts {
		out = append(out, t.Op)
	}
	return out
}

// Check holds appends to the final log and returns what contradicts it,
// in the order of appends and then of the log.
func Check(appends []Append, entries []Entry) []Anomaly {
	l := index(entries)
	var bad []Anomaly
	appended := map[string]bool{}
	for _, a := range appends {
		if len(a.Attempts) == 0 {
			continue
		}
		appended[a.Value] = true
		offs := l.byValue[a.Value]
		could := 0 // the attempts that may have put the value in the log
		for _, t := range a.Attempts {
			if t.Outcome != Failed {
				could++
			}
		}
		switch {
		case could == 0 && len(offs) > 0:
			bad = append(bad, Anomaly{Kind: "failed-visible", Value: a.Value, Key: a.Key, Ops: ops(a),
				Detail: fmt.Sprintf("the log refused every attempt at %s, and holds it at %v", a.Value, offs)})
		case len(offs) > 1 && (a.Idempotent || len(offs) > could):
			bad = append(bad, Anomaly{Kind: "duplicate", Value: a.Value, Key: a.Key, Ops: ops(a),
				Detail: fmt.Sprintf("the log holds %s at %v, from %d attempts not refused (idempotent: %v)", a.Value, offs, could, a.Idempotent)})
		}
		for _, t := range a.Attempts {
			if t.Outcome != OK {
				continue
			}
			e, ok := l.byOffset[t.Offset]
			switch {
			case !ok:
				bad = append(bad, Anomaly{Kind: "lost", Value: a.Value, Key: a.Key, Ops: []int{t.Op},
					Detail: fmt.Sprintf("%s was acked at %d, and the log holds nothing there", a.Value, t.Offset)})
			case e.Value != a.Value || e.Key != a.Key:
				bad = append(bad, Anomaly{Kind: "ack-mismatch", Value: a.Value, Key: a.Key, Ops: []int{t.Op},
					Detail: fmt.Sprintf("%s (key %q) was acked at %d, and the log holds %s (key %q) there", a.Value, a.Key, t.Offset, e.Value, e.Key)})
			}
		}
		// Every copy the log holds was accepted on the condition: a second
		// copy follows the first, if nothing else.
		if a.AfterKey != nil || a.AfterLog != nil {
			for _, off := range offs {
				if a.AfterKey != nil {
					if prev := before(l.byKey[a.Key], off); prev != *a.AfterKey {
						bad = append(bad, Anomaly{Kind: "condition", Value: a.Value, Key: a.Key, Ops: ops(a),
							Detail: fmt.Sprintf("the log holds %s at %d, after %d under key %q, though it was conditional on %d", a.Value, off, prev, a.Key, *a.AfterKey)})
					}
				}
				if a.AfterLog != nil {
					if prev := before(l.offsets, off); prev != *a.AfterLog {
						bad = append(bad, Anomaly{Kind: "condition", Value: a.Value, Key: a.Key, Ops: ops(a),
							Detail: fmt.Sprintf("the log holds %s at %d, after %d, though it was conditional on the log's last entry being %d", a.Value, off, prev, *a.AfterLog)})
					}
				}
			}
		}
	}
	for _, off := range l.offsets {
		if e := l.byOffset[off]; !appended[e.Value] {
			bad = append(bad, Anomaly{Kind: "phantom", Value: e.Value, Key: e.Key,
				Detail: fmt.Sprintf("the log holds %s at %d (key %q), which no client appended", e.Value, off, e.Key)})
		}
	}
	bad = append(bad, l.collisions...)
	return append(bad, realtimeOrder(appends, l)...)
}

// realtimeOrder reports each append the log holds before one that was
// acked before it was sent: the latest such, for each. An acked append is
// where its ack put it; the other is at the first place the log holds it.
func realtimeOrder(appends []Append, l *log) []Anomaly {
	type placed struct {
		a         Append
		call, ret int64
		first     uint64 // the first offset the log holds the value at
		acked     uint64 // the offset its first ack named, if the log holds the value there; first otherwise
	}
	var in []placed
	for _, a := range appends {
		if len(a.Attempts) == 0 {
			continue
		}
		if offs := l.byValue[a.Value]; len(offs) > 0 {
			call, ret, ackOff := timing(a)
			p := placed{a, call, ret, offs[0], offs[0]}
			if _, found := slices.BinarySearch(offs, ackOff); found {
				p.acked = ackOff
			}
			in = append(in, p)
		}
	}
	byCall := slices.Clone(in)
	slices.SortStableFunc(byCall, func(x, y placed) int { return cmp.Compare(x.call, y.call) })
	byRet := slices.DeleteFunc(slices.Clone(in), func(p placed) bool { return p.ret == linearize.Pending })
	slices.SortStableFunc(byRet, func(x, y placed) int { return cmp.Compare(x.ret, y.ret) })
	var bad []Anomaly
	var done *placed // of the appends acked so far, the one at the highest offset
	i := 0
	for _, b := range byCall {
		for ; i < len(byRet) && byRet[i].ret < b.call; i++ {
			if done == nil || byRet[i].acked > done.acked {
				done = &byRet[i]
			}
		}
		if done != nil && done.acked > b.first {
			bad = append(bad, Anomaly{Kind: "realtime-order", Value: b.a.Value, Key: b.a.Key, Ops: append(ops(done.a), ops(b.a)...),
				Detail: fmt.Sprintf("the log holds %s at %d, before %s at %d, which was acked before %s was sent", b.a.Value, b.first, done.a.Value, done.acked, b.a.Value)})
		}
	}
	return bad
}

// Read is a read of the last entry under a key.
type Read struct {
	// Op names the read in reports.
	Op  int
	Key string
	// Call and Return are when the read was sent and its answer came, on
	// the appends' clock.
	Call, Return int64
	// Offset and Value are the entry read, or 0 and "" if the key had
	// none.
	Offset uint64
	Value  string
}

// readIn is an operation on one key, as the search models it: an append
// that took effect at an offset, or a read.
type readIn struct {
	off  uint64 // an append's offset, or 0 for a read
	op   int
	read bool
}

// CheckReads checks reads of each key's last entry for linearizability:
// each must return the key's last entry at some instant between its call
// and its return, in an order that respects real time, with the key's
// appends in the order the log holds them. A read of an entry the log does
// not hold is a dirty read, and is left out of the search; so is a read of
// an entry no client appended, which Check reports as a phantom, and the
// entry itself, since when it landed is anyone's guess. It returns what it
// found, and the keys whose search did not finish within ctx and lim.
func CheckReads(ctx context.Context, appends []Append, entries []Entry, reads []Read, lim linearize.Limits) ([]Anomaly, []string) {
	l := index(entries)
	var bad []Anomaly
	byKey := map[string][]linearize.Op[readIn, uint64]{}
	placed := map[string][]uint64{} // each key's entries some append put there
	for _, a := range appends {
		if len(a.Attempts) == 0 {
			continue
		}
		offs := l.byValue[a.Value]
		if len(offs) == 0 {
			continue
		}
		call, ret, _ := timing(a)
		for i, off := range offs {
			r := ret
			if i > 0 {
				r = linearize.Pending // a duplicate: when it landed is unknown
			}
			k := l.byOffset[off].Key
			placed[k] = append(placed[k], off)
			byKey[k] = append(byKey[k], linearize.Op[readIn, uint64]{Call: call, Return: r, Input: readIn{off: off, op: a.Attempts[0].Op}})
		}
	}
	for _, offs := range placed {
		slices.Sort(offs)
	}
	for _, r := range reads {
		if r.Offset != 0 {
			e, ok := l.byOffset[r.Offset]
			if !ok || e.Value != r.Value || e.Key != r.Key {
				bad = append(bad, Anomaly{Kind: "dirty-read", Value: r.Value, Key: r.Key, Ops: []int{r.Op},
					Detail: fmt.Sprintf("a read of %q returned %s at %d, which the log does not hold there", r.Key, r.Value, r.Offset)})
				continue
			}
			if _, found := slices.BinarySearch(placed[r.Key], r.Offset); !found {
				continue // a phantom
			}
		}
		byKey[r.Key] = append(byKey[r.Key], linearize.Op[readIn, uint64]{Call: r.Call, Return: r.Return, Input: readIn{op: r.Op, read: true}, Output: r.Offset})
	}
	var unknown []string
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		hist := byKey[key]
		offs := placed[key]
		last := func(pos int) uint64 {
			if pos == 0 {
				return 0
			}
			return offs[pos-1]
		}
		m := linearize.Model[int, readIn, uint64]{
			Init: func() int { return 0 },
			Step: func(pos int, in readIn, out uint64) (bool, int) {
				if in.read {
					return last(pos) == out, pos
				}
				return pos < len(offs) && offs[pos] == in.off, pos + 1
			},
		}
		res, err := linearize.CheckWithin(ctx, m, hist, lim)
		switch res.Outcome {
		case linearize.Illegal:
			bad = append(bad, explain(key, hist, res, last))
		case linearize.Unknown:
			unknown = append(unknown, fmt.Sprintf("%s: %v", key, err))
		}
	}
	return bad, unknown
}

// explain says where the search of a key got stuck: the operation it
// could not place, the key's last entry then, and the operations nearest
// it, those concurrent with it first.
func explain(key string, hist []linearize.Op[readIn, uint64], res linearize.Result[int], last func(int) uint64) Anomaly {
	a := Anomaly{Kind: "nonlinearizable-read", Key: key}
	var b strings.Builder
	fmt.Fprintf(&b, "key %q: after %d appends (the last at %d), no order places op %d", key, res.State, last(res.State), hist[res.Stuck].Input.op)
	stuck := hist[res.Stuck]
	a.Ops = []int{stuck.Input.op}
	if stuck.Input.read {
		fmt.Fprintf(&b, ", a read [%d,%d] of the entry at %d", stuck.Call, stuck.Return, stuck.Output)
	} else {
		fmt.Fprintf(&b, ", an append at %d", stuck.Input.off)
	}
	var near []linearize.Op[readIn, uint64]
	for i, o := range hist {
		if i != res.Stuck {
			near = append(near, o)
		}
	}
	// gap is how far an operation is from the stuck one, in the history's
	// clock, whatever its unit: 0 for one concurrent with it.
	gap := func(o linearize.Op[readIn, uint64]) int64 {
		switch {
		case o.Return < stuck.Call:
			return stuck.Call - o.Return
		case stuck.Return < o.Call:
			return o.Call - stuck.Return
		}
		return 0
	}
	slices.SortStableFunc(near, func(x, y linearize.Op[readIn, uint64]) int { return cmp.Compare(gap(x), gap(y)) })
	for _, o := range near[:min(len(near), 8)] {
		a.Ops = append(a.Ops, o.Input.op)
	}
	if len(near) > 0 {
		b.WriteString("; near it, ops")
		for _, op := range a.Ops[1:] {
			fmt.Fprintf(&b, " %d", op)
		}
	}
	a.Detail = b.String()
	return a
}
