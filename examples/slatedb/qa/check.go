//go:build unix

package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dotnwat/torx/linearize"
)

// A history is every operation the clients ran against the database, as
// they saw it. Each operation is on one key, or on several at once (a
// batch, a scan); each value written is unique, so a value read names the
// write that wrote it.

// opKind is what an operation did.
type opKind string

const (
	opPut   opKind = "put"   // a put or a delete of one key
	opBatch opKind = "batch" // a batch of puts and deletes
	opGet   opKind = "get"   // a read of one key
	opScan  opKind = "scan"  // a read of every key
)

// op is one operation and what came of it.
type op struct {
	ID     int       `json:"id"`
	Client int       `json:"client"`
	Proc   string    `json:"proc"`   // the process it went to
	Launch int       `json:"launch"` // which of that process's launches
	Kind   opKind    `json:"kind"`
	Call   time.Time `json:"call"`
	Return time.Time `json:"return"`
	// Writes are the keys a put or batch wrote, and their values, nil for
	// a delete.
	Writes []write `json:"writes,omitempty"`
	// Durable is whether a write awaited durability.
	Durable bool `json:"durable,omitempty"`
	// Key is the key a get read; Read is what a get or scan read.
	Key  string          `json:"key,omitempty"`
	Read map[string]read `json:"read,omitempty"`
	// Seq is the write's sequence number, when it is known.
	Seq uint64 `json:"seq,omitempty"`
	// Outcome: "ok", "fail" (it did not happen), or "info" (it may have).
	Outcome string `json:"outcome"`
	Err     string `json:"error,omitempty"`
	// Zombie marks an operation sent to a writer that another writer had
	// already fenced.
	Zombie bool `json:"zombie,omitempty"`
	// Reader marks a read from a DbReader, which may lag the writer.
	Reader bool `json:"reader,omitempty"`
	// Snapshot names the snapshot a scan read, "proc/launch/id", and
	// SnapSeq is the sequence number it reads at. A snapshot reads what
	// the writer holds in memory, durable or not.
	Snapshot string `json:"snapshot,omitempty"`
	SnapSeq  uint64 `json:"snap_seq,omitempty"`
	// Clone names the clone a scan read, by its path. A clone is of a
	// checkpoint taken once every write was flushed, and never written.
	Clone string `json:"clone,omitempty"`
}

type write struct {
	Key   string  `json:"key"`
	Value *string `json:"value"`
}

type read struct {
	Found bool   `json:"found"`
	Value string `json:"value,omitempty"`
	Seq   uint64 `json:"seq,omitempty"`
}

// anomaly is something the history shows the database got wrong.
type anomaly struct {
	Kind   string `json:"kind"`
	Key    string `json:"key,omitempty"`
	Detail string `json:"detail"`
	Ops    []int  `json:"ops,omitempty"`
}

// register is a key's state in the model: its value, if it has one.
type register struct {
	found bool
	value string
}

// regIn is an operation on one key: a write of value (nil deletes), or a
// read.
type regIn struct {
	write bool
	value *string
}

var registerModel = linearize.Model[register, regIn, read]{
	Init: func() register { return register{} },
	Step: func(s register, in regIn, out read) (bool, register) {
		if in.write {
			if in.value == nil {
				return true, register{}
			}
			return true, register{found: true, value: *in.value}
		}
		if !out.Found {
			return !s.found, s
		}
		return s.found && s.value == out.Value, s
	},
	Same: func(in1 regIn, _ read, in2 regIn, _ read) bool {
		if !in1.write || !in2.write {
			return false
		}
		if in1.value == nil || in2.value == nil {
			return in1.value == nil && in2.value == nil
		}
		return *in1.value == *in2.value
	},
}

// checker checks a history.
type checker struct {
	ops []op
	// written maps each value to the op that wrote it.
	written map[string]int
	// budget bounds the search of each key's history.
	budget time.Duration
	limits linearize.Limits
}

func newChecker(ops []op) *checker {
	c := &checker{ops: ops, written: map[string]int{}, budget: checkBudget, limits: linearize.Limits{Configurations: 2_000_000}}
	for i, o := range ops {
		for _, w := range o.Writes {
			if w.Value != nil {
				c.written[*w.Value] = i
			}
		}
	}
	return c
}

// check runs every check and returns what they found, and the keys whose
// search was cut short.
func (c *checker) check(ctx context.Context) (anomalies []anomaly, unknown []string) {
	anomalies = append(anomalies, c.phantoms()...)
	anomalies = append(anomalies, c.snapshots()...)
	anomalies = append(anomalies, c.monotonic()...)
	anomalies = append(anomalies, c.repeatable()...)
	lin, unk := c.linearizable(ctx)
	return append(anomalies, lin...), unk
}

// phantoms finds values read that no client wrote, or wrote to another key.
func (c *checker) phantoms() []anomaly {
	var out []anomaly
	for i, o := range c.ops {
		for k, r := range o.Read {
			if !r.Found {
				continue
			}
			w, ok := c.written[r.Value]
			if !ok {
				out = append(out, anomaly{Kind: "phantom", Key: k, Ops: []int{i},
					Detail: fmt.Sprintf("op %d read %q, which no client wrote", i, r.Value)})
				continue
			}
			if !slices.ContainsFunc(c.ops[w].Writes, func(x write) bool { return x.Key == k && x.Value != nil && *x.Value == r.Value }) {
				out = append(out, anomaly{Kind: "wrong-key", Key: k, Ops: []int{i, w},
					Detail: fmt.Sprintf("op %d read %q under %s, written by op %d to another key", i, r.Value, k, w)})
			}
			if c.ops[w].Outcome == "fail" {
				out = append(out, anomaly{Kind: "failed-write-read", Key: k, Ops: []int{i, w},
					Detail: fmt.Sprintf("op %d read %q, which op %d wrote and was told failed (%s)", i, r.Value, w, c.ops[w].Err)})
			}
			if c.ops[w].Seq != 0 && r.Seq != 0 && c.ops[w].Seq != r.Seq && !c.ops[w].Zombie && c.ops[w].Outcome == "ok" {
				out = append(out, anomaly{Kind: "seq-mismatch", Key: k, Ops: []int{i, w},
					Detail: fmt.Sprintf("op %d read %q at seq %d, but op %d was told it wrote it at seq %d", i, r.Value, r.Seq, w, c.ops[w].Seq)})
			}
		}
	}
	return out
}

// snapshots checks that each scan is a snapshot: the state after some
// prefix of the database's writes, in sequence order -- some sequence
// number S such that every key holds the newest of its writes at or below
// S. Only writes known to be in the database's durable history count:
// those acknowledged durable, and those some read returned, with the
// sequence numbers reads report. Each key a scan read bounds S: a value at
// seq s puts S in [s, the key's next write); an absent key puts S where
// the key's newest write is a delete, or before its first. A scan is a
// snapshot if the bounds of all its keys meet. A key with a delete whose
// sequence number is unknown bounds nothing when absent.
func (c *checker) snapshots() []anomaly {
	type dw struct {
		seq   uint64
		value *string
		op    int // the write
	}
	durable := map[string][]dw{}
	vague := map[string]bool{} // keys with a delete of unknown seq
	seen := map[string]bool{}
	add := func(k string, seq uint64, v *string, op int) {
		id := fmt.Sprintf("%s@%d", k, seq)
		if seen[id] {
			return
		}
		seen[id] = true
		durable[k] = append(durable[k], dw{seq, v, op})
	}
	for i, o := range c.ops {
		if o.Outcome == "fail" || o.Zombie {
			continue
		}
		known := o.Outcome == "ok" && o.Durable && o.Seq != 0
		for _, w := range o.Writes {
			switch {
			case known:
				add(w.Key, o.Seq, w.Value, i)
			case w.Value == nil:
				vague[w.Key] = true
			}
		}
		if o.Snapshot != "" {
			continue // what it read may not be durable
		}
		for k, r := range o.Read {
			if r.Found && r.Seq != 0 {
				v := r.Value
				add(k, r.Seq, &v, c.written[r.Value])
			}
		}
	}
	for k := range durable {
		sort.Slice(durable[k], func(i, j int) bool { return durable[k][i].seq < durable[k][j].seq })
	}
	const inf = ^uint64(0)
	type span struct{ lo, hi uint64 } // [lo, hi)
	intersect := func(a, b []span) []span {
		var out []span
		for _, x := range a {
			for _, y := range b {
				if lo, hi := max(x.lo, y.lo), min(x.hi, y.hi); lo < hi {
					out = append(out, span{lo, hi})
				}
			}
		}
		return out
	}
	var out []anomaly
	for i, o := range c.ops {
		if o.Kind != opScan || o.Outcome != "ok" || (o.Snapshot != "" && !sameLaunch(o)) {
			continue
		}
		cand := []span{{0, inf}}
		if o.SnapSeq != 0 {
			cand = []span{{o.SnapSeq, o.SnapSeq + 1}} // a snapshot says where it reads
		}
		var culprit string
		for _, k := range slices.Sorted(maps.Keys(o.Read)) {
			r := o.Read[k]
			ws := durable[k]
			if o.Snapshot != "" {
				// A snapshot reads its writer's memory, which a later writer's
				// durable history need not agree with past the point this one
				// made durable: a writer that crashes loses the sequence numbers
				// of what it had not made durable, and the next reuses them. So
				// it answers to its own writer's writes only.
				ws = slices.DeleteFunc(slices.Clone(ws), func(w dw) bool {
					return c.ops[w.op].Proc != o.Proc || c.ops[w.op].Launch != o.Launch
				})
			}
			var allowed []span
			if r.Found {
				next := inf
				if j := sort.Search(len(ws), func(j int) bool { return ws[j].seq > r.Seq }); j < len(ws) {
					next = ws[j].seq
				}
				allowed = []span{{r.Seq, next}}
			} else {
				if vague[k] {
					continue
				}
				first := inf
				if len(ws) > 0 {
					first = ws[0].seq
				}
				allowed = []span{{0, first}}
				for j, w := range ws {
					if w.value != nil {
						continue
					}
					next := inf
					if j+1 < len(ws) {
						next = ws[j+1].seq
					}
					allowed = append(allowed, span{w.seq, next})
				}
			}
			cand = intersect(cand, allowed)
			if len(cand) == 0 {
				culprit = k
				break
			}
		}
		if culprit == "" {
			continue
		}
		var b strings.Builder
		fmt.Fprintf(&b, "scan op %d is no snapshot: no sequence number explains every key it read, up to %s (read as ", i, culprit)
		if r := o.Read[culprit]; r.Found {
			fmt.Fprintf(&b, "%q at seq %d)", short(r.Value), r.Seq)
		} else {
			b.WriteString("absent)")
		}
		fmt.Fprintf(&b, "; %s's durable writes:", culprit)
		for _, w := range durable[culprit] {
			v := "delete"
			if w.value != nil {
				v = short(*w.value)
			}
			fmt.Fprintf(&b, " %d:%s", w.seq, v)
		}
		out = append(out, anomaly{Kind: "scan-not-snapshot", Key: culprit, Ops: []int{i}, Detail: b.String()})
	}
	return out
}

// monotonic checks that each reader launch never goes back in time: once
// it has read a key at seq s, a read it begins later sees seq s or later,
// or the key absent only if a delete of it may have come after.
func (c *checker) monotonic() []anomaly {
	type slot struct {
		proc   string
		launch int
		key    string
	}
	deletes := map[string][]uint64{} // key -> seqs of its deletes, 0 for unknown
	for _, o := range c.ops {
		if o.Outcome == "fail" {
			continue
		}
		for _, w := range o.Writes {
			if w.Value == nil {
				seq := o.Seq
				if o.Outcome != "ok" {
					seq = 0
				}
				deletes[w.Key] = append(deletes[w.Key], seq)
			}
		}
	}
	deletedAfter := func(k string, s uint64) bool {
		for _, d := range deletes[k] {
			if d == 0 || d > s {
				return true
			}
		}
		return false
	}
	type obs struct {
		r   read
		op  int
		ret time.Time
	}
	last := map[slot]obs{} // the newest read returned so far
	idx := make([]int, len(c.ops))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return c.ops[idx[a]].Call.Before(c.ops[idx[b]].Call) })
	var out []anomaly
	// Reads in call order; a read is compared with the newest read of its
	// slot that returned before it was called.
	var done []int // reads returned, by return time, waiting to count
	for _, i := range idx {
		o := c.ops[i]
		if !o.Reader || o.Outcome != "ok" {
			continue
		}
		// Count every read that returned before this one was called.
		var keep []int
		for _, d := range done {
			p := c.ops[d]
			if p.Return.Before(o.Call) {
				for k, r := range p.Read {
					sl := slot{p.Proc, p.Launch, k}
					if r.Found && (!last[sl].r.Found || r.Seq > last[sl].r.Seq) {
						last[sl] = obs{r, d, p.Return}
					}
				}
			} else {
				keep = append(keep, d)
			}
		}
		done = keep
		for k, r := range o.Read {
			prev, ok := last[slot{o.Proc, o.Launch, k}]
			if !ok || !prev.r.Found {
				continue
			}
			if r.Found && r.Seq >= prev.r.Seq {
				continue
			}
			if !r.Found && deletedAfter(k, prev.r.Seq) {
				continue
			}
			out = append(out, anomaly{Kind: "reader-went-back", Key: k, Ops: []int{prev.op, i},
				Detail: fmt.Sprintf("%s (launch %d) read %s at seq %d in op %d, then %s in op %d", o.Proc, o.Launch, k, prev.r.Seq, prev.op, describe(r), i)})
		}
		done = append(done, i)
	}
	return out
}

// sameLaunch reports whether a scan of a snapshot went to the launch of
// the writer that opened the snapshot. A scan sent after a restart reaches
// another process, where the snapshot's ID names nothing -- or, in an old
// run whose IDs restarted with each process, another snapshot.
func sameLaunch(o op) bool {
	return strings.HasPrefix(o.Snapshot, fmt.Sprintf("%s/%d/", o.Proc, o.Launch))
}

// repeatable checks that every scan of one snapshot read the same.
func (c *checker) repeatable() []anomaly {
	first := map[string]int{}
	var out []anomaly
	for i, o := range c.ops {
		name := o.Snapshot
		switch {
		case o.Outcome != "ok":
			continue
		case o.Clone != "":
			name = "clone " + o.Clone
		case o.Snapshot == "" || !sameLaunch(o):
			continue
		}
		j, ok := first[name]
		if !ok {
			first[name] = i
			continue
		}
		for _, k := range slices.Sorted(maps.Keys(o.Read)) {
			if a, b := c.ops[j].Read[k], o.Read[k]; a != b {
				kind := "snapshot-not-repeatable"
				if o.Clone != "" {
					kind = "clone-changed"
				}
				out = append(out, anomaly{Kind: kind, Key: k, Ops: []int{j, i},
					Detail: fmt.Sprintf("%s at seq %d read %s as %s in op %d, then as %s in op %d", name, o.SnapSeq, k, describe(a), j, describe(b), i)})
				break
			}
		}
	}
	return out
}

// linearizable checks each key's history against a register: the order
// SlateDB's reads of durable data and durable writes promise. A write's
// effect may come at any time after its call; one acknowledged without
// awaiting durability, or with no answer, may never come -- unless a
// later write of the same writer was acknowledged durable, which makes
// every write before it in sequence order durable too.
func (c *checker) linearizable(ctx context.Context) (anomalies []anomaly, unknown []string) {
	t0 := c.ops[0].Call
	for _, o := range c.ops {
		if o.Call.Before(t0) {
			t0 = o.Call
		}
	}
	at := func(t time.Time) int64 { return int64(t.Sub(t0)) }

	// For each writer launch, the durable acknowledgements in sequence
	// order, to find when a write became durable at the latest.
	type ack struct {
		seq uint64
		at  time.Time
	}
	acks := map[string][]ack{}
	for _, o := range c.ops {
		if len(o.Writes) > 0 && o.Durable && o.Outcome == "ok" && o.Seq != 0 {
			id := fmt.Sprintf("%s/%d", o.Proc, o.Launch)
			acks[id] = append(acks[id], ack{o.Seq, o.Return})
		}
	}
	// durableBy is when a write of seq on launch id was durable, at the
	// latest: the earliest return of a durable ack of that launch at or
	// above seq.
	durableBy := func(id string, seq uint64) (time.Time, bool) {
		var best time.Time
		for _, a := range acks[id] {
			if a.seq >= seq && (best.IsZero() || a.at.Before(best)) {
				best = a.at
			}
		}
		return best, !best.IsZero()
	}

	perKey := map[string][]linearize.Op[regIn, read]{}
	opOf := map[string][]int{}
	for i, o := range c.ops {
		if o.Outcome == "fail" {
			continue
		}
		switch {
		case len(o.Writes) > 0:
			ret := linearize.Pending
			switch {
			case o.Outcome == "ok" && o.Durable:
				ret = at(o.Return)
			case o.Seq != 0:
				if t, ok := durableBy(fmt.Sprintf("%s/%d", o.Proc, o.Launch), o.Seq); ok {
					ret = at(t)
				}
			}
			for _, w := range o.Writes {
				perKey[w.Key] = append(perKey[w.Key], linearize.Op[regIn, read]{
					Client: o.Client, Call: at(o.Call), Return: ret, Input: regIn{write: true, value: w.Value}})
				opOf[w.Key] = append(opOf[w.Key], i)
			}
		case o.Outcome == "ok" && !o.Zombie && !o.Reader && o.Snapshot == "" && o.Clone == "":
			for k, r := range o.Read {
				perKey[k] = append(perKey[k], linearize.Op[regIn, read]{
					Client: o.Client, Call: at(o.Call), Return: at(o.Return), Input: regIn{}, Output: r})
				opOf[k] = append(opOf[k], i)
			}
		}
	}
	for _, k := range slices.Sorted(maps.Keys(perKey)) {
		hist := perKey[k]
		kctx, cancel := context.WithTimeout(ctx, c.budget)
		res, err := linearize.CheckWithin(kctx, registerModel, hist, c.limits)
		cancel()
		switch res.Outcome {
		case linearize.Ok:
		case linearize.Illegal:
			stuck := opOf[k][res.Stuck]
			var b strings.Builder
			fmt.Fprintf(&b, "no order of %s's %d operations explains them; stuck at op %d", k, len(hist), stuck)
			if res.State.found {
				fmt.Fprintf(&b, " with %s holding %q", k, res.State.value)
			} else {
				fmt.Fprintf(&b, " with %s holding nothing", k)
			}
			anomalies = append(anomalies, anomaly{Kind: "nonlinearizable", Key: k, Ops: []int{stuck}, Detail: b.String()})
		default:
			unknown = append(unknown, fmt.Sprintf("%s (%d ops): %v", k, len(hist), err))
		}
	}
	return anomalies, unknown
}
