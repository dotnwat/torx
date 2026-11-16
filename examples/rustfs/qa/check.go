//go:build unix

package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx/linearize"
)

// anomaly is one thing the checker found wrong.
type anomaly struct {
	// Kind is what went wrong:
	//   corrupt-read     a GET returned bytes no write wrote, or a value's
	//                    bytes under another value's ETag
	//   nonlinearizable  no order of a key's operations explains what the
	//                    clients saw: a lost or resurrected write, a stale
	//                    read, a precondition that held when it did not
	//   check-timeout    the search for an order ran out of time
	//   unexpected-exit  a server exited without being stopped
	Kind    string        `json:"kind"`
	Key     string        `json:"key,omitempty"`
	Node    string        `json:"node,omitempty"`
	Process string        `json:"process,omitempty"`
	At      time.Duration `json:"at,omitempty"`
	Detail  string        `json:"detail"`
	// Ops is the operations around a nonlinearizable key's violation.
	Ops []Op `json:"ops,omitempty"`
}

type anomalies struct {
	mu   sync.Mutex
	list []anomaly
}

func (a *anomalies) add(x anomaly) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.list = append(a.list, x)
}

func (a *anomalies) all() []anomaly {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.list)
}

// keyOps turns a history into the operations of each key, as the model
// takes them: a listing becomes a read of every key in keys, and a read
// that did not answer is left out, since it changed nothing. ops[k][i]
// came from src[k][i].
func keyOps(history []Op, keys []string) (ops map[string][]linearize.Op[keyIn, keyOut], src map[string][]Op) {
	ops = map[string][]linearize.Op[keyIn, keyOut]{}
	src = map[string][]Op{}
	add := func(k string, op Op, in keyIn, out keyOut) {
		ret := int64(op.End)
		if out.result == unknown {
			ret = linearize.Pending
		}
		ops[k] = append(ops[k], linearize.Op[keyIn, keyOut]{Call: int64(op.Start), Return: ret, Input: in, Output: out})
		src[k] = append(src[k], op)
	}
	for _, op := range history {
		if op.Process == "nemesis" {
			continue
		}
		in := keyIn{f: op.F, value: op.Value, expect: op.Expect}
		var out keyOut
		switch op.Outcome {
		case Ok:
			out.result = done
		case Info:
			out.result = unknown
		case Fail:
			switch op.Code {
			case "PreconditionFailed":
				out.result = precondition
			case "NoSuchKey":
				out.result = noSuchKey
			default:
				continue // certainly no effect, and nothing seen
			}
		}
		switch op.F {
		case "get", "head":
			if out.result != done {
				continue
			}
			in.value, out.read = "", op.Value
			add(op.Key, op, in, out)
		case "list":
			if out.result != done {
				continue
			}
			for _, k := range keys {
				kop := op
				kop.Key = k
				add(k, kop, in, keyOut{result: done, read: op.Listed[k]})
			}
		case "list-versions":
			// Checked by checkVersions.
		default:
			add(op.Key, op, in, out)
		}
	}
	return ops, src
}

// checkVersions checks the lists of versions of a bucket with versioning
// that the servers gave at the end: a version is never removed here, only
// added, so every version a write made and was acknowledged for must be
// listed (lost-version); every version listed must be one a write made, its
// value the one that write wrote (phantom-version); and each key's versions
// must be in an order that keeps a write acknowledged before another began
// older than it (version-order).
func checkVersions(history []Op) []anomaly {
	byVersion := map[string]Op{} // version id -> the write that made it
	var acked []Op
	for _, op := range history {
		if op.Process == "nemesis" || !isWrite(op.F) || op.Version == "" {
			continue
		}
		byVersion[op.Version] = op
		if op.Outcome == Ok {
			acked = append(acked, op)
		}
	}
	var out []anomaly
	for _, l := range history {
		if l.F != "list-versions" || l.Outcome != Ok {
			continue
		}
		// A list of versions gives each key's versions newest first, and
		// its delete markers newest first, but RustFS lists the delete
		// markers apart from the versions: across the two, only the times
		// they were made order them.
		type place struct {
			marker bool
			pos    int // among its key's versions, or its delete markers; 0 newest
			at     time.Time
		}
		pos := map[string]place{}
		perKey := map[[2]string]int{}
		for _, v := range l.Versions {
			kind := [2]string{v.Key, strconv.FormatBool(v.DeleteMarker)}
			at, _ := time.Parse(time.RFC3339Nano, v.Modified)
			pos[v.Version] = place{v.DeleteMarker, perKey[kind], at}
			perKey[kind]++
			w, ok := byVersion[v.Version]
			switch {
			case !ok:
				// A write whose answer was lost made a version no one
				// knows the id of: find one that may have made it, by
				// the value it wrote, or a delete of the key for a delete
				// marker.
				if !slices.ContainsFunc(history, func(op Op) bool {
					return isWrite(op.F) && op.Key == v.Key && op.Outcome == Info && op.Value == v.Value
				}) {
					out = append(out, anomaly{Kind: "phantom-version", Key: v.Key, Node: l.Node, Process: l.Process,
						Detail: fmt.Sprintf("%s lists version %s (%s) of %s, which no write made", l.Node, v.Version, describeVersion(v), v.Key)})
				}
			case w.Key != v.Key || v.DeleteMarker != (w.Value == "") || !v.DeleteMarker && v.Value != w.Value:
				out = append(out, anomaly{Kind: "phantom-version", Key: v.Key, Node: l.Node, Process: l.Process,
					Detail: fmt.Sprintf("%s lists version %s of %s as %s, which %s's %s made", l.Node, v.Version, v.Key, describeVersion(v), w.Process, describe(w))})
			}
		}
		for _, w := range acked {
			if _, ok := pos[w.Version]; !ok {
				out = append(out, anomaly{Kind: "lost-version", Key: w.Key, Node: l.Node, Process: w.Process, At: w.Start,
					Detail: fmt.Sprintf("%s lists no version %s of %s, which %s's %s at %v-%v made and was acknowledged", l.Node, w.Version, w.Key,
						w.Process, describe(w), w.Start.Round(time.Millisecond), w.End.Round(time.Millisecond)), Ops: []Op{w}})
			}
		}
		for _, a := range acked {
			for _, b := range acked {
				pa, oka := pos[a.Version]
				pb, okb := pos[b.Version]
				newer := pb.pos < pa.pos
				if pa.marker != pb.marker {
					newer = !pb.at.Before(pa.at)
				}
				if a.Key == b.Key && oka && okb && a.End < b.Start && !newer {
					out = append(out, anomaly{Kind: "version-order", Key: a.Key, Node: l.Node, Process: b.Process, At: b.Start,
						Detail: fmt.Sprintf("%s lists %s's %s, acknowledged at %v, as newer than %s's %s, begun at %v", l.Node,
							a.Process, describe(a), a.End.Round(time.Millisecond), b.Process, describe(b), b.Start.Round(time.Millisecond)),
						Ops: []Op{a, b}})
				}
			}
		}
	}
	return out
}

func describeVersion(v VersionEntry) string {
	if v.DeleteMarker {
		return "a delete marker"
	}
	return "value " + v.Value
}

// checkLinear checks each key's history for linearizability, keys in
// parallel, each key's search bounded by budget, and returns what it found.
//
// A write with no answer may take effect at any time after its call, and a
// history with many of them can take the search a long time. So each key is
// checked first without the unanswered writes whose values no operation
// ever saw, and with each unanswered write whose value was seen made to
// return by the end of the first operation that saw it. That history is
// linearizable only if the whole one is: a write left out is one placed
// last, where it changes nothing anyone saw. Only a key whose pruned
// history is not linearizable is searched again in full -- a write left out
// may have made a precondition fail -- and only one whose full history is
// not linearizable either is an anomaly.
func checkLinear(ctx context.Context, model linearize.Model[string, keyIn, keyOut], history []Op, keys []string, budget time.Duration) []anomaly {
	ops, src := keyOps(history, keys)
	var mu sync.Mutex
	var out []anomaly
	var wg sync.WaitGroup
	sem := make(chan struct{}, checkParallel)
	for _, k := range keys {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			pops, psrc := prune(ops[k], src[k])
			res, err := linearize.CheckWithin(cctx, model, pops, checkLimits)
			var a *anomaly
			switch res.Outcome {
			case linearize.Unknown:
				a = &anomaly{Kind: "check-timeout", Key: k,
					Detail: fmt.Sprintf("%d operations, %d configurations explored: %v", len(pops), res.Explored, err)}
			case linearize.Illegal:
				var core []int
				a, core = explain(cctx, model, k, res, pops, psrc)
				if len(pops) == len(ops[k]) {
					break
				}
				// The unanswered writes nobody saw, left out, could only
				// have explained a precondition that failed: placed
				// anywhere else, each makes the key hold a value no read
				// saw and no precondition named. A core with no such
				// failure is a core of the full history too.
				if core != nil && !slices.ContainsFunc(core, func(i int) bool { return pops[i].Output.result == precondition }) {
					a.Detail += fmt.Sprintf(" (a core of the history with the %d unanswered writes nobody saw left out, and of the full history: it has no failed precondition they could explain)",
						len(ops[k])-len(pops))
					break
				}
				full, err := linearize.CheckWithin(cctx, model, ops[k], checkLimits)
				switch full.Outcome {
				case linearize.Ok:
					a = nil
				case linearize.Illegal:
					a.Detail += fmt.Sprintf(" (a core of the history with the %d unanswered writes nobody saw left out; the full history admits no order either)",
						len(ops[k])-len(pops))
				case linearize.Unknown:
					a.Detail += fmt.Sprintf(" (a core of the history with the %d unanswered writes nobody saw left out, which may explain its failed precondition; the search of the full history ran out: %v)",
						len(ops[k])-len(pops), err)
				}
			}
			if a != nil {
				mu.Lock()
				out = append(out, *a)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	slices.SortFunc(out, func(a, b anomaly) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// The searches of keys' histories run checkParallel at a time, each
// remembering at most checkLimits' configurations: about a gigabyte for a
// key with a thousand operations.
const checkParallel = 2

var checkLimits = linearize.Limits{Configurations: 4 << 20}

// prune leaves out the unanswered writes of a key's history whose values no
// operation saw, and makes each unanswered write whose value was seen
// return when the first operation that saw it did, since it took effect
// before that.
func prune(ops []linearize.Op[keyIn, keyOut], src []Op) ([]linearize.Op[keyIn, keyOut], []Op) {
	seen := map[string]int64{} // value -> the earliest return of an operation that saw it
	saw := func(v string, ret int64) {
		if v == "" {
			return
		}
		if t, ok := seen[v]; !ok || ret < t {
			seen[v] = ret
		}
	}
	for _, op := range ops {
		if op.Output.result != done {
			continue
		}
		switch op.Input.f {
		case "get", "head", "list":
			saw(op.Output.read, op.Return)
		case "put-if-match", "delete-if-match":
			saw(op.Input.expect, op.Return)
		}
	}
	var pops []linearize.Op[keyIn, keyOut]
	var psrc []Op
	for i, op := range ops {
		if op.Output.result == unknown && op.Input.value != "" {
			t, ok := seen[op.Input.value]
			if !ok {
				continue
			}
			op.Return = max(op.Call, t)
		}
		pops = append(pops, op)
		psrc = append(psrc, src[i])
	}
	return pops, psrc
}

// explain describes a key whose history is not linearizable by its core:
// the few operations that, with every other operation weakened, still admit
// no order (linearize.Minimize). Ops lists the core, and for context the
// writes from the first write of a value the core names to the core's end.
// It returns the core, or nil if the search for one failed.
func explain(ctx context.Context, model linearize.Model[string, keyIn, keyOut], k string, res linearize.Result[string], ops []linearize.Op[keyIn, keyOut], src []Op) (*anomaly, []int) {
	stuck := src[res.Stuck]
	a := &anomaly{Kind: "nonlinearizable", Key: k, Node: stuck.Node, Process: stuck.Process, At: stuck.Start}
	mctx, cancel := context.WithTimeout(ctx, minimizeBudget)
	defer cancel()
	core, err := linearize.Minimize(mctx, model, ops, weaken)
	shown := core
	if err != nil {
		core, shown = nil, []int{res.Stuck}
	}
	named := map[string]bool{}
	var b strings.Builder
	fmt.Fprintf(&b, "no order of %d operations explains these %d:", len(ops), len(shown))
	lo, hi := src[shown[0]].Start, src[shown[0]].End
	for _, i := range shown {
		op := src[i]
		fmt.Fprintf(&b, "\n  %v-%v %s %s", op.Start.Round(time.Microsecond), op.End.Round(time.Microsecond), op.Process, describe(op))
		a.Ops = append(a.Ops, op)
		named[op.Value], named[op.Expect] = true, true
		lo, hi = min(lo, op.Start), max(hi, op.End)
	}
	for _, op := range src {
		if isWrite(op.F) && op.Value != "" && named[op.Value] {
			lo = min(lo, op.Start)
		}
	}
	var context []Op
	for i, op := range src {
		if !isWrite(op.F) || slices.Contains(shown, i) || op.Start > hi {
			continue
		}
		if op.End >= lo || ops[i].Return == linearize.Pending && op.Start >= lo-5*time.Second {
			context = append(context, op)
		}
	}
	if len(context) > 60 {
		context = context[len(context)-60:]
	}
	// An unanswered write a read saw took effect before that read ended,
	// which the check relies on: show the first read that saw each.
	for _, w := range context {
		if w.Outcome != Info || w.Value == "" {
			continue
		}
		for _, op := range src {
			if !isWrite(op.F) && op.Outcome == Ok && op.Value == w.Value || op.F == "list" && op.Listed[k] == w.Value {
				context = append(context, op)
				break
			}
		}
	}
	a.Ops = append(a.Ops, context...)
	slices.SortStableFunc(a.Ops, func(x, y Op) int { return int(x.Start - y.Start) })
	a.Detail = b.String()
	return a, core
}

// minimizeBudget bounds the search for a key's core.
const minimizeBudget = time.Minute

// weaken is how linearize.Minimize may weaken an operation on a key: a
// read, or a conditional write whose precondition failed, changes nothing
// and may be left out; a conditional write that succeeded may be made
// unconditional; and the rest -- writes and deletes, which take effect
// whether they answered or not, and conditional writes with no answer --
// stay as they are.
func weaken(op linearize.Op[keyIn, keyOut]) (linearize.Op[keyIn, keyOut], linearize.Weakening) {
	switch op.Input.f {
	case "get", "head", "list":
		return op, linearize.Drop
	case "put-if-absent", "put-if-match", "delete-if-match":
		switch op.Output.result {
		case precondition, noSuchKey:
			return op, linearize.Drop
		case done:
			if op.Input.f == "delete-if-match" {
				op.Input.f = "delete"
			} else {
				op.Input.f = "put"
			}
			op.Input.expect = ""
			return op, linearize.Replace
		}
	}
	return op, linearize.Keep
}

func isWrite(f string) bool {
	switch f {
	case "put", "put-if-absent", "put-if-match", "multipart", "delete", "delete-if-match":
		return true
	}
	return false
}

// describe renders an operation briefly: "put c3-17 ok", "get -> c1-4".
func describe(op Op) string {
	var b strings.Builder
	b.WriteString(op.F)
	if op.F == "list" {
		v := op.Listed[op.Key]
		if v == "" {
			v = "no object"
		}
		fmt.Fprintf(&b, " -> %s", v)
	} else if isWrite(op.F) {
		if op.Value != "" {
			b.WriteString(" " + op.Value)
		}
		if op.Expect != "" {
			b.WriteString(" if " + op.Expect)
		}
	} else {
		v := op.Value
		if v == "" {
			v = "no object"
		}
		fmt.Fprintf(&b, " -> %s", v)
	}
	b.WriteString(" " + string(op.Outcome))
	if op.Code != "" {
		b.WriteString(" " + op.Code)
	}
	if op.Node != "" {
		b.WriteString(" via " + op.Node)
	}
	return b.String()
}
