//go:build unix

package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
	"github.com/dotnwat/torx/linearize"
)

// anomaly is something the checker found that Kudu's guarantees rule out.
type anomaly struct {
	Kind string `json:"kind"`
	Key  *int64 `json:"key,omitempty"`
	Txn  string `json:"txn,omitempty"`
	// Ops are the history's operations that show it, as indices.
	Ops    []int  `json:"ops,omitempty"`
	Detail string `json:"detail"`
}

func (a anomaly) String() string {
	var b strings.Builder
	b.WriteString(a.Kind)
	if a.Key != nil {
		fmt.Fprintf(&b, " key %d", *a.Key)
	}
	if a.Txn != "" {
		fmt.Fprintf(&b, " txn %s", a.Txn)
	}
	fmt.Fprintf(&b, ": %s", a.Detail)
	if len(a.Ops) > 0 {
		fmt.Fprintf(&b, " (ops %v)", a.Ops)
	}
	return b.String()
}

// The per-key model: a row that is absent ("") or holds a value. Every
// value written is unique, so a read names the write it saw.
type kvIn struct {
	Kind string // a row operation's kind, or "read"
	V    string
	Op   int // the history's operation it came from
	// Chosen is a read at a snapshot the server chose, at "now": Kudu
	// documents that such a read may not be repeatable around a leader
	// change (KUDU-1188), as a deposed leader serves what it has.
	Chosen bool
}

type kvOut struct {
	Code    string // a write's result: OK, AlreadyPresent, NotFound; "" if unknown
	V       string // what a read saw
	Unknown bool
}

var kvModel = linearize.Model[string, kvIn, kvOut]{
	Init: func() string { return "" },
	Step: kvStep,
	Same: func(a kvIn, ao kvOut, b kvIn, bo kvOut) bool {
		return a.Kind == b.Kind && a.V == b.V
	},
}

func kvStep(s string, in kvIn, out kvOut) (bool, string) {
	present := s != ""
	if in.Kind == "read" {
		return out.V == s, s
	}
	if out.Unknown {
		// A write whose outcome is unknown takes effect as it would have
		// had it succeeded, whatever it was.
		switch in.Kind {
		case "upsert", "upsert_ignore":
			return true, in.V
		case "insert", "insert_ignore":
			if present {
				return true, s
			}
			return true, in.V
		case "update", "update_ignore":
			if present {
				return true, in.V
			}
			return true, s
		case "delete", "delete_ignore":
			return true, ""
		}
		return false, s
	}
	switch in.Kind {
	case "upsert", "upsert_ignore":
		return out.Code == "OK", in.V
	case "insert":
		switch out.Code {
		case "OK":
			return !present, in.V
		case "AlreadyPresent":
			return present, s
		}
	case "insert_ignore":
		if out.Code == "OK" {
			if present {
				return true, s
			}
			return true, in.V
		}
	case "update":
		switch out.Code {
		case "OK":
			return present, in.V
		case "NotFound":
			return !present, s
		}
	case "update_ignore":
		if out.Code == "OK" {
			if present {
				return true, in.V
			}
			return true, s
		}
	case "delete":
		switch out.Code {
		case "OK":
			return present, ""
		case "NotFound":
			return !present, s
		}
	case "delete_ignore":
		if out.Code == "OK" {
			return true, ""
		}
	}
	return false, s
}

// kvWeaken weakens an operation for Minimize: a read, or a write Kudu
// refused, can be left out; a conditional write that succeeded becomes the
// unconditional one; any other write, one whose outcome is unknown.
func kvWeaken(o linearize.Op[kvIn, kvOut]) (linearize.Op[kvIn, kvOut], linearize.Weakening) {
	switch {
	case o.Input.Kind == "read":
		return o, linearize.Drop
	case o.Output.Unknown:
		return o, linearize.Keep
	case o.Output.Code != "OK":
		return o, linearize.Drop
	}
	w := o
	switch o.Input.Kind {
	case "insert", "update":
		w.Input.Kind = "upsert"
		return w, linearize.Replace
	case "delete":
		w.Input.Kind = "delete_ignore"
		return w, linearize.Replace
	}
	w.Output = kvOut{Unknown: true}
	w.Return = linearize.Pending
	return w, linearize.Replace
}

// kvOp is a row operation or an observation of one key, placed in Kudu's
// timestamp order.
type kvOp struct {
	key int64
	op  linearize.Op[kvIn, kvOut]
	// ts is a known operation's timestamp, an unknown write's lower bound,
	// or a read's snapshot; read is whether the operation is a read.
	ts   int64
	read bool
}

// keyHistories turns the history into one history per key, in Kudu's
// timestamp order: every write Kudu answered sits at its timestamp; a
// snapshot read sits just before its snapshot timestamp, since a snapshot
// at t holds the writes before t; a write whose outcome is unknown may take
// effect at any timestamp the server could have given it -- past the
// client's propagated timestamp and the wall clock when it was called, the
// clock the servers on this host read -- or never. Keys of transactions
// are left out: they take effect at their commit, which txnCheck checks.
func keyHistories(hist []op) (map[int64][]kvOp, []anomaly) {
	var bad []anomaly
	written := map[int64]bool{}
	var out []kvOp
	for _, o := range hist {
		if o.Kind != "write" || o.Txn != "" || o.ai() {
			continue
		}
		for i, w := range o.Ops {
			written[w.K] = true
			var res kudu0
			if i < len(o.Results) {
				res = kudu0{o.Results[i].Code, o.Results[i].TS}
			}
			in := kvIn{Kind: w.Op, V: w.V, Op: o.Index}
			if res.code == "OK" || definite(res.code) {
				if res.ts <= 0 {
					bad = append(bad, anomaly{Kind: "no-timestamp", Key: new(w.K), Ops: []int{o.Index},
						Detail: fmt.Sprintf("%s answered %s with no timestamp", w.Op, res.code)})
					continue
				}
				out = append(out, kvOp{key: w.K, ts: res.ts, op: linearize.Op[kvIn, kvOut]{
					Client: o.Client, Input: in, Output: kvOut{Code: res.code}}})
				continue
			}
			lo := o.Call / 1000 << 12 // the wall clock in microseconds, as a hybrid timestamp
			if o.PropBefore >= 0 && o.PropBefore+1 > lo {
				lo = o.PropBefore + 1
			}
			out = append(out, kvOp{key: w.K, ts: lo, op: linearize.Op[kvIn, kvOut]{
				Client: o.Client, Input: in, Output: kvOut{Unknown: true}}})
		}
	}
	for _, o := range hist {
		if o.Kind != "read" || o.ai() || o.Read == nil || o.Read.Mode == "latest" || o.Read.Mode == "diff" || o.NodeErr != "" || o.Code != "" || o.SnapshotTS <= 0 {
			continue
		}
		seen := map[int64]string{}
		for _, r := range o.Rows {
			seen[r.K] = r.V
		}
		var inRange []int64
		if len(o.Read.Keys) > 0 {
			inRange = o.Read.Keys
		} else {
			lo, hi := int64(0), keyLimit
			if o.Read.Lo != nil {
				lo = *o.Read.Lo
			}
			if o.Read.Hi != nil {
				hi = *o.Read.Hi
			}
			for k := range written {
				if k >= lo && k < hi {
					inRange = append(inRange, k)
				}
			}
			for k := range seen {
				if !written[k] && !isTxnKey(k) {
					inRange = append(inRange, k)
				}
			}
		}
		for _, k := range inRange {
			if isTxnKey(k) {
				continue
			}
			chosen := o.Read.TS == 0
			out = append(out, kvOp{key: k, ts: o.SnapshotTS, read: true, op: linearize.Op[kvIn, kvOut]{
				Client: o.Client, Input: kvIn{Kind: "read", Op: o.Index, Chosen: chosen}, Output: kvOut{V: seen[k]}}})
		}
	}
	// Place everything on Kudu's timeline, whose snapshots are exclusive.
	var line linearize.Timeline
	for i, x := range out {
		if i == 0 || x.ts < line.Base {
			line.Base = x.ts
		}
	}
	byKey := map[int64][]kvOp{}
	for _, x := range out {
		switch {
		case x.read:
			x.op.Call, x.op.Return = line.Read(x.ts)
		case x.op.Output.Unknown:
			x.op.Call, x.op.Return = line.Unknown(x.ts)
		default:
			x.op.Call, x.op.Return = line.Write(x.ts)
		}
		byKey[x.key] = append(byKey[x.key], x)
	}
	return byKey, bad
}

type kudu0 struct {
	code string
	ts   int64
}

func isTxnKey(k int64) bool { return k >= txnBase && k < keyLimit }

// minimizeKeys is how many failing keys checkKeys shrinks to their core;
// shrinking costs many checks, and a fault that fails one key's history
// often fails dozens alike.
const minimizeKeys = 4

// checkKeys checks every key's history against the model, and shrinks the
// history of the first few keys that fail to the operations that show why;
// for the rest it reports where the search got stuck.
func checkKeys(ctx context.Context, byKey map[int64][]kvOp, budget time.Duration) (bad []anomaly, unknown []int64) {
	minimized := 0
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		xs := byKey[k]
		ops := make([]linearize.Op[kvIn, kvOut], len(xs))
		for i, x := range xs {
			ops[i] = x.op
		}
		cctx, cancel := context.WithTimeout(ctx, budget)
		res, err := linearize.CheckWithin(cctx, kvModel, ops, linearize.Limits{Configurations: 4 << 20})
		cancel()
		switch {
		case err != nil || res.Outcome == linearize.Unknown:
			unknown = append(unknown, k)
			continue
		case res.Outcome == linearize.Ok:
			continue
		}
		// A history that only reads at snapshots the server chose break
		// is the known issue, not a broken history.
		kind := "timestamp-order"
		var explicit []linearize.Op[kvIn, kvOut]
		for _, o := range ops {
			if !o.Input.Chosen {
				explicit = append(explicit, o)
			}
		}
		if len(explicit) < len(ops) {
			cctx, cancel := context.WithTimeout(ctx, budget)
			r2, err := linearize.CheckWithin(cctx, kvModel, explicit, linearize.Limits{Configurations: 4 << 20})
			cancel()
			if err == nil && r2.Outcome == linearize.Ok {
				kind = "stale-read"
			}
		}
		// Unminimized, the operations to show are the one the search could
		// not place, and the few before it.
		core := []int{res.Stuck}
		if n := len(res.Longest); n > 0 {
			core = append(slices.Clone(res.Longest[max(0, n-3):]), res.Stuck)
		}
		if minimized < minimizeKeys {
			minimized++
			mctx, cancel := context.WithTimeout(ctx, budget)
			if c, err := linearize.Minimize(mctx, kvModel, ops, kvWeaken); err == nil {
				core = c
			}
			cancel()
		}
		var lines []string
		var idx []int
		for _, i := range core {
			o := ops[i]
			idx = append(idx, o.Input.Op)
			lines = append(lines, describeKV(o))
		}
		slices.Sort(idx)
		idx = slices.Compact(idx)
		detail := "no order of the key's operations by Kudu's timestamps explains what was read: "
		if kind == "stale-read" {
			detail = "only reads at snapshots the server chose break the key's order by Kudu's timestamps: "
		}
		bad = append(bad, anomaly{Kind: kind, Key: new(k), Ops: idx, Detail: detail + strings.Join(lines, "; ")})
	}
	return bad, unknown
}

func describeKV(o linearize.Op[kvIn, kvOut]) string {
	at := fmt.Sprintf("@%d", o.Call)
	if o.Return == linearize.Pending {
		at = fmt.Sprintf("@>%d", o.Call)
	}
	switch {
	case o.Input.Kind == "read":
		v := o.Output.V
		if v == "" {
			v = "<absent>"
		}
		snap := "explicit"
		if o.Input.Chosen {
			snap = "chosen"
		}
		return fmt.Sprintf("op %d read %s %s (%s snapshot)", o.Input.Op, v, at, snap)
	case o.Output.Unknown:
		return fmt.Sprintf("op %d %s %s unknown %s", o.Input.Op, o.Input.Kind, o.Input.V, at)
	}
	return fmt.Sprintf("op %d %s %s %s %s", o.Input.Op, o.Input.Kind, o.Input.V, o.Output.Code, at)
}

// opChecks checks what each operation says on its own: that Kudu kept the
// client's causality -- a write's timestamp and a snapshot it picked are
// past the timestamp the client propagated -- and that a read returned
// sound rows: in what it asked for, each key once, with the payload its
// value implies. (A fault-tolerant scan is in key order within a tablet,
// but the table's tablets are hash partitions, so not across them.)
func opChecks(hist []op, valuesWritten map[string]bool) []anomaly {
	var bad []anomaly
	for _, o := range hist {
		switch o.Kind {
		case "write":
			if o.Txn != "" {
				continue
			}
			for i, r := range o.Results {
				if r.Code == "OK" && o.PropBefore >= 0 && r.TS <= o.PropBefore {
					bad = append(bad, anomaly{Kind: "write-before-propagated", Key: new(o.Ops[i].K), Ops: []int{o.Index},
						Detail: fmt.Sprintf("a write got timestamp %d, not past the client's propagated %d", r.TS, o.PropBefore)})
				}
			}
		case "read":
			if o.NodeErr != "" || o.Code != "" {
				continue
			}
			r := o.Read
			if (r.Mode == "ryw" || (r.Mode == "snapshot" && r.TS == 0)) && o.PropBefore >= 0 && o.SnapshotTS <= o.PropBefore {
				bad = append(bad, anomaly{Kind: "snapshot-before-propagated", Ops: []int{o.Index},
					Detail: fmt.Sprintf("a %s read picked snapshot %d, not past the client's propagated %d", r.Mode, o.SnapshotTS, o.PropBefore)})
			}
			if r.Mode == "snapshot" && r.TS > 0 && o.SnapshotTS != r.TS {
				bad = append(bad, anomaly{Kind: "snapshot-moved", Ops: []int{o.Index},
					Detail: fmt.Sprintf("a read asked for snapshot %d and read at %d", r.TS, o.SnapshotTS)})
			}
			seen := map[[2]int64]bool{}
			for _, x := range o.Rows {
				if seen[[2]int64{x.K, x.ID}] {
					bad = append(bad, anomaly{Kind: "duplicate-row", Key: new(x.K), Ops: []int{o.Index}, Detail: fmt.Sprintf("a scan returned the key (id %d) twice", x.ID)})
				}
				seen[[2]int64{x.K, x.ID}] = true
				if !inRead(r, x.K) {
					bad = append(bad, anomaly{Kind: "row-out-of-range", Key: new(x.K), Ops: []int{o.Index}, Detail: "a scan returned a key it did not ask for"})
				}
				if x.BadPay {
					bad = append(bad, anomaly{Kind: "corrupt-payload", Key: new(x.K), Ops: []int{o.Index}, Detail: fmt.Sprintf("value %s came with a payload it did not write", x.V)})
				}
				if r.Mode == "latest" && x.V != "" && !valuesWritten[x.V] {
					bad = append(bad, anomaly{Kind: "latest-read-unwritten", Key: new(x.K), Ops: []int{o.Index},
						Detail: fmt.Sprintf("a READ_LATEST read saw %s, which no write that could have taken effect wrote", x.V)})
				}
			}
		}
	}
	return bad
}

func inRead(r *kudu.ReadReq, k int64) bool {
	if len(r.Keys) > 0 {
		return slices.Contains(r.Keys, k)
	}
	return (r.Lo == nil || k >= *r.Lo) && (r.Hi == nil || k < *r.Hi)
}

// possiblyWritten is every value a write wrote that may have taken effect:
// not one Kudu refused.
func possiblyWritten(hist []op) map[string]bool {
	out := map[string]bool{}
	for _, o := range hist {
		if o.Kind != "write" {
			continue
		}
		for i, w := range o.Ops {
			if w.V == "" {
				continue
			}
			if i < len(o.Results) && definite(o.Results[i].Code) {
				continue
			}
			out[w.V] = true
		}
	}
	return out
}
