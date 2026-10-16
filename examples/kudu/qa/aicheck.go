//go:build unix

package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dotnwat/torx/linearize"
)

// aiCheck checks aiTable, whose rows a value names, each written once, and
// whose ids the tablet servers assign. Kudu documents that the ids are
// "monotonically increasing per tablet", and an update or delete names a
// row by its key and its id, so a row's id must never change: every scan
// that sees a value sees it with one id; within a tablet, rows committed
// later have greater ids, and no two rows share one. And each row's history
// must be legal in Kudu's timestamp order: it is present from its insert
// until a delete that named it by the id a scan saw, and absent otherwise.
// keyTablets names the tablet each key of aiTable is in.
func aiCheck(ctx context.Context, hist []op, keyTablets []string, budget time.Duration) []anomaly {
	var bad []anomaly

	// A row's id, as each scan saw it.
	type sighting struct {
		k, id int64
		op    int
	}
	first := map[string]sighting{}
	for _, o := range hist {
		if o.Kind != "read" || !o.ai() || o.NodeErr != "" || o.Code != "" {
			continue
		}
		for _, x := range o.Rows {
			s := sighting{x.K, x.ID, o.Index}
			f, ok := first[x.V]
			switch {
			case !ok:
				first[x.V] = s
			case f.k != s.k || f.id != s.id:
				bad = append(bad, anomaly{Kind: "auto-id-changed", Key: ptr(x.K), Ops: []int{f.op, o.Index},
					Detail: fmt.Sprintf("row %s was key %d id %d, and later key %d id %d", x.V, f.k, f.id, s.k, s.id)})
				first[x.V] = s
			}
		}
	}

	// Within a tablet, ids follow the order inserts committed in.
	type insert struct {
		ts int64
		v  string
		op int
	}
	inserts := map[string]insert{}
	for _, o := range hist {
		if o.Kind != "write" || !o.ai() || len(o.Ops) != 1 || o.Ops[0].Op != "insert" {
			continue
		}
		if len(o.Results) == 1 && o.Results[0].Code == "OK" {
			inserts[o.Ops[0].V] = insert{o.Results[0].TS, o.Ops[0].V, o.Index}
		}
	}
	byTablet := map[string][]insert{}
	for v, in := range inserts {
		s, ok := first[v]
		if !ok || s.k < 0 || int(s.k) >= len(keyTablets) {
			continue
		}
		byTablet[keyTablets[s.k]] = append(byTablet[keyTablets[s.k]], in)
	}
	for _, t := range slices.Sorted(maps.Keys(byTablet)) {
		ins := byTablet[t]
		slices.SortFunc(ins, func(a, b insert) int { return compare(a.ts, b.ts) })
		ids := map[int64]string{}
		for i, in := range ins {
			id := first[in.v].id
			if other, ok := ids[id]; ok {
				bad = append(bad, anomaly{Kind: "auto-id-reused", Ops: []int{inserts[other].op, in.op},
					Detail: fmt.Sprintf("rows %s and %s of tablet %s both have id %d", other, in.v, t, id)})
			}
			ids[id] = in.v
			if i > 0 {
				prev := ins[i-1]
				if pid := first[prev.v].id; pid >= id {
					bad = append(bad, anomaly{Kind: "auto-id-order", Ops: []int{prev.op, in.op},
						Detail: fmt.Sprintf("row %s committed at %d before row %s at %d in tablet %s, but has id %d, not less than %d",
							prev.v, prev.ts, in.v, in.ts, t, pid, id)})
				}
			}
		}
	}

	// Each row's history on Kudu's timeline: its insert, the deletes that
	// meant it, and whether each scan saw it.
	key := map[string]int64{}
	var line linearize.Timeline
	var placed []kvOp
	place := func(v string, ts int64, read bool, o linearize.Op[kvIn, kvOut]) {
		k, ok := key[v]
		if !ok {
			k = int64(len(key))
			key[v] = k
		}
		placed = append(placed, kvOp{key: k, ts: ts, read: read, op: o})
	}
	lowerBound := func(o op) int64 {
		lo := o.Call / 1000 << 12
		if o.PropBefore >= 0 && o.PropBefore+1 > lo {
			lo = o.PropBefore + 1
		}
		return lo
	}
	for _, o := range hist {
		if o.Kind != "write" || !o.ai() || len(o.Ops) != 1 {
			continue
		}
		v, kind := o.Ops[0].V, o.Ops[0].Op
		if kind == "delete" {
			v = o.Target
		}
		in := kvIn{Kind: kind, V: v, Op: o.Index}
		if len(o.Results) == 1 && (o.Results[0].Code == "OK" || definite(o.Results[0].Code)) && o.Results[0].TS > 0 {
			place(v, o.Results[0].TS, false, linearize.Op[kvIn, kvOut]{Client: o.Client, Input: in, Output: kvOut{Code: o.Results[0].Code}})
		} else {
			place(v, lowerBound(o), false, linearize.Op[kvIn, kvOut]{Client: o.Client, Input: in, Output: kvOut{Unknown: true}})
		}
	}
	for _, o := range hist {
		if o.Kind != "read" || !o.ai() || o.NodeErr != "" || o.Code != "" || o.SnapshotTS <= 0 {
			continue
		}
		saw := map[string]bool{}
		for _, x := range o.Rows {
			saw[x.V] = true
		}
		for v := range key {
			out := kvOut{}
			if saw[v] {
				out.V = v
			}
			place(v, o.SnapshotTS, true, linearize.Op[kvIn, kvOut]{Client: o.Client,
				Input: kvIn{Kind: "read", Op: o.Index, Chosen: o.Read.TS == 0}, Output: out})
		}
		for v := range saw {
			if _, ok := key[v]; !ok {
				bad = append(bad, anomaly{Kind: "ai-phantom", Ops: []int{o.Index}, Detail: fmt.Sprintf("a scan saw row %s, which no insert wrote", v)})
			}
		}
	}
	for i, x := range placed {
		if i == 0 || x.ts < line.Base {
			line.Base = x.ts
		}
	}
	byRow := map[int64][]kvOp{}
	for _, x := range placed {
		switch {
		case x.read:
			x.op.Call, x.op.Return = line.Read(x.ts)
		case x.op.Output.Unknown:
			x.op.Call, x.op.Return = line.Unknown(x.ts)
		default:
			x.op.Call, x.op.Return = line.Write(x.ts)
		}
		byRow[x.key] = append(byRow[x.key], x)
	}
	names := map[int64]string{}
	for v, k := range key {
		names[k] = v
	}
	rows, _ := checkKeys(ctx, byRow, budget)
	for _, a := range rows {
		if a.Kind == "timestamp-order" {
			a.Kind = "ai-timestamp-order"
		}
		a.Detail = fmt.Sprintf("row %s: %s", names[*a.Key], a.Detail)
		a.Key = nil
		bad = append(bad, a)
	}
	return bad
}
