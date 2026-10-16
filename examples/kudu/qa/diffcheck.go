//go:build unix

package main

import (
	"fmt"
	"slices"
	"sort"
)

// replay is a key's states in Kudu's timestamp order: the state after each
// write Kudu answered, by the write's timestamp.
type replay struct {
	ts     []int64
	states []string
}

// at is the key's state in a snapshot at t, which holds the writes before t.
func (r replay) at(t int64) string {
	i := sort.Search(len(r.ts), func(i int) bool { return r.ts[i] >= t })
	if i == 0 {
		return ""
	}
	return r.states[i-1]
}

// replays replays the contended and fresh keys from the writes Kudu
// answered, and leaves out every key with a write of unknown outcome, which
// may have landed anywhere after its call.
func replays(hist []op) map[int64]replay {
	type w struct {
		ts   int64
		in   kvIn
		code string
	}
	writes := map[int64][]w{}
	unknown := map[int64]bool{}
	for _, o := range hist {
		if o.Kind != "write" || o.Txn != "" {
			continue
		}
		for i, x := range o.Ops {
			if i < len(o.Results) && (o.Results[i].Code == "OK" || definite(o.Results[i].Code)) && o.Results[i].TS > 0 {
				writes[x.K] = append(writes[x.K], w{o.Results[i].TS, kvIn{Kind: x.Op, V: x.V, Op: o.Index}, o.Results[i].Code})
			} else {
				unknown[x.K] = true
			}
		}
	}
	out := map[int64]replay{}
	for k, ws := range writes {
		if unknown[k] {
			continue
		}
		slices.SortFunc(ws, func(a, b w) int { return compare(a.ts, b.ts) })
		var r replay
		s := ""
		for _, x := range ws {
			if ok, next := kvStep(s, x.in, kvOut{Code: x.code}); ok {
				s = next
			}
			r.ts = append(r.ts, x.ts)
			r.states = append(r.states, s)
		}
		out[k] = r
	}
	return out
}

// diffCheck checks every diff scan, as an incremental backup takes one: a
// diff scan from S to E returns each key whose state in a snapshot at E
// differs from its state at S, with its state at E -- deleted, if absent --
// and no key whose state is the same, except one absent at both, inserted
// and deleted between, which it may return as deleted.
func diffCheck(hist []op) []anomaly {
	rs := replays(hist)
	var bad []anomaly
	for _, o := range hist {
		if o.Kind != "read" || o.Read == nil || o.Read.Mode != "diff" || o.NodeErr != "" || o.Code != "" {
			continue
		}
		s, e := o.Read.StartTS, o.Read.TS
		got := map[int64]row{}
		for _, x := range o.Rows {
			got[x.K] = x
		}
		for k, r := range rs {
			if !inRead(o.Read, k) {
				continue
			}
			before, after := r.at(s), r.at(e)
			x, returned := got[k]
			switch {
			case !returned && before != after:
				bad = append(bad, anomaly{Kind: "diff-missing", Key: ptr(k), Ops: []int{o.Index},
					Detail: fmt.Sprintf("a diff scan from %d to %d left out a key that went from %s to %s", s, e, show(before), show(after))})
			case returned && before == after && before != "":
				bad = append(bad, anomaly{Kind: "diff-extra", Key: ptr(k), Ops: []int{o.Index},
					Detail: fmt.Sprintf("a diff scan from %d to %d returned a key that held %s throughout", s, e, before)})
			case returned && x.Del && after != "":
				bad = append(bad, anomaly{Kind: "diff-wrong", Key: ptr(k), Ops: []int{o.Index},
					Detail: fmt.Sprintf("a diff scan from %d to %d returned as deleted a key that held %s at its end", s, e, after)})
			case returned && !x.Del && x.V != after:
				bad = append(bad, anomaly{Kind: "diff-wrong", Key: ptr(k), Ops: []int{o.Index},
					Detail: fmt.Sprintf("a diff scan from %d to %d returned %s for a key that held %s at its end", s, e, show(x.V), show(after))})
			}
		}
	}
	return bad
}

func show(v string) string {
	if v == "" {
		return "<absent>"
	}
	return v
}
