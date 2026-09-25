//go:build unix

package main

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// Anomaly is something a checker found wrong with a history. An error-level
// anomaly is a guarantee rqlite makes that the history shows broken, and it
// fails the job; a warning is behavior worth a look that rqlite does not
// promise against, such as a weak read that trailed the leader it was sent to.
type Anomaly struct {
	Kind     string  `json:"kind"`
	Severity string  `json:"severity"` // "error" or "warn"
	Detail   string  `json:"detail"`
	Op       *Op     `json:"op,omitempty"`
	Values   []int64 `json:"values,omitempty"`
}

const (
	sevError = "error"
	sevWarn  = "warn"
)

// maxValuesShown bounds the values an anomaly lists; the count is always
// given in full.
const maxValuesShown = 20

// checkSet checks a history of the set workload: clients add unique values to
// a table, and read back the values above a lower bound. final is the table's
// content read at the strong level once every fault was healed. It returns
// every anomaly it finds:
//
//   - lost: a value whose add was acknowledged is missing from final.
//   - stale-read: a read at the linearizable or strong level missed a value
//     known to be committed before the read began. A value is known committed
//     from the moment its add was acknowledged or from the end of the first
//     read of any level that saw it: a node's database holds only entries
//     Raft has committed, so any read that sees a value proves it committed,
//     and a linearizable read must reflect everything committed before it.
//     The same miss by a weak read is a warning, since rqlite documents that
//     a leader deposed moments ago may still serve one.
//   - stale-fresh-read: a none read bounded by freshness in strict mode
//     (modeFresh) missed a value known committed more than readFreshness
//     before it began. rqlite documents that such a read is refused rather
//     than served from data out of date by more than the freshness.
//   - duplicate: a value appears more than once. Every add writes a distinct
//     value, once; the table does not enforce it, so a write applied twice
//     shows.
//   - phantom: a value no client ever wrote appears.
//   - resurrected: a value whose add definitely failed appears.
func checkSet(ops []Op, final []int64) []Anomaly {
	adds := map[int64]Op{}
	var reads []Op
	for _, op := range ops {
		switch op.F {
		case "add":
			adds[op.Value] = op
		case "read":
			if op.Outcome == Ok {
				reads = append(reads, op)
			}
		}
	}

	// visible is when each value became known committed.
	visible := map[int64]time.Duration{}
	see := func(v int64, at time.Duration) {
		if t, ok := visible[v]; !ok || at < t {
			visible[v] = at
		}
	}
	for v, op := range adds {
		if op.Outcome == Ok {
			see(v, op.End)
		}
	}
	for _, r := range reads {
		for _, v := range r.Values {
			see(v, r.End)
		}
	}
	known := slices.Sorted(maps.Keys(visible))

	var out []Anomaly
	reported := map[string]map[int64]bool{} // kind -> values already reported
	firstReport := func(kind string, vs []int64) []int64 {
		if reported[kind] == nil {
			reported[kind] = map[int64]bool{}
		}
		var fresh []int64
		for _, v := range vs {
			if !reported[kind][v] {
				reported[kind][v] = true
				fresh = append(fresh, v)
			}
		}
		return fresh
	}
	// contentAnomalies checks what a set of values holds that it must not.
	contentAnomalies := func(where string, op *Op, values []int64) {
		counts := map[int64]int{}
		for _, v := range values {
			counts[v]++
		}
		var dups, phantoms, resurrected []int64
		for _, v := range slices.Sorted(maps.Keys(counts)) {
			if counts[v] > 1 {
				dups = append(dups, v)
			}
			add, ok := adds[v]
			switch {
			case !ok:
				phantoms = append(phantoms, v)
			case add.Outcome == Fail:
				resurrected = append(resurrected, v)
			}
		}
		if vs := firstReport("duplicate", dups); len(vs) > 0 {
			out = append(out, anomaly("duplicate", sevError, op, vs,
				"%s holds %d value(s) more than once; each was written once", where, len(vs)))
		}
		if vs := firstReport("phantom", phantoms); len(vs) > 0 {
			out = append(out, anomaly("phantom", sevError, op, vs,
				"%s holds %d value(s) no client wrote", where, len(vs)))
		}
		if vs := firstReport("resurrected", resurrected); len(vs) > 0 {
			out = append(out, anomaly("resurrected", sevError, op, vs,
				"%s holds %d value(s) whose add definitely failed", where, len(vs)))
		}
	}

	for i := range reads {
		r := &reads[i]
		contentAnomalies(fmt.Sprintf("a %s read at %s", r.Mode, r.Node), r, r.Values)
		// A read must see what was committed before it began, less its
		// allowance: a freshness read may trail by its freshness.
		var allowance time.Duration
		kind, sev := "stale-read", sevError
		switch r.Mode {
		case rqlite.LevelNone:
			continue // a none read promises nothing about currency
		case rqlite.LevelWeak:
			kind, sev = "weak-stale-read", sevWarn
		case modeFresh:
			kind, allowance = "stale-fresh-read", readFreshness
		}
		have := map[int64]bool{}
		for _, v := range r.Values {
			have[v] = true
		}
		var missing []int64
		for _, v := range known[upperBound(known, r.Lower):] {
			if visible[v] < r.Start-allowance && !have[v] {
				missing = append(missing, v)
			}
		}
		if len(missing) == 0 {
			continue
		}
		first := missing[0]
		out = append(out, anomaly(kind, sev, r, missing,
			"a %s read at %s began %s after value %d was known committed, and it and %d other value(s) were missing",
			r.Mode, r.Node, (r.Start-visible[first]).Round(time.Millisecond), first, len(missing)-1))
	}

	contentAnomalies("the final read", nil, final)
	inFinal := map[int64]bool{}
	for _, v := range final {
		inFinal[v] = true
	}
	var lost []int64
	for _, v := range slices.Sorted(maps.Keys(adds)) {
		if adds[v].Outcome == Ok && !inFinal[v] {
			lost = append(lost, v)
		}
	}
	if len(lost) > 0 {
		out = append(out, anomaly("lost", sevError, nil, lost,
			"%d acknowledged value(s) are missing from the final read", len(lost)))
	}
	return out
}

// upperBound returns the index of the first value in sorted greater than x.
func upperBound(sorted []int64, x int64) int {
	i, found := slices.BinarySearch(sorted, x)
	if found {
		i++
	}
	return i
}

// anomaly builds an Anomaly, listing at most maxValuesShown of vs.
func anomaly(kind, sev string, op *Op, vs []int64, format string, args ...any) Anomaly {
	a := Anomaly{Kind: kind, Severity: sev, Detail: fmt.Sprintf(format, args...)}
	if op != nil {
		c := *op
		if len(c.Values) > maxValuesShown {
			c.Values = nil // the read's own values are in the history
		}
		a.Op = &c
	}
	if len(vs) > maxValuesShown {
		vs = vs[:maxValuesShown]
	}
	a.Values = vs
	return a
}
