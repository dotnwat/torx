//go:build unix

package main

import (
	"fmt"
	"slices"
)

// txnInfo is a transaction as the history tells it: the rows it inserted,
// and how it ended.
type txnInfo struct {
	id     string
	client int
	ops    []int // the history's operations that made it up
	rows   map[int64]string
	// outcome is committed, aborted, or unknown.
	outcome string
	// visibleBy is the client's propagated timestamp after its commit: a
	// snapshot past it must hold the transaction.
	visibleBy int64
}

// transactions gathers the history's transactions. A transaction is
// committed if its commit was acknowledged; aborted if it was rolled back,
// never committed, or its commit failed as aborted; and unknown if its
// commit's outcome is.
func transactions(hist []op) map[string]*txnInfo {
	txns := map[string]*txnInfo{}
	for _, o := range hist {
		switch o.Kind {
		case "begin":
			if o.Txn != "" {
				txns[o.Txn] = &txnInfo{id: o.Txn, client: o.Client, ops: []int{o.Index}, rows: map[int64]string{}, outcome: "aborted"}
			}
		case "write":
			if t := txns[o.Txn]; t != nil {
				t.ops = append(t.ops, o.Index)
				for _, w := range o.Ops {
					t.rows[w.K] = w.V
				}
			}
		case "commit":
			if t := txns[o.Txn]; t != nil {
				t.ops = append(t.ops, o.Index)
				switch {
				case o.NodeErr == "" && o.Code == "" && o.Error == "":
					t.outcome, t.visibleBy = "committed", o.PropAfter
				case o.Code == "Aborted":
				default:
					t.outcome = "unknown"
				}
			}
		case "rollback":
			if t := txns[o.Txn]; t != nil {
				t.ops = append(t.ops, o.Index)
			}
		}
	}
	return txns
}

// txnCheck checks every snapshot read of the transactions' keys: a
// snapshot holds all of a transaction's rows or none, never an aborted
// one's, and a committed one's past the timestamp its commit propagated;
// once a snapshot holds a transaction, every later snapshot does. final
// is the read after every fault healed, which must hold every committed
// transaction.
func txnCheck(hist []op, final int) []anomaly {
	txns := transactions(hist)
	owner := map[int64]*txnInfo{}
	for _, t := range txns {
		for k := range t.rows {
			owner[k] = t
		}
	}
	type sighting struct {
		ts      int64
		op      int
		visible bool
	}
	seen := map[string][]sighting{}
	var bad []anomaly
	for _, o := range hist {
		if o.Kind != "read" || o.Read == nil || o.Read.Mode != "snapshot" || o.NodeErr != "" || o.Code != "" || o.SnapshotTS <= 0 {
			continue
		}
		lo, hi := int64(0), keyLimit
		if o.Read.Lo != nil {
			lo = *o.Read.Lo
		}
		if o.Read.Hi != nil {
			hi = *o.Read.Hi
		}
		if hi <= txnBase || lo >= keyLimit || len(o.Read.Keys) > 0 {
			continue
		}
		got := map[*txnInfo]map[int64]string{}
		for _, r := range o.Rows {
			if !isTxnKey(r.K) {
				continue
			}
			t := owner[r.K]
			if t == nil {
				bad = append(bad, anomaly{Kind: "txn-phantom", Key: ptr(r.K), Ops: []int{o.Index},
					Detail: fmt.Sprintf("a snapshot held %s, which no transaction inserted", r.V)})
				continue
			}
			if got[t] == nil {
				got[t] = map[int64]string{}
			}
			got[t][r.K] = r.V
		}
		for _, t := range txns {
			rows := got[t]
			for k, v := range rows {
				if v != t.rows[k] {
					bad = append(bad, anomaly{Kind: "txn-wrong-value", Key: ptr(k), Txn: t.id, Ops: append(slices.Clone(t.ops), o.Index),
						Detail: fmt.Sprintf("a snapshot held %s where the transaction inserted %s", v, t.rows[k])})
				}
			}
			switch {
			case len(rows) > 0 && t.outcome == "aborted":
				bad = append(bad, anomaly{Kind: "txn-aborted-read", Txn: t.id, Ops: append(slices.Clone(t.ops), o.Index),
					Detail: fmt.Sprintf("a snapshot at %d held %d rows of a transaction that did not commit", o.SnapshotTS, len(rows))})
			case len(rows) > 0 && len(rows) < len(t.rows):
				bad = append(bad, anomaly{Kind: "txn-fractured", Txn: t.id, Ops: append(slices.Clone(t.ops), o.Index),
					Detail: fmt.Sprintf("a snapshot at %d held %d of the transaction's %d rows", o.SnapshotTS, len(rows), len(t.rows))})
			case len(rows) == 0 && t.outcome == "committed" && t.visibleBy > 0 && o.SnapshotTS > t.visibleBy:
				bad = append(bad, anomaly{Kind: "txn-missing", Txn: t.id, Ops: append(slices.Clone(t.ops), o.Index),
					Detail: fmt.Sprintf("a snapshot at %d held none of a transaction whose commit propagated %d", o.SnapshotTS, t.visibleBy)})
			case len(rows) == 0 && t.outcome == "committed" && o.Index == final:
				bad = append(bad, anomaly{Kind: "txn-lost", Txn: t.id, Ops: append(slices.Clone(t.ops), o.Index),
					Detail: "the final read held none of a committed transaction"})
			}
			if t.outcome != "aborted" {
				seen[t.id] = append(seen[t.id], sighting{ts: o.SnapshotTS, op: o.Index, visible: len(rows) > 0})
			}
		}
	}
	for id, ss := range seen {
		slices.SortFunc(ss, func(a, b sighting) int { return compare(a.ts, b.ts) })
		var lastVisible *sighting
		for i := range ss {
			s := ss[i]
			if s.visible {
				lastVisible = &ss[i]
				continue
			}
			if lastVisible != nil && lastVisible.ts < s.ts {
				bad = append(bad, anomaly{Kind: "txn-unseen", Txn: id, Ops: []int{lastVisible.op, s.op},
					Detail: fmt.Sprintf("a snapshot at %d held the transaction and a later one at %d did not", lastVisible.ts, s.ts)})
				break
			}
		}
	}
	return bad
}

func compare(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
