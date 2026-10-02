//go:build unix

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/slatedb/qa/slatedb"
	"github.com/dotnwat/torx/listappend"
)

// Transactions run over keys of their own, each holding a list: a list
// is its elements with a space between. A transaction reads keys and
// appends to them -- an append reads the key and writes back what it read
// with the element added -- and the history is checked for the anomalies
// its isolation level forbids (listappend).

func txnKey(i int) string { return fmt.Sprintf("t%02d", i) }

func parseList(s *string) []string {
	if s == nil || *s == "" {
		return nil
	}
	return strings.Fields(*s)
}

// txn runs one transaction of 1 to 4 reads and appends.
func (j *chaosJob) txn(ctx context.Context, c *slatedb.Client, proc string, id, n int, rng *rand.Rand) {
	var ops []slatedb.TxnOp
	for i := range 1 + rng.IntN(4) {
		k := txnKey(rng.IntN(j.txnKeys))
		if rng.IntN(2) == 0 {
			ops = append(ops, slatedb.TxnOp{Op: "get", Key: k})
		} else {
			ops = append(ops, slatedb.TxnOp{Op: "append", Key: k, Value: fmt.Sprintf("%d.%d.%d", id, n, i)})
		}
	}
	j.runTxn(ctx, c, proc, id, ops)
}

// runTxn runs a transaction and records it.
func (j *chaosJob) runTxn(ctx context.Context, c *slatedb.Client, proc string, id int, ops []slatedb.TxnOp) txnRecord {
	rec := txnRecord{Client: id, Proc: proc, Ops: ops, Call: time.Now()}
	results, _, err := c.Txn(ctx, j.isolation, ops, true)
	rec.Return = time.Now()
	switch {
	case err == nil:
		rec.Status = listappend.Committed
		rec.Results = results
	case slatedb.Definite(err) || refused(err):
		// SlateDB refused it: a conflict, or a writer that could not take
		// it.
		rec.Status = listappend.Aborted
	default:
		rec.Status = listappend.Unknown
	}
	if err != nil {
		rec.Err = err.Error()
	}
	j.mu.Lock()
	rec.ID = len(j.txnLog)
	j.txnLog = append(j.txnLog, rec)
	j.mu.Unlock()
	return rec
}

// txnRecord is a transaction as the client saw it.
type txnRecord struct {
	ID      int               `json:"id"`
	Client  int               `json:"client"`
	Proc    string            `json:"proc"`
	Call    time.Time         `json:"call"`
	Return  time.Time         `json:"return"`
	Status  listappend.Status `json:"status"`
	Ops     []slatedb.TxnOp   `json:"ops"`
	Results []*string         `json:"results,omitempty"`
	Err     string            `json:"error,omitempty"`
}

// finalClient is the client of the final reads, made once every fault is
// healed.
const finalClient = -1

// toTxn makes a record a transaction to check. SlateDB's transactions read
// what the writer holds in memory, durable or not (slatedb#1147), so a
// transaction that only reads may read a write the writer then loses, in
// a crash or to a writer that fences it; one that also writes and commits
// durably made what it read durable too, since a write is durable only
// once every write before it is. So the reads of a transaction that only
// reads are not checked -- it counts as one whose outcome is unknown --
// but for the final reads, which no fault follows.
func toTxn(rec txnRecord) listappend.Txn {
	t := listappend.Txn{ID: rec.ID, Process: rec.Client, Call: rec.Call.UnixNano(), Return: rec.Return.UnixNano(), Status: rec.Status}
	writes := slices.ContainsFunc(rec.Ops, func(op slatedb.TxnOp) bool { return op.Op != "get" })
	if t.Status == listappend.Committed && !writes && rec.Client != finalClient {
		t.Status = listappend.Unknown
	}
	for i, op := range rec.Ops {
		var read []string
		if t.Status == listappend.Committed && i < len(rec.Results) {
			read = parseList(rec.Results[i])
		}
		switch op.Op {
		case "get":
			t.Mops = append(t.Mops, listappend.Mop{Key: op.Key, Read: read})
		case "append":
			// An append reads the list, then writes it back longer.
			t.Mops = append(t.Mops, listappend.Mop{Key: op.Key, Read: read},
				listappend.Mop{Append: true, Key: op.Key, Value: op.Value})
		}
	}
	return t
}

// finalTxn reads every transaction key in one transaction, retrying until
// it commits.
func (j *chaosJob) finalTxn(ctx context.Context) error {
	if j.isolation == "" {
		return nil
	}
	c := slatedb.NewClient(j.db.URL(j.writer()))
	defer c.CloseIdle()
	var ops []slatedb.TxnOp
	for i := range j.txnKeys {
		ops = append(ops, slatedb.TxnOp{Op: "get", Key: txnKey(i)})
	}
	for {
		octx, cancel := context.WithTimeout(ctx, opTimeout)
		t := j.runTxn(octx, c, j.writer(), finalClient, ops)
		cancel()
		if t.Status == listappend.Committed {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(time.Second)
	}
}

// checkTxns checks the transactions for the anomalies their level forbids:
// SSI is serializable, and SI snapshot isolation; at both, a transaction
// sees every one committed before it began.
func (j *chaosJob) checkTxns() (forbidden, allowed []anomaly) {
	j.mu.Lock()
	recs := slices.Clone(j.txnLog)
	j.mu.Unlock()
	return checkTxns(recs, j.isolation)
}

func checkTxns(recs []txnRecord, isolation string) (forbidden, allowed []anomaly) {
	if len(recs) == 0 {
		return nil, nil
	}
	var txns []listappend.Txn
	for _, r := range recs {
		txns = append(txns, toTxn(r))
	}
	level := "serializable"
	if isolation == "si" {
		level = "snapshot-isolation"
	}
	bad := append(listappend.Forbidden(level), "lost")
	for _, a := range listappend.Check(txns, listappend.Options{Realtime: true}) {
		x := anomaly{Kind: "txn-" + a.Kind, Key: a.Key, Detail: a.Detail, Ops: a.Txns}
		if slices.Contains(bad, a.Kind) {
			forbidden = append(forbidden, x)
		} else {
			allowed = append(allowed, x)
		}
	}
	return forbidden, allowed
}
