//go:build unix

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/dotnwat/torx/examples/slatedb/qa/slatedb"
	"github.com/dotnwat/torx/listappend"
)

// With merges, clients append to keys of their own through an append
// merge operator -- a blind append, which reads nothing -- and read them
// back from the writer, durable data only. Each append and each read is a
// transaction of one operation to listappend, with realtime order: a read
// must see every append acknowledged durable before it began, in one order
// all reads agree on.

func mergeKey(i int) string { return fmt.Sprintf("m%02d", i) }

const mergeKeys = 8

// merge appends a unique element to a merge key, or reads one.
func (j *chaosJob) merge(ctx context.Context, c *slatedb.Client, w string, id, n int, rng *rand.Rand) {
	k := mergeKey(rng.IntN(mergeKeys))
	rec := txnRecord{Client: id, Proc: w, Call: time.Now()}
	if rng.IntN(2) == 0 {
		v := fmt.Sprintf("%d.%d", id, n)
		rec.Ops = []slatedb.TxnOp{{Op: "append", Key: k, Value: v}}
		_, err := c.Merge(ctx, k, v, true)
		rec.Return = time.Now()
		rec.Status, rec.Err = outcome(err)
	} else {
		rec.Ops = []slatedb.TxnOp{{Op: "get", Key: k}}
		v, err := c.Get(ctx, k, slatedb.Remote)
		rec.Return = time.Now()
		rec.Status, rec.Err = outcome(err)
		if err == nil {
			if v.Found {
				s := v.Value
				rec.Results = []*string{&s}
			} else {
				rec.Results = []*string{nil}
			}
		}
	}
	j.mu.Lock()
	rec.ID = len(j.mergeLog)
	j.mergeLog = append(j.mergeLog, rec)
	j.mu.Unlock()
}

// outcome is a status for an operation's error: a definite failure aborts
// it, any other failure leaves it in doubt.
func outcome(err error) (listappend.Status, string) {
	switch {
	case err == nil:
		return listappend.Committed, ""
	case slatedb.Definite(err) || refused(err):
		return listappend.Aborted, err.Error()
	default:
		return listappend.Unknown, err.Error()
	}
}

// finalMerges reads every merge key from the writer.
func (j *chaosJob) finalMerges(ctx context.Context) error {
	if !j.merges {
		return nil
	}
	w := j.writer()
	c := slatedb.NewClient(j.db.URL(w))
	defer c.CloseIdle()
	for i := range mergeKeys {
		k := mergeKey(i)
		for {
			rec := txnRecord{Client: finalClient, Proc: w, Call: time.Now(), Ops: []slatedb.TxnOp{{Op: "get", Key: k}}}
			octx, cancel := context.WithTimeout(ctx, opTimeout)
			v, err := c.Get(octx, k, slatedb.Remote)
			cancel()
			rec.Return = time.Now()
			rec.Status, rec.Err = outcome(err)
			if err == nil {
				if v.Found {
					s := v.Value
					rec.Results = []*string{&s}
				} else {
					rec.Results = []*string{nil}
				}
			}
			j.mu.Lock()
			rec.ID = len(j.mergeLog)
			j.mergeLog = append(j.mergeLog, rec)
			j.mu.Unlock()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return fmt.Errorf("reading %s: %w", k, err)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}

// checkMerges checks the merges and the reads of merged keys.
func (j *chaosJob) checkMerges() []anomaly {
	j.mu.Lock()
	recs := slices.Clone(j.mergeLog)
	j.mu.Unlock()
	return checkMerges(recs)
}

func checkMerges(recs []txnRecord) []anomaly {
	var txns []listappend.Txn
	for _, r := range recs {
		t := listappend.Txn{ID: r.ID, Process: r.Client, Call: r.Call.UnixNano(), Return: r.Return.UnixNano(), Status: r.Status}
		op := r.Ops[0]
		switch op.Op {
		case "append":
			t.Mops = []listappend.Mop{{Append: true, Key: op.Key, Value: op.Value}}
		case "get":
			var read []string
			if r.Status == listappend.Committed && len(r.Results) > 0 {
				read = parseList(r.Results[0])
			}
			t.Mops = []listappend.Mop{{Key: op.Key, Read: read}}
		}
		txns = append(txns, t)
	}
	var out []anomaly
	for _, a := range listappend.Check(txns, listappend.Options{Realtime: true}) {
		out = append(out, anomaly{Kind: "merge-" + a.Kind, Key: a.Key, Detail: a.Detail, Ops: a.Txns})
	}
	return out
}
