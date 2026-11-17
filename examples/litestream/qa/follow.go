//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/litestream/model"
	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
	"github.com/dotnwat/torx/objstore"
)

// followRegressJob is litestream.follow-regress. A follower (`litestream
// restore -f`) applies the LTX files the replica gains, one at a time, each
// over the database it keeps. When one of a poll's files cannot be read,
// the follower keeps the TXID it began the poll at, though the files before
// that one are applied; its next poll applies them again, over the newer
// pages. If that poll fails too, the follower's database is left with the
// first file's pages over the second's: the transactions the second
// committed are gone from the pages both wrote, and still in the pages only
// the second wrote.
//
// The object store fails the follower's reads of the files the job names,
// as a transient error -- or retention deleting a file between the
// follower's listing and its read -- would.
type followRegressJob struct{ reproJob }

func (r *followRegressJob) Declare(jc *torx.JobContext) {
	r.reproJob.Declare(jc)
	r.follow = true
}

// ltxKey is the replica's object of level-0 TXID txid.
func ltxKey(txid uint64) string {
	return fmt.Sprintf("%s/0000/%016x-%016x.ltx", replicaPath, txid, txid)
}

// failRead fails every read of the follower's of txid's file, until the
// heal.
func (r *followRegressJob) failRead(txid uint64) func() {
	id := r.store.AddRule(objstore.Rule{
		Name: fmt.Sprintf("fail-%x", txid), Clients: []string{"follow1"},
		Ops: []objstore.Op{objstore.OpGet}, Prefix: ltxKey(txid), Prob: 1, Action: objstore.Fail,
	})
	return func() { r.store.RemoveRule(id) }
}

var consecutiveErrors = regexp.MustCompile(`consecutive_errors=(\d+)`)

// waitErrors waits until the follower has logged its nth error in a row.
func (r *followRegressJob) waitErrors(ctx context.Context, n int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := r.svc.Tail(ctx, followName(1))
		if err != nil {
			return err
		}
		ms := consecutiveErrors.FindAllStringSubmatch(string(b), -1)
		if len(ms) > 0 {
			if k, _ := strconv.Atoi(ms[len(ms)-1][1]); k >= n {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the follower did not log error %d in a row within 30s", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// sidecar is the TXID the follower says it has applied.
func (r *followRegressJob) sidecar(ctx context.Context) string {
	n := r.svc.Nodes()[1]
	b, err := n.ReadFile(ctx, r.followDB(n)+"-txid")
	if err != nil {
		return "none"
	}
	return strings.TrimSpace(string(b))
}

func (r *followRegressJob) Run(ctx context.Context, jc *torx.JobContext) error {
	defer r.writeRecord()
	if _, err := r.commit(ctx, put(1, 0, 1, 100)); err != nil {
		return err
	}
	a, err := r.syncUntil(ctx)
	if err != nil {
		return err
	}
	t := a.ReplicaTXID
	r.ls.FollowInterval = 200 * time.Millisecond
	if err := r.startFollower(ctx, 1); err != nil {
		return err
	}
	// The follower restores txid t, then follows; hold it there.
	healT1 := r.failRead(t + 1)
	deadline := time.Now().Add(30 * time.Second)
	for r.sidecar(ctx) == "none" {
		if time.Now().After(deadline) {
			return errors.New("the follower wrote no txid within 30s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	jc.Log("info", fmt.Sprintf("the follower restored txid %d (its sidecar says %s)", t, r.sidecar(ctx)))

	// t+1 and t+2 both write t0's row 1, and the txlog; t+2 also writes a
	// row of t1, which t+1 does not touch. t+3 writes t2.
	txns := []model.Txn{
		{UID: 2, Ops: []model.Op{{Kind: model.Put, Table: 0, Key: 1, Size: 100}}},
		{UID: 3, Ops: []model.Op{{Kind: model.Put, Table: 0, Key: 1, Size: 100}, {Kind: model.Put, Table: 1, Key: 7, Size: 100}}},
		{UID: 4, Ops: []model.Op{{Kind: model.Put, Table: 2, Key: 1, Size: 100}}},
	}
	for i, txn := range txns {
		if _, err := r.commit(ctx, txn); err != nil {
			return err
		}
		if a, err := r.syncUntil(ctx); err != nil {
			return err
		} else if a.ReplicaTXID != t+uint64(i)+1 {
			return fmt.Errorf("transaction %d went to txid %d, not %d", txn.UID, a.ReplicaTXID, t+uint64(i)+1)
		}
	}
	read := func(what string) (*obsRec, error) {
		n := r.svc.Nodes()[1]
		o := r.beginObs(obsFollow, n, litestream.Target{})
		err := r.read(ctx, n, o, r.followDB(n), true)
		r.endObs(o, err)
		if err != nil {
			return o, err
		}
		r.logReport(fmt.Sprintf("%s (sidecar %s)", what, r.sidecar(ctx)), o)
		return o, nil
	}

	// One poll applies t+1 and t+2, and fails on t+3.
	healT3 := r.failRead(t + 3)
	healT1()
	if err := r.waitErrors(ctx, 1); err != nil {
		return err
	}
	o1, err := read("after a poll that applied t+1 and t+2 and failed on t+3, the follower")
	if err != nil {
		return err
	}
	// The next applies t+1 again, and fails on t+2.
	healT2 := r.failRead(t + 2)
	if err := r.waitErrors(ctx, 2); err != nil {
		return err
	}
	o2, err := read("after a poll that failed on t+2, the follower")
	if err != nil {
		return err
	}
	healT2()
	healT3()

	c := newChecker(r.txns, r.specs, nil, []*obsRec{o1, o2}, r.bases, 0, nil, nil, nil)
	found, _ := c.check()
	var errs []error
	if len(o2.Report.TxLog) < len(o1.Report.TxLog) {
		errs = append(errs, fmt.Errorf("the follower went back from %d transactions to %d", len(o1.Report.TxLog), len(o2.Report.TxLog)))
	}
	for _, a := range found {
		errs = append(errs, fmt.Errorf("%s: %s", a.Kind, a.Detail))
	}
	return errors.Join(errs...)
}
