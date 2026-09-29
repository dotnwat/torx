//go:build unix

package main

import (
	"context"
	"fmt"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/tigerbeetle/qa/tigerbeetle"
	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// smokeJob is tigerbeetle.smoke: a cluster of three replicas, two accounts,
// and one transfer between them, read back. It is the smallest job that
// proves the binary runs, the replicas form a cluster, and the client
// reaches it.
type smokeJob struct {
	torx.JobBase
	db *tigerbeetle.Service
}

func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.db = tigerbeetle.New(serviceName, 3)
	j.db.SetFlags(smallCache...)
	jc.Register(j.db)
}

func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	c, err := j.db.NewClient()
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.CreateAccounts([]tb.Account{
		{ID: tb.ToUint128(1), Ledger: 1, Code: 1},
		{ID: tb.ToUint128(2), Ledger: 1, Code: 1},
	})
	if err != nil {
		return err
	}
	for i, r := range res {
		if r.Status != tb.AccountCreated {
			return fmt.Errorf("account %d: %v", i+1, r.Status)
		}
	}
	tr, err := c.CreateTransfers([]tb.Transfer{
		{ID: tb.ToUint128(10), DebitAccountID: tb.ToUint128(1), CreditAccountID: tb.ToUint128(2), Amount: tb.ToUint128(7), Ledger: 1, Code: 1},
	})
	if err != nil {
		return err
	}
	if tr[0].Status != tb.TransferCreated {
		return fmt.Errorf("transfer: %v", tr[0].Status)
	}
	accts, err := c.LookupAccounts([]tb.Uint128{tb.ToUint128(1), tb.ToUint128(2)})
	if err != nil {
		return err
	}
	if len(accts) != 2 {
		return fmt.Errorf("looked up %d accounts, want 2", len(accts))
	}
	if d, cr := accts[0].DebitsPosted.BigInt(), accts[1].CreditsPosted.BigInt(); d.Int64() != 7 || cr.Int64() != 7 {
		return fmt.Errorf("balances: debits_posted=%v credits_posted=%v, want 7 and 7", &d, &cr)
	}
	jc.SetSummary(fmt.Sprintf("3 replicas up; transfer committed at timestamp %d", tr[0].Timestamp))
	return nil
}

// smallCache sizes a replica's caches for a machine that runs several
// replicas beside each other: a release build's default grid cache is a
// gigabyte per replica.
var smallCache = []string{"--cache-grid=256MiB"}
