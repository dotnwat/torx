//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

const (
	ledger = 1
	// maxBatch bounds the events in one create_transfers batch: well under
	// what a replica started with --development takes in one request.
	maxBatch = 32
)

// bank is the workload's shared state: the accounts, the ids it hands out,
// and what the clients have learned so far -- pending transfers they saw
// created, which later events post or void, and events whose outcome is
// known, which later batches resubmit.
type bank struct {
	accounts []u128 // ids of the accounts created at the start
	missing  u128   // an account id never created
	idHi     uint64 // the high half of every transfer id this run hands out
	next     atomic.Uint64

	mu         sync.Mutex
	pendings   []pendingRef
	plain      []u128 // transfers created without flags.pending
	submitted  []tb.Transfer
	timestamps []uint64 // of transfers seen created, for queries' timestamp ranges
}

type pendingRef struct {
	id     u128
	amount u128
}

// accountFlags gives the workload's accounts a mix of the balance limits
// TigerBeetle enforces, so transfers fail on them often, and history on half.
func accountFlags(i int) uint16 {
	var f uint16
	switch i % 4 {
	case 1:
		f |= aDrNotExceedCr
	case 2:
		f |= aCrNotExceedDr
	}
	if i%2 == 1 {
		f |= aHistory
	}
	return f
}

func newBank(rng *rand.Rand, accounts int) *bank {
	b := &bank{idHi: rng.Uint64() | 1}
	for i := 1; i <= accounts; i++ {
		b.accounts = append(b.accounts, u(uint64(i)))
	}
	b.missing = u(uint64(accounts + 1))
	return b
}

// newAccounts are the tb.Accounts the run starts with.
func (b *bank) newAccounts() []tb.Account {
	var out []tb.Account
	for i, id := range b.accounts {
		out = append(out, tb.Account{ID: id.tb(), Ledger: ledger, Code: 1, Flags: accountFlags(i + 1)})
	}
	return out
}

func (b *bank) id() u128 { return u128{b.idHi, b.next.Add(1)} }

// learn takes in a create's answer: pending transfers created, for later
// posts and voids, and events with definite outcomes, for resubmission.
func (b *bank) learn(events []tb.Transfer, results []tb.CreateTransferResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, r := range results {
		e := events[i]
		if r.Status == tb.TransferCreated {
			if len(b.timestamps) < 100_000 {
				b.timestamps = append(b.timestamps, r.Timestamp)
			}
			switch {
			case e.Flags&fPending != 0:
				b.pendings = append(b.pendings, pendingRef{fromTB(e.ID), fromTB(e.Amount)})
			case e.Flags&(fPost|fVoid) == 0:
				b.plain = append(b.plain, fromTB(e.ID))
			}
		}
		if e.Flags&fLinked == 0 && len(b.submitted) < 100_000 {
			b.submitted = append(b.submitted, e)
		}
	}
}

// batch draws a create_transfers batch.
func (b *bank) batch(rng *rand.Rand, process int) []tb.Transfer {
	n := 1 + rng.IntN(maxBatch)
	if rng.IntN(2) == 0 {
		n = 1 + rng.IntN(4)
	}
	var out []tb.Transfer
	for len(out) < n {
		switch r := rng.IntN(100); {
		case r < 12:
			// A linked chain of two to four events, now and then left open.
			k := 2 + rng.IntN(3)
			for i := range k {
				t := b.event(rng, process, rng.IntN(88))
				if i < k-1 || rng.IntN(10) == 0 {
					t.Flags |= fLinked
				}
				out = append(out, t)
			}
		default:
			out = append(out, b.event(rng, process, r-12))
		}
	}
	// A chain left open ahead of other events runs on into them; one left
	// open at the batch's end fails as linked_event_chain_open.
	return out
}

// event draws one event from r in [0, 88).
func (b *bank) event(rng *rand.Rand, process int, r int) tb.Transfer {
	t := tb.Transfer{
		ID:         b.id().tb(),
		Ledger:     ledger,
		Code:       uint16(1 + rng.IntN(3)),
		UserData64: uint64(process),
		UserData32: uint32(rng.IntN(8)),
		Amount:     u(uint64(1 + rng.IntN(100))).tb(),
	}
	if rng.IntN(100) == 0 {
		t.Amount = u(0).tb()
	}
	dr, cr := b.pair(rng)
	t.DebitAccountID, t.CreditAccountID = dr.tb(), cr.tb()
	b.mu.Lock()
	pendings, plain := b.pendings, b.plain
	b.mu.Unlock()
	switch {
	case r < 38: // a plain transfer
	case r < 55: // a pending one, which may time out, or close an account until voided
		t.Flags |= fPending
		if rng.IntN(3) == 0 {
			t.Timeout = uint32(1 + rng.IntN(3))
		}
		// A closing transfer always times out, so the account it closes
		// opens again on its own if nothing voids it.
		switch rng.IntN(100) {
		case 0:
			t.Flags |= fClosingDr
			t.Timeout = uint32(1 + rng.IntN(3))
		case 1:
			t.Flags |= fClosingCr
			t.Timeout = uint32(1 + rng.IntN(3))
		}
	case r < 67 && len(pendings) > 0: // post a pending transfer
		p := pendings[rng.IntN(len(pendings))]
		t = b.resolve(rng, t, p, fPost)
	case r < 75 && len(pendings) > 0: // void one
		p := pendings[rng.IntN(len(pendings))]
		t = b.resolve(rng, t, p, fVoid)
	case r < 83: // balance an account's debits against its credits, or the reverse
		if rng.IntN(2) == 0 {
			t.Flags |= fBalancingDr
		} else {
			t.Flags |= fBalancingCr
		}
		if rng.IntN(2) == 0 {
			t.Amount = maxU128.tb()
		}
	default: // an event that must fail, each for its own reason
		switch rng.IntN(4) {
		case 0:
			t.CreditAccountID = t.DebitAccountID // accounts_must_be_different
		case 1:
			t.DebitAccountID = b.missing.tb() // debit_account_not_found, transient
		case 2:
			t.Flags |= fPost // pending_transfer_not_found, transient
			t.PendingID = b.id().tb()
			t.DebitAccountID, t.CreditAccountID = tb.Uint128{}, tb.Uint128{}
		case 3:
			if len(plain) > 0 { // pending_transfer_not_pending
				t.Flags |= fVoid
				t.PendingID = plain[rng.IntN(len(plain))].tb()
				t.DebitAccountID, t.CreditAccountID = tb.Uint128{}, tb.Uint128{}
				t.Amount = u(0).tb()
			}
		}
	}
	return t
}

// resolve turns t into a post or void of p, with the amount drawn around
// p's: all of it, part of it, or more than it.
func (b *bank) resolve(rng *rand.Rand, t tb.Transfer, p pendingRef, flag uint16) tb.Transfer {
	t.Flags |= flag
	t.PendingID = p.id.tb()
	t.DebitAccountID, t.CreditAccountID = tb.Uint128{}, tb.Uint128{}
	t.Code = 0
	if rng.IntN(4) == 0 {
		t.Ledger = 0
	}
	switch r := rng.IntN(20); {
	case flag == fVoid && r < 16:
		t.Amount = u(0).tb()
	case flag == fPost && r < 10:
		t.Amount = maxU128.tb()
	case r < 17:
		t.Amount = p.amount.tb()
	case r < 19 && !p.amount.isZero():
		t.Amount = u(1 + rng.Uint64N(p.amount.lo)).tb()
	default:
		t.Amount = p.amount.mustAdd(u(1)).tb()
	}
	return t
}

func (b *bank) pair(rng *rand.Rand) (u128, u128) {
	i := rng.IntN(len(b.accounts))
	j := rng.IntN(len(b.accounts) - 1)
	if j >= i {
		j++
	}
	return b.accounts[i], b.accounts[j]
}

// resubmission draws one to three events already submitted, resubmitted
// unchanged: each must come back exists, id_already_failed, or the error it
// had before.
func (b *bank) resubmission(rng *rand.Rand) []tb.Transfer {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.submitted) == 0 {
		return nil
	}
	n := 1 + rng.IntN(3)
	var out []tb.Transfer
	for range n {
		out = append(out, b.submitted[rng.IntN(len(b.submitted))])
	}
	return out
}

// lookupIDs draws transfer ids to look up: some submitted, some never.
func (b *bank) lookupIDs(rng *rand.Rand) []tb.Uint128 {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 1 + rng.IntN(16)
	var out []tb.Uint128
	for range n {
		if len(b.submitted) == 0 || rng.IntN(8) == 0 {
			out = append(out, u128{b.idHi, rng.Uint64()}.tb())
			continue
		}
		out = append(out, b.submitted[rng.IntN(len(b.submitted))].ID)
	}
	return out
}

// tsRange draws a timestamp range for a query: most often unbounded, else
// bounded on one side or both by timestamps of transfers seen created, give
// or take one.
func (b *bank) tsRange(rng *rand.Rand) (uint64, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pick := func() uint64 {
		if len(b.timestamps) == 0 {
			return 0
		}
		return b.timestamps[rng.IntN(len(b.timestamps))] + uint64(rng.IntN(3)) - 1
	}
	switch rng.IntN(6) {
	case 0:
		return pick(), 0
	case 1:
		return 0, pick()
	case 2:
		lo, hi := pick(), pick()
		return min(lo, hi), max(lo, hi)
	}
	return 0, 0
}

// query draws one of the four scans and its filter into op.
func (b *bank) query(rng *rand.Rand, op *Op) {
	limit := uint32(1 + rng.IntN(64))
	if rng.IntN(8) == 0 {
		limit = uint32(1 + rng.IntN(2000))
	}
	lo, hi := b.tsRange(rng)
	switch r := rng.IntN(100); {
	case r < 60:
		f := &tb.AccountFilter{
			AccountID:    b.accounts[rng.IntN(len(b.accounts))].tb(),
			TimestampMin: lo, TimestampMax: hi, Limit: limit,
		}
		switch rng.IntN(3) {
		case 0:
			f.Flags = tb.AccountFilterFlags{Debits: true}.ToUint32()
		case 1:
			f.Flags = tb.AccountFilterFlags{Credits: true}.ToUint32()
		default:
			f.Flags = tb.AccountFilterFlags{Debits: true, Credits: true}.ToUint32()
		}
		if rng.IntN(2) == 0 {
			f.Flags |= tb.AccountFilterFlags{Reversed: true}.ToUint32()
		}
		if rng.IntN(4) == 0 {
			f.Code = uint16(1 + rng.IntN(3))
		}
		if rng.IntN(4) == 0 {
			f.UserData32 = uint32(rng.IntN(8))
		}
		if rng.IntN(6) == 0 {
			f.UserData64 = uint64(rng.IntN(8))
		}
		op.AccountFilter = f
		op.F = "account-transfers"
		if r >= 35 {
			op.F = "account-balances"
		}
	case r < 85:
		f := &tb.QueryFilter{TimestampMin: lo, TimestampMax: hi, Limit: limit}
		switch rng.IntN(4) {
		case 0:
			f.Code = uint16(1 + rng.IntN(3))
		case 1:
			f.UserData32 = uint32(rng.IntN(8))
		case 2:
			f.UserData64 = uint64(rng.IntN(8))
			f.Ledger = ledger
		default:
			f.Code = uint16(1 + rng.IntN(3))
			f.UserData32 = uint32(rng.IntN(8))
		}
		if rng.IntN(2) == 0 {
			f.Flags = tb.QueryFilterFlags{Reversed: true}.ToUint32()
		}
		op.QueryFilter = f
		op.F = "query-transfers"
	default:
		op.ChangesFilter = &tb.ChangeEventsFilter{TimestampMin: lo, TimestampMax: hi, Limit: limit}
		op.F = "change-events"
	}
}

// allIDs are the ids of every account, and one never created.
func (b *bank) allIDs() []tb.Uint128 {
	var out []tb.Uint128
	for _, id := range b.accounts {
		out = append(out, id.tb())
	}
	return append(out, b.missing.tb())
}

// client is one process of the workload: a session of its own with the
// cluster, replaced when the cluster evicts it.
type client struct {
	name    string
	process int
	connect func() (tb.Client, error)
	mu      sync.Mutex
	c       tb.Client
	closed  bool
}

func (c *client) get() (tb.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, tb.ErrClientClosed
	}
	if c.c == nil {
		cl, err := c.connect()
		if err != nil {
			return nil, err
		}
		c.c = cl
	}
	return c.c, nil
}

// drop forgets an evicted session, so the next op registers a new one.
func (c *client) drop(cl tb.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.c == cl {
		c.c = nil
		go cl.Close()
	}
}

// Close ends the session; an op blocked on it returns ErrClientClosed.
func (c *client) Close() {
	c.mu.Lock()
	cl := c.c
	c.c, c.closed = nil, true
	c.mu.Unlock()
	if cl != nil {
		cl.Close()
	}
}

// run issues operations until ctx is done, recording each in h. A call to
// the cluster blocks until the cluster answers -- the client retries for as
// long as it takes -- so an op begun during a fault ends when the fault
// heals, and is definite then.
func (c *client) run(ctx context.Context, b *bank, h *History, rng *rand.Rand) {
	for ctx.Err() == nil {
		cl, err := c.get()
		if err != nil {
			if errors.Is(err, tb.ErrClientClosed) {
				return
			}
			continue
		}
		op := Op{Process: c.name, Start: h.Now()}
		switch r := rng.IntN(100); {
		case r < 62:
			op.F = "transfer"
			op.Events = b.batch(rng, c.process)
		case r < 67:
			op.F = "transfer"
			op.Events = b.resubmission(rng)
			if op.Events == nil {
				continue
			}
		case r < 82:
			op.F = "accounts"
			op.IDs = b.allIDs()
		case r < 90:
			op.F = "lookup"
			op.IDs = b.lookupIDs(rng)
		default:
			b.query(rng, &op)
		}
		switch op.F {
		case "transfer":
			op.Results, err = cl.CreateTransfers(op.Events)
			if err == nil && len(op.Results) != len(op.Events) {
				err = fmt.Errorf("%d results for %d events", len(op.Results), len(op.Events))
			}
		case "accounts":
			op.Accounts, err = cl.LookupAccounts(op.IDs)
		case "lookup":
			op.Transfers, err = cl.LookupTransfers(op.IDs)
		case "account-transfers":
			op.Transfers, err = cl.GetAccountTransfers(*op.AccountFilter)
		case "account-balances":
			op.Balances, err = cl.GetAccountBalances(*op.AccountFilter)
		case "query-transfers":
			op.Transfers, err = cl.QueryTransfers(*op.QueryFilter)
		case "change-events":
			op.Changes, err = cl.GetChangeEvents(*op.ChangesFilter)
		}
		op.End = h.Now()
		op.Outcome = Ok
		if err != nil {
			op.Outcome, op.Err, op.Results = Info, err.Error(), nil
			if errors.Is(err, tb.ErrClientEvicted) {
				c.drop(cl)
			}
		}
		h.Add(op)
		if op.F == "transfer" && op.Outcome == Ok {
			b.learn(op.Events, op.Results)
		}
	}
}
