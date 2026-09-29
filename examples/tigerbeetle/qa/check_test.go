//go:build unix

package main

import (
	"testing"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// newTestModel is a model with accounts 1..4: 1 may not debit past its
// credits, 2 may not credit past its debits, 3 and 4 are unlimited.
func newTestModel() *ledgerModel {
	var accounts []acct
	weights := map[u128][4]uint64{}
	for i := uint64(1); i <= 4; i++ {
		a := acct{id: u(i), ledger: 1, code: 1, timestamp: i}
		switch i {
		case 1:
			a.flags = aDrNotExceedCr
		case 2:
			a.flags = aCrNotExceedDr
		}
		accounts = append(accounts, a)
		weights[a.id] = [4]uint64{i, i * 3, i * 5, i * 7}
	}
	return newLedgerModel(accounts, weights)
}

func tr(id, dr, cr, amount uint64, flags uint16) xfer {
	return xfer{id: u(id), debit: u(dr), credit: u(cr), amount: u(amount), ledger: 1, code: 1, flags: flags}
}

func statuses(rs []result) []status {
	out := make([]status, len(rs))
	for i, r := range rs {
		out[i] = r.status
	}
	return out
}

func wantStatuses(t *testing.T, got []result, want ...status) {
	t.Helper()
	g := statuses(got)
	if len(g) != len(want) {
		t.Fatalf("got %v, want %v", g, want)
	}
	for i := range g {
		if g[i] != want[i] {
			t.Fatalf("event %d: got %v, want %v (all: %v)", i, g[i], want[i], g)
		}
	}
}

func TestModelLimitsAndBurnedIDs(t *testing.T) {
	m := newTestModel()
	// Account 1 has no credits to debit against: a transient failure that
	// burns the id.
	wantStatuses(t, m.applyBatch([]xfer{tr(10, 1, 3, 5, 0)}, 100), tb.TransferExceedsCredits)
	wantStatuses(t, m.applyBatch([]xfer{tr(10, 1, 3, 5, 0)}, 200), tb.TransferIDAlreadyFailed)
	// Credit it first, then the debit fits; resubmitting it says exists
	// with its original timestamp.
	rs := m.applyBatch([]xfer{tr(11, 3, 1, 7, 0), tr(12, 1, 3, 5, 0)}, 302)
	wantStatuses(t, rs, tb.TransferCreated, tb.TransferCreated)
	if rs[0].timestamp != 301 || rs[1].timestamp != 302 {
		t.Fatalf("timestamps %d %d, want 301 302", rs[0].timestamp, rs[1].timestamp)
	}
	rs = m.applyBatch([]xfer{tr(12, 1, 3, 5, 0)}, 400)
	wantStatuses(t, rs, tb.TransferExists)
	if rs[0].timestamp != 302 {
		t.Fatalf("exists carries timestamp %d, want the original 302", rs[0].timestamp)
	}
	wantStatuses(t, m.applyBatch([]xfer{tr(12, 1, 3, 6, 0)}, 500), tb.TransferExistsWithDifferentAmount)
	// A non-transient failure does not burn its id.
	wantStatuses(t, m.applyBatch([]xfer{tr(13, 3, 3, 1, 0)}, 600), tb.TransferAccountsMustBeDifferent)
	wantStatuses(t, m.applyBatch([]xfer{tr(13, 3, 4, 1, 0)}, 700), tb.TransferCreated)
}

func TestModelLinkedChains(t *testing.T) {
	m := newTestModel()
	before := m.hash
	// The chain's second event fails, so the first is rolled back with it;
	// the event after the chain stands alone.
	rs := m.applyBatch([]xfer{
		tr(20, 3, 4, 10, fLinked),
		tr(21, 1, 4, 10, 0), // exceeds_credits: account 1 has none
		tr(22, 4, 3, 1, 0),
	}, 103)
	wantStatuses(t, rs, tb.TransferLinkedEventFailed, tb.TransferExceedsCredits, tb.TransferCreated)
	if _, ok := m.transfers[u(20)]; ok {
		t.Fatal("the rolled-back event's transfer exists")
	}
	if !m.orphaned[u(21)] || m.orphaned[u(20)] {
		t.Fatalf("burned ids: %v, want only 21", m.orphaned)
	}
	// Only transfer 22 moved money.
	m2 := newTestModel()
	m2.applyBatch([]xfer{tr(22, 4, 3, 1, 0)}, 1)
	if m.hash == before || m.hash != m2.hash {
		t.Fatal("the rollback left balances other than transfer 22's")
	}
	// A chain left open at the end of the batch fails as a whole.
	rs = m.applyBatch([]xfer{tr(23, 3, 4, 1, fLinked), tr(24, 3, 4, 1, fLinked)}, 202)
	wantStatuses(t, rs, tb.TransferLinkedEventFailed, tb.TransferLinkedEventChainOpen)
}

func TestModelPendingAndBalancing(t *testing.T) {
	m := newTestModel()
	wantStatuses(t, m.applyBatch([]xfer{tr(30, 3, 4, 50, fPending)}, 100), tb.TransferCreated)
	post := xfer{id: u(31), pendingID: u(30), amount: u(20), flags: fPost}
	void := xfer{id: u(32), pendingID: u(30), flags: fVoid}
	rs := m.applyBatch([]xfer{post, void}, 202)
	wantStatuses(t, rs, tb.TransferCreated, tb.TransferPendingTransferAlreadyPosted)
	a3, a4 := m.accounts[u(3)], m.accounts[u(4)]
	if !a3.debitsPending.isZero() || a3.debitsPosted != u(20) || a4.creditsPosted != u(20) {
		t.Fatalf("after posting 20 of 50: %+v %+v", *a3, *a4)
	}
	// Account 1 has credits of 0 and debits of 0: a balancing debit moves
	// nothing but is created.
	bal := tr(33, 1, 3, 0, fBalancingDr)
	bal.amount = maxU128
	wantStatuses(t, m.applyBatch([]xfer{bal}, 300), tb.TransferCreated)
	if m.transfers[u(33)].amount != u(0) {
		t.Fatalf("balancing moved %v, want 0", m.transfers[u(33)].amount)
	}
	// Credit account 1 by 9, and a balancing debit takes exactly 9.
	wantStatuses(t, m.applyBatch([]xfer{tr(34, 3, 1, 9, 0)}, 400), tb.TransferCreated)
	bal = tr(35, 1, 4, 0, fBalancingDr)
	bal.amount = maxU128
	wantStatuses(t, m.applyBatch([]xfer{bal}, 500), tb.TransferCreated)
	if m.transfers[u(35)].amount != u(9) {
		t.Fatalf("balancing moved %v, want 9", m.transfers[u(35)].amount)
	}
	// Resubmitting the balancing transfer with any amount at least what it
	// moved says exists.
	wantStatuses(t, m.applyBatch([]xfer{bal}, 600), tb.TransferExists)
}

func TestCut(t *testing.T) {
	// Six transfers, two to a batch.
	var created []createdRec
	for i := range 6 {
		ts := uint64(10 * (i + 1))
		created = append(created, createdRec{x: xfer{id: u(uint64(i)), timestamp: ts}, ts: ts, step: i / 2})
	}
	m := []int{0, 1, 2, 3, 4, 5}
	for _, tc := range []struct {
		name     string
		ts       []uint64
		limit    int
		reversed bool
		lo, hi   int
		ok       bool
	}{
		{"a full prefix", []uint64{10, 20, 30}, 3, false, 0, 3, true},
		{"all visible at the window's end", []uint64{10, 20, 30, 40}, 10, false, 0, 2, true},
		{"fewer than visible and under the limit", []uint64{10, 20, 30}, 10, false, 3, 3, false},
		{"a gap", []uint64{10, 30}, 3, false, 0, 3, false},
		{"newest first", []uint64{40, 30}, 2, true, 2, 2, true},
		{"newest first, from a prefix the window rules out", []uint64{40, 30}, 2, true, 3, 3, false},
		{"newest first, not all under the limit", []uint64{40, 30}, 5, true, 0, 3, false},
		{"nothing, before anything", nil, 5, false, 0, 0, true},
		{"nothing, when something must be visible", nil, 5, false, 1, 3, false},
		{"over the limit", []uint64{10, 20}, 1, false, 0, 3, false},
	} {
		_, why := cut(created, m, tc.ts, tc.limit, tc.reversed, tc.lo, tc.hi)
		if (why == "") != tc.ok {
			t.Errorf("%s: ok=%v (%s), want %v", tc.name, why == "", why, tc.ok)
		}
	}
}

func TestModelExpiry(t *testing.T) {
	m := newTestModel()
	p := tr(40, 3, 4, 30, fPending|fClosingDr)
	p.timeout = 2
	wantStatuses(t, m.applyBatch([]xfer{p}, 1_000_000_000), tb.TransferCreated)
	if m.accounts[u(3)].flags&aClosed == 0 {
		t.Fatal("the closing transfer left its account open")
	}
	// Before the timeout, the expiry is early.
	if _, why := m.expire(2_000_000_000, u(40)); why == "" {
		t.Fatal("an expiry a second before the timeout was taken")
	}
	// A post past the timeout is expired even with no expiry run yet.
	post := xfer{id: u(41), pendingID: u(40), amount: maxU128, flags: fVoid}
	post.amount = u(0)
	wantStatuses(t, m.applyBatch([]xfer{post}, 3_000_000_000), tb.TransferPendingTransferExpired)
	if _, why := m.expire(3_000_000_001, u(40)); why != "" {
		t.Fatal(why)
	}
	a3 := m.accounts[u(3)]
	if !a3.debitsPending.isZero() || a3.flags&aClosed != 0 {
		t.Fatalf("after the expiry: %+v", *a3)
	}
	if _, why := m.expire(3_000_000_002, u(40)); why == "" {
		t.Fatal("a second expiry of the same transfer was taken")
	}
}
