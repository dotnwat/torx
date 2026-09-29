//go:build unix

package main

import (
	"fmt"
	"sort"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// createdRec is an account event as the replay made it: a transfer created,
// as stored, or a pending transfer expired, with the accounts just after it,
// which is what the event records, and the step of the replay it came in.
type createdRec struct {
	x      xfer
	ts     uint64 // the account event's timestamp: the transfer's, or its expiry's
	dr, cr acct
	kind   tb.ChangeEventType
	step   int
}

// scanIndex finds the created transfers a scan matches.
type scanIndex struct {
	events    []int // every account event, in serial order
	transfers []int // the events that created transfers
	byDebit   map[u128][]int
	byCredit  map[u128][]int
}

func newScanIndex(created []createdRec) *scanIndex {
	ix := &scanIndex{byDebit: map[u128][]int{}, byCredit: map[u128][]int{}}
	for i, r := range created {
		ix.events = append(ix.events, i)
		if r.kind == tb.ChangeEventTwoPhaseExpired {
			continue
		}
		ix.transfers = append(ix.transfers, i)
		ix.byDebit[r.x.debit] = append(ix.byDebit[r.x.debit], i)
		ix.byCredit[r.x.credit] = append(ix.byCredit[r.x.credit], i)
	}
	return ix
}

func inRange(ts, lo, hi uint64) bool { return ts >= lo && (hi == 0 || ts <= hi) }

// accountMatches is what get_account_transfers and get_account_balances
// scan: transfers debiting or crediting the account, as the flags say,
// narrowed by every non-zero field of the filter.
func (ix *scanIndex) accountMatches(created []createdRec, f *tb.AccountFilter) []int {
	flags := f.AccountFilterFlags()
	id := fromTB(f.AccountID)
	var base []int
	switch {
	case flags.Debits && flags.Credits:
		base = mergeSorted(ix.byDebit[id], ix.byCredit[id])
	case flags.Debits:
		base = ix.byDebit[id]
	case flags.Credits:
		base = ix.byCredit[id]
	}
	var out []int
	for _, i := range base {
		x := created[i].x
		if !inRange(x.timestamp, f.TimestampMin, f.TimestampMax) ||
			(f.UserData128 != (tb.Uint128{}) && x.ud128 != fromTB(f.UserData128)) ||
			(f.UserData64 != 0 && x.ud64 != f.UserData64) ||
			(f.UserData32 != 0 && x.ud32 != f.UserData32) ||
			(f.Code != 0 && x.code != f.Code) {
			continue
		}
		out = append(out, i)
	}
	return out
}

// queryMatches is what query_transfers scans: transfers matching every
// non-zero field of the filter.
func (ix *scanIndex) queryMatches(created []createdRec, f *tb.QueryFilter) []int {
	var out []int
	for _, i := range ix.transfers {
		x := created[i].x
		if !inRange(x.timestamp, f.TimestampMin, f.TimestampMax) ||
			(f.UserData128 != (tb.Uint128{}) && x.ud128 != fromTB(f.UserData128)) ||
			(f.UserData64 != 0 && x.ud64 != f.UserData64) ||
			(f.UserData32 != 0 && x.ud32 != f.UserData32) ||
			(f.Ledger != 0 && x.ledger != f.Ledger) ||
			(f.Code != 0 && x.code != f.Code) {
			continue
		}
		out = append(out, i)
	}
	return out
}

// changeMatches is what get_change_events scans: every account event in the
// range, one per transfer created and one per expiry.
func (ix *scanIndex) changeMatches(created []createdRec, f *tb.ChangeEventsFilter) []int {
	var out []int
	for _, i := range ix.events {
		if inRange(created[i].ts, f.TimestampMin, f.TimestampMax) {
			out = append(out, i)
		}
	}
	return out
}

func mergeSorted(a, b []int) []int {
	out := make([]int, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	return out
}

// cut checks a scan's result against the transfers it matches, m, in serial
// order: the result must be the first limit of those visible at some prefix
// of the serial order -- the last limit, newest first, when reversed -- and
// that prefix must lie within [lo, hi], the op's window. ts are the result's
// timestamps. It returns the indexes into m the result holds, in result
// order, or why the result cannot be such a scan.
func cut(created []createdRec, m []int, ts []uint64, limit int, reversed bool, lo, hi int) ([]int, string) {
	n := len(ts)
	if n > limit {
		return nil, fmt.Sprintf("%d results for a limit of %d", n, limit)
	}
	step := func(j int) int { return created[m[j]].step }
	// k is the number of steps in the prefix: m[j] is visible at k when
	// step(j) < k.
	kmin, kmax := lo, hi
	atLeast := func(v int) { kmin = max(kmin, v) }
	atMost := func(v int) { kmax = min(kmax, v) }
	pos := make([]int, n)
	if !reversed {
		for i := range n {
			if i >= len(m) || created[m[i]].ts != ts[i] {
				return nil, fmt.Sprintf("result %d (timestamp %d) is not the scan's match %d", i, ts[i], i)
			}
			pos[i] = i
		}
		if n > 0 {
			atLeast(step(n-1) + 1)
		}
		if n < limit && n < len(m) {
			atMost(step(n))
		}
	} else {
		if n == 0 {
			if len(m) > 0 {
				atMost(step(0))
			}
		} else {
			j := sort.Search(len(m), func(j int) bool { return created[m[j]].ts >= ts[0] })
			if j == len(m) || created[m[j]].ts != ts[0] {
				return nil, fmt.Sprintf("result 0 (timestamp %d) matches no transfer the scan covers", ts[0])
			}
			for i := range n {
				if j-i < 0 || created[m[j-i]].ts != ts[i] {
					return nil, fmt.Sprintf("result %d (timestamp %d) does not follow result %d, newest first", i, ts[i], i-1)
				}
				pos[i] = j - i
			}
			atLeast(step(j) + 1)
			if j+1 < len(m) {
				atMost(step(j + 1))
			}
			if n < limit && j-n+1 != 0 {
				return nil, fmt.Sprintf("%d results for a limit of %d, but %d older matches were left out", n, limit, j-n+1)
			}
		}
	}
	if kmin > kmax {
		return nil, fmt.Sprintf("no prefix of the serial order within the op's window (%d..%d) gives this result (it needs %d..%d)", lo, hi, kmin, kmax)
	}
	return pos, ""
}

// checkQueries checks every scan the clients ran.
func (c *checker) checkQueries(batches []*batch, steps int, created []createdRec) {
	ix := newScanIndex(created)
	history := map[u128]bool{}
	for _, a := range c.initial {
		history[a.id] = a.flags&aHistory != 0
	}
	for i := range c.ops {
		op := &c.ops[i]
		if op.Outcome != Ok {
			continue
		}
		var m []int
		var ts []uint64
		var limit int
		var reversed bool
		switch op.F {
		case "account-transfers", "account-balances":
			f := op.AccountFilter
			m = ix.accountMatches(created, f)
			limit, reversed = int(f.Limit), f.AccountFilterFlags().Reversed
			if op.F == "account-balances" && !history[fromTB(f.AccountID)] {
				m = nil // balances are kept only for an account with flags.history
			}
		case "query-transfers":
			m = ix.queryMatches(created, op.QueryFilter)
			limit, reversed = int(op.QueryFilter.Limit), op.QueryFilter.QueryFilterFlags().Reversed
		case "change-events":
			m = ix.changeMatches(created, op.ChangesFilter)
			limit = int(op.ChangesFilter.Limit)
		default:
			continue
		}
		c.stats[op.F]++
		switch op.F {
		case "account-transfers", "query-transfers":
			for _, t := range op.Transfers {
				ts = append(ts, t.Timestamp)
			}
		case "account-balances":
			for _, b := range op.Balances {
				ts = append(ts, b.Timestamp)
			}
		case "change-events":
			for _, e := range op.Changes {
				ts = append(ts, e.Timestamp)
			}
		}
		lo, hi := window(batches, steps, op.Start, op.End)
		pos, why := cut(created, m, ts, limit, reversed, lo, hi)
		if why != "" {
			c.add("wrong-scan", sevError, "%s %s at %v..%v with %s: %s", op.Process, op.F, op.Start, op.End, filterString(op), why)
			continue
		}
		for r, p := range pos {
			rec := created[m[p]]
			var bad string
			switch op.F {
			case "account-transfers", "query-transfers":
				if got := xferFrom(op.Transfers[r]); got != rec.x {
					bad = fmt.Sprintf("found %v; it is %v", transferJSON(op.Transfers[r]), transferJSON(rec.x.tb()))
				}
			case "account-balances":
				a := rec.dr
				if rec.x.credit == fromTB(op.AccountFilter.AccountID) {
					a = rec.cr
				}
				b := op.Balances[r]
				if fromTB(b.DebitsPending) != a.debitsPending || fromTB(b.DebitsPosted) != a.debitsPosted ||
					fromTB(b.CreditsPending) != a.creditsPending || fromTB(b.CreditsPosted) != a.creditsPosted {
					bad = fmt.Sprintf("balance after transfer %v at %d is dp=%v dP=%v cp=%v cP=%v, want %v", rec.x.id, b.Timestamp,
						fromTB(b.DebitsPending), fromTB(b.DebitsPosted), fromTB(b.CreditsPending), fromTB(b.CreditsPosted), accountJSON(tbAccount(a)))
				}
			case "change-events":
				if d := changeDiff(op.Changes[r], rec, c.initial); d != "" {
					bad = fmt.Sprintf("change event for transfer %v at %d: %s", rec.x.id, rec.x.timestamp, d)
				}
			}
			if bad != "" {
				c.add("wrong-scan-result", sevError, "%s %s at %v..%v with %s: result %d: %s", op.Process, op.F, op.Start, op.End, filterString(op), r, bad)
				break
			}
		}
	}
}

func filterString(op *Op) string {
	switch {
	case op.AccountFilter != nil:
		f := op.AccountFilter
		return fmt.Sprintf("account=%v flags=%#x code=%d ud64=%d ud32=%d ts=[%d,%d] limit=%d",
			fromTB(f.AccountID), f.Flags, f.Code, f.UserData64, f.UserData32, f.TimestampMin, f.TimestampMax, f.Limit)
	case op.QueryFilter != nil:
		f := op.QueryFilter
		return fmt.Sprintf("ledger=%d code=%d ud64=%d ud32=%d ts=[%d,%d] limit=%d flags=%#x",
			f.Ledger, f.Code, f.UserData64, f.UserData32, f.TimestampMin, f.TimestampMax, f.Limit, f.Flags)
	case op.ChangesFilter != nil:
		f := op.ChangesFilter
		return fmt.Sprintf("ts=[%d,%d] limit=%d", f.TimestampMin, f.TimestampMax, f.Limit)
	}
	return ""
}

// changeDiff compares a change event with the model's, field by field.
func changeDiff(e tb.ChangeEvent, r createdRec, initial []acct) string {
	static := map[u128]acct{}
	for _, a := range initial {
		static[a.id] = a
	}
	type field struct {
		name      string
		got, want any
	}
	x := r.x
	fields := []field{
		{"transfer_id", fromTB(e.TransferID), x.id},
		{"transfer_amount", fromTB(e.TransferAmount), x.amount},
		{"transfer_pending_id", fromTB(e.TransferPendingID), x.pendingID},
		{"transfer_user_data_128", fromTB(e.TransferUserData128), x.ud128},
		{"transfer_user_data_64", e.TransferUserData64, x.ud64},
		{"transfer_user_data_32", e.TransferUserData32, x.ud32},
		{"transfer_timeout", e.TransferTimeout, x.timeout},
		{"transfer_code", e.TransferCode, x.code},
		{"transfer_flags", e.TransferFlags, x.flags},
		{"ledger", e.Ledger, x.ledger},
		{"type", e.Type, r.kind},
		{"debit_account_id", fromTB(e.DebitAccountID), r.dr.id},
		{"debit_account_debits_pending", fromTB(e.DebitAccountDebitsPending), r.dr.debitsPending},
		{"debit_account_debits_posted", fromTB(e.DebitAccountDebitsPosted), r.dr.debitsPosted},
		{"debit_account_credits_pending", fromTB(e.DebitAccountCreditsPending), r.dr.creditsPending},
		{"debit_account_credits_posted", fromTB(e.DebitAccountCreditsPosted), r.dr.creditsPosted},
		{"debit_account_code", e.DebitAccountCode, r.dr.code},
		{"debit_account_flags", e.DebitAccountFlags, r.dr.flags},
		{"credit_account_id", fromTB(e.CreditAccountID), r.cr.id},
		{"credit_account_debits_pending", fromTB(e.CreditAccountDebitsPending), r.cr.debitsPending},
		{"credit_account_debits_posted", fromTB(e.CreditAccountDebitsPosted), r.cr.debitsPosted},
		{"credit_account_credits_pending", fromTB(e.CreditAccountCreditsPending), r.cr.creditsPending},
		{"credit_account_credits_posted", fromTB(e.CreditAccountCreditsPosted), r.cr.creditsPosted},
		{"credit_account_code", e.CreditAccountCode, r.cr.code},
		{"credit_account_flags", e.CreditAccountFlags, r.cr.flags},
		{"timestamp", e.Timestamp, r.ts},
		{"transfer_timestamp", e.TransferTimestamp, x.timestamp},
		{"debit_account_timestamp", e.DebitAccountTimestamp, static[x.debit].timestamp},
		{"credit_account_timestamp", e.CreditAccountTimestamp, static[x.credit].timestamp},
	}
	for _, f := range fields {
		if f.got != f.want {
			return fmt.Sprintf("%s is %v, want %v", f.name, f.got, f.want)
		}
	}
	return ""
}
