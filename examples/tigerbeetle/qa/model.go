//go:build unix

package main

import (
	"fmt"
	"slices"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// The model is a sequential reimplementation of TigerBeetle's state machine
// for create_transfers, transcribed from src/state_machine.zig
// (create_transfer, post_or_void_pending_transfer, their idempotency
// checks, and execute_expire_pending_transfers) in the order its checks run, since the first failing check names
// the result. It covers what the workload sends, which is everything but
// imported events; expiries, which the cluster runs on its own, are told to
// it (expire). The checker replays the cluster's history through it in the
// cluster's own serial order, the order of the timestamps the cluster
// assigned, and compares every result.

// Transfer flags, as TigerBeetle lays them out.
const (
	fLinked        = 1 << 0
	fPending       = 1 << 1
	fPost          = 1 << 2
	fVoid          = 1 << 3
	fBalancingDr   = 1 << 4
	fBalancingCr   = 1 << 5
	fClosingDr     = 1 << 6
	fClosingCr     = 1 << 7
	fImported      = 1 << 8
	fPaddingShift  = 9
	aDrNotExceedCr = 1 << 1 // debits_must_not_exceed_credits
	aCrNotExceedDr = 1 << 2 // credits_must_not_exceed_debits
	aHistory       = 1 << 3
	aClosed        = 1 << 5
)

// xfer is a transfer as the model holds it.
type xfer struct {
	id, debit, credit, amount, pendingID, ud128 u128
	ud64                                        uint64
	ud32, timeout, ledger                       uint32
	code, flags                                 uint16
	timestamp                                   uint64
}

func xferFrom(t tb.Transfer) xfer {
	return xfer{
		id: fromTB(t.ID), debit: fromTB(t.DebitAccountID), credit: fromTB(t.CreditAccountID),
		amount: fromTB(t.Amount), pendingID: fromTB(t.PendingID), ud128: fromTB(t.UserData128),
		ud64: t.UserData64, ud32: t.UserData32, timeout: t.Timeout, ledger: t.Ledger,
		code: t.Code, flags: t.Flags, timestamp: t.Timestamp,
	}
}

func (x xfer) tb() tb.Transfer {
	return tb.Transfer{
		ID: x.id.tb(), DebitAccountID: x.debit.tb(), CreditAccountID: x.credit.tb(),
		Amount: x.amount.tb(), PendingID: x.pendingID.tb(), UserData128: x.ud128.tb(),
		UserData64: x.ud64, UserData32: x.ud32, Timeout: x.timeout, Ledger: x.ledger,
		Code: x.code, Flags: x.flags, Timestamp: x.timestamp,
	}
}

// acct is an account as the model holds it.
type acct struct {
	id                            u128
	debitsPending, debitsPosted   u128
	creditsPending, creditsPosted u128
	ledger                        uint32
	code, flags                   uint16
	timestamp                     uint64
}

func acctFrom(a tb.Account) acct {
	return acct{
		id: fromTB(a.ID), debitsPending: fromTB(a.DebitsPending), debitsPosted: fromTB(a.DebitsPosted),
		creditsPending: fromTB(a.CreditsPending), creditsPosted: fromTB(a.CreditsPosted),
		ledger: a.Ledger, code: a.Code, flags: a.Flags, timestamp: a.Timestamp,
	}
}

func (a *acct) debitsExceedCredits(amount u128) bool {
	return a.flags&aDrNotExceedCr != 0 && a.creditsPosted.less(a.debitsPending.mustAdd(a.debitsPosted).mustAdd(amount))
}

func (a *acct) creditsExceedDebits(amount u128) bool {
	return a.flags&aCrNotExceedDr != 0 && a.debitsPosted.less(a.creditsPending.mustAdd(a.creditsPosted).mustAdd(amount))
}

type pendingStatus int

const (
	pendingNone pendingStatus = iota
	pendingPending
	pendingPosted
	pendingVoided
	pendingExpired
)

// expiresAt is when a pending transfer with a timeout expires.
func (x *xfer) expiresAt() uint64 { return x.timestamp + uint64(x.timeout)*1e9 }

// status is a create_transfers result.
type status = tb.CreateTransferStatus

// transient reports whether a failure burns the transfer's id, so that the
// same id fails with id_already_failed from then on (CreateTransferStatus.transient).
func transient(s status) bool {
	switch s {
	case tb.TransferDebitAccountNotFound, tb.TransferCreditAccountNotFound,
		tb.TransferPendingTransferNotFound, tb.TransferExceedsCredits, tb.TransferExceedsDebits,
		tb.TransferDebitAccountAlreadyClosed, tb.TransferCreditAccountAlreadyClosed:
		return true
	}
	return false
}

// ledgerModel is the state a replay has built.
type ledgerModel struct {
	accounts  map[u128]*acct
	transfers map[u128]*xfer
	pending   map[u128]pendingStatus // pending transfer id -> its status
	orphaned  map[u128]bool          // ids burned by a transient failure
	// undo reverts what the linked chain in progress changed, newest last.
	undo []func()
	// hash is a linear hash of every account's balances, kept current, so a
	// read's balances can be matched to the state after some batch.
	hash    uint64
	weights map[u128][4]uint64
}

func newLedgerModel(accounts []acct, weights map[u128][4]uint64) *ledgerModel {
	m := &ledgerModel{
		accounts:  map[u128]*acct{},
		transfers: map[u128]*xfer{},
		pending:   map[u128]pendingStatus{},
		orphaned:  map[u128]bool{},
		weights:   weights,
	}
	for _, a := range accounts {
		m.accounts[a.id] = &a
		m.hash += m.acctHash(&a)
	}
	return m
}

// acctHash is an account's term in the linear hash.
func (m *ledgerModel) acctHash(a *acct) uint64 {
	w := m.weights[a.id]
	return w[0]*(a.debitsPending.lo^a.debitsPending.hi*31) + w[1]*(a.debitsPosted.lo^a.debitsPosted.hi*31) +
		w[2]*(a.creditsPending.lo^a.creditsPending.hi*31) + w[3]*(a.creditsPosted.lo^a.creditsPosted.hi*31)
}

// hashOf is the linear hash of a read's accounts, over the accounts the model
// knows; it equals m.hash exactly when every known account's balances match.
func (m *ledgerModel) hashOf(accounts []acct) uint64 {
	var h uint64
	for i := range accounts {
		if _, ok := m.weights[accounts[i].id]; ok {
			h += m.acctHash(&accounts[i])
		}
	}
	return h
}

// setAccount replaces an account's state, keeping the hash and, inside a
// chain, the undo log.
func (m *ledgerModel) setAccount(a *acct, next acct) {
	prev := *a
	m.hash += m.acctHash(&next) - m.acctHash(a)
	*a = next
	if m.undo != nil {
		m.undo = append(m.undo, func() {
			m.hash += m.acctHash(&prev) - m.acctHash(a)
			*a = prev
		})
	}
}

func (m *ledgerModel) insertTransfer(t *xfer) {
	m.transfers[t.id] = t
	if m.undo != nil {
		m.undo = append(m.undo, func() { delete(m.transfers, t.id) })
	}
}

func (m *ledgerModel) setPending(id u128, s pendingStatus) {
	prev, had := m.pending[id]
	m.pending[id] = s
	if m.undo != nil {
		m.undo = append(m.undo, func() {
			if had {
				m.pending[id] = prev
			} else {
				delete(m.pending, id)
			}
		})
	}
}

// result is one event's outcome in a replayed batch.
type result struct {
	status    status
	timestamp uint64 // the created or existing transfer's, else the event's slot
	// For a created transfer: its accounts' state just after it, which its
	// account event records, and the event's kind.
	dr, cr acct
	kind   tb.ChangeEventType
}

// applyBatch executes a create_transfers batch whose last event has
// timestamp ts, as execute_create does: events in order, a linked chain
// rolled back as a whole when any of its events fails, and a transient
// failure's id burned.
func (m *ledgerModel) applyBatch(events []xfer, ts uint64) []result {
	n := uint64(len(events))
	results := make([]result, len(events))
	chain := -1
	broken := false
	for i := range events {
		e := &events[i]
		slot := ts - n + uint64(i) + 1
		var r result
		func() {
			if e.flags&fLinked != 0 {
				if chain < 0 {
					chain = i
					m.undo = []func(){}
				}
				if i == len(events)-1 {
					r = result{status: tb.TransferLinkedEventChainOpen, timestamp: slot}
					return
				}
			}
			if broken {
				r = result{status: tb.TransferLinkedEventFailed, timestamp: slot}
				return
			}
			if e.flags&fImported != 0 {
				r = result{status: tb.TransferImportedEventNotExpected, timestamp: slot}
				return
			}
			if e.timestamp != 0 {
				r = result{status: tb.TransferTimestampMustBeZero, timestamp: slot}
				return
			}
			s, t := m.createTransfer(slot, e)
			r = result{status: s, timestamp: slot}
			if s == tb.TransferCreated || s == tb.TransferExists {
				r.timestamp = t
			}
			if s == tb.TransferCreated {
				st := m.transfers[e.id]
				r.dr, r.cr = *m.accounts[st.debit], *m.accounts[st.credit]
				switch {
				case st.flags&fPending != 0:
					r.kind = tb.ChangeEventTwoPhasePending
				case st.flags&fPost != 0:
					r.kind = tb.ChangeEventTwoPhasePosted
				case st.flags&fVoid != 0:
					r.kind = tb.ChangeEventTwoPhaseVoided
				default:
					r.kind = tb.ChangeEventSinglePhase
				}
			}
		}()
		if r.status != tb.TransferCreated {
			if chain >= 0 && !broken {
				broken = true
				m.rollback()
				for j := chain; j < i; j++ {
					results[j].status = tb.TransferLinkedEventFailed
				}
			}
			if transient(r.status) {
				m.orphaned[e.id] = true
			}
		}
		results[i] = r
		if chain >= 0 && (e.flags&fLinked == 0 || r.status == tb.TransferLinkedEventChainOpen) {
			if !broken {
				m.undo = nil // persist
			}
			chain, broken = -1, false
		}
	}
	return results
}

func (m *ledgerModel) rollback() {
	for _, undo := range slices.Backward(m.undo) {
		undo()
	}
	m.undo = nil
}

// createTransfer is state_machine.zig's create_transfer.
func (m *ledgerModel) createTransfer(ts uint64, t *xfer) (status, uint64) {
	if t.flags>>fPaddingShift != 0 {
		return tb.TransferReservedFlag, 0
	}
	if t.id.isZero() {
		return tb.TransferIDMustNotBeZero, 0
	}
	if t.id == maxU128 {
		return tb.TransferIDMustNotBeIntMax, 0
	}
	if e, ok := m.transfers[t.id]; ok {
		return m.createTransferExists(t, e)
	}
	if m.orphaned[t.id] {
		return tb.TransferIDAlreadyFailed, 0
	}
	if t.flags&(fPost|fVoid) != 0 {
		return m.postOrVoid(ts, t)
	}
	switch {
	case t.debit.isZero():
		return tb.TransferDebitAccountIDMustNotBeZero, 0
	case t.debit == maxU128:
		return tb.TransferDebitAccountIDMustNotBeIntMax, 0
	case t.credit.isZero():
		return tb.TransferCreditAccountIDMustNotBeZero, 0
	case t.credit == maxU128:
		return tb.TransferCreditAccountIDMustNotBeIntMax, 0
	case t.credit == t.debit:
		return tb.TransferAccountsMustBeDifferent, 0
	case !t.pendingID.isZero():
		return tb.TransferPendingIDMustBeZero, 0
	}
	if t.flags&fPending == 0 {
		if t.timeout != 0 {
			return tb.TransferTimeoutReservedForPendingTransfer, 0
		}
		if t.flags&(fClosingDr|fClosingCr) != 0 {
			return tb.TransferClosingTransferMustBePending, 0
		}
	}
	if t.ledger == 0 {
		return tb.TransferLedgerMustNotBeZero, 0
	}
	if t.code == 0 {
		return tb.TransferCodeMustNotBeZero, 0
	}
	dr, ok := m.accounts[t.debit]
	if !ok {
		return tb.TransferDebitAccountNotFound, 0
	}
	cr, ok := m.accounts[t.credit]
	if !ok {
		return tb.TransferCreditAccountNotFound, 0
	}
	if dr.ledger != cr.ledger {
		return tb.TransferAccountsMustHaveTheSameLedger, 0
	}
	if t.ledger != dr.ledger {
		return tb.TransferTransferMustHaveTheSameLedgerAsAccounts, 0
	}
	if dr.flags&aClosed != 0 {
		return tb.TransferDebitAccountAlreadyClosed, 0
	}
	if cr.flags&aClosed != 0 {
		return tb.TransferCreditAccountAlreadyClosed, 0
	}
	amount := t.amount
	if t.flags&fBalancingDr != 0 {
		drBalance := dr.debitsPosted.mustAdd(dr.debitsPending)
		amount = minU128(amount, dr.creditsPosted.satSub(drBalance))
	}
	if t.flags&fBalancingCr != 0 {
		crBalance := cr.creditsPosted.mustAdd(cr.creditsPending)
		amount = minU128(amount, cr.debitsPosted.satSub(crBalance))
	}
	overflows := func(a, b u128) bool { _, o := a.add(b); return o }
	if t.flags&fPending != 0 {
		if overflows(amount, dr.debitsPending) {
			return tb.TransferOverflowsDebitsPending, 0
		}
		if overflows(amount, cr.creditsPending) {
			return tb.TransferOverflowsCreditsPending, 0
		}
	}
	if overflows(amount, dr.debitsPosted) {
		return tb.TransferOverflowsDebitsPosted, 0
	}
	if overflows(amount, cr.creditsPosted) {
		return tb.TransferOverflowsCreditsPosted, 0
	}
	if overflows(amount, dr.debitsPending.mustAdd(dr.debitsPosted)) {
		return tb.TransferOverflowsDebits, 0
	}
	if overflows(amount, cr.creditsPending.mustAdd(cr.creditsPosted)) {
		return tb.TransferOverflowsCredits, 0
	}
	// The workload's timeouts are seconds: overflows_timeout cannot occur.
	if dr.debitsExceedCredits(amount) {
		return tb.TransferExceedsCredits, 0
	}
	if cr.creditsExceedDebits(amount) {
		return tb.TransferExceedsDebits, 0
	}

	stored := *t
	stored.amount = amount
	stored.timestamp = ts
	m.insertTransfer(&stored)
	drNew, crNew := *dr, *cr
	if t.flags&fPending != 0 {
		drNew.debitsPending = drNew.debitsPending.mustAdd(amount)
		crNew.creditsPending = crNew.creditsPending.mustAdd(amount)
		m.setPending(t.id, pendingPending)
	} else {
		drNew.debitsPosted = drNew.debitsPosted.mustAdd(amount)
		crNew.creditsPosted = crNew.creditsPosted.mustAdd(amount)
	}
	if t.flags&fClosingDr != 0 {
		drNew.flags |= aClosed
	}
	if t.flags&fClosingCr != 0 {
		crNew.flags |= aClosed
	}
	m.setAccount(dr, drNew)
	m.setAccount(cr, crNew)
	return tb.TransferCreated, ts
}

// expire is one event of execute_expire_pending_transfers: the pending
// transfer id expires at ts, releasing what it held and reopening the
// accounts it closed. It reports why the expiry is wrong, if it is: the
// transfer is not pending, or its timeout has not passed.
func (m *ledgerModel) expire(ts uint64, id u128) (result, string) {
	p, ok := m.transfers[id]
	switch {
	case !ok:
		return result{}, "no such transfer"
	case p.flags&fPending == 0 || p.timeout == 0:
		return result{}, "not a pending transfer with a timeout"
	case m.pending[id] != pendingPending:
		return result{}, fmt.Sprintf("not pending (status %d)", m.pending[id])
	case p.expiresAt() > ts:
		return result{}, fmt.Sprintf("expired at %d, before its timeout at %d", ts, p.expiresAt())
	}
	dr, cr := m.accounts[p.debit], m.accounts[p.credit]
	drNew, crNew := *dr, *cr
	drNew.debitsPending = drNew.debitsPending.sub(p.amount)
	crNew.creditsPending = crNew.creditsPending.sub(p.amount)
	if p.flags&fClosingDr != 0 {
		drNew.flags &^= aClosed
	}
	if p.flags&fClosingCr != 0 {
		crNew.flags &^= aClosed
	}
	m.setAccount(dr, drNew)
	m.setAccount(cr, crNew)
	m.pending[id] = pendingExpired
	return result{status: tb.TransferCreated, timestamp: ts, dr: drNew, cr: crNew, kind: tb.ChangeEventTwoPhaseExpired}, ""
}

// createTransferExists is create_transfer_exists: the result of resubmitting
// an id that names a transfer already created.
func (m *ledgerModel) createTransferExists(t, e *xfer) (status, uint64) {
	if t.flags != e.flags {
		return tb.TransferExistsWithDifferentFlags, 0
	}
	if t.pendingID != e.pendingID {
		return tb.TransferExistsWithDifferentPendingID, 0
	}
	if t.timeout != e.timeout {
		return tb.TransferExistsWithDifferentTimeout, 0
	}
	if t.flags&(fPost|fVoid) != 0 {
		return m.postOrVoidExists(t, e, m.transfers[t.pendingID])
	}
	if t.debit != e.debit {
		return tb.TransferExistsWithDifferentDebitAccountID, 0
	}
	if t.credit != e.credit {
		return tb.TransferExistsWithDifferentCreditAccountID, 0
	}
	if t.flags&(fBalancingDr|fBalancingCr) != 0 {
		if t.amount.less(e.amount) {
			return tb.TransferExistsWithDifferentAmount, 0
		}
	} else if t.amount != e.amount {
		return tb.TransferExistsWithDifferentAmount, 0
	}
	switch {
	case t.ud128 != e.ud128:
		return tb.TransferExistsWithDifferentUserData128, 0
	case t.ud64 != e.ud64:
		return tb.TransferExistsWithDifferentUserData64, 0
	case t.ud32 != e.ud32:
		return tb.TransferExistsWithDifferentUserData32, 0
	case t.ledger != e.ledger:
		return tb.TransferExistsWithDifferentLedger, 0
	case t.code != e.code:
		return tb.TransferExistsWithDifferentCode, 0
	}
	return tb.TransferExists, e.timestamp
}

// postOrVoid is post_or_void_pending_transfer.
func (m *ledgerModel) postOrVoid(ts uint64, t *xfer) (status, uint64) {
	switch {
	case t.flags&fPost != 0 && t.flags&fVoid != 0,
		t.flags&(fPending|fBalancingDr|fBalancingCr|fClosingDr|fClosingCr) != 0:
		return tb.TransferFlagsAreMutuallyExclusive, 0
	case t.pendingID.isZero():
		return tb.TransferPendingIDMustNotBeZero, 0
	case t.pendingID == maxU128:
		return tb.TransferPendingIDMustNotBeIntMax, 0
	case t.pendingID == t.id:
		return tb.TransferPendingIDMustBeDifferent, 0
	case t.timeout != 0:
		return tb.TransferTimeoutReservedForPendingTransfer, 0
	}
	p, ok := m.transfers[t.pendingID]
	if !ok {
		return tb.TransferPendingTransferNotFound, 0
	}
	if p.flags&fPending == 0 {
		return tb.TransferPendingTransferNotPending, 0
	}
	dr, cr := m.accounts[p.debit], m.accounts[p.credit]
	switch {
	case !t.debit.isZero() && t.debit != p.debit:
		return tb.TransferPendingTransferHasDifferentDebitAccountID, 0
	case !t.credit.isZero() && t.credit != p.credit:
		return tb.TransferPendingTransferHasDifferentCreditAccountID, 0
	case t.ledger > 0 && t.ledger != p.ledger:
		return tb.TransferPendingTransferHasDifferentLedger, 0
	case t.code > 0 && t.code != p.code:
		return tb.TransferPendingTransferHasDifferentCode, 0
	}
	var amount u128
	if t.flags&fVoid != 0 {
		amount = t.amount
		if amount.isZero() {
			amount = p.amount
		}
	} else {
		amount = t.amount
		if amount == maxU128 {
			amount = p.amount
		}
	}
	if p.amount.less(amount) {
		return tb.TransferExceedsPendingTransferAmount, 0
	}
	if t.flags&fVoid != 0 && amount.less(p.amount) {
		return tb.TransferPendingTransferHasDifferentAmount, 0
	}
	switch m.pending[p.id] {
	case pendingPosted:
		return tb.TransferPendingTransferAlreadyPosted, 0
	case pendingVoided:
		return tb.TransferPendingTransferAlreadyVoided, 0
	case pendingExpired:
		return tb.TransferPendingTransferExpired, 0
	}
	// Past its timeout a pending transfer is expired, whether or not the
	// pulse that expires it has run yet.
	if p.timeout != 0 && p.expiresAt() <= ts {
		return tb.TransferPendingTransferExpired, 0
	}
	if dr.flags&aClosed != 0 && t.flags&fVoid == 0 {
		return tb.TransferDebitAccountAlreadyClosed, 0
	}
	if cr.flags&aClosed != 0 && t.flags&fVoid == 0 {
		return tb.TransferCreditAccountAlreadyClosed, 0
	}

	stored := xfer{
		id: t.id, debit: p.debit, credit: p.credit,
		ud128: t.ud128, ud64: t.ud64, ud32: t.ud32,
		ledger: p.ledger, code: p.code, pendingID: t.pendingID,
		timestamp: ts, flags: t.flags, amount: amount,
	}
	if stored.ud128.isZero() {
		stored.ud128 = p.ud128
	}
	if stored.ud64 == 0 {
		stored.ud64 = p.ud64
	}
	if stored.ud32 == 0 {
		stored.ud32 = p.ud32
	}
	m.insertTransfer(&stored)
	if t.flags&fPost != 0 {
		m.setPending(p.id, pendingPosted)
	} else {
		m.setPending(p.id, pendingVoided)
	}
	drNew, crNew := *dr, *cr
	drNew.debitsPending = drNew.debitsPending.sub(p.amount)
	crNew.creditsPending = crNew.creditsPending.sub(p.amount)
	if t.flags&fPost != 0 {
		drNew.debitsPosted = drNew.debitsPosted.mustAdd(amount)
		crNew.creditsPosted = crNew.creditsPosted.mustAdd(amount)
	}
	if t.flags&fVoid != 0 {
		if p.flags&fClosingDr != 0 {
			drNew.flags &^= aClosed
		}
		if p.flags&fClosingCr != 0 {
			crNew.flags &^= aClosed
		}
	}
	m.setAccount(dr, drNew)
	m.setAccount(cr, crNew)
	return tb.TransferCreated, ts
}

// postOrVoidExists is post_or_void_pending_transfer_exists.
func (m *ledgerModel) postOrVoidExists(t, e, p *xfer) (status, uint64) {
	if !t.debit.isZero() && t.debit != e.debit {
		return tb.TransferExistsWithDifferentDebitAccountID, 0
	}
	if !t.credit.isZero() && t.credit != e.credit {
		return tb.TransferExistsWithDifferentCreditAccountID, 0
	}
	if t.flags&fVoid != 0 {
		if t.amount.isZero() {
			if e.amount != p.amount {
				return tb.TransferExistsWithDifferentAmount, 0
			}
		} else if t.amount != e.amount {
			return tb.TransferExistsWithDifferentAmount, 0
		}
	}
	if t.flags&fPost != 0 {
		if t.amount == maxU128 {
			if e.amount != p.amount {
				return tb.TransferExistsWithDifferentAmount, 0
			}
		} else if t.amount != e.amount {
			return tb.TransferExistsWithDifferentAmount, 0
		}
	}
	userData := []struct {
		zero, differ, parentDiffer bool
		s                          status
	}{
		{t.ud128.isZero(), t.ud128 != e.ud128, e.ud128 != p.ud128, tb.TransferExistsWithDifferentUserData128},
		{t.ud64 == 0, t.ud64 != e.ud64, e.ud64 != p.ud64, tb.TransferExistsWithDifferentUserData64},
		{t.ud32 == 0, t.ud32 != e.ud32, e.ud32 != p.ud32, tb.TransferExistsWithDifferentUserData32},
	}
	for _, d := range userData {
		if (d.zero && d.parentDiffer) || (!d.zero && d.differ) {
			return d.s, 0
		}
	}
	if t.ledger != 0 && t.ledger != e.ledger {
		return tb.TransferExistsWithDifferentLedger, 0
	}
	if t.code != 0 && t.code != e.code {
		return tb.TransferExistsWithDifferentCode, 0
	}
	return tb.TransferExists, e.timestamp
}
