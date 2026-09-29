//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// Outcome is how an operation ended, as a checker must read it. Ok means the
// cluster answered; a create's answer says per event whether it took effect.
// Info is indeterminate: the request was sent and no answer came back -- the
// client was evicted or closed first -- so it may have executed or not.
type Outcome string

const (
	Ok   Outcome = "ok"
	Info Outcome = "info"
)

// Op is one completed operation in a history: who issued it, what it was,
// when it began and ended relative to the start of the history, and what it
// returned. The nemesis records its faults as operations too, so a history
// reads as one timeline of what the clients did and what was done to the
// cluster.
type Op struct {
	Process string        `json:"process"` // "client-3", "nemesis", "final"
	F       string        `json:"f"`       // "transfer", "accounts", "lookup"; for the nemesis, the fault
	Node    string        `json:"node,omitempty"`
	Start   time.Duration `json:"start"`
	End     time.Duration `json:"end"`
	Outcome Outcome       `json:"outcome"`
	Err     string        `json:"error,omitempty"`

	// A transfer op: the events sent and, when Ok, one result per event.
	Events  []tb.Transfer             `json:"-"`
	Results []tb.CreateTransferResult `json:"-"`
	// An accounts op: the accounts read.
	Accounts []tb.Account `json:"-"`
	// A lookup op: the ids asked for and the transfers found.
	IDs       []tb.Uint128  `json:"-"`
	Transfers []tb.Transfer `json:"-"`

	// A query op: the filter it ran with, one of three kinds, and what it
	// found -- Transfers for get_account_transfers and query_transfers,
	// Balances for get_account_balances, Changes for get_change_events.
	AccountFilter *tb.AccountFilter      `json:"account_filter,omitempty"`
	QueryFilter   *tb.QueryFilter        `json:"query_filter,omitempty"`
	ChangesFilter *tb.ChangeEventsFilter `json:"changes_filter,omitempty"`
	Balances      []tb.AccountBalance    `json:"-"`
	Changes       []tb.ChangeEvent       `json:"-"`
}

// MarshalJSON renders the op with its ids and amounts in decimal, for a
// person reading the history.
func (op Op) MarshalJSON() ([]byte, error) {
	type plain Op
	out := struct {
		plain
		Events    []map[string]any `json:"events,omitempty"`
		Results   []string         `json:"results,omitempty"`
		Accounts  []map[string]any `json:"accounts,omitempty"`
		IDs       []string         `json:"ids,omitempty"`
		Transfers []map[string]any `json:"transfers,omitempty"`
		Balances  []string         `json:"balances,omitempty"`
		Changes   []string         `json:"changes,omitempty"`
	}{plain: plain(op)}
	for _, b := range op.Balances {
		out.Balances = append(out.Balances, fmt.Sprintf("%d:dp=%v,dP=%v,cp=%v,cP=%v", b.Timestamp,
			fromTB(b.DebitsPending), fromTB(b.DebitsPosted), fromTB(b.CreditsPending), fromTB(b.CreditsPosted)))
	}
	for _, e := range op.Changes {
		out.Changes = append(out.Changes, fmt.Sprintf("%d:%v:%v", e.Timestamp, e.Type, fromTB(e.TransferID)))
	}
	for _, e := range op.Events {
		out.Events = append(out.Events, transferJSON(e))
	}
	for _, r := range op.Results {
		out.Results = append(out.Results, r.Status.String()+"@"+itoa(r.Timestamp))
	}
	for _, a := range op.Accounts {
		out.Accounts = append(out.Accounts, accountJSON(a))
	}
	for _, id := range op.IDs {
		out.IDs = append(out.IDs, fromTB(id).String())
	}
	for _, t := range op.Transfers {
		out.Transfers = append(out.Transfers, transferJSON(t))
	}
	return json.Marshal(out)
}

func transferJSON(t tb.Transfer) map[string]any {
	x := xferFrom(t)
	m := map[string]any{"id": x.id.String(), "amount": x.amount.String(), "flags": flagNames(x.flags)}
	if !x.debit.isZero() {
		m["dr"] = x.debit.String()
	}
	if !x.credit.isZero() {
		m["cr"] = x.credit.String()
	}
	if !x.pendingID.isZero() {
		m["pending"] = x.pendingID.String()
	}
	if x.timestamp != 0 {
		m["ts"] = x.timestamp
	}
	return m
}

func accountJSON(a tb.Account) map[string]any {
	x := acctFrom(a)
	return map[string]any{
		"id": x.id.String(), "dp": x.debitsPending.String(), "dP": x.debitsPosted.String(),
		"cp": x.creditsPending.String(), "cP": x.creditsPosted.String(), "flags": x.flags, "ts": x.timestamp,
	}
}

func flagNames(f uint16) string {
	names := []string{"linked", "pending", "post", "void", "bal_dr", "bal_cr", "closing_dr", "closing_cr", "imported"}
	var b bytes.Buffer
	for i, n := range names {
		if f&(1<<i) != 0 {
			if b.Len() > 0 {
				b.WriteByte('|')
			}
			b.WriteString(n)
		}
	}
	return b.String()
}

func itoa(v uint64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// History records operations from many goroutines.
type History struct {
	t0  time.Time
	mu  sync.Mutex
	ops []Op
}

// NewHistory starts a history now.
func NewHistory() *History { return &History{t0: time.Now()} }

// Now is the time since the history started.
func (h *History) Now() time.Duration { return time.Since(h.t0) }

// Add records a completed operation.
func (h *History) Add(op Op) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// Ops returns the operations recorded so far, ordered by start.
func (h *History) Ops() []Op {
	h.mu.Lock()
	ops := slices.Clone(h.ops)
	h.mu.Unlock()
	slices.SortStableFunc(ops, func(a, b Op) int { return int(a.Start - b.Start) })
	return ops
}

// NDJSON renders the history one operation per line.
func (h *History) NDJSON() []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, op := range h.Ops() {
		_ = enc.Encode(op)
	}
	return b.Bytes()
}
