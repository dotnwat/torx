//go:build unix

package main

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// Severity says whether an anomaly fails the job.
type Severity string

const (
	sevError Severity = "error"
	sevWarn  Severity = "warn"
)

// Anomaly is one thing the checker found wrong.
type Anomaly struct {
	Kind     string   `json:"kind"`
	Severity Severity `json:"severity"`
	Detail   string   `json:"detail"`
}

// batch is a create_transfers op placed in the cluster's serial order.
type batch struct {
	op     *Op
	last   uint64 // the timestamp of its last event's slot
	placed bool
	// step is its place among the replay's steps: the batches, and between
	// them the expiries the cluster ran on its own.
	step int
}

// expiry is a pending transfer the cluster expired, at timestamp ts, as its
// change events report.
type expiry struct {
	ts uint64
	id u128
}

func (b *batch) first() uint64 { return b.last - uint64(len(b.op.Events)) + 1 }

// checker holds what a check needs: the history, and the final state read
// once every fault was healed.
type checker struct {
	ops       []Op
	accounts  []tb.Account         // the final read of every account
	transfers map[u128]tb.Transfer // the final lookup of every id submitted
	expiries  []expiry             // every expiry, from the final read of the change events
	initial   []acct               // the accounts as created
	anomalies []Anomaly
	stats     map[string]int
	limit     int // report at most this many anomalies of a kind
	counts    map[string]int
}

func (c *checker) add(kind string, sev Severity, format string, args ...any) {
	c.counts[kind]++
	if c.counts[kind] > c.limit {
		return
	}
	c.anomalies = append(c.anomalies, Anomaly{Kind: kind, Severity: sev, Detail: fmt.Sprintf(format, args...)})
}

// check runs every check and returns the anomalies found. The first
// anomaly of a kind is the one to read: a divergence between the model and
// the cluster tends to cascade into others after it.
func (c *checker) check() []Anomaly {
	c.stats, c.counts = map[string]int{}, map[string]int{}
	if c.limit == 0 {
		c.limit = 5
	}
	batches := c.place()
	hashes, model, created := c.replay(batches)
	c.checkFinal(model)
	c.checkRealtime(batches)
	c.checkReads(batches, hashes, model)
	c.checkQueries(batches, len(hashes)-1, created)
	for kind, n := range c.counts {
		if n > c.limit {
			c.anomalies = append(c.anomalies, Anomaly{Kind: kind, Severity: sevWarn,
				Detail: fmt.Sprintf("%d more %s anomalies not shown", n-c.limit, kind)})
		}
	}
	return c.anomalies
}

// place finds each create_transfers op's position in the serial order. An
// answered op carries its events' timestamps: every event but one found to
// exist already has the timestamp of its slot in the batch, and the slots are
// consecutive, ending at the batch's own timestamp. An op with no answer is
// placed by the transfers it created, if it created any, which the final
// lookup finds at their slots' timestamps; one that created none left no
// trace but the ids a transient failure may have burned.
func (c *checker) place() []*batch {
	var out []*batch
	claimed := map[u128]bool{} // ids some answered op reports it created
	for i := range c.ops {
		op := &c.ops[i]
		if op.F != "transfer" || op.Outcome != Ok {
			continue
		}
		b := &batch{op: op}
		n := uint64(len(op.Events))
		for j, r := range op.Results {
			if r.Status == tb.TransferExists {
				continue
			}
			last := r.Timestamp + n - uint64(j) - 1
			if b.placed && last != b.last {
				c.add("result-timestamps", sevError, "%s at %v: event %d's timestamp %d puts the batch's end at %d, the earlier events' at %d",
					op.Process, op.Start, j, r.Timestamp, last, b.last)
				continue
			}
			b.last, b.placed = last, true
			if r.Status == tb.TransferCreated {
				claimed[fromTB(op.Events[j].ID)] = true
			}
		}
		if b.placed {
			out = append(out, b)
		}
	}
	for i := range c.ops {
		op := &c.ops[i]
		if op.F != "transfer" || op.Outcome != Info {
			continue
		}
		b := &batch{op: op}
		n := uint64(len(op.Events))
		for j, e := range op.Events {
			id := fromTB(e.ID)
			t, ok := c.transfers[id]
			if !ok || claimed[id] {
				continue
			}
			last := t.Timestamp + n - uint64(j) - 1
			if b.placed && last != b.last {
				// Another unanswered op resubmitted the same id.
				continue
			}
			b.last, b.placed = last, true
		}
		if b.placed {
			for _, e := range op.Events {
				claimed[fromTB(e.ID)] = true
			}
			out = append(out, b)
			c.stats["unanswered-placed"]++
		} else {
			c.stats["unanswered-invisible"]++
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].last < out[j].last })
	for i := 1; i < len(out); i++ {
		if out[i].first() <= out[i-1].last {
			c.add("overlapping-batches", sevError, "%s at %v holds slots %d..%d, and %s at %v %d..%d",
				out[i-1].op.Process, out[i-1].op.Start, out[i-1].first(), out[i-1].last,
				out[i].op.Process, out[i].op.Start, out[i].first(), out[i].last)
		}
	}
	return out
}

// replay runs the placed batches through the model in serial order, with the
// cluster's expiries between them, and compares each event's result. Each
// batch and each expiry is a step. It returns the hash of the accounts'
// balances after each step, hashes[s] being the state after the first s
// steps, and the account events the steps made.
func (c *checker) replay(batches []*batch) ([]uint64, *ledgerModel, []createdRec) {
	rng := rand.New(rand.NewPCG(1, 2))
	weights := map[u128][4]uint64{}
	for _, a := range c.initial {
		weights[a.id] = [4]uint64{rng.Uint64() | 1, rng.Uint64() | 1, rng.Uint64() | 1, rng.Uint64() | 1}
	}
	m := newLedgerModel(c.initial, weights)
	// Ids an unanswered op that left no trace may have burned.
	maybeBurned := map[u128]bool{}
	placed := map[*Op]bool{}
	for _, b := range batches {
		placed[b.op] = true
	}
	for i := range c.ops {
		op := &c.ops[i]
		if op.F == "transfer" && op.Outcome == Info && !placed[op] {
			for _, e := range op.Events {
				maybeBurned[fromTB(e.ID)] = true
			}
		}
	}
	hashes := []uint64{m.hash}
	var created []createdRec
	expiries := slices.Clone(c.expiries)
	slices.SortFunc(expiries, func(a, b expiry) int { return cmp.Compare(a.ts, b.ts) })
	// expireUntil applies, each as a step of its own, the expiries before ts.
	expireUntil := func(ts uint64) {
		for len(expiries) > 0 && expiries[0].ts < ts {
			e := expiries[0]
			expiries = expiries[1:]
			r, why := m.expire(e.ts, e.id)
			if why != "" {
				c.add("wrong-expiry", sevError, "the cluster expired transfer %v at %d: %s", e.id, e.ts, why)
				continue
			}
			created = append(created, createdRec{x: *m.transfers[e.id], ts: e.ts, dr: r.dr, cr: r.cr, kind: r.kind, step: len(hashes) - 1})
			hashes = append(hashes, m.hash)
		}
	}
	for _, b := range batches {
		expireUntil(b.first())
		k := len(hashes) - 1
		b.step = k
		events := make([]xfer, len(b.op.Events))
		for i, e := range b.op.Events {
			events[i] = xferFrom(e)
		}
		results := m.applyBatch(events, b.last)
		for i, r := range results {
			if r.status == tb.TransferCreated {
				x := *m.transfers[events[i].id]
				created = append(created, createdRec{x: x, ts: x.timestamp, dr: r.dr, cr: r.cr, kind: r.kind, step: k})
			}
		}
		for i, want := range results {
			id := events[i].id
			if b.op.Outcome == Info {
				// The model's outcome must match what the final lookup found.
				t, exists := c.transfers[id]
				made := want.status == tb.TransferCreated
				if made && (!exists || t.Timestamp != want.timestamp) {
					c.add("lost-write", sevError, "%s at %v (unanswered): event %d (id %v) created at %d by the model, but the cluster has %s",
						b.op.Process, b.op.Start, i, id, want.timestamp, describe(t, exists))
				}
				continue
			}
			got := b.op.Results[i]
			if got.Status == want.status && (got.Timestamp == want.timestamp) {
				c.stats[got.Status.String()]++
				continue
			}
			if got.Status == tb.TransferIDAlreadyFailed && maybeBurned[id] && !m.orphaned[id] {
				// An unanswered op burned the id: take the cluster's word.
				m.orphaned[id] = true
				continue
			}
			c.add("wrong-result", sevError, "%s at %v..%v: event %d of %d (id %v, %s) returned %v@%d, the model %v@%d",
				b.op.Process, b.op.Start, b.op.End, i, len(events), id, flagNames(events[i].flags),
				got.Status, got.Timestamp, want.status, want.timestamp)
		}
		hashes = append(hashes, m.hash)
	}
	expireUntil(^uint64(0))
	// A pending transfer whose timeout passed well before the last event
	// must have expired: the primary runs expiries as their time comes.
	var last uint64
	if n := len(batches); n > 0 {
		last = batches[n-1].last
	}
	for id, st := range m.pending {
		p := m.transfers[id]
		if st == pendingPending && p.timeout != 0 && p.expiresAt()+10e9 < last {
			c.add("unexpired", sevError, "pending transfer %v timed out at %d, and the cluster had not expired it by %d", id, p.expiresAt(), last)
		}
	}
	return hashes, m, created
}

func describe(t tb.Transfer, exists bool) string {
	if !exists {
		return "no such transfer"
	}
	return fmt.Sprintf("it at %d", t.Timestamp)
}

// checkFinal compares the model's final state to the cluster's: the same
// transfers, field for field, and the same balances.
func (c *checker) checkFinal(m *ledgerModel) {
	for id, t := range c.transfers {
		mt, ok := m.transfers[id]
		if !ok {
			c.add("phantom-transfer", sevError, "the cluster has transfer %v at %d, which no replayed event created: %v",
				id, t.Timestamp, transferJSON(t))
			continue
		}
		if got, want := xferFrom(t), *mt; got != want {
			c.add("transfer-differs", sevError, "transfer %v: the cluster has %v, the model %v", id, transferJSON(t), transferJSON(want.tb()))
		}
	}
	for id, mt := range m.transfers {
		if _, ok := c.transfers[id]; !ok {
			c.add("lost-write", sevError, "transfer %v created at %d is missing from the cluster", id, mt.timestamp)
		}
	}
	for _, a := range c.accounts {
		got := acctFrom(a)
		want, ok := m.accounts[got.id]
		if !ok {
			c.add("phantom-account", sevError, "the cluster has account %v, never created", got.id)
			continue
		}
		if got != *want {
			c.add("balance-differs", sevError, "account %v: the cluster has %v, the model %v", got.id, accountJSON(a), accountJSON(tbAccount(*want)))
		}
	}
	c.stats["transfers"] = len(c.transfers)
	c.stats["expiries"] = len(c.expiries)
}

func tbAccount(a acct) tb.Account {
	return tb.Account{ID: a.id.tb(), DebitsPending: a.debitsPending.tb(), DebitsPosted: a.debitsPosted.tb(),
		CreditsPending: a.creditsPending.tb(), CreditsPosted: a.creditsPosted.tb(),
		Ledger: a.ledger, Code: a.code, Flags: a.flags, Timestamp: a.timestamp}
}

// checkRealtime checks the serial order against real time: a batch
// answered before another was sent must come before it.
func (c *checker) checkRealtime(batches []*batch) {
	var answered []*batch
	for _, b := range batches {
		if b.op.Outcome == Ok {
			answered = append(answered, b)
		}
	}
	byEnd := slices.Clone(answered)
	sort.Slice(byEnd, func(i, j int) bool { return byEnd[i].op.End < byEnd[j].op.End })
	byStart := slices.Clone(batches)
	sort.Slice(byStart, func(i, j int) bool { return byStart[i].op.Start < byStart[j].op.Start })
	var maxLast uint64
	var maxBatch *batch
	k := 0
	for _, b := range byStart {
		for k < len(byEnd) && byEnd[k].op.End < b.op.Start {
			if byEnd[k].last > maxLast {
				maxLast, maxBatch = byEnd[k].last, byEnd[k]
			}
			k++
		}
		if maxBatch != nil && b.first() <= maxLast {
			c.add("realtime-order", sevError, "%s's batch sent at %v was ordered at %d, before %s's batch answered at %v at %d",
				b.op.Process, b.op.Start, b.first(), maxBatch.op.Process, maxBatch.op.End, maxLast)
		}
	}
}

// window is where in the serial order an op that began at start and ended
// at end took effect: after every batch answered before start, and before
// every batch sent after end. It returns the prefixes of the replay's steps
// the op may have seen: at least lo steps and at most hi, of steps in all.
func window(batches []*batch, steps int, start, end time.Duration) (lo, hi int) {
	hi = steps
	for _, b := range batches {
		if b.op.Outcome == Ok && b.op.End < start && b.step+1 > lo {
			lo = b.step + 1
		}
		if b.op.Start > end && b.step < hi {
			hi = b.step
		}
	}
	return lo, hi
}

// checkReads checks every read against the serial order: an accounts read
// must see the balances after some prefix of the batches within its window,
// and a lookup must find exactly the transfers created in some such prefix.
func (c *checker) checkReads(batches []*batch, hashes []uint64, m *ledgerModel) {
	createdAt := map[u128]int{} // transfer id -> the step of the batch that created it
	for _, b := range batches {
		for _, e := range b.op.Events {
			id := fromTB(e.ID)
			if t, ok := c.transfers[id]; ok && t.Timestamp >= b.first() && t.Timestamp <= b.last {
				createdAt[id] = b.step
			}
		}
	}
	accountIDs := map[u128]bool{}
	for _, a := range c.initial {
		accountIDs[a.id] = true
	}
	for i := range c.ops {
		op := &c.ops[i]
		if op.Outcome != Ok {
			continue
		}
		switch op.F {
		case "accounts":
			c.stats["reads"]++
			lo, hi := window(batches, len(hashes)-1, op.Start, op.End)
			if len(op.Accounts) != len(c.initial) {
				c.add("wrong-read", sevError, "%s at %v read %d accounts, want %d", op.Process, op.Start, len(op.Accounts), len(c.initial))
				continue
			}
			var got []acct
			for _, a := range op.Accounts {
				x := acctFrom(a)
				if !accountIDs[x.id] {
					c.add("wrong-read", sevError, "%s at %v read account %v, never created", op.Process, op.Start, x.id)
				}
				got = append(got, x)
			}
			h := m.hashOf(got)
			ok := false
			for k := lo; k <= hi && k < len(hashes); k++ {
				if hashes[k] == h {
					ok = true
					break
				}
			}
			if !ok {
				seen := -1
				for k := range hashes {
					if hashes[k] == h {
						seen = k
					}
				}
				c.add("stale-read", sevError, "%s at %v..%v read balances matching %s; its window is after step %d through %d",
					op.Process, op.Start, op.End, prefixName(seen), lo, hi)
			}
		case "lookup":
			c.stats["lookups"]++
			lo, hi := window(batches, len(hashes)-1, op.Start, op.End)
			found := map[u128]bool{}
			for _, t := range op.Transfers {
				id := fromTB(t.ID)
				found[id] = true
				final, ok := c.transfers[id]
				if !ok {
					c.add("wrong-lookup", sevError, "%s at %v found transfer %v, which the cluster no longer has", op.Process, op.Start, id)
					continue
				}
				if final != t {
					c.add("wrong-lookup", sevError, "%s at %v found transfer %v as %v; it is %v", op.Process, op.Start, id, transferJSON(t), transferJSON(final))
				}
			}
			// Some prefix k in [lo, hi] holds exactly the found ones.
			minK, maxK := lo, hi
			for _, id := range op.IDs {
				k, created := createdAt[fromTB(id)]
				if !created {
					continue
				}
				if found[fromTB(id)] {
					minK = max(minK, k+1)
				} else {
					maxK = min(maxK, k)
				}
			}
			if minK > maxK {
				c.add("stale-lookup", sevError, "%s at %v..%v: no prefix of the serial order within its window (%d..%d) holds exactly the transfers it found",
					op.Process, op.Start, op.End, lo, hi)
			}
		}
	}
}

func prefixName(k int) string {
	if k < 0 {
		return "no prefix of the serial order"
	}
	return fmt.Sprintf("the state after step %d", k)
}

// summary is a one-line account of what the check covered.
func (c *checker) summary() string {
	var parts []string
	for _, k := range []string{"transfers", "expiries", "reads", "lookups", "account-transfers", "account-balances", "query-transfers", "change-events", "unanswered-placed", "unanswered-invisible"} {
		parts = append(parts, fmt.Sprintf("%s=%d", k, c.stats[k]))
	}
	return strings.Join(parts, " ")
}
