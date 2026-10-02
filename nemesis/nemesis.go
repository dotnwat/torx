// Package nemesis schedules faults: it injects one at a time, drawn at
// random by weight, holds it for a while, heals it, rests, and goes again,
// until its context ends. The faults themselves -- a crash, a partition, a
// full disk -- are the job's to write, out of the service it tests and the
// fault packages (netfault, diskfault, resfault); the nemesis is the loop
// every chaos job otherwise writes for itself.
//
// A fault's Inject returns how to heal it, and the nemesis heals it after
// the hold even when its context has ended meanwhile, so no fault outlasts
// the run. Every injection and heal is reported to Record, with when it
// began and ended, for the job's history: a history that interleaves the
// faults with the clients' operations is the one place to read what a
// client saw against what was being done to the system at the time.
//
// Several nemeses may run at once over one system, each with faults of its
// own; Holds lets them, and a supervisor, keep track of the nodes a fault
// holds down.
package nemesis

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// Heal undoes a fault.
type Heal func(ctx context.Context) error

// Fault is one kind of fault the nemesis can inject.
type Fault struct {
	// Name names the fault in the history: "crash", "partition-half".
	Name string
	// Weight is how often the nemesis picks the fault, relative to the
	// others' weights. A fault of weight 0 is never picked.
	Weight int
	// Inject applies the fault, and returns how to heal it -- nil for a
	// fault that needs no heal, such as one that is over once applied --
	// and a description of what it hit: nodes, drives, the groups of a
	// partition. An error says the fault could not be applied, or not
	// entirely: the heal it returns, if any, still runs after the hold.
	// ErrNoTarget says there was nothing to apply it to just now, such as
	// no node up to crash; the nemesis records that and draws again after
	// a rest.
	Inject func(ctx context.Context, rng *rand.Rand) (heal Heal, target string, err error)
}

// ErrNoTarget is what a fault's Inject returns when there is nothing for it
// to act on at the moment.
var ErrNoTarget = errors.New("nemesis: no target for the fault")

// Event is a fault injected, or healed.
type Event struct {
	// Fault is the fault's name; a heal is the fault's name too, with Heal
	// set.
	Fault  string
	Heal   bool
	Target string
	Start  time.Time
	End    time.Time
	// Err is why the fault, or its heal, failed, or ErrNoTarget.
	Err error
}

// Schedule is how long faults last and how long the system rests between
// them. Every duration is drawn uniformly from its range.
type Schedule struct {
	// Quiet is the rest between a heal and the next fault.
	QuietMin, QuietMax time.Duration
	// Hold is how long a fault lasts before it is healed.
	HoldMin, HoldMax time.Duration
	// One hold in LongEvery lasts from HoldMax to LongHold instead, when
	// both are set: long enough, say, that a node cut off falls further
	// behind than its peers' logs reach.
	LongHold  time.Duration
	LongEvery int
}

// DefaultSchedule rests half a second to three seconds, and holds a fault
// half a second to eight seconds, one in ten up to thirty.
var DefaultSchedule = Schedule{
	QuietMin: 500 * time.Millisecond, QuietMax: 3 * time.Second,
	HoldMin: 500 * time.Millisecond, HoldMax: 8 * time.Second,
	LongHold: 30 * time.Second, LongEvery: 10,
}

// Nemesis injects faults until its context ends.
type Nemesis struct {
	Faults   []Fault
	Schedule Schedule
	// Rand draws the faults and their timing, and is passed to Inject. Give
	// each nemesis a stream of the job's seed (JobContext.Rand) so a run can
	// be repeated.
	Rand *rand.Rand
	// Before, if set, runs before each fault: a supervisor's pass that
	// restarts what exited on its own, say, so each fault starts from a
	// system that is whole except for the faults still held.
	Before func(ctx context.Context)
	// Record, if set, is told of every fault and heal as each ends.
	Record func(Event)
}

// Run injects faults one at a time until ctx ends, and heals the fault in
// effect then before it returns.
func (n *Nemesis) Run(ctx context.Context) {
	total := 0
	for _, f := range n.Faults {
		total += max(f.Weight, 0)
	}
	if total == 0 {
		<-ctx.Done()
		return
	}
	s := n.Schedule
	for {
		if !Sleep(ctx, n.between(s.QuietMin, s.QuietMax)) {
			return
		}
		if n.Before != nil {
			n.Before(ctx)
			if ctx.Err() != nil {
				return
			}
		}
		f := n.pick(total)
		// A fault and its heal run whole even if ctx ends meanwhile: one
		// cut short would leave the system in a state no heal knows.
		fctx := context.WithoutCancel(ctx)
		ev := Event{Fault: f.Name, Start: time.Now()}
		heal, target, err := f.Inject(fctx, n.Rand)
		ev.End, ev.Target, ev.Err = time.Now(), target, err
		n.record(ev)
		if heal == nil {
			continue
		}
		hold := n.between(s.HoldMin, s.HoldMax)
		if s.LongEvery > 0 && s.LongHold > s.HoldMax && n.Rand.IntN(s.LongEvery) == 0 {
			hold = n.between(s.HoldMax, s.LongHold)
		}
		Sleep(ctx, hold)
		ev = Event{Fault: f.Name, Heal: true, Target: target, Start: time.Now()}
		ev.Err = heal(fctx)
		ev.End = time.Now()
		n.record(ev)
	}
}

func (n *Nemesis) record(ev Event) {
	if n.Record != nil {
		n.Record(ev)
	}
}

func (n *Nemesis) pick(total int) Fault {
	r := n.Rand.IntN(total)
	for _, f := range n.Faults {
		if r -= max(f.Weight, 0); r < 0 {
			return f
		}
	}
	return n.Faults[len(n.Faults)-1]
}

func (n *Nemesis) between(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(n.Rand.Int64N(int64(hi-lo)))
}

// Sleep waits for d or until ctx ends, and reports whether d passed.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Holds counts the faults holding each target down -- a node crashed until
// its heal restarts it, a disk full until its heal frees it -- so that
// nemeses running at once, and a supervisor restarting what exited on its
// own, leave alone what a fault holds. Targets are names: of nodes, of
// drives. The zero Holds holds nothing.
type Holds struct {
	mu sync.Mutex
	n  map[string]int
}

// Hold marks targets held by one more fault, and returns how to release
// them, which is safe to call more than once.
func (h *Holds) Hold(targets ...string) (release func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == nil {
		h.n = map[string]int{}
	}
	targets = slices.Clone(targets)
	for _, t := range targets {
		h.n[t]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, t := range targets {
				if h.n[t]--; h.n[t] <= 0 {
					delete(h.n, t)
				}
			}
		})
	}
}

// Held reports whether any fault holds target.
func (h *Holds) Held(target string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n[target] > 0
}

// Released wraps a heal so it releases what the fault held once it has
// run, whether or not it succeeded: a heal that failed leaves the target to
// the supervisor.
func Released(release func(), heal Heal) Heal {
	return func(ctx context.Context) error {
		defer release()
		if heal == nil {
			return nil
		}
		return heal(ctx)
	}
}
