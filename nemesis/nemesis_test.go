package nemesis

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

var fast = Schedule{QuietMin: time.Millisecond, QuietMax: 2 * time.Millisecond, HoldMin: time.Millisecond, HoldMax: 2 * time.Millisecond}

// events records what a nemesis reports.
type events struct {
	mu  sync.Mutex
	evs []Event
}

func (e *events) record(ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evs = append(e.evs, ev)
}

func (e *events) all() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Event(nil), e.evs...)
}

func TestRunAlternatesFaultsAndHeals(t *testing.T) {
	var mu sync.Mutex
	down := false
	var evs events
	n := &Nemesis{
		Schedule: fast,
		Rand:     rand.New(rand.NewPCG(1, 1)),
		Record:   evs.record,
		Faults: []Fault{
			{Name: "crash", Weight: 1, Inject: func(ctx context.Context, rng *rand.Rand) (Heal, string, error) {
				mu.Lock()
				defer mu.Unlock()
				if down {
					t.Error("a fault injected while the last is in effect")
				}
				down = true
				return func(ctx context.Context) error {
					mu.Lock()
					defer mu.Unlock()
					down = false
					return nil
				}, "n1", nil
			}},
			{Name: "never", Weight: 0, Inject: func(ctx context.Context, rng *rand.Rand) (Heal, string, error) {
				t.Error("a fault of weight 0 was picked")
				return nil, "", nil
			}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	n.Run(ctx)
	if down {
		t.Fatal("the last fault outlasted the run")
	}
	got := evs.all()
	if len(got) < 4 || len(got)%2 != 0 {
		t.Fatalf("%d events, want an even number of at least 4", len(got))
	}
	for i, ev := range got {
		if ev.Fault != "crash" || ev.Target != "n1" || ev.Heal != (i%2 == 1) || ev.End.Before(ev.Start) {
			t.Fatalf("event %d: %+v", i, ev)
		}
	}
}

func TestHealRunsAfterCancel(t *testing.T) {
	healed := make(chan error, 1)
	injected := make(chan struct{})
	n := &Nemesis{
		Schedule: Schedule{QuietMin: time.Millisecond, QuietMax: time.Millisecond, HoldMin: time.Hour, HoldMax: time.Hour},
		Rand:     rand.New(rand.NewPCG(1, 1)),
		Faults: []Fault{{Name: "pause", Weight: 1, Inject: func(ctx context.Context, rng *rand.Rand) (Heal, string, error) {
			close(injected)
			return func(ctx context.Context) error {
				// The heal's context outlives the run's.
				healed <- ctx.Err()
				return nil
			}, "", nil
		}}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	<-injected
	cancel()
	select {
	case err := <-healed:
		if err != nil {
			t.Fatalf("the heal ran with a canceled context: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the fault was not healed when the run ended")
	}
	<-done
}

func TestNoTargetDrawsAgain(t *testing.T) {
	var evs events
	n := &Nemesis{
		Schedule: fast,
		Rand:     rand.New(rand.NewPCG(1, 1)),
		Record:   evs.record,
		Faults: []Fault{{Name: "crash", Weight: 1, Inject: func(ctx context.Context, rng *rand.Rand) (Heal, string, error) {
			return nil, "", ErrNoTarget
		}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	n.Run(ctx)
	got := evs.all()
	if len(got) < 2 {
		t.Fatalf("%d events", len(got))
	}
	for _, ev := range got {
		if ev.Heal || !errors.Is(ev.Err, ErrNoTarget) {
			t.Fatalf("event %+v", ev)
		}
	}
}

func TestBeforeRunsBeforeEachFault(t *testing.T) {
	var mu sync.Mutex
	befores, faults := 0, 0
	n := &Nemesis{
		Schedule: fast,
		Rand:     rand.New(rand.NewPCG(1, 1)),
		Before: func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			befores++
		},
		Faults: []Fault{{Name: "f", Weight: 1, Inject: func(ctx context.Context, rng *rand.Rand) (Heal, string, error) {
			mu.Lock()
			defer mu.Unlock()
			faults++
			if befores != faults {
				t.Errorf("fault %d after %d supervisor passes", faults, befores)
			}
			return nil, "", nil
		}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	n.Run(ctx)
	if faults == 0 {
		t.Fatal("no faults")
	}
}

func TestNoFaultsWaits(t *testing.T) {
	n := &Nemesis{Rand: rand.New(rand.NewPCG(1, 1))}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	n.Run(ctx)
}

func TestHolds(t *testing.T) {
	var h Holds
	if h.Held("n1") {
		t.Fatal("the zero Holds holds n1")
	}
	r1 := h.Hold("n1", "n2")
	r2 := h.Hold("n1")
	r1()
	r1() // a second release is a no-op
	if !h.Held("n1") || h.Held("n2") {
		t.Fatalf("after one release: n1 %v n2 %v", h.Held("n1"), h.Held("n2"))
	}
	healed := false
	heal := Released(r2, func(ctx context.Context) error {
		if !h.Held("n1") {
			t.Error("released before the heal ran")
		}
		healed = true
		return errors.New("heal failed")
	})
	if err := heal(context.Background()); err == nil || !healed {
		t.Fatalf("heal: %v, ran %v", err, healed)
	}
	if h.Held("n1") {
		t.Fatal("a failed heal kept its hold")
	}
}
