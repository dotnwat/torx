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
		if ev.Fault != "crash" || ev.Target != "n1" || ev.Heal != (i%2 == 1) || ev.Held != (i%2 == 0) || ev.End.Before(ev.Start) {
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

func TestWindows(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(int64(s), 0) }
	evs := []Event{
		{Fault: "partition", Target: "a", Start: at(10), End: at(11), Held: true},
		{Fault: "stepdown", Target: "s", Start: at(12), End: at(13)},
		{Fault: "pause", Target: "b", Start: at(14), End: at(14), Held: true},
		{Fault: "partition", Target: "a", Heal: true, Start: at(20), End: at(21)},
		{Fault: "partition", Target: "a", Start: at(30), End: at(31), Held: true},
		{Fault: "partition", Target: "a", Heal: true, Start: at(40), End: at(41)},
		{Fault: "crash", Target: "c", Start: at(50), End: at(50), Held: true, Err: errors.New("half applied")},
		{Fault: "crash", Start: at(60), End: at(60), Err: ErrNoTarget},
	}
	want := []Window{
		{"partition", "a", at(10), at(21)},
		{"stepdown", "s", at(12), at(13)},
		{"pause", "b", at(14), time.Time{}}, // never healed
		{"partition", "a", at(30), at(41)},
		{"crash", "c", at(50), time.Time{}},
	}
	got := Windows(evs)
	if len(got) != len(want) {
		t.Fatalf("windows %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("window %d: %+v, want %+v", i, got[i], want[i])
		}
	}

	w := Window{Start: at(10), End: at(20)}
	for _, tc := range []struct {
		from, to int
		after    time.Duration
		want     bool
	}{
		{0, 9, 0, false},
		{0, 10, 0, true},
		{15, 16, 0, true},
		{20, 30, 0, true},
		{21, 30, 0, false},
		{25, 30, 5 * time.Second, true},
		{26, 30, 5 * time.Second, false},
	} {
		if got := w.Overlaps(at(tc.from), at(tc.to), tc.after); got != tc.want {
			t.Errorf("[%d,%d] after %v: %v, want %v", tc.from, tc.to, tc.after, got, tc.want)
		}
	}
	if !(Window{Start: at(10)}).Overlaps(at(1000), at(1001), 0) {
		t.Errorf("a window never healed ended")
	}
}
