package torx

import (
	"errors"
	"testing"
)

func TestPortAllocatorHost(t *testing.T) {
	if got := NewPortAllocator("").Host(); got != "127.0.0.1" {
		t.Errorf("empty host = %q, want 127.0.0.1", got)
	}
	if got := NewPortAllocator("10.0.0.5").Host(); got != "10.0.0.5" {
		t.Errorf("host = %q, want 10.0.0.5", got)
	}
	if got := (&PortAllocator{}).Host(); got != "127.0.0.1" {
		t.Errorf("zero-value host = %q, want 127.0.0.1", got)
	}
}

func TestPortAllocatorDistinctAndRelease(t *testing.T) {
	a := NewPortAllocator("")
	seen := map[int]bool{}
	var ports []int
	for range 5 {
		p, err := a.Allocate()
		if err != nil {
			t.Fatalf("Allocate: %v", err)
		}
		if p <= 0 {
			t.Errorf("port = %d, want > 0", p)
		}
		if seen[p] {
			t.Errorf("port %d handed out twice", p)
		}
		seen[p] = true
		ports = append(ports, p)
	}
	if len(a.leased) != 5 {
		t.Errorf("leased = %d, want 5", len(a.leased))
	}

	a.Release(ports[0])
	if len(a.leased) != 4 {
		t.Errorf("after release, leased = %d, want 4", len(a.leased))
	}

	a.Release(999999) // not leased: no-op
	if len(a.leased) != 4 {
		t.Errorf("after no-op release, leased = %d, want 4", len(a.leased))
	}
}

func TestPortAllocatorExhaustion(t *testing.T) {
	orig := probeFreePort
	defer func() { probeFreePort = orig }()
	probeFreePort = func(string) (int, error) { return 5000, nil }

	a := NewPortAllocator("")
	if _, err := a.Allocate(); err != nil {
		t.Fatalf("first Allocate: %v", err)
	}
	// The only port the probe ever offers is now leased, so the next
	// allocation cannot find a fresh one.
	_, err := a.Allocate()
	if !errors.Is(err, ErrAllocation) {
		t.Errorf("exhausted Allocate err = %v, want ErrAllocation", err)
	}
}

func TestPortAllocatorProbeError(t *testing.T) {
	orig := probeFreePort
	defer func() { probeFreePort = orig }()
	boom := errors.New("listen failed")
	probeFreePort = func(string) (int, error) { return 0, boom }

	_, err := NewPortAllocator("").Allocate()
	if !errors.Is(err, ErrAllocation) {
		t.Errorf("Allocate err = %v, want wraps ErrAllocation", err)
	}
	if !errors.Is(err, boom) {
		t.Errorf("Allocate err = %v, want wraps the probe error", err)
	}
}

func TestPortAllocatorBoundedProbes(t *testing.T) {
	orig := probeFreePort
	defer func() { probeFreePort = orig }()
	var probes int
	probeFreePort = func(host string) (int, error) {
		probes++
		return orig(host)
	}

	const n = 300
	a := NewPortAllocator("")
	seen := make(map[int]struct{}, n)
	for i := range n {
		p, err := a.Allocate()
		if err != nil {
			t.Fatalf("Allocate %d: %v", i, err)
		}
		if _, dup := seen[p]; dup {
			t.Fatalf("port %d handed out twice", p)
		}
		seen[p] = struct{}{}
	}

	// OS ephemeral rotation should make each allocation cost ~1 probe, so the
	// total stays near n. A quadratic scan past every leased port would need
	// roughly n*n/2 probes; the generous bound below still catches that.
	if probes > 3*n {
		t.Errorf("used %d probes for %d ports, want <= %d (O(1) per allocation)", probes, n, 3*n)
	}
}

func TestScratch(t *testing.T) {
	s := MakeScratch("/var/lib/torx", "node-0")
	if s.Root != "/var/lib/torx/node-0" {
		t.Errorf("Root = %q, want /var/lib/torx/node-0", s.Root)
	}
	if got := s.Sub("data", "redpanda.log"); got != "/var/lib/torx/node-0/data/redpanda.log" {
		t.Errorf("Sub = %q, want /var/lib/torx/node-0/data/redpanda.log", got)
	}
	if got := s.Sub(); got != "/var/lib/torx/node-0" {
		t.Errorf("Sub() = %q, want the root", got)
	}
}

func TestMakeScratchDisjoint(t *testing.T) {
	a := MakeScratch("/scratch", "node-0")
	b := MakeScratch("/scratch", "node-1")
	if a.Root == b.Root {
		t.Errorf("distinct keys produced the same root %q", a.Root)
	}
}

func TestMakeScratchInvalidKeyPanics(t *testing.T) {
	for _, key := range []string{"", "a/b", "/abs", "trailing/"} {
		t.Run(key, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("MakeScratch(base, %q) did not panic", key)
				}
			}()
			MakeScratch("/base", key)
		})
	}
}
