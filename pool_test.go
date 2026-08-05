package torx

import (
	"errors"
	"testing"
)

// testNode builds a local node whose scratch root lives in a per-test
// temporary directory, so state one test's jobs leave on a node can never
// leak into another test's readiness checks or collected artifacts.
func testNode(t *testing.T, name string, labels ...string) *Node {
	t.Helper()
	return NewNode(NodeConfig{
		Name:      name,
		Resources: Resources{Labels: NewLabels(labels...)},
		Backend:   LocalBackend{},
		Scratch:   MakeScratch(t.TempDir(), name),
		Ports:     NewPortAllocator(""),
	})
}

func TestPoolAllocateAndFree(t *testing.T) {
	p := NewPool([]*Node{testNode(t, "a"), testNode(t, "b"), testNode(t, "c")})
	if p.Size() != 3 || p.Available() != 3 || p.InUse() != 0 {
		t.Fatalf("init: size=%d avail=%d inuse=%d", p.Size(), p.Available(), p.InUse())
	}

	sub, err := p.Allocate(Homogeneous(2, NodeSpec{}))
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if sub.Size() != 2 {
		t.Errorf("sub size = %d, want 2", sub.Size())
	}
	if p.Available() != 1 || p.InUse() != 2 {
		t.Errorf("after alloc: avail=%d inuse=%d, want 1/2", p.Available(), p.InUse())
	}

	p.Free(sub)
	if p.Available() != 3 || p.InUse() != 0 {
		t.Errorf("after free: avail=%d inuse=%d, want 3/0", p.Available(), p.InUse())
	}

	p.Free(sub) // idempotent
	if p.Available() != 3 {
		t.Errorf("double free changed availability: %d", p.Available())
	}
}

func TestPoolAllocateAtomicFailure(t *testing.T) {
	p := NewPool([]*Node{testNode(t, "a"), testNode(t, "b")})

	_, err := p.Allocate(Homogeneous(3, NodeSpec{}))
	if !errors.Is(err, ErrAllocation) {
		t.Errorf("err = %v, want ErrAllocation", err)
	}
	// A failed allocation must consume nothing.
	if p.Available() != 2 || p.InUse() != 0 {
		t.Errorf("partial allocation: avail=%d inuse=%d, want 2/0", p.Available(), p.InUse())
	}
}

func TestPoolMostConstrainedFirst(t *testing.T) {
	// nvme is listed first; a naive in-order greedy would give it to the "any"
	// spec and then fail the "needs-nvme" spec. Most-constrained-first matching
	// reserves nvme for the spec that needs it.
	p := NewPool([]*Node{testNode(t, "nvme", "nvme"), testNode(t, "plain")})
	spec := PoolSpec{Nodes: []NodeSpec{
		{Role: "any"},
		{Role: "needs-nvme", Required: Resources{Labels: NewLabels("nvme")}},
	}}

	sub, err := p.Allocate(spec)
	if err != nil {
		t.Fatalf("Allocate failed though a valid assignment exists: %v", err)
	}
	nodes := sub.Nodes()
	if nodes[0].Name() != "plain" {
		t.Errorf("any-spec got %q, want plain", nodes[0].Name())
	}
	if nodes[1].Name() != "nvme" {
		t.Errorf("needs-nvme spec got %q, want nvme", nodes[1].Name())
	}
}

// cpuNode is a node advertising a CPU count, for matching tests.
func cpuNode(name string, cpus float64) *Node {
	return NewNode(NodeConfig{
		Name:      name,
		Resources: Resources{CPUs: Some(cpus)},
		Backend:   LocalBackend{},
		Scratch:   MakeScratch("/tmp/torx-test", name),
		Ports:     NewPortAllocator(""),
	})
}

// TestPoolMatchesHeterogeneousDemand is the regression for greedy allocation
// wrongly rejecting a feasible request. Candidates are 8-CPU then 2-CPU; specs
// need 2 then need 8. A greedy pass lets the need-2 spec take the 8-CPU node
// (first satisfier) and then fails need-8, though 2->2 and 8->8 is feasible.
func TestPoolMatchesHeterogeneousDemand(t *testing.T) {
	p := NewPool([]*Node{cpuNode("big", 8), cpuNode("small", 2)})
	spec := PoolSpec{Nodes: []NodeSpec{
		{Role: "need-2", Required: Resources{CPUs: Some(2.0)}},
		{Role: "need-8", Required: Resources{CPUs: Some(8.0)}},
	}}

	sub, err := p.Allocate(spec)
	if err != nil {
		t.Fatalf("Allocate rejected a feasible assignment: %v", err)
	}
	nodes := sub.Nodes()
	if nodes[0].Name() != "small" {
		t.Errorf("need-2 spec got %q, want small", nodes[0].Name())
	}
	if nodes[1].Name() != "big" {
		t.Errorf("need-8 spec got %q, want big", nodes[1].Name())
	}
}

// TestPoolCanEverFitHeterogeneous guards the same feasibility check the driver
// uses to decide a job can never run: it must not declare a runnable job
// permanently unschedulable.
func TestPoolCanEverFitHeterogeneous(t *testing.T) {
	p := NewPool([]*Node{cpuNode("big", 8), cpuNode("small", 2)})
	spec := PoolSpec{Nodes: []NodeSpec{
		{Required: Resources{CPUs: Some(2.0)}},
		{Required: Resources{CPUs: Some(8.0)}},
	}}
	if !p.CanEverFit(spec) {
		t.Errorf("CanEverFit = false for a feasible request")
	}
	// Truly infeasible: two specs both need 8 CPUs, only one 8-CPU node exists.
	tooMuch := PoolSpec{Nodes: []NodeSpec{
		{Required: Resources{CPUs: Some(8.0)}},
		{Required: Resources{CPUs: Some(8.0)}},
	}}
	if p.CanEverFit(tooMuch) {
		t.Errorf("CanEverFit = true though only one 8-CPU node exists")
	}
}

func TestPoolLabelUnsatisfiable(t *testing.T) {
	p := NewPool([]*Node{testNode(t, "plain")})
	spec := PoolSpec{Nodes: []NodeSpec{{Required: Resources{Labels: NewLabels("nvme")}}}}
	if _, err := p.Allocate(spec); !errors.Is(err, ErrAllocation) {
		t.Errorf("err = %v, want ErrAllocation", err)
	}
}

func TestPoolCanAllocate(t *testing.T) {
	p := NewPool([]*Node{testNode(t, "a"), testNode(t, "b")})
	if !p.CanAllocate(Homogeneous(2, NodeSpec{})) {
		t.Errorf("CanAllocate(2) = false, want true")
	}
	if p.CanAllocate(Homogeneous(3, NodeSpec{})) {
		t.Errorf("CanAllocate(3) = true, want false")
	}
	if p.Available() != 2 {
		t.Errorf("CanAllocate consumed nodes: avail=%d, want 2", p.Available())
	}
}

func TestPoolMaxUsed(t *testing.T) {
	p := NewPool([]*Node{testNode(t, "a"), testNode(t, "b"), testNode(t, "c")})

	s1, err := p.Allocate(Homogeneous(2, NodeSpec{}))
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if p.MaxUsed() != 2 {
		t.Errorf("MaxUsed = %d, want 2", p.MaxUsed())
	}

	p.Free(s1)
	if _, err := p.Allocate(Homogeneous(1, NodeSpec{})); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if p.MaxUsed() != 2 {
		t.Errorf("MaxUsed = %d, want 2 (high-water mark, not current)", p.MaxUsed())
	}
}
