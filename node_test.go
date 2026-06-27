package torx

import (
	"context"
	"strings"
	"testing"
)

func TestNode(t *testing.T) {
	n := NewNode(NodeConfig{
		Name:      "node-0",
		Role:      "broker",
		Resources: Resources{CPUs: Some(4.0), Labels: NewLabels("nvme")},
		Backend:   LocalBackend{},
		Scratch:   MakeScratch("/tmp/torx-test", "node-0"),
		Ports:     NewPortAllocator(""),
	})

	if n.Name() != "node-0" {
		t.Errorf("Name = %q, want node-0", n.Name())
	}
	if n.Role() != "broker" {
		t.Errorf("Role = %q, want broker", n.Role())
	}
	if cpus, ok := n.Resources().CPUs.Get(); !ok || cpus != 4.0 {
		t.Errorf("Resources.CPUs = (%v, %v), want (4, true)", cpus, ok)
	}
	if n.Scratch().Root != "/tmp/torx-test/node-0" {
		t.Errorf("Scratch root = %q", n.Scratch().Root)
	}
	if ss := n.ServiceScratch("kafka-0"); ss.Root != "/tmp/torx-test/node-0/kafka-0" {
		t.Errorf("ServiceScratch root = %q", ss.Root)
	}

	// The embedded Backend's methods are promoted onto the Node.
	res, err := n.Exec(context.Background(), Command("echo", "hi"))
	if err != nil {
		t.Fatalf("Exec via node: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != "hi" {
		t.Errorf("Exec stdout = %q, want hi", res.Stdout)
	}

	p, err := n.AllocatePort()
	if err != nil {
		t.Fatalf("AllocatePort: %v", err)
	}
	if p <= 0 {
		t.Errorf("AllocatePort = %d, want > 0", p)
	}
	n.ReleasePort(p)
}

func TestNodesCoLocatedAreDisjoint(t *testing.T) {
	// Co-located nodes share one port allocator and have distinct scratch roots.
	ports := NewPortAllocator("")
	base := "/tmp/torx-test"
	a := NewNode(NodeConfig{Name: "node-0", Backend: LocalBackend{}, Scratch: MakeScratch(base, "node-0"), Ports: ports})
	b := NewNode(NodeConfig{Name: "node-1", Backend: LocalBackend{}, Scratch: MakeScratch(base, "node-1"), Ports: ports})

	if a.Scratch().Root == b.Scratch().Root {
		t.Errorf("co-located nodes share a scratch root: %q", a.Scratch().Root)
	}

	seen := map[int]bool{}
	for _, n := range []*Node{a, b, a, b} {
		p, err := n.AllocatePort()
		if err != nil {
			t.Fatalf("AllocatePort: %v", err)
		}
		if seen[p] {
			t.Errorf("port %d handed out twice across co-located nodes", p)
		}
		seen[p] = true
	}
}
