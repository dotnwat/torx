package torx

import (
	"os"
	"path/filepath"
	"testing"
)

func init() {
	// A registered non-local backend kind, so remote-node manifests can be
	// exercised without linking a real remote backend.
	RegisterBackend("manifest-remote", func(BackendDescriptor) (Backend, error) { return LocalBackend{}, nil })
}

func TestLoadManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	doc := `{
	  "nodes": [
	    {"name": "n0", "scratch": "/var/tmp/torx/n0",
	     "resources": {"cpus": 4, "memory_mb": 8192, "labels": ["ssd"]},
	     "backend": {"kind": "local"}},
	    {"name": "n1", "role": "client", "address": "10.0.0.9", "scratch": "/var/tmp/torx/n1",
	     "backend": {"kind": "manifest-remote", "host": "10.0.0.9"}}
	  ]
	}`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if len(m.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(m.Nodes))
	}
	if m.Nodes[0].Name != "n0" || m.Nodes[1].Address != "10.0.0.9" || m.Nodes[1].Backend.Kind != "manifest-remote" {
		t.Errorf("parsed manifest = %+v", m.Nodes)
	}
}

func TestManifestResourcesConversion(t *testing.T) {
	cpus := 4.0
	mem := int64(8192)
	got := ManifestResources{CPUs: &cpus, MemoryMB: &mem, Labels: []string{"ssd", "bare-metal"}}.resources()
	if c, ok := got.CPUs.Get(); !ok || c != 4 {
		t.Errorf("CPUs = (%v, %v), want (4, true)", c, ok)
	}
	if mmb, ok := got.MemoryMB.Get(); !ok || mmb != 8192 {
		t.Errorf("MemoryMB = (%v, %v), want (8192, true)", mmb, ok)
	}
	if !got.Labels.Has("ssd") || !got.Labels.Has("bare-metal") {
		t.Errorf("labels = %v, want ssd + bare-metal", got.Labels)
	}
	// An omitted quantity stays unspecified rather than becoming zero.
	if _, ok := (ManifestResources{}).resources().CPUs.Get(); ok {
		t.Errorf("omitted CPUs should be unset")
	}
}

func manifestNode(name, kind string, labels ...string) ManifestNode {
	return ManifestNode{
		Name:      name,
		Scratch:   "/var/tmp/torx/" + name,
		Resources: ManifestResources{Labels: labels},
		Backend:   BackendDescriptor{Kind: kind},
	}
}

func TestPoolFromManifest(t *testing.T) {
	pool, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{
		manifestNode("n0", "local"),
		manifestNode("n1", "local"),
	}})
	if err != nil {
		t.Fatalf("PoolFromManifest: %v", err)
	}
	if pool.Size() != 2 {
		t.Errorf("pool size = %d, want 2", pool.Size())
	}
}

func TestPoolFromManifestHeterogeneous(t *testing.T) {
	pool, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{
		manifestNode("plain", "local"),
		manifestNode("fast", "local", "ssd"),
	}})
	if err != nil {
		t.Fatalf("PoolFromManifest: %v", err)
	}
	// A spec requiring the ssd label must land on the labeled node.
	sub, err := pool.Allocate(Homogeneous(1, NodeSpec{Required: Resources{Labels: NewLabels("ssd")}}))
	if err != nil {
		t.Fatalf("allocate ssd: %v", err)
	}
	if sub.Nodes()[0].Name() != "fast" {
		t.Errorf("ssd spec matched %q, want fast", sub.Nodes()[0].Name())
	}
}

func TestPoolFromManifestRemoteKind(t *testing.T) {
	pool, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{{
		Name:    "r0",
		Address: "10.0.0.9",
		Scratch: "/var/tmp/torx/r0",
		Backend: BackendDescriptor{Kind: "manifest-remote", Host: "10.0.0.9"},
	}}})
	if err != nil {
		t.Fatalf("PoolFromManifest: %v", err)
	}
	sub, err := pool.Allocate(Homogeneous(1, NodeSpec{}))
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	// The remote recipe and reachable address round-trip through the node.
	d := descriptorOf(sub.Nodes()[0])
	if d.Backend.Kind != "manifest-remote" || d.Backend.Host != "10.0.0.9" || d.Address != "10.0.0.9" {
		t.Errorf("descriptor = %+v, want the remote recipe and address", d)
	}
}

func TestPoolFromManifestUnknownKind(t *testing.T) {
	_, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{{
		Name: "x", Scratch: "/s", Backend: BackendDescriptor{Kind: "nope"},
	}}})
	if err == nil {
		t.Fatal("expected an error for an unregistered backend kind")
	}
}

func TestPoolFromManifestErrors(t *testing.T) {
	cases := map[string]Manifest{
		"no nodes":        {},
		"missing name":    {Nodes: []ManifestNode{{Scratch: "/s", Backend: BackendDescriptor{Kind: "local"}}}},
		"missing scratch": {Nodes: []ManifestNode{{Name: "n", Backend: BackendDescriptor{Kind: "local"}}}},
		"duplicate name": {Nodes: []ManifestNode{
			{Name: "dup", Scratch: "/a", Backend: BackendDescriptor{Kind: "local"}},
			{Name: "dup", Scratch: "/b", Backend: BackendDescriptor{Kind: "local"}},
		}},
	}
	for name, m := range cases {
		if _, err := PoolFromManifest(m); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestPoolFromManifestPorts(t *testing.T) {
	pool, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{{
		Name:    "r0",
		Scratch: "/var/tmp/torx/r0",
		Backend: BackendDescriptor{Kind: "local"},
		Ports:   &PortRange{Min: 45000, Max: 45010},
	}}})
	if err != nil {
		t.Fatalf("PoolFromManifest: %v", err)
	}
	sub, err := pool.Allocate(Homogeneous(1, NodeSpec{}))
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if p, err := sub.Nodes()[0].AllocatePort(); err != nil || p < 45000 || p >= 45010 {
		t.Errorf("allocated port = (%d, %v), want it in [45000,45010)", p, err)
	}
}

func TestPoolFromManifestBadPortRange(t *testing.T) {
	_, err := PoolFromManifest(Manifest{Nodes: []ManifestNode{{
		Name: "r0", Scratch: "/s", Backend: BackendDescriptor{Kind: "local"},
		Ports: &PortRange{Min: 50000, Max: 40000}, // max <= min
	}}})
	if err == nil {
		t.Fatal("expected an error for an invalid port range")
	}
}
