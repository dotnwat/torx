// The node manifest: the boundary between provisioning and torx.
//
// A manifest describes the nodes a run executes against -- nodes an out-of-band
// provisioner (a static inventory, docker compose, a cloud API) has already
// prepared. torx only consumes a manifest; producing one is outside the
// framework. LoadManifest reads a manifest from a JSON file, and PoolFromManifest
// turns it into a Pool the scheduler allocates from. This is the general form of
// the localhost-only pool the driver builds when no manifest is given.
package torx

import (
	"encoding/json"
	"fmt"
	"os"
)

// Manifest is a set of provisioned nodes for a run.
type Manifest struct {
	Nodes []ManifestNode `json:"nodes"`
}

// ManifestNode describes one node: its identity and role, the address clients
// reach it at, the resources it offers, a working-directory root, and the
// backend that reaches it.
type ManifestNode struct {
	Name      string            `json:"name"`
	Role      string            `json:"role,omitempty"`
	Address   string            `json:"address,omitempty"`
	Resources ManifestResources `json:"resources,omitempty"`
	Scratch   string            `json:"scratch"`
	Backend   BackendDescriptor `json:"backend"`
	Ports     *PortRange        `json:"ports,omitempty"`
}

// ManifestResources is the JSON form of a node's Resources. Optional quantities
// are pointers so an omitted field stays unspecified rather than becoming zero.
type ManifestResources struct {
	CPUs     *float64 `json:"cpus,omitempty"`
	MemoryMB *int64   `json:"memory_mb,omitempty"`
	Labels   []string `json:"labels,omitempty"`
}

// resources converts the manifest form to the internal Resources.
func (r ManifestResources) resources() Resources {
	res := Resources{Labels: NewLabels(r.Labels...)}
	if r.CPUs != nil {
		res.CPUs = Some(*r.CPUs)
	}
	if r.MemoryMB != nil {
		res.MemoryMB = Some(int(*r.MemoryMB))
	}
	return res
}

// LoadManifest reads and parses a manifest from a JSON file.
func LoadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("manifest: read %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("manifest: parse %s: %w", path, err)
	}
	return m, nil
}

// PoolFromManifest builds a Pool from a manifest. It rejects an empty manifest,
// a node without a name or scratch root, a duplicate name, and a backend kind
// that is neither the built-in "local" nor a registered remote kind (a likely
// missing blank import). Nodes on the same host share one port allocator so
// co-located services never lease the same port. A node's live backend is built
// only for the local kind: the driver never dials a remote node, so its backend
// stays nil here and the worker rebuilds it from the descriptor.
func PoolFromManifest(m Manifest) (*Pool, error) {
	if len(m.Nodes) == 0 {
		return nil, fmt.Errorf("manifest: no nodes")
	}
	seen := make(map[string]struct{}, len(m.Nodes))
	allocators := make(map[string]*PortAllocator)
	nodes := make([]*Node, len(m.Nodes))
	for i, mn := range m.Nodes {
		if mn.Name == "" {
			return nil, fmt.Errorf("manifest: node %d has no name", i)
		}
		if _, dup := seen[mn.Name]; dup {
			return nil, fmt.Errorf("manifest: duplicate node name %q", mn.Name)
		}
		seen[mn.Name] = struct{}{}
		if mn.Scratch == "" {
			return nil, fmt.Errorf("manifest: node %q has no scratch", mn.Name)
		}

		var backend Backend
		switch mn.Backend.Kind {
		case "", "local":
			backend = LocalBackend{}
		default:
			if _, ok := lookupBackend(mn.Backend.Kind); !ok {
				return nil, fmt.Errorf("manifest: node %q has unknown backend kind %q (is its package blank-imported?)", mn.Name, mn.Backend.Kind)
			}
		}

		ports, err := nodeAllocator(allocators, mn)
		if err != nil {
			return nil, err
		}
		nodes[i] = NewNode(NodeConfig{
			Name:       mn.Name,
			Role:       mn.Role,
			Addr:       mn.Address,
			Resources:  mn.Resources.resources(),
			Backend:    backend,
			Descriptor: mn.Backend,
			Scratch:    Scratch{Root: mn.Scratch},
			Ports:      ports,
		})
	}
	return NewPool(nodes), nil
}

// nodeAllocator returns the port allocator for a node. A node with an explicit
// range gets its own range allocator; otherwise it shares a probe allocator with
// other nodes on the same host (its reachable address, then backend host, then
// the loopback), created the first time that host is seen.
func nodeAllocator(allocators map[string]*PortAllocator, mn ManifestNode) (*PortAllocator, error) {
	if r := mn.Ports; r != nil {
		if r.Min <= 0 || r.Max <= r.Min {
			return nil, fmt.Errorf("manifest: node %q has an invalid port range [%d,%d)", mn.Name, r.Min, r.Max)
		}
		return NewRangePortAllocator(r.Min, r.Max), nil
	}
	host := mn.Address
	if host == "" {
		host = mn.Backend.Host
	}
	if host == "" {
		host = defaultPortHost
	}
	if a, ok := allocators[host]; ok {
		return a, nil
	}
	a := NewPortAllocator(host)
	allocators[host] = a
	return a, nil
}
