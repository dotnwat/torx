// Node: a logical execution target.
//
// A Node bundles a Backend (the transport that runs commands and moves files)
// with the per-node coordination a service needs: a scratch directory, a view
// of free-port allocation shared with other nodes on the same host, the node's
// resource capacity and labels, a role, and a logical name. Services bind to
// Nodes and drive them through the promoted Backend methods, never touching the
// transport directly. On a single host, nodes stay disjoint through their
// separate scratch roots and a shared port allocator.
package torx

// Node is a logical execution target: a Backend plus per-node coordination. Its
// identity and resources are fixed at construction, so a Node is safe for the
// concurrent use that fanning out across nodes implies.
type Node struct {
	// Backend is embedded so a Node exposes the transport's methods (Exec,
	// WriteFile, Signal, ...) directly.
	Backend

	name      string
	role      string
	resources Resources
	scratch   Scratch
	ports     *PortAllocator
}

// NodeConfig describes how to build a Node. Ports is shared across nodes
// co-located on the same host, so they never lease the same port.
type NodeConfig struct {
	Name      string
	Role      string
	Resources Resources
	Backend   Backend
	Scratch   Scratch
	Ports     *PortAllocator
}

// NewNode builds a Node from cfg.
func NewNode(cfg NodeConfig) *Node {
	return &Node{
		Backend:   cfg.Backend,
		name:      cfg.Name,
		role:      cfg.Role,
		resources: cfg.Resources,
		scratch:   cfg.Scratch,
		ports:     cfg.Ports,
	}
}

// Name is the node's logical identity.
func (n *Node) Name() string { return n.name }

// Role is the node's human-facing role, or "" if unset.
func (n *Node) Role() string { return n.role }

// Resources is the node's capacity.
func (n *Node) Resources() Resources { return n.resources }

// Scratch is the node's working-directory root.
func (n *Node) Scratch() Scratch { return n.scratch }

// ServiceScratch derives a disjoint working-directory root for a service on this
// node. key must be a single path component (see MakeScratch).
func (n *Node) ServiceScratch(key string) Scratch {
	return MakeScratch(n.scratch.Root, key)
}

// AllocatePort leases a free TCP port from the host's shared allocator, so
// co-located nodes never pick the same one.
func (n *Node) AllocatePort() (int, error) {
	return n.ports.Allocate()
}

// ReleasePort returns a previously leased port to the shared allocator.
func (n *Node) ReleasePort(port int) {
	n.ports.Release(port)
}
