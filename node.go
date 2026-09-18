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

	name       string
	role       string
	addr       string
	resources  Resources
	scratch    Scratch
	ports      *PortAllocator
	descriptor BackendDescriptor
}

// NodeConfig describes how to build a Node. Backend is the live transport for
// in-process use; Descriptor is the serializable recipe the driver hands a
// worker so it can rebuild that transport (see descriptorOf). Addr is the node's
// reachable address, defaulting to the backend host and then the loopback. Ports
// is shared across nodes co-located on the same host, so they never lease the
// same port.
type NodeConfig struct {
	Name       string
	Role       string
	Addr       string
	Resources  Resources
	Backend    Backend
	Descriptor BackendDescriptor
	Scratch    Scratch
	Ports      *PortAllocator
}

// NewNode builds a Node from cfg.
func NewNode(cfg NodeConfig) *Node {
	return &Node{
		Backend:    cfg.Backend,
		name:       cfg.Name,
		role:       cfg.Role,
		addr:       cfg.Addr,
		resources:  cfg.Resources,
		scratch:    cfg.Scratch,
		ports:      cfg.Ports,
		descriptor: cfg.Descriptor,
	}
}

// Name is the node's logical identity.
func (n *Node) Name() string { return n.name }

// Role is the node's human-facing role, or "" if unset.
func (n *Node) Role() string { return n.role }

// defaultNodeAddr is the reachable address assumed when a node specifies none,
// matching the single-host case where everything runs on the loopback.
const defaultNodeAddr = "127.0.0.1"

// Addr is the address at which the node is reachable -- what a service binds and
// advertises to clients. It falls back to the backend's host and finally to the
// loopback, so a single-host run needs no explicit address.
func (n *Node) Addr() string {
	switch {
	case n.addr != "":
		return n.addr
	case n.descriptor.Host != "":
		return n.descriptor.Host
	default:
		return defaultNodeAddr
	}
}

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
