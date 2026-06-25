// Coordination primitives for work co-located on a single host.
//
// PortAllocator hands out distinct free TCP ports and remembers them, so
// multiple services or nodes sharing a host never pick the same one. Scratch
// computes per-node / per-service working-directory paths. These are in-process
// helpers; creating the directories happens on the node through its backend.
package torx

import (
	"fmt"
	"net"
	"path"
	"strings"
	"sync"
)

const (
	maxPortAttempts = 100
	defaultPortHost = "127.0.0.1"
)

// PortAllocator hands out distinct free TCP ports for one host and tracks them
// so a port is never handed out twice; a single allocator should be shared
// across work co-located on the same host. It is safe for concurrent use.
//
// The OS assigns an ephemeral port via a bind to port 0; the probe socket is
// closed before the port is returned so a service can bind it. There is an
// unavoidable race between returning a port and a service binding it -- another
// process could claim it first -- so callers confirm the service came up with a
// readiness check.
type PortAllocator struct {
	host   string
	mu     sync.Mutex
	leased map[int]struct{}
}

// NewPortAllocator returns an allocator for host. An empty host defaults to
// 127.0.0.1.
func NewPortAllocator(host string) *PortAllocator {
	return &PortAllocator{host: host}
}

// Host returns the host this allocator probes.
func (a *PortAllocator) Host() string {
	if a.host == "" {
		return defaultPortHost
	}
	return a.host
}

// Allocate returns a free TCP port this allocator has not already handed out,
// or an error wrapping ErrAllocation if it cannot find one within
// maxPortAttempts probes -- a fail-safe (such as an exhausted ephemeral range)
// rather than an unbounded loop.
func (a *PortAllocator) Allocate() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for range maxPortAttempts {
		port, err := probeFreePort(a.Host())
		if err != nil {
			return 0, Wrap(ErrAllocation, "coordination: allocate port", err)
		}
		// A handed-out port stays free at the OS level until its service binds
		// it, so a probe can legitimately return one that is still leased; skip
		// it so the port is never handed out twice. Port rotation makes this
		// rare, so the loop almost always exits on its first iteration.
		if _, taken := a.leased[port]; taken {
			continue
		}
		if a.leased == nil {
			a.leased = make(map[int]struct{})
		}
		a.leased[port] = struct{}{}
		return port, nil
	}
	return 0, Wrap(ErrAllocation, "coordination: allocate port",
		fmt.Errorf("no free port on %s after %d attempts", a.Host(), maxPortAttempts))
}

// Release returns a port to the allocator so it may be handed out again.
// Releasing a port that was not leased is a no-op.
func (a *PortAllocator) Release(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.leased, port)
}

// probeFreePort asks the OS for an unused TCP port on host via a bind to port
// 0. Closing the listener frees the port immediately -- an unconnected listener
// does not enter TIME_WAIT -- yet the next bind(0) returns a different port,
// because the kernel selects from ip_local_port_range at a rotating, randomized
// offset rather than re-offering the one just freed. That rotation is why
// Allocate almost always succeeds on its first probe, making n allocations cost
// O(n) probes rather than O(n^2). It is a variable so tests can exercise the
// exhaustion and error paths without real network access.
var probeFreePort = func(host string) (int, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Scratch is a working-directory root on a node, with a helper to derive paths
// beneath it. Paths use forward slashes (the node is treated as POSIX) and are
// computed in process; nothing is created on the local filesystem.
type Scratch struct {
	Root string
}

// Sub returns the path to a file or directory beneath this scratch root.
func (s Scratch) Sub(parts ...string) string {
	return path.Join(append([]string{s.Root}, parts...)...)
}

// MakeScratch mints a scratch root base/key for a node or service. key must be
// a single, non-empty path component (no separators) so that co-located work
// lands in disjoint, predictable directories. MakeScratch panics on a
// separator: keys are framework-generated identifiers, so one is a programming
// error.
func MakeScratch(base, key string) Scratch {
	if key == "" || strings.Contains(key, "/") {
		panic(fmt.Sprintf("torx: scratch key must be a non-empty path component: %q", key))
	}
	return Scratch{Root: path.Join(base, key)}
}
