//go:build unix

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

// PortAllocator hands out distinct free TCP ports and tracks them so a port is
// never handed out twice; a single allocator should be shared across work
// co-located on the same host. It is safe for concurrent use. It works one of
// two ways: by probing (a bind to port 0 on its host) or by leasing from a fixed
// range. Either way there is an unavoidable race between returning a port and a
// service binding it -- another process could claim it first -- so callers
// confirm the service came up with a readiness check.
//
// Probing suits co-located local work, where torx can bind on the host. Range
// leasing suits a dedicated remote node whose port space torx neither shares nor
// can probe from the driver host.
type PortAllocator struct {
	host     string
	rangeMin int
	rangeMax int
	mu       sync.Mutex
	leased   map[int]struct{}
}

// NewPortAllocator returns an allocator that finds free ports on host by probing
// a bind to port 0. An empty host defaults to 127.0.0.1.
func NewPortAllocator(host string) *PortAllocator {
	return &PortAllocator{host: host}
}

// NewRangePortAllocator returns an allocator that hands out ports from the
// half-open range [lo, hi) without probing, for a node whose port space torx
// cannot probe from here. The caller's readiness check closes the bind race
// exactly as for the probe allocator.
func NewRangePortAllocator(lo, hi int) *PortAllocator {
	return &PortAllocator{rangeMin: lo, rangeMax: hi}
}

// PortRange bounds the TCP ports a node's allocator hands out, as the half-open
// interval [Min, Max). It is how a manifest and the wire describe a remote
// node's port space; an absent range means probe for free ports instead.
type PortRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
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
	if a.rangeMax > a.rangeMin {
		return a.allocateFromRange()
	}
	return a.allocateByProbe()
}

// allocateByProbe finds a free port by binding to port 0 on the host, skipping
// any it has already handed out. The caller holds a.mu.
func (a *PortAllocator) allocateByProbe() (int, error) {
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
		a.lease(port)
		return port, nil
	}
	return 0, Wrap(ErrAllocation, "coordination: allocate port",
		fmt.Errorf("no free port on %s after %d attempts", a.Host(), maxPortAttempts))
}

// allocateFromRange hands out the lowest free port in [rangeMin, rangeMax). The
// caller holds a.mu.
func (a *PortAllocator) allocateFromRange() (int, error) {
	for p := a.rangeMin; p < a.rangeMax; p++ {
		if _, taken := a.leased[p]; !taken {
			a.lease(p)
			return p, nil
		}
	}
	return 0, Wrap(ErrAllocation, "coordination: allocate port",
		fmt.Errorf("no free port in range [%d,%d)", a.rangeMin, a.rangeMax))
}

// lease records port as handed out. The caller holds a.mu.
func (a *PortAllocator) lease(port int) {
	if a.leased == nil {
		a.leased = make(map[int]struct{})
	}
	a.leased[port] = struct{}{}
}

// Range returns the allocator's lease range and whether it is in range mode.
func (a *PortAllocator) Range() (lo, hi int, ranged bool) {
	if a.rangeMax > a.rangeMin {
		return a.rangeMin, a.rangeMax, true
	}
	return 0, 0, false
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

// validComponent reports whether s is safe to use as a single path component: a
// non-empty name that is neither "." nor ".." and contains no separator, so
// joining it beneath a root cannot escape that root. It is the shared check for
// every framework identifier that becomes a directory name -- scratch keys, node
// and service names, artifact names -- since all of them are eventually joined
// into a scratch or results path.
func validComponent(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.Contains(s, "/")
}

// MakeScratch mints a scratch root base/key for a node or service. key must be a
// single, non-traversal path component (see validComponent) so that co-located
// work lands in disjoint, predictable directories beneath base. MakeScratch
// panics otherwise: keys are framework-generated identifiers, so a bad one is a
// programming error. The traversal check matters because the resulting root is
// later handed to a recursive remove during cleanup -- MakeScratch(base, "..")
// returning base's parent must never happen.
func MakeScratch(base, key string) Scratch {
	if !validComponent(key) {
		panic(fmt.Sprintf("torx: scratch key must be a single non-traversal path component: %q", key))
	}
	return Scratch{Root: path.Join(base, key)}
}
