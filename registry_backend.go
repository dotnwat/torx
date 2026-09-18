// The backend registry: backend kinds register a builder by name.
//
// A backend whose transport pulls in heavy dependencies (SSH, Docker, ...) lives
// in its own package and registers a builder from an init function, so the core
// can construct it from a descriptor without importing it -- the same
// compile-time registration the job registry uses, like a database/sql driver. A
// suite that needs such a backend blank-imports its package to link the
// registration. The "local" backend is built into the core and is not
// registered.

package torx

import (
	"fmt"
	"sync"
)

// BackendBuilder constructs a live Backend for one node from its descriptor,
// interpreting the descriptor's kind-specific Config. A backend package
// registers one under its kind with RegisterBackend.
type BackendBuilder func(d BackendDescriptor) (Backend, error)

var backendRegistry = struct {
	mu       sync.Mutex
	builders map[string]BackendBuilder
}{builders: map[string]BackendBuilder{}}

// RegisterBackend records the builder for a backend kind, intended to be called
// from a backend package's init function. It panics if kind is empty or "local"
// (which the core reserves and builds directly), if builder is nil, or if kind
// is already registered: registration happens at startup, so any of these is a
// programming error rather than a runtime condition.
func RegisterBackend(kind string, builder BackendBuilder) {
	if kind == "" || kind == "local" {
		panic(fmt.Sprintf("torx: cannot register reserved backend kind %q", kind))
	}
	if builder == nil {
		panic("torx: nil backend builder for kind " + kind)
	}
	backendRegistry.mu.Lock()
	defer backendRegistry.mu.Unlock()
	if _, dup := backendRegistry.builders[kind]; dup {
		panic("torx: duplicate backend registration: " + kind)
	}
	backendRegistry.builders[kind] = builder
}

// lookupBackend returns the builder registered for kind.
func lookupBackend(kind string) (BackendBuilder, bool) {
	backendRegistry.mu.Lock()
	defer backendRegistry.mu.Unlock()
	b, ok := backendRegistry.builders[kind]
	return b, ok
}
