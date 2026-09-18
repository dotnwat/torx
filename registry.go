// The job registry: jobs register themselves by id so the framework can
// discover them and a worker can reconstruct one from its id. A job file
// registers a factory in an init function:
//
//	func init() {
//		torx.Register("redis/compat.RoundTrip", func() torx.Job { return &RoundTrip{} })
//	}
//
// Because the driver and worker are the same binary, both see the same
// registrations, so a worker rebuilds a job from its id with a registry lookup
// rather than re-importing code.

package torx

import (
	"sort"
	"sync"
)

var registry = struct {
	mu        sync.Mutex
	factories map[string]func() Job
}{factories: map[string]func() Job{}}

// Register records a factory for the job identified by id. The factory returns a
// fresh job each call. Register panics on a duplicate id, since ids are
// compile-time constants and a collision is a programming error.
func Register(id string, factory func() Job) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, dup := registry.factories[id]; dup {
		panic("torx: duplicate job registration: " + id)
	}
	registry.factories[id] = factory
}

// RegisteredJobs returns the ids of all registered jobs, sorted.
func RegisteredJobs() []string {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	ids := make([]string, 0, len(registry.factories))
	for id := range registry.factories {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// lookupJob returns the factory registered for id.
func lookupJob(id string) (func() Job, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	f, ok := registry.factories[id]
	return f, ok
}
