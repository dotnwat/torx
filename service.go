// Service: a group of processes deployed across a set of nodes.
//
// The framework drives a service only through the coarse Service lifecycle
// (Start/Stop/Clean/Wait), so a service is free to implement those however it
// needs -- a per-node group, a single remote API call, a coordinated rolling
// restart. ServiceBase covers the common case: embed it, pass it the concrete
// service as its per-node hooks, and implement those hooks; it then supplies a
// standard lifecycle that stops and cleans each node before starting it. To
// customize a phase, override the corresponding method -- it shadows the default
// and the registry and jobs, which dispatch through the interface, call the
// override. A ServiceRegistry stops (StopAll) then cleans (CleanAll) a job's
// services in reverse registration order, running every step even on failure and
// aggregating the errors; a job's Teardown composes the two, collecting artifacts
// between them.
package torx

import (
	"context"
	"fmt"
	"sync"
)

// Service is a group of processes deployed across a set of nodes. The framework
// interacts with a service only through this interface, so any implementation of
// the lifecycle is acceptable. Name, Spec, Nodes, and Bind come from an embedded
// *ServiceBase.
type Service interface {
	// Name identifies the service.
	Name() string
	// Spec is the service's node demand, configured at construction.
	Spec() PoolSpec
	// Nodes are the nodes bound to the service. Do not mutate the result.
	Nodes() []*Node
	// Bind gives the service the nodes the framework allocated for it.
	Bind(nodes []*Node)

	// Start brings the service up on its nodes.
	Start(ctx context.Context) error
	// Stop halts the service.
	Stop(ctx context.Context) error
	// Clean removes the service's persistent state.
	Clean(ctx context.Context) error
	// Wait blocks until the service is ready.
	Wait(ctx context.Context) error
}

// PerNode is the per-node lifecycle ServiceBase drives across a service's nodes.
// A service that uses the default lifecycle implements these; a service that
// overrides every lifecycle method need not.
type PerNode interface {
	StartNode(ctx context.Context, n *Node) error
	StopNode(ctx context.Context, n *Node) error
	CleanNode(ctx context.Context, n *Node) error
	WaitNode(ctx context.Context, n *Node) error
}

// ServiceBase supplies a service's identity, node binding, and a default
// per-node lifecycle. A concrete service embeds *ServiceBase, passes itself as
// the hooks, and implements those hooks; overriding a lifecycle method shadows
// the default for that phase.
type ServiceBase struct {
	name  string
	spec  PoolSpec
	hooks PerNode
	nodes []*Node

	mu        sync.Mutex
	artifacts map[string][]Artifact // node name -> artifacts to collect
}

// NewServiceBase builds a ServiceBase. hooks is the concrete service, driven by
// the default lifecycle; it may be nil if the service overrides every lifecycle
// method. name becomes a directory component in the results tree and a scratch
// key, so it must be a single non-traversal path component; a bad name is a
// programming error and panics.
func NewServiceBase(name string, spec PoolSpec, hooks PerNode) *ServiceBase {
	if !validComponent(name) {
		panic(fmt.Sprintf("torx: service name must be a single non-traversal path component: %q", name))
	}
	return &ServiceBase{name: name, spec: spec, hooks: hooks}
}

// Name identifies the service.
func (b *ServiceBase) Name() string { return b.name }

// Spec is the service's node demand.
func (b *ServiceBase) Spec() PoolSpec { return b.spec }

// Nodes are the nodes bound to the service.
func (b *ServiceBase) Nodes() []*Node { return b.nodes }

// Bind gives the service its allocated nodes.
func (b *ServiceBase) Bind(nodes []*Node) { b.nodes = nodes }

// AddArtifact registers a node-local file to collect after the job, placed under
// the service's directory in the results tree. Use it for outputs the framework
// does not capture automatically -- a --log-file target, a data dump, a metrics
// file. StartCaptured uses it to register captured console output.
func (b *ServiceBase) AddArtifact(n *Node, a Artifact) {
	if !validComponent(a.Name) {
		// a.Name is joined into the collection destination, so a separator or ".."
		// could write the artifact outside the job's results directory.
		panic(fmt.Sprintf("torx: artifact name must be a single non-traversal path component: %q", a.Name))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.artifacts == nil {
		b.artifacts = make(map[string][]Artifact)
	}
	b.artifacts[n.Name()] = append(b.artifacts[n.Name()], a)
}

// Artifacts returns the artifacts registered for n, implementing Archiver. A
// service may override this to compute its artifacts dynamically instead.
func (b *ServiceBase) Artifacts(n *Node) []Artifact {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Artifact(nil), b.artifacts[n.Name()]...)
}

// Start brings each node to a known state -- stopping any prior instance and
// removing stale data -- and then starts it, returning on the first failure to
// either establish that state or start, and leaving teardown to stop whatever
// already came up. A node whose preparation failed is not started: a second
// instance must not come up beside a process that could not be stopped, and a
// run must not proceed against data that could not be cleaned.
func (b *ServiceBase) Start(ctx context.Context) error {
	ctx = WithComponent(ctx, b.name)
	Logf(ctx, "info", "starting")
	for _, n := range b.nodes {
		if err := b.prepareNode(ctx, n); err != nil {
			return err
		}
		if err := b.hooks.StartNode(ctx, n); err != nil {
			return Wrap(ErrService, "service: start "+b.name, err)
		}
	}
	return nil
}

// prepareNode establishes n's known pre-start state by stopping any prior
// instance and removing stale data, aggregating both errors so neither is
// masked. A non-nil result means the precondition could not be established and
// the caller must not start onto the node.
func (b *ServiceBase) prepareNode(ctx context.Context, n *Node) error {
	var errs MultiError
	if err := b.hooks.StopNode(ctx, n); err != nil {
		errs.Append(Wrap(ErrService, "service: pre-stop "+b.name, err))
	}
	if err := b.hooks.CleanNode(ctx, n); err != nil {
		errs.Append(Wrap(ErrService, "service: pre-clean "+b.name, err))
	}
	return errs.Err()
}

// Stop stops every node, continuing past failures and aggregating the errors.
func (b *ServiceBase) Stop(ctx context.Context) error {
	ctx = WithComponent(ctx, b.name)
	Logf(ctx, "info", "stopping")
	var errs MultiError
	for _, n := range b.nodes {
		if err := b.hooks.StopNode(ctx, n); err != nil {
			errs.Append(Wrap(ErrService, "service: stop "+b.name, err))
		}
	}
	return errs.Err()
}

// Clean cleans every node, continuing past failures.
func (b *ServiceBase) Clean(ctx context.Context) error {
	ctx = WithComponent(ctx, b.name)
	Logf(ctx, "info", "cleaning")
	var errs MultiError
	for _, n := range b.nodes {
		if err := b.hooks.CleanNode(ctx, n); err != nil {
			errs.Append(Wrap(ErrService, "service: clean "+b.name, err))
		}
	}
	return errs.Err()
}

// Wait waits for every node, continuing past failures.
func (b *ServiceBase) Wait(ctx context.Context) error {
	ctx = WithComponent(ctx, b.name)
	Logf(ctx, "info", "waiting for readiness")
	var errs MultiError
	for _, n := range b.nodes {
		if err := b.hooks.WaitNode(ctx, n); err != nil {
			errs.Append(Wrap(ErrService, "service: wait "+b.name, err))
		}
	}
	if errs.Err() == nil {
		Logf(ctx, "info", "ready")
	}
	return errs.Err()
}

// ServiceRegistry records the services a job creates so they can be torn down
// together. It is safe for concurrent use.
type ServiceRegistry struct {
	mu       sync.Mutex
	services []Service
}

// Add registers svc.
func (r *ServiceRegistry) Add(svc Service) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services = append(r.services, svc)
}

// Services returns the registered services in registration order.
func (r *ServiceRegistry) Services() []Service {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Service(nil), r.services...)
}

// reversed returns the registered services in reverse registration order.
func (r *ServiceRegistry) reversed() []Service {
	all := r.Services()
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	return all
}

// StopAll stops every service in reverse registration order, dispatching to each
// service's Stop and running each even if some fail.
func (r *ServiceRegistry) StopAll(ctx context.Context) error {
	var errs MultiError
	for _, svc := range r.reversed() {
		errs.Append(svc.Stop(ctx))
	}
	return errs.Err()
}

// CleanAll cleans every service in reverse registration order.
func (r *ServiceRegistry) CleanAll(ctx context.Context) error {
	var errs MultiError
	for _, svc := range r.reversed() {
		errs.Append(svc.Clean(ctx))
	}
	return errs.Err()
}
