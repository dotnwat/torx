// Job: one test or benchmark.
//
// A job Declares the services it needs (pure: it constructs and configures them
// and registers them, with no allocation), then runs through Setup, Run, and
// Teardown. Run returns nil to pass and non-nil to fail; a benchmark additionally
// records an opaque result payload and a one-line summary. The framework drives a
// job only through the Job interface, so JobBase's default Setup (start every
// declared service) and Teardown (tear the services down and run finalizers) are
// just defaults: a job overrides either to take control, and the override is what
// the framework calls. There is no separate test/benchmark mode -- a job is a
// benchmark precisely when it records data.

package torx

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Params are the configuration values for one job variant (its parametrization).
// Values arrive as JSON, so numbers may be float64; the typed getters account
// for that and fall back to a default when a key is absent or mistyped.
type Params map[string]any

// Int returns the value at key as an int, or def if absent or not numeric.
func (p Params) Int(key string, def int) int {
	switch v := p[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return def
	}
}

// String returns the value at key as a string, or def if absent or not a string.
func (p Params) String(key, def string) string {
	if v, ok := p[key].(string); ok {
		return v
	}
	return def
}

// Bool returns the value at key as a bool, or def if absent or not a bool.
func (p Params) Bool(key string, def bool) bool {
	if v, ok := p[key].(bool); ok {
		return v
	}
	return def
}

// Job is one test or benchmark. A concrete job embeds JobBase, implements
// Declare and Run, and may override Setup or Teardown.
type Job interface {
	// Declare registers and configures the services the job needs. It must be
	// pure: no allocation, no side effects beyond registering services, so the
	// framework can call it to size the job and the worker can call it to
	// reconstruct the job identically.
	Declare(jc *JobContext)
	// Setup prepares the job to run, typically by starting its services.
	Setup(ctx context.Context, jc *JobContext) error
	// Run is the test or benchmark body; nil passes, non-nil fails.
	Run(ctx context.Context, jc *JobContext) error
	// Teardown releases what the job set up.
	Teardown(ctx context.Context, jc *JobContext) error
}

// JobBase supplies default Setup and Teardown. A concrete job embeds it and
// implements Declare and Run; overriding Setup or Teardown shadows the default.
type JobBase struct{}

// Setup starts every declared service and then waits for each to be ready, in
// registration order, returning on the first failure. Override to control start
// order, start services lazily, or skip auto-start.
func (JobBase) Setup(ctx context.Context, jc *JobContext) error {
	services := jc.Services()
	for _, svc := range services {
		if err := svc.Start(ctx); err != nil {
			return err
		}
	}
	for _, svc := range services {
		if err := svc.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Teardown stops the job's services, collects their artifacts, cleans them, and
// runs its finalizers -- in that order, all in reverse registration order,
// aggregating every error so none is masked. Collection happens after stop so
// logs are complete and before clean so they are not deleted first. Override to
// customize, calling jc.CollectArtifacts between stopping and cleaning to keep the
// services' logs.
func (JobBase) Teardown(ctx context.Context, jc *JobContext) error {
	var errs MultiError
	errs.Append(jc.registry.StopAll(ctx))
	errs.Append(jc.CollectArtifacts(ctx))
	errs.Append(jc.registry.CleanAll(ctx))
	errs.Append(jc.finalizers.Run(ctx))
	return errs.Err()
}

// JobContext is the framework handle a job uses across its lifecycle: it holds
// the job's parameters, the services it declares, the event sink it logs to, and
// the result payload it records. It is safe for concurrent use.
type JobContext struct {
	Params Params

	registry   ServiceRegistry
	finalizers Finalizers
	sink       EventSink

	// resultsDir is the job's directory in the results tree, or "" when the run
	// is not persisting results. passed records the job outcome for collection
	// (a failure gathers all artifacts; a pass gathers only CollectOnPass ones).
	// Both are set by the worker before teardown.
	resultsDir string
	passed     bool

	mu      sync.Mutex
	data    json.RawMessage
	summary string
}

// NewJobContext returns a JobContext for the given params and event sink; sink
// may be nil (logging becomes a no-op).
func NewJobContext(params Params, sink EventSink) *JobContext {
	return &JobContext{Params: params, sink: sink}
}

// Register declares a service the job needs.
func (jc *JobContext) Register(svc Service) { jc.registry.Add(svc) }

// Services returns the declared services in registration order.
func (jc *JobContext) Services() []Service { return jc.registry.Services() }

// CollectArtifacts gathers each declared service's artifacts into the job's
// results directory, under <service>/<node>/. It is a no-op when the run is not
// persisting results. JobBase.Teardown calls it between stopping and cleaning
// services; a job overriding Teardown should call it there too to keep its
// services' logs.
func (jc *JobContext) CollectArtifacts(ctx context.Context) error {
	if jc.resultsDir == "" {
		return nil
	}
	var errs MultiError
	for _, svc := range jc.registry.Services() {
		arch, ok := svc.(Archiver)
		if !ok {
			continue
		}
		cctx := WithComponent(ctx, svc.Name())
		for _, n := range svc.Nodes() {
			arts := arch.Artifacts(n)
			if len(arts) == 0 {
				continue
			}
			names := make([]string, len(arts))
			for i, a := range arts {
				names[i] = a.Name
			}
			Logf(cctx, "info", "collecting from %s: %s", n.Name(), strings.Join(names, ", "))
			dest := filepath.Join(jc.resultsDir, svc.Name(), n.Name())
			errs.Append(Collect(cctx, n, arts, jc.passed, dest))
		}
	}
	return errs.Err()
}

// PoolSpec is the job's total node demand: the concatenation of its services'
// specs in registration order.
func (jc *JobContext) PoolSpec() PoolSpec {
	var nodes []NodeSpec
	for _, svc := range jc.registry.Services() {
		nodes = append(nodes, svc.Spec().Nodes...)
	}
	return PoolSpec{Nodes: nodes}
}

// Bind distributes nodes across the declared services in registration order;
// each service receives as many nodes as its spec requested. Pass the nodes
// allocated for PoolSpec, in that order -- a sub-pool's Nodes in the driver, or
// the nodes rebuilt from the assignment in the worker.
func (jc *JobContext) Bind(nodes []*Node) {
	i := 0
	for _, svc := range jc.registry.Services() {
		n := svc.Spec().Size()
		svc.Bind(nodes[i : i+n])
		i += n
	}
}

// Log emits a log event to the job's event sink, recording the caller's source
// location.
func (jc *JobContext) Log(level, message string) {
	if jc.sink != nil {
		jc.sink.Emit(Event{Kind: EventLog, Level: level, Message: message, Time: time.Now(), Site: callerSite(2)})
	}
}

// Record stores v, marshaled to JSON, as the job's opaque result Data. torx does
// not interpret it (see JobResult); a benchmark records whatever it measured.
func (jc *JobContext) Record(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("job: record: %w", err)
	}
	jc.mu.Lock()
	jc.data = b
	jc.mu.Unlock()
	return nil
}

// SetSummary sets the job's one-line human-readable result summary.
func (jc *JobContext) SetSummary(s string) {
	jc.mu.Lock()
	jc.summary = s
	jc.mu.Unlock()
}

// Defer registers a cleanup callback run during teardown, in reverse order.
func (jc *JobContext) Defer(fn func(context.Context) error) { jc.finalizers.Add(fn) }

// Data returns the recorded result payload, or nil.
func (jc *JobContext) Data() json.RawMessage {
	jc.mu.Lock()
	defer jc.mu.Unlock()
	return jc.data
}

// Summary returns the recorded result summary, or "".
func (jc *JobContext) Summary() string {
	jc.mu.Lock()
	defer jc.mu.Unlock()
	return jc.summary
}
