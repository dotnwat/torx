//go:build unix

// The worker: run one job from an assignment and stream the result back.
//
// RunWorker reads an Assignment, reconstructs the job from its id (look it up,
// Declare it, rebuild its nodes, bind them), runs Setup/Run/Teardown, and
// streams lifecycle and log events followed by the final JobResult. Setup and
// Run observe the context, so cancelling it -- the driver's response to a
// deadline or a stop request -- unblocks them; Teardown then runs under a
// context detached from that cancellation so cleanup still completes. A panic in
// job code is recovered and turned into a failing result with its stack, rather
// than crashing the worker.

package torx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// RunWorker reads an assignment from in, runs the job, and streams messages to
// out. It returns an error only for a protocol or I/O failure; a job failure is
// reported as a failing result on the stream.
func RunWorker(ctx context.Context, in io.Reader, out io.Writer) error {
	a, err := DecodeAssignment(in)
	if err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	sink := &streamSink{w: NewMessageWriter(out)}
	result := execute(ctx, a, sink)
	if err := sink.write(ResultMessage(result)); err != nil {
		return fmt.Errorf("worker: write result: %w", err)
	}
	return nil
}

func execute(ctx context.Context, a Assignment, sink EventSink) JobResult {
	start := time.Now()
	// Results and events carry the variant id (base id plus parameters); the
	// factory is still looked up under the base id.
	id := variantID(a.JobID, a.Params)

	// When the run persists results, give the job its own directory and tee its
	// event stream into the on-disk trace alongside the caller's sink. A
	// persistence failure here -- the directory or the trace files could not be
	// created -- must not pass silently: the job would report its own status
	// while its slice of the requested results tree is missing. Such failures
	// accumulate into the result's PersistErr instead of failing the job.
	var persistErrs []string
	notePersist := func(err error) {
		if err != nil {
			persistErrs = append(persistErrs, err.Error())
		}
	}
	jobDir, dirErr := jobResultsDir(a.Session.ResultsDir, id)
	notePersist(dirErr)
	var trace *traceSink
	if jobDir != "" {
		if ts, err := newTraceSink(jobDir); err == nil {
			trace = ts
			sink = teeSink{sinks: []EventSink{sink, ts}}
		} else {
			notePersist(err)
		}
	}
	// The trace is closed in finish, before the result is finalized, so trace
	// write and close failures join PersistErr. This backstop only covers a
	// panic escaping past finish.
	defer func() {
		if trace != nil {
			_ = trace.Close()
		}
	}()
	// Carry the sink on the context so lifecycle and service code can narrate
	// into the trace; tag the worker's own lines, which services override with
	// their own name as they act.
	ctx = WithComponent(WithSink(ctx, sink), "worker")

	// finish closes the trace -- the job's last event has been emitted by the
	// time any path reaches it -- and stamps the variant's parameters and the
	// persistence failures seen so far, the job's own artifact writes that
	// failed among them, into the result, then writes result.json. Every
	// result carries the parameters, passing or failing, so a consumer never
	// has to decode them from the id. When the result.json write itself fails,
	// its error cannot land in the file that failed; it is carried on the
	// streamed result alone, which is how the driver learns of it.
	var jc *JobContext
	finish := func(res JobResult) JobResult {
		if trace != nil {
			notePersist(trace.Close())
			trace = nil
		}
		if jc != nil {
			for _, err := range jc.persistErrors() {
				notePersist(err)
			}
		}
		res.Params = a.Params
		res.Seed = a.Seed
		res.PersistErr = strings.Join(persistErrs, "; ")
		if err := writeResultJSON(jobDir, res); err != nil {
			notePersist(err)
			res.PersistErr = strings.Join(persistErrs, "; ")
		}
		return res
	}
	fail := func(err error) JobResult {
		return finish(JobResult{ID: id, Status: StatusFail, Start: start, Stop: time.Now(), Error: errorInfo(err)})
	}

	factory, ok := lookupJob(a.JobID)
	if !ok {
		return fail(fmt.Errorf("worker: unknown job %q", a.JobID))
	}
	jc = NewJobContext(a.Params, sink)
	jc.seed = a.Seed
	jc.resultsDir = jobDir
	// Declare and Bind are (or drive) job-supplied code; confine a panic in
	// either to this job's result, matching the recover wrapper Setup, Run, and
	// Teardown already run under.
	var job Job
	if err := recovered(func() error { job = factory(); job.Declare(jc); return nil }); err != nil {
		return fail(err)
	}

	nodes, err := buildNodes(a.Nodes)
	if err != nil {
		return fail(err)
	}
	if got, want := len(nodes), jc.PoolSpec().Size(); got != want {
		return fail(fmt.Errorf("worker: assignment has %d nodes, job needs %d", got, want))
	}
	if err := recovered(func() error { jc.Bind(nodes); return nil }); err != nil {
		return fail(err)
	}
	Emit(ctx, Event{Kind: EventRunning, Source: id})
	Logf(ctx, "info", "bound %d node(s): %s", len(nodes), nodeList(nodes))

	return finish(runJob(ctx, start, id, job, jc))
}

// nodeList renders bound nodes as "name (role), name, ..." for the trace.
func nodeList(nodes []*Node) string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name()
		if n.Role() != "" {
			names[i] += " (" + n.Role() + ")"
		}
	}
	return strings.Join(names, ", ")
}

func runJob(ctx context.Context, start time.Time, id string, job Job, jc *JobContext) JobResult {
	var runErr error
	if err := recovered(func() error { return job.Setup(ctx, jc) }); err != nil {
		runErr = err
	} else {
		runErr = recovered(func() error { return job.Run(ctx, jc) })
	}
	// Record the outcome before teardown so artifact collection can use it (a
	// failure gathers all artifacts; a pass gathers only the collect-on-pass set).
	jc.passed = runErr == nil
	// Teardown runs even when the context was cancelled, so detach from it.
	tdErr := recovered(func() error { return job.Teardown(context.WithoutCancel(ctx), jc) })

	res := JobResult{ID: id, Start: start, Stop: time.Now(), Summary: jc.Summary(), Data: jc.Data()}
	switch {
	case runErr != nil:
		res.Status = StatusFail
		res.Error = errorInfo(runErr)
		if tdErr != nil && res.Error != nil {
			// Keep the teardown failure visible rather than letting the run failure
			// mask it; the node is dirty either way.
			res.Error.Message += "; teardown also failed: " + tdErr.Error()
		}
	case tdErr != nil:
		res.Status = StatusFail
		res.Error = errorInfo(tdErr)
	default:
		res.Status = StatusPass
	}
	if tdErr != nil {
		// Stop/clean could not be confirmed, so a service may still be up or data
		// may be stale: the node is not safe to reuse.
		res.Dirty = true
	}
	Emit(ctx, Event{Kind: EventFinished, Source: id, Message: string(res.Status)})
	return res
}

func buildNodes(descs []NodeDescriptor) ([]*Node, error) {
	// Local nodes co-located on this host share one probe allocator; a node that
	// carries a range gets its own range allocator, since its port space is its
	// own and cannot be probed from here.
	local := NewPortAllocator("")
	nodes := make([]*Node, len(descs))
	for i, d := range descs {
		backend, err := buildBackend(d.Backend)
		if err != nil {
			return nil, err
		}
		ports := local
		if d.Ports != nil {
			ports = NewRangePortAllocator(d.Ports.Min, d.Ports.Max)
		}
		nodes[i] = NewNode(NodeConfig{
			Name:       d.Name,
			Role:       d.Role,
			Addr:       d.Address,
			Backend:    backend,
			Descriptor: d.Backend,
			Scratch:    Scratch{Root: d.Scratch},
			Ports:      ports,
		})
	}
	return nodes, nil
}

func buildBackend(d BackendDescriptor) (Backend, error) {
	switch d.Kind {
	case "", "local":
		return localBackendFrom(d)
	default:
		if build, ok := lookupBackend(d.Kind); ok {
			return build(d)
		}
		return nil, fmt.Errorf("worker: unknown backend kind %q (is its package blank-imported?)", d.Kind)
	}
}

// streamSink forwards a job's events to the driver as wire messages.
type streamSink struct {
	mu sync.Mutex
	w  *MessageWriter
}

func (s *streamSink) Emit(e Event) { _ = s.write(EventMessage(e)) }

func (s *streamSink) write(m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(m)
}

// panicError carries a recovered panic value and the stack at the recover point.
type panicError struct {
	value any
	stack string
}

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v", e.value) }

// recovered runs fn, converting a panic into a *panicError.
func recovered(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{value: r, stack: string(debug.Stack())}
		}
	}()
	return fn()
}

// errorInfo builds an ErrorInfo, attaching the stack when err is a recovered
// panic.
func errorInfo(err error) *ErrorInfo {
	info := ErrorInfoFrom(err)
	if info == nil {
		return nil
	}
	if pe, ok := errors.AsType[*panicError](err); ok {
		info.Stack = pe.stack
	}
	return info
}
