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
	// event stream into the on-disk trace alongside the caller's sink.
	jobDir := jobResultsDir(a.Session.ResultsDir, id)
	if jobDir != "" {
		if trace, err := newTraceSink(jobDir); err == nil {
			defer trace.Close()
			sink = teeSink{sinks: []EventSink{sink, trace}}
		}
	}
	// Carry the sink on the context so lifecycle and service code can narrate
	// into the trace; tag the worker's own lines, which services override with
	// their own name as they act.
	ctx = WithComponent(WithSink(ctx, sink), "worker")

	fail := func(err error) JobResult {
		res := JobResult{ID: id, Status: StatusFail, Start: start, Stop: time.Now(), Error: errorInfo(err)}
		writeResultJSON(jobDir, res)
		return res
	}

	factory, ok := lookupJob(a.JobID)
	if !ok {
		return fail(fmt.Errorf("worker: unknown job %q", a.JobID))
	}
	jc := NewJobContext(a.Params, sink)
	jc.resultsDir = jobDir
	job := factory()
	job.Declare(jc)

	nodes, err := buildNodes(a.Nodes)
	if err != nil {
		return fail(err)
	}
	if got, want := len(nodes), jc.PoolSpec().Size(); got != want {
		return fail(fmt.Errorf("worker: assignment has %d nodes, job needs %d", got, want))
	}
	jc.Bind(nodes)
	Emit(ctx, Event{Kind: EventRunning, Source: id})
	Logf(ctx, "info", "bound %d node(s): %s", len(nodes), nodeList(nodes))

	res := runJob(ctx, start, id, job, jc)
	writeResultJSON(jobDir, res)
	return res
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
	case tdErr != nil:
		res.Status = StatusFail
		res.Error = errorInfo(tdErr)
	default:
		res.Status = StatusPass
	}
	Emit(ctx, Event{Kind: EventFinished, Source: id, Message: string(res.Status)})
	return res
}

func buildNodes(descs []NodeDescriptor) ([]*Node, error) {
	ports := NewPortAllocator("")
	nodes := make([]*Node, len(descs))
	for i, d := range descs {
		backend, err := buildBackend(d.Backend)
		if err != nil {
			return nil, err
		}
		nodes[i] = NewNode(NodeConfig{
			Name:    d.Name,
			Role:    d.Role,
			Backend: backend,
			Scratch: Scratch{Root: d.Scratch},
			Ports:   ports,
		})
	}
	return nodes, nil
}

func buildBackend(d BackendDescriptor) (Backend, error) {
	switch d.Kind {
	case "", "local":
		return LocalBackend{}, nil
	default:
		return nil, fmt.Errorf("worker: unknown backend kind %q", d.Kind)
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
	var pe *panicError
	if errors.As(err, &pe) {
		info.Stack = pe.stack
	}
	return info
}
