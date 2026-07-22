// The driver: schedule jobs onto a pool and run them through workers.
//
// Run sizes each requested job by calling its Declare, fails the ones that can
// never fit the pool, and then schedules the rest largest-first: while a job
// fits the currently free nodes and fewer than MaxParallel are running, it
// allocates a sub-pool, launches the job through a WorkerLauncher, and on
// completion frees the sub-pool and records the result. How a job actually runs
// is the launcher's concern: InProcessLauncher runs it in this process (no
// isolation -- handy for tests and simple runs), while SelfExecLauncher spawns a
// worker subprocess. Cancelling the context (a deadline or a stop) halts
// scheduling and cancels running jobs; ExitFirst stops scheduling after the
// first failure.
package torx

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// JobRequest names a job to run and the parameters for this variant.
type JobRequest struct {
	ID     string
	Params Params

	// discErr, when non-nil, marks a request that could not be discovered because
	// the job's factory or Matrix panicked. The driver records it as a failing
	// result rather than scheduling it, so one broken job does not abort the run.
	discErr error
}

// RunOptions configure a driver run.
type RunOptions struct {
	MaxParallel int           // maximum concurrent jobs (default 1)
	Timeout     time.Duration // per-job timeout; 0 means none
	ExitFirst   bool          // stop scheduling after the first failure
	ResultsDir  string        // results root passed to each worker
	Sink        EventSink     // receives every job's events; must be concurrency-safe
	Reporters   []Reporter    // consume each result as it lands, then the aggregate
}

// WorkerLauncher runs one job described by an Assignment, forwarding its events
// to sink and returning its result. The error is non-nil only for a launch-level
// failure (e.g. a worker that could not be spawned), not a job failure.
type WorkerLauncher interface {
	Launch(ctx context.Context, a Assignment, sink EventSink) (JobResult, error)
}

// InProcessLauncher runs a job in the current process, with no isolation. It is
// the simplest launcher and the one tests use.
type InProcessLauncher struct{}

// Launch runs the job in process.
func (InProcessLauncher) Launch(ctx context.Context, a Assignment, sink EventSink) (JobResult, error) {
	return execute(ctx, a, sink), nil
}

type discardSink struct{}

func (discardSink) Emit(Event) {}

// sourceSink tags a job's events with its id before forwarding them.
type sourceSink struct {
	source string
	inner  EventSink
}

func (s sourceSink) Emit(e Event) {
	if e.Source == "" {
		e.Source = s.source
	}
	s.inner.Emit(e)
}

// Run schedules and runs the requested jobs against the pool using launcher and
// returns the aggregate result.
func Run(ctx context.Context, pool *Pool, launcher WorkerLauncher, requests []JobRequest, opts RunOptions) SuiteResult {
	if opts.MaxParallel < 1 {
		opts.MaxParallel = 1
	}

	// Persist the run when a results root is configured: a timestamped run
	// directory (with a "latest" symlink) whose path workers fill in per job.
	runDir := ""
	persistErr := ""
	if opts.ResultsDir != "" {
		stamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
		if d, err := makeRunDir(opts.ResultsDir, stamp); err == nil {
			runDir = d
			opts.ResultsDir = runDir
		} else {
			// The caller asked for a results tree and it could not be created. Run in
			// memory, but record the failure rather than silently turning a requested
			// tree into no results: Ok() and the CLI exit status must reflect it.
			persistErr = fmt.Sprintf("cannot create results tree under %q: %v", opts.ResultsDir, err)
			opts.ResultsDir = ""
		}
	}

	type plan struct {
		req  JobRequest
		spec PoolSpec
	}

	var results []JobResult
	// record keeps the aggregate and feeds each result to the reporters as it
	// lands; the driver records from a single goroutine, so reporters see results
	// one at a time and in completion order.
	record := func(res JobResult) {
		results = append(results, res)
		for _, rep := range opts.Reporters {
			rep.Report(res)
		}
	}

	var pending []plan
	for _, req := range requests {
		if req.discErr != nil {
			record(failResult(variantID(req.ID, req.Params), req.discErr))
			continue
		}
		spec, err := sizeJob(req)
		if err != nil {
			record(failResult(variantID(req.ID, req.Params), err))
			continue
		}
		if !pool.CanEverFit(spec) {
			record(failResult(variantID(req.ID, req.Params),
				fmt.Errorf("driver: job needs %d node(s), pool cannot satisfy it", spec.Size())))
			continue
		}
		pending = append(pending, plan{req: req, spec: spec})
	}
	// Largest first.
	sort.SliceStable(pending, func(i, j int) bool { return pending[i].spec.Size() > pending[j].spec.Size() })

	done := make(chan JobResult)
	active := 0
	stop := false

	for len(pending) > 0 || active > 0 {
		for !stop && ctx.Err() == nil && active < opts.MaxParallel {
			idx := -1
			for i := range pending {
				if pool.CanAllocate(pending[i].spec) {
					idx = i
					break
				}
			}
			if idx < 0 {
				break
			}
			p := pending[idx]
			pending = append(pending[:idx], pending[idx+1:]...)
			sub, err := pool.Allocate(p.spec)
			if err != nil {
				record(failResult(variantID(p.req.ID, p.req.Params), err))
				continue
			}
			active++
			go runOne(ctx, pool, launcher, p.req, sub, opts, done)
		}
		if active == 0 {
			break
		}
		res := <-done
		active--
		record(res)
		if (opts.ExitFirst && res.Status == StatusFail) || ctx.Err() != nil {
			stop = true
		}
	}

	// If scheduling stopped on purpose -- ExitFirst after a failure, or a cancelled
	// context -- any remaining pending jobs were deliberately not run (cancellation
	// is reflected by the run-level Cancelled flag below). Otherwise the loop ended
	// with jobs still pending only because the pool shrank below their demand: nodes
	// were quarantined after their cleanup could not be confirmed. Record a terminal
	// failure for those so they are not silently dropped.
	if !stop {
		for _, pl := range pending {
			record(failResult(variantID(pl.req.ID, pl.req.Params),
				fmt.Errorf("driver: no clean nodes remain; some were quarantined after teardown could not be confirmed")))
		}
	}

	// A cancelled context means scheduling stopped before every request was run,
	// so the suite is incomplete no matter how the recorded jobs fared. Marking it
	// here is what keeps Ok() -- and the CLI exit status -- from reporting success
	// for a run the operator or a deadline cut short.
	suite := SuiteResult{Jobs: results, Cancelled: ctx.Err() != nil, PersistErr: persistErr}
	if runDir != "" {
		writeRunJSON(runDir, suite)
	}
	for _, rep := range opts.Reporters {
		rep.Finish(suite)
	}
	return suite
}

func runOne(ctx context.Context, pool *Pool, launcher WorkerLauncher, req JobRequest, sub *SubPool, opts RunOptions, done chan<- JobResult) {
	jobCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		jobCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	id := variantID(req.ID, req.Params)
	var sink EventSink = discardSink{}
	if opts.Sink != nil {
		sink = sourceSink{source: id, inner: opts.Sink}
	}

	res, err := launcher.Launch(jobCtx, buildAssignment(req, sub, opts), sink)
	if err != nil {
		// A launch-level error means the worker could not be spawned, so the node
		// was never touched and stays clean.
		res = failResult(id, err)
	}
	// A dirty result is one whose node could not be confirmed clean; quarantine it
	// rather than returning it to the free set, where it could contaminate a later
	// job with a leftover service, held port, or stale data.
	if res.Dirty {
		pool.Evict(sub)
	} else {
		pool.Free(sub)
	}
	done <- res
}

func sizeJob(req JobRequest) (PoolSpec, error) {
	factory, ok := lookupJob(req.ID)
	if !ok {
		return PoolSpec{}, fmt.Errorf("driver: unknown job %q", req.ID)
	}
	jc := NewJobContext(req.Params, nil)
	// Declare is job-supplied code, and sizeJob runs it in the driver process
	// before any job is launched; a panic here must fail just this job rather
	// than take down the whole run.
	if err := recovered(func() error { factory().Declare(jc); return nil }); err != nil {
		return PoolSpec{}, fmt.Errorf("driver: job %q: %w", req.ID, err)
	}
	return jc.PoolSpec(), nil
}

func buildAssignment(req JobRequest, sub *SubPool, opts RunOptions) Assignment {
	nodes := sub.Nodes()
	descs := make([]NodeDescriptor, len(nodes))
	for i, n := range nodes {
		descs[i] = descriptorOf(n)
	}
	return Assignment{
		JobID:  req.ID,
		Params: req.Params,
		Nodes:  descs,
		Session: SessionConfig{
			ResultsDir: opts.ResultsDir,
			TimeoutMS:  int(opts.Timeout / time.Millisecond),
		},
	}
}

// descriptorOf serializes a node for an assignment, carrying the backend recipe
// the worker needs to rebuild its transport.
func descriptorOf(n *Node) NodeDescriptor {
	d := NodeDescriptor{
		Name:    n.Name(),
		Role:    n.Role(),
		Address: n.Addr(),
		Scratch: n.Scratch().Root,
		Backend: n.descriptor,
	}
	if n.ports != nil {
		if lo, hi, ranged := n.ports.Range(); ranged {
			d.Ports = &PortRange{Min: lo, Max: hi}
		}
	}
	return d
}

func failResult(id string, err error) JobResult {
	return JobResult{ID: id, Status: StatusFail, Error: ErrorInfoFrom(err)}
}
