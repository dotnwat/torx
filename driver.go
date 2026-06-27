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
}

// RunOptions configure a driver run.
type RunOptions struct {
	MaxParallel int           // maximum concurrent jobs (default 1)
	Timeout     time.Duration // per-job timeout; 0 means none
	ExitFirst   bool          // stop scheduling after the first failure
	ResultsDir  string        // results root passed to each worker
	Sink        EventSink     // receives every job's events; must be concurrency-safe
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

	type plan struct {
		req  JobRequest
		spec PoolSpec
	}

	var results []JobResult
	var pending []plan
	for _, req := range requests {
		spec, err := sizeJob(req)
		if err != nil {
			results = append(results, failResult(variantID(req.ID, req.Params), err))
			continue
		}
		if !pool.CanEverFit(spec) {
			results = append(results, failResult(variantID(req.ID, req.Params),
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
				results = append(results, failResult(variantID(p.req.ID, p.req.Params), err))
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
		results = append(results, res)
		if (opts.ExitFirst && res.Status == StatusFail) || ctx.Err() != nil {
			stop = true
		}
	}
	return SuiteResult{Jobs: results}
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
		res = failResult(id, err)
	}
	pool.Free(sub)
	done <- res
}

func sizeJob(req JobRequest) (PoolSpec, error) {
	factory, ok := lookupJob(req.ID)
	if !ok {
		return PoolSpec{}, fmt.Errorf("driver: unknown job %q", req.ID)
	}
	jc := NewJobContext(req.Params, nil)
	factory().Declare(jc)
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

// descriptorOf serializes a node for an assignment. v1 nodes are local.
func descriptorOf(n *Node) NodeDescriptor {
	return NodeDescriptor{
		Name:    n.Name(),
		Role:    n.Role(),
		Scratch: n.Scratch().Root,
		Backend: BackendDescriptor{Kind: "local"},
	}
}

func failResult(id string, err error) JobResult {
	return JobResult{ID: id, Status: StatusFail, Error: ErrorInfoFrom(err)}
}
