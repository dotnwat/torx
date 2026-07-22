package torx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	Register("dtest.job", func() Job { return &dtestJob{} })
}

var (
	dtestConcurrent    atomic.Int32
	dtestMaxConcurrent atomic.Int32
)

// dtestJob is a configurable driver-test job: "size" sets its node demand, and
// "block"/"track"/"fail" select Run behavior.
type dtestJob struct{ JobBase }

func (*dtestJob) Declare(jc *JobContext) {
	if n := jc.Params.Int("size", 1); n > 0 {
		jc.Register(newFakeServiceSpec("s", new([]string), n))
	}
}

func (*dtestJob) Run(ctx context.Context, jc *JobContext) error {
	switch {
	case jc.Params.Bool("block", false):
		<-ctx.Done()
		return ctx.Err()
	case jc.Params.Bool("track", false):
		c := dtestConcurrent.Add(1)
		for {
			m := dtestMaxConcurrent.Load()
			if c <= m || dtestMaxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		dtestConcurrent.Add(-1)
	case jc.Params.Bool("fail", false):
		return errors.New("nope")
	}
	return nil
}

func testPool(n int) *Pool {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = testNode(fmt.Sprintf("n%d", i))
	}
	return NewPool(nodes)
}

func sizedRequests(n, size int, extra Params) []JobRequest {
	reqs := make([]JobRequest, n)
	for i := range reqs {
		p := Params{"size": size}
		for k, v := range extra {
			p[k] = v
		}
		reqs[i] = JobRequest{ID: "dtest.job", Params: p}
	}
	return reqs
}

func TestRunSchedulesAllJobs(t *testing.T) {
	res := Run(context.Background(), testPool(2), InProcessLauncher{}, sizedRequests(5, 1, nil), RunOptions{MaxParallel: 2})
	if len(res.Jobs) != 5 {
		t.Fatalf("ran %d jobs, want 5", len(res.Jobs))
	}
	if !res.Ok() {
		t.Errorf("not all jobs passed:\n%s", res.Render())
	}
}

func TestSizeJobRecoversDeclarePanic(t *testing.T) {
	if _, err := sizeJob(JobRequest{ID: "wtest.declarepanic"}); err == nil {
		t.Fatal("sizeJob should return an error when Declare panics, not propagate the panic")
	} else if !strings.Contains(err.Error(), "declare-boom") {
		t.Errorf("error = %v, want it to mention declare-boom", err)
	}
}

func TestRunFailsDeclarePanicWithoutCrashing(t *testing.T) {
	res := Run(context.Background(), testPool(1), InProcessLauncher{}, []JobRequest{{ID: "wtest.declarepanic"}}, RunOptions{})
	if len(res.Jobs) != 1 {
		t.Fatalf("got %d job results, want 1", len(res.Jobs))
	}
	if res.Jobs[0].Status != StatusFail {
		t.Errorf("status = %v, want FAIL", res.Jobs[0].Status)
	}
}

func TestRunRespectsParallelismCap(t *testing.T) {
	dtestConcurrent.Store(0)
	dtestMaxConcurrent.Store(0)

	// Pool (4) is larger than MaxParallel (2), so concurrency is bounded by
	// MaxParallel, not the pool.
	res := Run(context.Background(), testPool(4), InProcessLauncher{}, sizedRequests(6, 1, Params{"track": true}), RunOptions{MaxParallel: 2})
	if len(res.Jobs) != 6 || !res.Ok() {
		t.Fatalf("jobs=%d ok=%v", len(res.Jobs), res.Ok())
	}
	if got := dtestMaxConcurrent.Load(); got != 2 {
		t.Errorf("max concurrent = %d, want 2 (MaxParallel cap)", got)
	}
}

func TestRunUnschedulable(t *testing.T) {
	// Needs 5 nodes; pool has 2.
	res := Run(context.Background(), testPool(2), InProcessLauncher{}, sizedRequests(1, 5, nil), RunOptions{})
	if len(res.Jobs) != 1 || res.Jobs[0].Status != StatusFail {
		t.Fatalf("result = %+v, want one FAIL", res.Jobs)
	}
}

func TestRunUnknownJob(t *testing.T) {
	res := Run(context.Background(), testPool(1), InProcessLauncher{}, []JobRequest{{ID: "dtest.nope"}}, RunOptions{})
	if len(res.Jobs) != 1 || res.Jobs[0].Status != StatusFail {
		t.Fatalf("result = %+v, want one FAIL", res.Jobs)
	}
}

func TestRunDeadline(t *testing.T) {
	res := Run(context.Background(), testPool(1), InProcessLauncher{},
		sizedRequests(1, 1, Params{"block": true}),
		RunOptions{Timeout: 50 * time.Millisecond})
	if len(res.Jobs) != 1 || res.Jobs[0].Status != StatusFail {
		t.Fatalf("result = %+v, want FAIL from deadline", res.Jobs)
	}
}

func TestRunExitFirst(t *testing.T) {
	reqs := []JobRequest{
		{ID: "dtest.job", Params: Params{"size": 1, "fail": true}},
		{ID: "dtest.job", Params: Params{"size": 1}},
		{ID: "dtest.job", Params: Params{"size": 1}},
	}
	res := Run(context.Background(), testPool(1), InProcessLauncher{}, reqs, RunOptions{MaxParallel: 1, ExitFirst: true})
	if len(res.Jobs) != 1 {
		t.Fatalf("ran %d jobs, want 1 (exit-first stops scheduling)", len(res.Jobs))
	}
	if res.Jobs[0].Status != StatusFail {
		t.Errorf("result = %v, want FAIL", res.Jobs[0].Status)
	}
}

func TestRunPartialResultOnCancel(t *testing.T) {
	reqs := sizedRequests(4, 1, Params{"block": true})
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan SuiteResult, 1)
	go func() {
		ch <- Run(ctx, testPool(2), InProcessLauncher{}, reqs, RunOptions{MaxParallel: 2})
	}()

	time.Sleep(30 * time.Millisecond) // let 2 jobs launch and block
	cancel()

	res := <-ch
	if len(res.Jobs) != 2 {
		t.Fatalf("recorded %d jobs, want 2 (the rest were never scheduled)", len(res.Jobs))
	}
	for _, j := range res.Jobs {
		if j.Status != StatusFail {
			t.Errorf("job status = %v, want FAIL from cancellation", j.Status)
		}
	}
}

func TestRunPreCancelledReportsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the scheduler launches nothing

	res := Run(ctx, testPool(1), InProcessLauncher{}, sizedRequests(3, 1, nil), RunOptions{})
	if !res.Cancelled {
		t.Errorf("res.Cancelled = false, want true for a pre-cancelled run")
	}
	if res.Ok() {
		t.Errorf("a cancelled run that ran nothing reported success: %+v", res)
	}
}

func TestRunNotCancelledOnCleanRun(t *testing.T) {
	res := Run(context.Background(), testPool(2), InProcessLauncher{}, sizedRequests(3, 1, nil), RunOptions{MaxParallel: 2})
	if res.Cancelled {
		t.Errorf("res.Cancelled = true for a run that completed normally")
	}
	if !res.Ok() {
		t.Errorf("clean run not Ok:\n%s", res.Render())
	}
}

func TestRunForwardsEventsTaggedBySource(t *testing.T) {
	var sink InMemoryEventSink
	Run(context.Background(), testPool(1), InProcessLauncher{},
		[]JobRequest{{ID: "wtest.pass"}}, RunOptions{Sink: &sink})

	tagged := false
	for _, e := range sink.Events() {
		if e.Kind == EventLog && e.Message == "ran" && e.Source == "wtest.pass" {
			tagged = true
		}
	}
	if !tagged {
		t.Errorf("expected a 'ran' log tagged with the job id; got %+v", sink.Events())
	}
}

func TestDescriptorOfCarriesBackendRecipe(t *testing.T) {
	// descriptorOf must emit the node's own backend recipe, not a hardcoded
	// "local", so a remote node's transport can be rebuilt in the worker.
	ssh := descriptorOf(NewNode(NodeConfig{
		Name:       "n0",
		Scratch:    MakeScratch("/tmp/torx-test", "n0"),
		Descriptor: BackendDescriptor{Kind: "ssh", Host: "10.0.0.5"},
	}))
	if ssh.Backend.Kind != "ssh" || ssh.Backend.Host != "10.0.0.5" {
		t.Errorf("ssh node descriptor backend = %+v, want {ssh 10.0.0.5}", ssh.Backend)
	}
	if ssh.Address != "10.0.0.5" {
		t.Errorf("ssh node descriptor address = %q, want 10.0.0.5 (defaulted from host)", ssh.Address)
	}

	local := descriptorOf(NewNode(NodeConfig{
		Name:       "n1",
		Scratch:    MakeScratch("/tmp/torx-test", "n1"),
		Descriptor: BackendDescriptor{Kind: "local"},
	}))
	if local.Backend.Kind != "local" {
		t.Errorf("local node descriptor kind = %q, want local", local.Backend.Kind)
	}
	if local.Address != "127.0.0.1" {
		t.Errorf("local node descriptor address = %q, want 127.0.0.1", local.Address)
	}
}

func TestDescriptorOfCarriesPortRange(t *testing.T) {
	// A range-allocator node serializes its range; a probe-allocator node does
	// not, so the worker rebuilds the matching allocator.
	ranged := NewNode(NodeConfig{
		Name:    "r0",
		Scratch: MakeScratch("/tmp/torx-test", "r0"),
		Ports:   NewRangePortAllocator(30000, 31000),
	})
	if d := descriptorOf(ranged); d.Ports == nil || d.Ports.Min != 30000 || d.Ports.Max != 31000 {
		t.Errorf("range node descriptor ports = %+v, want [30000,31000)", d.Ports)
	}
	probe := NewNode(NodeConfig{
		Name:    "p0",
		Scratch: MakeScratch("/tmp/torx-test", "p0"),
		Ports:   NewPortAllocator(""),
	})
	if d := descriptorOf(probe); d.Ports != nil {
		t.Errorf("probe node descriptor ports = %+v, want nil", d.Ports)
	}
}
