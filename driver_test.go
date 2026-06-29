package torx

import (
	"context"
	"errors"
	"fmt"
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

	local := descriptorOf(NewNode(NodeConfig{
		Name:       "n1",
		Scratch:    MakeScratch("/tmp/torx-test", "n1"),
		Descriptor: BackendDescriptor{Kind: "local"},
	}))
	if local.Backend.Kind != "local" {
		t.Errorf("local node descriptor kind = %q, want local", local.Backend.Kind)
	}
}
