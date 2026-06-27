package torx

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
)

// newFakeServiceSpec builds a fake service with a node demand but no bound nodes,
// for exercising Declare sizing and Bind.
func newFakeServiceSpec(name string, log *[]string, size int) *fakeService {
	f := &fakeService{log: log, failStart: map[string]bool{}, failStop: map[string]bool{}}
	f.ServiceBase = NewServiceBase(name, Homogeneous(size, NodeSpec{}), f)
	return f
}

// fakeJob uses the default JobBase lifecycle and registers given services.
type fakeJob struct {
	JobBase
	services []Service
}

func (j *fakeJob) Declare(jc *JobContext) {
	for _, s := range j.services {
		jc.Register(s)
	}
}

func (j *fakeJob) Run(context.Context, *JobContext) error { return nil }

var _ Job = (*fakeJob)(nil)

func TestParams(t *testing.T) {
	p := Params{"n": 5, "f": float64(3), "name": "x", "flag": true}
	if got := p.Int("n", 0); got != 5 {
		t.Errorf("Int(n) = %d, want 5", got)
	}
	if got := p.Int("f", 0); got != 3 { // JSON numbers arrive as float64
		t.Errorf("Int(f) = %d, want 3", got)
	}
	if got := p.Int("missing", 7); got != 7 {
		t.Errorf("Int(missing) = %d, want 7 (default)", got)
	}
	if got := p.String("name", ""); got != "x" {
		t.Errorf("String(name) = %q, want x", got)
	}
	if got := p.Bool("flag", false); !got {
		t.Errorf("Bool(flag) = false, want true")
	}
	if got := (Params(nil)).Int("x", 9); got != 9 {
		t.Errorf("nil Params Int = %d, want 9", got)
	}
}

func TestJobDeclareRegistersAndSizes(t *testing.T) {
	var log []string
	j := &fakeJob{services: []Service{
		newFakeServiceSpec("a", &log, 2),
		newFakeServiceSpec("b", &log, 3),
	}}
	jc := NewJobContext(nil, nil)
	j.Declare(jc)

	if len(jc.Services()) != 2 {
		t.Errorf("services = %d, want 2", len(jc.Services()))
	}
	if jc.PoolSpec().Size() != 5 {
		t.Errorf("PoolSpec size = %d, want 5", jc.PoolSpec().Size())
	}
}

func TestJobContextBind(t *testing.T) {
	var log []string
	a := newFakeServiceSpec("a", &log, 2)
	b := newFakeServiceSpec("b", &log, 3)
	jc := NewJobContext(nil, nil)
	jc.Register(a)
	jc.Register(b)

	pool := NewPool([]*Node{
		testNode("0"), testNode("1"), testNode("2"), testNode("3"), testNode("4"),
	})
	sub, err := pool.Allocate(jc.PoolSpec())
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	jc.Bind(sub)

	if len(a.Nodes()) != 2 {
		t.Errorf("a got %d nodes, want 2", len(a.Nodes()))
	}
	if len(b.Nodes()) != 3 {
		t.Errorf("b got %d nodes, want 3", len(b.Nodes()))
	}
}

func TestJobBaseSetupStartsServices(t *testing.T) {
	var log []string
	jc := NewJobContext(nil, nil)
	jc.Register(newFakeService("a", &log, []*Node{testNode("n")}))

	var base JobBase
	if err := base.Setup(context.Background(), jc); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !slices.Contains(log, "a:start:n") {
		t.Errorf("service not started: %v", log)
	}
	if !slices.Contains(log, "a:wait:n") {
		t.Errorf("service not waited: %v", log)
	}
}

func TestJobBaseTeardownTearsDownAndRunsFinalizers(t *testing.T) {
	var log []string
	jc := NewJobContext(nil, nil)
	jc.Register(newFakeService("a", &log, []*Node{testNode("n")}))
	finalized := false
	jc.Defer(func(context.Context) error { finalized = true; return nil })

	var base JobBase
	if err := base.Teardown(context.Background(), jc); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if !slices.Contains(log, "a:stop:n") || !slices.Contains(log, "a:clean:n") {
		t.Errorf("service not torn down: %v", log)
	}
	if !finalized {
		t.Errorf("finalizer not run")
	}
}

func TestJobContextRecordAndSummary(t *testing.T) {
	jc := NewJobContext(nil, nil)
	if err := jc.Record(map[string]int{"throughput": 100}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	jc.SetSummary("100 ops/s")

	var got map[string]int
	if err := json.Unmarshal(jc.Data(), &got); err != nil {
		t.Fatalf("Data not valid JSON: %v", err)
	}
	if got["throughput"] != 100 {
		t.Errorf("Data = %v, want throughput=100", got)
	}
	if jc.Summary() != "100 ops/s" {
		t.Errorf("Summary = %q", jc.Summary())
	}
}

// customSetupJob overrides Setup; the framework should call the override.
type customSetupJob struct {
	JobBase
	setupCalled bool
}

func (j *customSetupJob) Declare(*JobContext)                    {}
func (j *customSetupJob) Run(context.Context, *JobContext) error { return nil }
func (j *customSetupJob) Setup(context.Context, *JobContext) error {
	j.setupCalled = true
	return nil
}

func TestJobOverrideSetup(t *testing.T) {
	j := &customSetupJob{}
	var job Job = j
	if err := job.Setup(context.Background(), NewJobContext(nil, nil)); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !j.setupCalled {
		t.Errorf("override Setup was not called via the interface")
	}
}
