package torx

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// fakeService uses the default ServiceBase lifecycle and records each per-node
// hook call into a shared log so tests can assert ordering across services.
type fakeService struct {
	*ServiceBase
	log       *[]string
	failStart map[string]bool
	failStop  map[string]bool
	failClean map[string]bool
}

func newFakeService(name string, log *[]string, nodes []*Node) *fakeService {
	f := &fakeService{log: log, failStart: map[string]bool{}, failStop: map[string]bool{}, failClean: map[string]bool{}}
	f.ServiceBase = NewServiceBase(name, Homogeneous(len(nodes), NodeSpec{}), f)
	f.Bind(nodes)
	return f
}

func (f *fakeService) record(op, node string) { *f.log = append(*f.log, f.Name()+":"+op+":"+node) }

func (f *fakeService) StartNode(_ context.Context, n *Node) error {
	f.record("start", n.Name())
	if f.failStart[n.Name()] {
		return errors.New("start boom")
	}
	return nil
}

func (f *fakeService) StopNode(_ context.Context, n *Node) error {
	f.record("stop", n.Name())
	if f.failStop[n.Name()] {
		return errors.New("stop boom")
	}
	return nil
}

func (f *fakeService) CleanNode(_ context.Context, n *Node) error {
	f.record("clean", n.Name())
	if f.failClean[n.Name()] {
		return errors.New("clean boom")
	}
	return nil
}

func (f *fakeService) WaitNode(_ context.Context, n *Node) error {
	f.record("wait", n.Name())
	return nil
}

func TestServiceBase(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0"), testNode(t, "n1")})
	if f.Name() != "svc" {
		t.Errorf("Name = %q", f.Name())
	}
	if f.Spec().Size() != 2 {
		t.Errorf("Spec size = %d, want 2", f.Spec().Size())
	}
	if len(f.Nodes()) != 2 {
		t.Errorf("Nodes = %d, want 2", len(f.Nodes()))
	}
}

func TestNewServiceBaseRejectsTraversalName(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "../escape"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NewServiceBase(%q) did not panic", name)
				}
			}()
			NewServiceBase(name, Homogeneous(1, NodeSpec{}), nil)
		})
	}
}

func TestAddArtifactRejectsTraversalName(t *testing.T) {
	f := newFakeService("svc", new([]string), []*Node{testNode(t, "n0")})
	defer func() {
		if recover() == nil {
			t.Errorf("AddArtifact with a traversal name did not panic")
		}
	}()
	f.AddArtifact(testNode(t, "n0"), Artifact{Name: "../escape.log", Path: "/x"})
}

func TestServiceStartStopsAndCleansFirst(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0")})
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	want := []string{"svc:stop:n0", "svc:clean:n0", "svc:start:n0"}
	if !slices.Equal(log, want) {
		t.Errorf("calls = %v, want %v", log, want)
	}
}

func TestServiceStartFailsFast(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0"), testNode(t, "n1")})
	f.failStart["n0"] = true

	err := f.Start(context.Background())
	if !errors.Is(err, ErrService) {
		t.Errorf("err = %v, want ErrService", err)
	}
	if slices.Contains(log, "svc:start:n1") {
		t.Errorf("started n1 after n0 failed: %v", log)
	}
}

func TestServiceStartAbortsWhenPreStopFails(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0")})
	f.failStop["n0"] = true

	err := f.Start(context.Background())
	if !errors.Is(err, ErrService) {
		t.Errorf("err = %v, want ErrService", err)
	}
	// A node that could not be stopped must not be started onto.
	if slices.Contains(log, "svc:start:n0") {
		t.Errorf("started n0 despite a failed pre-stop: %v", log)
	}
}

func TestServiceStartAbortsWhenPreCleanFails(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0")})
	f.failClean["n0"] = true

	err := f.Start(context.Background())
	if !errors.Is(err, ErrService) {
		t.Errorf("err = %v, want ErrService", err)
	}
	// A node whose stale data could not be removed must not be started onto.
	if slices.Contains(log, "svc:start:n0") {
		t.Errorf("started n0 despite a failed pre-clean: %v", log)
	}
}

func TestServiceStopAggregatesAcrossNodes(t *testing.T) {
	var log []string
	f := newFakeService("svc", &log, []*Node{testNode(t, "n0"), testNode(t, "n1")})
	f.failStop["n0"] = true
	f.failStop["n1"] = true

	err := f.Stop(context.Background())
	if !errors.Is(err, ErrService) {
		t.Errorf("err = %v, want ErrService", err)
	}
	if !slices.Contains(log, "svc:stop:n0") || !slices.Contains(log, "svc:stop:n1") {
		t.Errorf("not all nodes stopped: %v", log)
	}
}

func TestServiceRegistryStopThenCleanLIFO(t *testing.T) {
	var log []string
	var reg ServiceRegistry
	reg.Add(newFakeService("a", &log, []*Node{testNode(t, "n")}))
	reg.Add(newFakeService("b", &log, []*Node{testNode(t, "n")}))

	// The teardown sequence JobBase drives: stop every service, then clean every
	// service, each in reverse registration order.
	if err := reg.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if err := reg.CleanAll(context.Background()); err != nil {
		t.Fatalf("CleanAll: %v", err)
	}
	want := []string{"b:stop:n", "a:stop:n", "b:clean:n", "a:clean:n"}
	if !slices.Equal(log, want) {
		t.Errorf("teardown order = %v, want %v", log, want)
	}
}

func TestServiceRegistryStopAggregatesAndCleanStillRuns(t *testing.T) {
	var log []string
	var reg ServiceRegistry
	a := newFakeService("a", &log, []*Node{testNode(t, "n")})
	b := newFakeService("b", &log, []*Node{testNode(t, "n")})
	a.failStop["n"] = true
	b.failStop["n"] = true
	reg.Add(a)
	reg.Add(b)

	err := reg.StopAll(context.Background())
	if err == nil {
		t.Fatalf("StopAll err = nil, want aggregated errors")
	}
	msg := err.Error()
	if !strings.Contains(msg, "service: stop a") || !strings.Contains(msg, "service: stop b") {
		t.Errorf("aggregated error missing a cause: %v", err)
	}
	// Clean must still run even though every stop failed, so logs are not stranded.
	if err := reg.CleanAll(context.Background()); err != nil {
		t.Fatalf("CleanAll: %v", err)
	}
	if !slices.Contains(log, "a:clean:n") || !slices.Contains(log, "b:clean:n") {
		t.Errorf("clean did not run after stop failures: %v", log)
	}
}

// customStopService overrides Stop with its own behavior instead of the default
// per-node teardown, exercising the override path.
type customStopService struct {
	*ServiceBase
	log *[]string
}

func newCustomStopService(name string, log *[]string, nodes []*Node) *customStopService {
	c := &customStopService{log: log}
	c.ServiceBase = NewServiceBase(name, Homogeneous(len(nodes), NodeSpec{}), c)
	c.Bind(nodes)
	return c
}

func (c *customStopService) StartNode(context.Context, *Node) error { return nil }
func (c *customStopService) StopNode(_ context.Context, n *Node) error {
	*c.log = append(*c.log, c.Name()+":node-stop:"+n.Name())
	return nil
}
func (c *customStopService) CleanNode(context.Context, *Node) error { return nil }
func (c *customStopService) WaitNode(context.Context, *Node) error  { return nil }

// Stop overrides the default per-node teardown.
func (c *customStopService) Stop(context.Context) error {
	*c.log = append(*c.log, c.Name()+":custom-stop")
	return nil
}

func TestServiceCanOverrideLifecycle(t *testing.T) {
	var log []string
	var reg ServiceRegistry
	reg.Add(newCustomStopService("custom", &log, []*Node{testNode(t, "n0"), testNode(t, "n1")}))

	if err := reg.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	// The registry dispatched through the interface to the override: a single
	// custom-stop, and none of the default per-node StopNode calls.
	want := []string{"custom:custom-stop"}
	if !slices.Equal(log, want) {
		t.Errorf("StopAll log = %v, want %v (override should be used)", log, want)
	}
}
