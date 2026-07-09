package torx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func init() {
	Register("wtest.pass", func() Job { return &wPassJob{} })
	Register("wtest.fail", func() Job { return &wFailJob{} })
	Register("wtest.panic", func() Job { return &wPanicJob{} })
	Register("wtest.block", func() Job { return &wBlockJob{} })
	Register("wtest.nodes", func() Job { return &wNodeJob{} })
}

type wPassJob struct{ JobBase }

func (*wPassJob) Declare(*JobContext) {}
func (*wPassJob) Run(_ context.Context, jc *JobContext) error {
	jc.Log("info", "ran")
	jc.SetSummary("ok")
	return nil
}

type wFailJob struct{ JobBase }

func (*wFailJob) Declare(*JobContext)                    {}
func (*wFailJob) Run(context.Context, *JobContext) error { return errors.New("boom") }

type wPanicJob struct{ JobBase }

func (*wPanicJob) Declare(*JobContext)                    {}
func (*wPanicJob) Run(context.Context, *JobContext) error { panic("kaboom") }

type wBlockJob struct{ JobBase }

func (*wBlockJob) Declare(*JobContext) {}
func (*wBlockJob) Run(ctx context.Context, _ *JobContext) error {
	<-ctx.Done()
	return ctx.Err()
}
func (*wBlockJob) Teardown(_ context.Context, jc *JobContext) error {
	jc.Log("info", "teardown-ran")
	return nil
}

type wNodeJob struct {
	JobBase
	svc *fakeService
}

func (j *wNodeJob) Declare(jc *JobContext) {
	j.svc = newFakeServiceSpec("s", new([]string), 2)
	jc.Register(j.svc)
}
func (j *wNodeJob) Run(_ context.Context, jc *JobContext) error {
	jc.SetSummary(fmt.Sprintf("nodes=%d", len(j.svc.Nodes())))
	return nil
}

func runWorker(t *testing.T, a Assignment) []Message {
	t.Helper()
	var in bytes.Buffer
	if err := EncodeAssignment(&in, a); err != nil {
		t.Fatalf("encode assignment: %v", err)
	}
	var out bytes.Buffer
	if err := RunWorker(context.Background(), &in, &out); err != nil {
		t.Fatalf("RunWorker: %v", err)
	}
	return readMessages(t, &out)
}

func readMessages(t *testing.T, r io.Reader) []Message {
	t.Helper()
	var msgs []Message
	mr := NewMessageReader(r)
	for {
		m, err := mr.Read()
		if errors.Is(err, io.EOF) {
			return msgs
		}
		if err != nil {
			t.Fatalf("read message: %v", err)
		}
		msgs = append(msgs, m)
	}
}

func resultOf(msgs []Message) *JobResult {
	for _, m := range msgs {
		if m.Result != nil {
			return m.Result
		}
	}
	return nil
}

func hasEvent(msgs []Message, kind EventKind) bool {
	for _, m := range msgs {
		if m.Event != nil && m.Event.Kind == kind {
			return true
		}
	}
	return false
}

func hasLog(msgs []Message, substr string) bool {
	for _, m := range msgs {
		if m.Event != nil && m.Event.Kind == EventLog && strings.Contains(m.Event.Message, substr) {
			return true
		}
	}
	return false
}

func TestRunWorkerPass(t *testing.T) {
	msgs := runWorker(t, Assignment{JobID: "wtest.pass"})

	if !hasEvent(msgs, EventRunning) {
		t.Errorf("missing RUNNING event")
	}
	if !hasLog(msgs, "ran") {
		t.Errorf("missing 'ran' log event")
	}
	r := resultOf(msgs)
	if r == nil || r.Status != StatusPass {
		t.Fatalf("result = %+v, want PASS", r)
	}
	if r.Summary != "ok" {
		t.Errorf("summary = %q, want ok", r.Summary)
	}
}

func TestRunWorkerFail(t *testing.T) {
	r := resultOf(runWorker(t, Assignment{JobID: "wtest.fail"}))
	if r == nil || r.Status != StatusFail {
		t.Fatalf("result = %+v, want FAIL", r)
	}
	if r.Error == nil || !strings.Contains(r.Error.Message, "boom") {
		t.Errorf("error = %+v, want it to mention boom", r.Error)
	}
}

func TestRunWorkerUnknownJob(t *testing.T) {
	r := resultOf(runWorker(t, Assignment{JobID: "wtest.does-not-exist"}))
	if r == nil || r.Status != StatusFail {
		t.Fatalf("result = %+v, want FAIL", r)
	}
	if r.Error == nil || !strings.Contains(r.Error.Message, "unknown job") {
		t.Errorf("error = %+v, want it to mention unknown job", r.Error)
	}
}

func TestRunWorkerPanicBecomesFailure(t *testing.T) {
	r := resultOf(runWorker(t, Assignment{JobID: "wtest.panic"}))
	if r == nil || r.Status != StatusFail {
		t.Fatalf("result = %+v, want FAIL", r)
	}
	if r.Error == nil || !strings.Contains(r.Error.Message, "kaboom") {
		t.Errorf("error = %+v, want it to mention kaboom", r.Error)
	}
	if r.Error.Stack == "" {
		t.Errorf("a recovered panic should carry a stack")
	}
}

func TestRunWorkerRebuildsAndBindsNodes(t *testing.T) {
	a := Assignment{JobID: "wtest.nodes", Nodes: []NodeDescriptor{
		{Name: "n0", Scratch: "/tmp/torx/n0", Backend: BackendDescriptor{Kind: "local"}},
		{Name: "n1", Scratch: "/tmp/torx/n1", Backend: BackendDescriptor{Kind: "local"}},
	}}
	r := resultOf(runWorker(t, a))
	if r == nil || r.Status != StatusPass {
		t.Fatalf("result = %+v, want PASS", r)
	}
	if r.Summary != "nodes=2" {
		t.Errorf("summary = %q, want nodes=2 (service should be bound 2 nodes)", r.Summary)
	}
}

func TestRunWorkerTeardownOnCancel(t *testing.T) {
	var in bytes.Buffer
	if err := EncodeAssignment(&in, Assignment{JobID: "wtest.block"}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out bytes.Buffer

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunWorker(ctx, &in, &out) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RunWorker: %v", err)
	}

	msgs := readMessages(t, &out)
	r := resultOf(msgs)
	if r == nil || r.Status != StatusFail {
		t.Fatalf("result = %+v, want FAIL from cancellation", r)
	}
	if !hasLog(msgs, "teardown-ran") {
		t.Errorf("teardown did not run after cancellation")
	}
}

func TestBuildNodesPortRange(t *testing.T) {
	// A descriptor carrying a range yields a node whose allocator leases from it.
	nodes, err := buildNodes([]NodeDescriptor{
		{Name: "r0", Scratch: "/tmp/torx/r0", Backend: BackendDescriptor{Kind: "local"}, Ports: &PortRange{Min: 40000, Max: 40010}},
	})
	if err != nil {
		t.Fatalf("buildNodes: %v", err)
	}
	p, err := nodes[0].AllocatePort()
	if err != nil {
		t.Fatalf("AllocatePort: %v", err)
	}
	if p < 40000 || p >= 40010 {
		t.Errorf("allocated port %d, want it in [40000,40010)", p)
	}
}
