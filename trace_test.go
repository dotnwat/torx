package torx

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func init() {
	Register("ttest.capture", func() Job { return &captureJob{} })
}

// captureSvc captures its process output via StartCaptured, exercising the
// capture-and-collect path end to end.
type captureSvc struct {
	*ServiceBase
	handle interface{ Close() error }
}

func newCaptureSvc() *captureSvc {
	s := &captureSvc{}
	s.ServiceBase = NewServiceBase("cap", Homogeneous(1, NodeSpec{}), s)
	return s
}

func (s *captureSvc) StartNode(ctx context.Context, n *Node) error {
	// Write the output, then stay alive so StopNode is what ends the process --
	// the write is complete well before collection.
	h, err := s.StartCaptured(ctx, n, Command("sh", "-c", "printf captured-output; sleep 30"))
	s.handle = h
	return err
}

func (s *captureSvc) StopNode(_ context.Context, _ *Node) error {
	if s.handle != nil {
		return s.handle.Close()
	}
	return nil
}

func (s *captureSvc) CleanNode(_ context.Context, _ *Node) error { return nil }

// WaitNode blocks until the captured output has appeared, the way a real service
// waits for readiness before the job proceeds (and before teardown can stop it).
func (s *captureSvc) WaitNode(ctx context.Context, n *Node) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	logPath := filepath.Join(n.ServiceScratch("cap").Root, "stdout.log")
	return WaitForLog(ctx, func(ctx context.Context) ([]byte, error) {
		return n.ReadFile(ctx, logPath)
	}, "captured-output")
}

type captureJob struct{ JobBase }

func (*captureJob) Declare(jc *JobContext)                     { jc.Register(newCaptureSvc()) }
func (*captureJob) Run(_ context.Context, _ *JobContext) error { return nil }

func TestRenderEvent(t *testing.T) {
	at := time.Date(2026, 6, 27, 12, 30, 5, 0, time.UTC)
	log := renderEvent(Event{Kind: EventLog, Level: "warn", Component: "redis", Message: "careful", Time: at, Site: &Site{File: "svc.go", Line: 42}})
	for _, want := range []string{"[redis]", "WARN", "careful", "svc.go:42"} {
		if !strings.Contains(log, want) {
			t.Errorf("log render %q missing %q", log, want)
		}
	}
	fin := renderEvent(Event{Kind: EventFinished, Message: "PASS", Time: at})
	if !strings.Contains(fin, "FINISHED") || !strings.Contains(fin, "PASS") {
		t.Errorf("finished render = %q", fin)
	}
}

func TestExecuteWritesResultsTree(t *testing.T) {
	runDir := t.TempDir()
	a := Assignment{JobID: "wtest.pass", Session: SessionConfig{ResultsDir: runDir}}

	res := execute(context.Background(), a, discardSink{})
	if res.Status != StatusPass {
		t.Fatalf("status = %v, want PASS", res.Status)
	}

	jobDir := filepath.Join(runDir, "wtest.pass")
	for _, f := range []string{"events.ndjson", "test_log", "result.json"} {
		if _, err := os.Stat(filepath.Join(jobDir, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}

	var got JobResult
	b, _ := os.ReadFile(filepath.Join(jobDir, "result.json"))
	if err := json.Unmarshal(b, &got); err != nil || got.Status != StatusPass {
		t.Errorf("result.json = %s (err %v)", b, err)
	}

	events, _ := os.ReadFile(filepath.Join(jobDir, "events.ndjson"))
	for _, want := range []string{"running", "ran", "finished"} {
		if !strings.Contains(string(events), want) {
			t.Errorf("events.ndjson missing %q:\n%s", want, events)
		}
	}
}

func TestRunWritesResultsTree(t *testing.T) {
	root := t.TempDir()
	res := Run(context.Background(), testPool(1), InProcessLauncher{}, sizedRequests(1, 1, nil),
		RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("run failed:\n%s", res.Render())
	}

	// latest symlink resolves to a run dir containing run.json.
	target, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest symlink: %v", err)
	}
	runDir := filepath.Join(root, target)
	if _, err := os.Stat(filepath.Join(runDir, "run.json")); err != nil {
		t.Errorf("run.json missing: %v", err)
	}

	// At least one per-job directory holds a result.json.
	entries, _ := os.ReadDir(runDir)
	foundJob := false
	for _, e := range entries {
		if e.IsDir() {
			if _, err := os.Stat(filepath.Join(runDir, e.Name(), "result.json")); err == nil {
				foundJob = true
			}
		}
	}
	if !foundJob {
		t.Errorf("no per-job directory with result.json under %s", runDir)
	}
}

func TestRunTraceHasLifecycle(t *testing.T) {
	root := t.TempDir()
	res := Run(context.Background(), testPool(1), InProcessLauncher{},
		[]JobRequest{{ID: "ttest.capture"}}, RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("run failed:\n%s", res.Render())
	}

	target, _ := os.Readlink(filepath.Join(root, "latest"))
	log, err := os.ReadFile(filepath.Join(root, target, "ttest.capture", "test_log"))
	if err != nil {
		t.Fatalf("test_log: %v", err)
	}
	// The trace narrates the whole job: node binding, service start and the
	// process launch, readiness, the job body's end, stop, and collection. The
	// service's lines carry its component tag instead of repeating its name.
	for _, want := range []string{
		"[worker]",
		"RUNNING",
		"bound 1 node",
		"[cap]",
		"starting",
		"exec sh -c",
		"ready",
		"stopping",
		"collecting from n0: stdout.log",
		"FINISHED PASS",
	} {
		if !strings.Contains(string(log), want) {
			t.Errorf("test_log missing %q:\n%s", want, log)
		}
	}
}

func TestRunCollectsServiceArtifacts(t *testing.T) {
	root := t.TempDir()
	res := Run(context.Background(), testPool(1), InProcessLauncher{},
		[]JobRequest{{ID: "ttest.capture"}}, RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("run failed:\n%s", res.Render())
	}

	target, _ := os.Readlink(filepath.Join(root, "latest"))
	logPath := filepath.Join(root, target, "ttest.capture", "cap", "n0", "stdout.log")
	got, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("collected service log missing at %s: %v", logPath, err)
	}
	if !strings.Contains(string(got), "captured-output") {
		t.Errorf("collected log = %q, want it to contain the captured output", got)
	}
}
