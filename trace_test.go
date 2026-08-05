package torx

import (
	"context"
	"encoding/json"
	"errors"
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

func TestTraceSinkSurfacesWriteFailure(t *testing.T) {
	// Reopen the trace files read-only, so every write fails the way it would on
	// a disk that filled up (or started erroring) after the files were opened. A
	// trace that opens and then silently truncates must be reported: the write
	// failure has to come back from Close, which is how it reaches PersistErr.
	dir := t.TempDir()
	for _, name := range []string{"events.ndjson", "test_log"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	events, err := os.Open(filepath.Join(dir, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	human, err := os.Open(filepath.Join(dir, "test_log"))
	if err != nil {
		t.Fatal(err)
	}
	s := &traceSink{events: events, human: human, enc: json.NewEncoder(events)}

	s.Emit(Event{Kind: EventLog, Message: "lost", Time: time.Now()})
	if err := s.Close(); err == nil {
		t.Errorf("Close = nil though every trace write failed")
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
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{}, sizedRequests(1, 1, nil),
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
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
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

func TestVariantDirNameShortUnchanged(t *testing.T) {
	// An id whose sanitized form fits the bound keeps it verbatim, so existing
	// results trees keep their readable directory names.
	id := `j[p="a/b"]`
	if got, want := variantDirName(id), sanitizeID(id); got != want {
		t.Errorf("short id renamed: got %q, want %q", got, want)
	}
}

func TestVariantDirNameBounded(t *testing.T) {
	long := `j[p="` + strings.Repeat("x", 400) + `1"]`
	name := variantDirName(long)
	if len(name) > maxVariantDirName {
		t.Errorf("bounded name is %d bytes, want <= %d", len(name), maxVariantDirName)
	}
	if name != variantDirName(long) {
		t.Errorf("variantDirName is not deterministic")
	}
	// Two ids that differ only beyond the readable prefix must still get
	// distinct directories; only the digest distinguishes them.
	other := `j[p="` + strings.Repeat("x", 400) + `2"]`
	if variantDirName(other) == name {
		t.Errorf("ids differing past the prefix mapped to the same directory %q", name)
	}
}

func TestVariantDirNameEscapeNotSplit(t *testing.T) {
	// "ab" then slashes: every slash sanitizes to a three-byte escape, and the
	// two-byte lead misaligns the prefix cut so it would land mid-escape. The
	// prefix must hold only whole escapes.
	name := variantDirName("ab" + strings.Repeat("/", 400))
	sep := strings.LastIndex(name, "%-")
	if sep < 0 {
		t.Fatalf("bounded name %q has no %%- separator", name)
	}
	prefix := name[:sep]
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == '%' {
			if i+2 >= len(prefix) {
				t.Fatalf("prefix %q ends in a split escape", prefix)
			}
			i += 2
		}
	}
}

// TestExecuteOversizedVariantID is the regression test for variant ids longer
// than the filesystem's name limit. Deriving the directory from the raw id used
// to fail with ENAMETOOLONG, and the failure was swallowed: the job passed
// while writing no result.json, no trace, and no artifacts.
func TestExecuteOversizedVariantID(t *testing.T) {
	runDir := t.TempDir()
	params := Params{"p": strings.Repeat("x", 300)}
	a := Assignment{JobID: "wtest.pass", Params: params, Session: SessionConfig{ResultsDir: runDir}}

	res := execute(context.Background(), a, discardSink{})
	if res.Status != StatusPass {
		t.Fatalf("status = %v, want PASS", res.Status)
	}
	if res.PersistErr != "" {
		t.Fatalf("PersistErr = %q, want empty", res.PersistErr)
	}

	entries, err := os.ReadDir(runDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("run dir entries = %v (err %v), want exactly the variant directory", entries, err)
	}
	name := entries[0].Name()
	if len(name) > maxVariantDirName {
		t.Errorf("variant directory name is %d bytes, want <= %d", len(name), maxVariantDirName)
	}

	b, err := os.ReadFile(filepath.Join(runDir, name, "result.json"))
	if err != nil {
		t.Fatalf("result.json missing: %v", err)
	}
	var got JobResult
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("result.json: %v", err)
	}
	if want := variantID("wtest.pass", params); got.ID != want {
		t.Errorf("result.json id = %q, want the full variant id %q", got.ID, want)
	}
}

func TestExecuteSurfacesPersistFailure(t *testing.T) {
	// A results dir that is a regular file: the variant directory cannot be
	// created underneath it. The job itself still passes; the missing results
	// must be visible on the result and fail the suite.
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	a := Assignment{JobID: "wtest.pass", Session: SessionConfig{ResultsDir: file}}

	res := execute(context.Background(), a, discardSink{})
	if res.Status != StatusPass {
		t.Fatalf("status = %v, want PASS (the job itself succeeded)", res.Status)
	}
	if res.PersistErr == "" {
		t.Errorf("PersistErr empty though the variant directory could not be created")
	}
	if !strings.Contains(res.Render(), "results not persisted") {
		t.Errorf("Render() does not surface the persistence failure:\n%s", res.Render())
	}
	if (SuiteResult{Jobs: []JobResult{res}}).Ok() {
		t.Errorf("suite with an unpersisted job reported Ok")
	}
}

func TestSanitizeIDInjective(t *testing.T) {
	// A "/" inside a value and its percent-encoding must map to distinct
	// components, or the two variants would share a result directory.
	a := sanitizeID(`j[p="a/b"]`)
	b := sanitizeID(`j[p="a%2Fb"]`)
	if a == b {
		t.Errorf("distinct ids sanitized to the same component: %q", a)
	}
	if strings.ContainsRune(a, '/') {
		t.Errorf("sanitizeID left a path separator in %q", a)
	}
}

func TestMakeRunDirUnique(t *testing.T) {
	root := t.TempDir()
	stamp := "2026-01-02T03-04-05Z"

	d1, err := makeRunDir(root, stamp)
	if err != nil {
		t.Fatalf("first makeRunDir: %v", err)
	}
	d2, err := makeRunDir(root, stamp)
	if err != nil {
		t.Fatalf("second makeRunDir: %v", err)
	}
	if d1 == d2 {
		t.Fatalf("two runs with the same stamp shared a directory: %s", d1)
	}
	for _, d := range []string{d1, d2} {
		if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
			t.Errorf("run dir %s not created: %v", d, err)
		}
	}
	// latest resolves to the most recent run.
	target, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("readlink latest: %v", err)
	}
	if target != filepath.Base(d2) {
		t.Errorf("latest -> %q, want %q", target, filepath.Base(d2))
	}
}

func TestRunUsesExactRunDir(t *testing.T) {
	// The launcher handshake: the caller creates the run directory and writes
	// its metadata into it before the run; torx uses the directory exactly as
	// given and fills in the per-variant subdirectories and run.json.
	runDir := t.TempDir()
	marker := filepath.Join(runDir, "invocation.json")
	if err := os.WriteFile(marker, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
		sizedRequests(1, 1, nil), RunOptions{RunDir: runDir})
	if !res.Ok() {
		t.Fatalf("run failed:\n%s", res.Render())
	}

	if _, err := os.Stat(filepath.Join(runDir, "run.json")); err != nil {
		t.Errorf("run.json missing from the exact run dir: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(runDir, "latest")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("latest symlink present in run-dir mode (stat err %v)", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("launcher metadata lost: %v", err)
	}
	foundJob := false
	entries, _ := os.ReadDir(runDir)
	for _, e := range entries {
		if e.IsDir() {
			if _, err := os.Stat(filepath.Join(runDir, e.Name(), "result.json")); err == nil {
				foundJob = true
			}
		}
	}
	if !foundJob {
		t.Errorf("no per-variant directory with result.json under %s", runDir)
	}
}

func TestRunRejectsMissingRunDir(t *testing.T) {
	// The launcher creates the run directory before the run; a missing one means
	// the handshake was not honored, and must be surfaced rather than papered
	// over by quietly creating the directory.
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
		sizedRequests(1, 1, nil), RunOptions{RunDir: filepath.Join(t.TempDir(), "absent")})
	if res.PersistErr == "" {
		t.Errorf("PersistErr empty though the run directory does not exist")
	}
	if res.Ok() {
		t.Errorf("run reported Ok despite an unusable run directory")
	}
}

func TestRunRunDirAndResultsDirExclusive(t *testing.T) {
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
		sizedRequests(1, 1, nil), RunOptions{RunDir: t.TempDir(), ResultsDir: t.TempDir()})
	if res.PersistErr == "" || res.Ok() {
		t.Errorf("conflicting RunDir+ResultsDir not surfaced: persist=%q ok=%v", res.PersistErr, res.Ok())
	}
}

func TestRunSurfacesResultsDirFailure(t *testing.T) {
	// A results dir whose parent is a regular file cannot be created, so the
	// requested tree fails. The run must not silently report success with no
	// results.
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
		sizedRequests(1, 1, nil), RunOptions{ResultsDir: filepath.Join(file, "results")})
	if res.PersistErr == "" {
		t.Errorf("PersistErr empty though the results tree could not be created")
	}
	if res.Ok() {
		t.Errorf("run reported Ok despite failing to persist the requested results tree")
	}
}

func TestRunCollectsServiceArtifacts(t *testing.T) {
	root := t.TempDir()
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{},
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
