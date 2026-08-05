package torx

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func captureTestNode(t *testing.T) *Node {
	t.Helper()
	return NewNode(NodeConfig{
		Name:    "n0",
		Backend: LocalBackend{},
		Scratch: MakeScratch(t.TempDir(), "n0"),
		Ports:   NewPortAllocator(""),
	})
}

func TestShQuote(t *testing.T) {
	cases := map[string]string{
		"plain": "'plain'",
		"a b":   "'a b'",
		"it's":  `'it'\''s'`,
		"":      "''",
	}
	for in, want := range cases {
		if got := shQuote(in); got != want {
			t.Errorf("shQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestShJoin(t *testing.T) {
	got := shJoin("redis-server", []string{"--port", "63 79"})
	want := "'redis-server' '--port' '63 79'"
	if got != want {
		t.Errorf("shJoin = %q, want %q", got, want)
	}
}

func TestStartCapturedCapturesOutput(t *testing.T) {
	n := captureTestNode(t)
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.Bind([]*Node{n})

	handle, err := svc.StartCaptured(context.Background(), n, Command("sh", "-c", "printf 'hello from svc'"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}
	defer handle.Close()

	// The captured file is registered for collection.
	arts := svc.Artifacts(n)
	if len(arts) != 1 || arts[0].Name != "stdout.log" || !arts[0].CollectOnPass {
		t.Fatalf("artifacts = %+v, want one collect-on-pass stdout.log", arts)
	}

	// The process writes asynchronously; poll the captured file for its output.
	deadline := time.Now().Add(2 * time.Second)
	var content []byte
	for time.Now().Before(deadline) {
		content, _ = os.ReadFile(arts[0].Path)
		if strings.Contains(string(content), "hello from svc") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(string(content), "hello from svc") {
		t.Errorf("captured log = %q, want it to contain the process output", content)
	}
}

func TestStartCapturedRemovesStaleLog(t *testing.T) {
	n := captureTestNode(t)
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.Bind([]*Node{n})

	// Plant a log at the capture path, as a previous incarnation of the service
	// would leave behind on a node whose scratch persists across runs.
	ctx := context.Background()
	dir := n.ServiceScratch("svc").Root
	if err := n.Mkdir(ctx, dir); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	logPath := filepath.Join(dir, "stdout.log")
	if err := n.WriteFile(ctx, logPath, []byte("stale output")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The launched process produces no output, so any content observed at the
	// path from here on could only be the stale file.
	handle, err := svc.StartCaptured(ctx, n, Command("sleep", "30"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}
	defer handle.Close()

	// The stale log is removed before the process is launched, so once
	// StartCaptured returns a poll of the file must never see a previous
	// incarnation's output -- only its absence, or the empty file created by
	// the new process's redirect.
	if content, err := os.ReadFile(logPath); err == nil && strings.Contains(string(content), "stale output") {
		t.Errorf("stale log survived StartCaptured: %q", content)
	}
}

func TestCollectArtifacts(t *testing.T) {
	n := captureTestNode(t)
	ctx := context.Background()
	if err := n.Mkdir(ctx, n.Scratch().Root); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dataPath := filepath.Join(n.Scratch().Root, "data.txt")
	if err := n.WriteFile(ctx, dataPath, []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}

	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.Bind([]*Node{n})
	svc.AddArtifact(n, Artifact{Name: "data.txt", Path: dataPath, CollectOnPass: true})

	jc := NewJobContext(nil, nil)
	jc.Register(svc)
	jc.resultsDir = t.TempDir()
	jc.passed = true

	if err := jc.CollectArtifacts(ctx); err != nil {
		t.Fatalf("CollectArtifacts: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(jc.resultsDir, "svc", "n0", "data.txt"))
	if err != nil {
		t.Fatalf("collected file: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("collected content = %q, want payload", got)
	}
}

func TestCollectArtifactsNoResultsDir(t *testing.T) {
	jc := NewJobContext(nil, nil)
	if err := jc.CollectArtifacts(context.Background()); err != nil {
		t.Errorf("CollectArtifacts with no results dir = %v, want nil (no-op)", err)
	}
}
