package torx

import (
	"context"
	"io"
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

// staleProbeBackend is a Backend whose Stream, instead of launching anything,
// records whether path still existed at the moment of launch. Checking at the
// Stream boundary pins StartCaptured's remove-before-launch ordering exactly:
// a check made after StartCaptured returns could be satisfied by a real
// child's own truncating redirect and miss a missing removal.
type staleProbeBackend struct {
	LocalBackend
	path          string
	streamed      bool
	staleAtLaunch bool
}

func (b *staleProbeBackend) Stream(ctx context.Context, cmd Cmd) (io.ReadCloser, error) {
	b.streamed = true
	if _, err := os.Stat(b.path); err == nil {
		b.staleAtLaunch = true
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func TestStartCapturedRemovesStaleLog(t *testing.T) {
	probe := &staleProbeBackend{}
	n := NewNode(NodeConfig{
		Name:    "n0",
		Backend: probe,
		Scratch: MakeScratch(t.TempDir(), "n0"),
		Ports:   NewPortAllocator(""),
	})
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.Bind([]*Node{n})

	// Plant a log at the capture path, as a previous incarnation of the service
	// would leave behind on a node whose scratch persists across runs.
	ctx := context.Background()
	dir := n.ServiceScratch("svc").Root
	if err := n.Mkdir(ctx, dir); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	probe.path = filepath.Join(dir, "stdout.log")
	if err := n.WriteFile(ctx, probe.path, []byte("stale output")); err != nil {
		t.Fatalf("write: %v", err)
	}

	handle, err := svc.StartCaptured(ctx, n, Command("true"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}
	defer handle.Close()

	if !probe.streamed {
		t.Fatalf("StartCaptured returned without launching the process")
	}
	if probe.staleAtLaunch {
		t.Errorf("stale stdout.log still present when the process was launched; StartCaptured must remove it first")
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
