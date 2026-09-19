package torx

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
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

// waitForLog polls the file at path until its content contains substr and
// returns that content; a captured process writes asynchronously, so a test
// must poll rather than read once. It fails the test if the content never
// appears.
func waitForLog(t *testing.T, path, substr string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var content []byte
	for time.Now().Before(deadline) {
		content, _ = os.ReadFile(path)
		if strings.Contains(string(content), substr) {
			return string(content)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("log %s = %q, want it to contain %q", path, content, substr)
	return ""
}

// artifactNames lists the names registered for n on svc, sorted, checking
// along the way that each is collected on pass.
func artifactNames(t *testing.T, svc *ServiceBase, n *Node) []string {
	t.Helper()
	var names []string
	for _, a := range svc.Artifacts(n) {
		if !a.CollectOnPass {
			t.Errorf("artifact %s is not collected on pass", a.Name)
		}
		names = append(names, a.Name)
	}
	slices.Sort(names)
	return names
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
	waitForLog(t, arts[0].Path, "hello from svc")
}

// TestStartCapturedRelaunch launches several processes in turn on one node and
// checks what each policy leaves for collection: truncate keeps only the last
// process's output, under a single registration of stdout.log; rotate keeps
// every earlier process's output as stdout.<k>.log beside it.
func TestStartCapturedRelaunch(t *testing.T) {
	// launch starts a process that prints msg and waits until it has.
	launch := func(t *testing.T, svc *ServiceBase, n *Node, msg string) {
		t.Helper()
		handle, err := svc.StartCaptured(context.Background(), n, Command("sh", "-c", "printf "+msg))
		if err != nil {
			t.Fatalf("StartCaptured(%s): %v", msg, err)
		}
		defer handle.Close()
		waitForLog(t, filepath.Join(n.ServiceScratch("svc").Root, "stdout.log"), msg)
	}

	t.Run("truncate", func(t *testing.T) {
		n := captureTestNode(t)
		svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
		svc.Bind([]*Node{n})
		launch(t, svc, n, "first")
		launch(t, svc, n, "second")

		dir := n.ServiceScratch("svc").Root
		if content := waitForLog(t, filepath.Join(dir, "stdout.log"), "second"); strings.Contains(content, "first") {
			t.Errorf("stdout.log = %q, want the first process's output discarded", content)
		}
		if _, err := os.Stat(filepath.Join(dir, "stdout.1.log")); err == nil {
			t.Errorf("stdout.1.log exists; truncate must not rotate")
		}
		if got, want := artifactNames(t, svc, n), []string{"stdout.log"}; !slices.Equal(got, want) {
			t.Errorf("artifacts = %v, want %v (one registration across launches)", got, want)
		}
	})

	t.Run("rotate", func(t *testing.T) {
		n := captureTestNode(t)
		svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
		svc.SetCapturePolicy(CaptureRotate)
		svc.Bind([]*Node{n})
		launch(t, svc, n, "first")
		launch(t, svc, n, "second")
		launch(t, svc, n, "third")

		dir := n.ServiceScratch("svc").Root
		for name, want := range map[string]string{"stdout.1.log": "first", "stdout.2.log": "second", "stdout.log": "third"} {
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || string(got) != want {
				t.Errorf("%s = %q (%v), want %q", name, got, err, want)
			}
		}
		want := []string{"stdout.1.log", "stdout.2.log", "stdout.log"}
		if got := artifactNames(t, svc, n); !slices.Equal(got, want) {
			t.Errorf("artifacts = %v, want %v", got, want)
		}
	})
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

// probeNode builds a node on a staleProbeBackend watching the service's
// capture path, with the service scratch directory already created.
func probeNode(t *testing.T) (*Node, *staleProbeBackend) {
	t.Helper()
	probe := &staleProbeBackend{}
	n := NewNode(NodeConfig{
		Name:    "n0",
		Backend: probe,
		Scratch: MakeScratch(t.TempDir(), "n0"),
		Ports:   NewPortAllocator(""),
	})
	dir := n.ServiceScratch("svc").Root
	if err := n.Mkdir(context.Background(), dir); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	probe.path = filepath.Join(dir, "stdout.log")
	return n, probe
}

// TestStartCapturedRemovesStaleLog checks that the first launch on a node
// removes a log left there by an earlier run, whatever the policy: rotation is
// for this run's own previous process, and an earlier run's output must never
// be kept as if it were.
func TestStartCapturedRemovesStaleLog(t *testing.T) {
	for name, policy := range map[string]CapturePolicy{"truncate": CaptureTruncate, "rotate": CaptureRotate} {
		t.Run(name, func(t *testing.T) {
			n, probe := probeNode(t)
			svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
			svc.SetCapturePolicy(policy)
			svc.Bind([]*Node{n})

			// Plant a log at the capture path, as a previous run of the service
			// would leave behind on a node whose scratch persists across runs.
			ctx := context.Background()
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
			if got, want := artifactNames(t, svc, n), []string{"stdout.log"}; !slices.Equal(got, want) {
				t.Errorf("artifacts = %v, want %v (a stale log is never rotated)", got, want)
			}
		})
	}
}

// TestStartCapturedRotatesBeforeLaunch pins the ordering of a rotation the way
// TestStartCapturedRemovesStaleLog pins a removal: the previous process's log
// must already be moved aside when the next process is launched, or a
// readiness poll in the window before the child's redirect runs would match
// the previous process's output.
func TestStartCapturedRotatesBeforeLaunch(t *testing.T) {
	n, probe := probeNode(t)
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.SetCapturePolicy(CaptureRotate)
	svc.Bind([]*Node{n})
	ctx := context.Background()

	// The first launch does not run anything on this backend, so stand in for
	// the process it would have started by writing its output.
	first, err := svc.StartCaptured(ctx, n, Command("true"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}
	defer first.Close()
	if err := n.WriteFile(ctx, probe.path, []byte("first process")); err != nil {
		t.Fatalf("write: %v", err)
	}

	probe.streamed, probe.staleAtLaunch = false, false
	second, err := svc.StartCaptured(ctx, n, Command("true"))
	if err != nil {
		t.Fatalf("StartCaptured: %v", err)
	}
	defer second.Close()

	if !probe.streamed {
		t.Fatalf("StartCaptured returned without launching the process")
	}
	if probe.staleAtLaunch {
		t.Errorf("previous stdout.log still present when the process was launched; StartCaptured must rotate it first")
	}
	rotated := filepath.Join(filepath.Dir(probe.path), "stdout.1.log")
	if got, err := os.ReadFile(rotated); err != nil || string(got) != "first process" {
		t.Errorf("stdout.1.log = %q (%v), want the first process's output", got, err)
	}
	if got, want := artifactNames(t, svc, n), []string{"stdout.1.log", "stdout.log"}; !slices.Equal(got, want) {
		t.Errorf("artifacts = %v, want %v", got, want)
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
