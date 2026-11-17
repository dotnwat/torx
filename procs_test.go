//go:build unix

package torx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newTestProcs(t *testing.T) (*Procs, *Node) {
	t.Helper()
	n := captureTestNode(t)
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.SetCapturePolicy(CaptureRotate)
	if err := n.Mkdir(context.Background(), n.ServiceScratch("svc").Root); err != nil {
		t.Fatal(err)
	}
	return NewProcs(svc), n
}

// sleeper is a process that runs until it is stopped, and says so on
// SIGTERM.
func sleeper(word string) Cmd {
	return Command("sh", "-c", `trap 'echo `+word+` got TERM; exit 0' TERM; echo `+word+` up; while :; do sleep 0.05; done`)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProcsLifecycle(t *testing.T) {
	ctx := context.Background()
	p, n := newTestProcs(t)
	if err := p.Launch(ctx, "a", n, sleeper("a")); err != nil {
		t.Fatal(err)
	}
	if err := p.Launch(ctx, "a", n, sleeper("a")); err == nil {
		t.Fatal("a second Launch of a slot succeeded")
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a up")
	if !p.Running("a") || p.Paused("a") || p.Launches("a") != 1 || p.Node("a") != n {
		t.Fatalf("after launch: running %v paused %v launches %d", p.Running("a"), p.Paused("a"), p.Launches("a"))
	}
	if err := p.Restart(ctx, "a"); err == nil {
		t.Fatal("Restart of a running process succeeded")
	}

	if err := p.Pause(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if !p.Paused("a") {
		t.Fatal("not paused after Pause")
	}
	if err := p.Resume(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if err := p.Crash(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if p.Running("a") {
		t.Fatal("running after Crash")
	}
	if err := p.Crash(ctx, "a"); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Crash of a crashed process: %v, want ErrNotRunning", err)
	}

	if err := p.Restart(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a up")
	// A paused process is resumed to act on the stop signal.
	if err := p.Pause(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	clean, err := p.Terminate(ctx, "a", StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second})
	if err != nil || !clean {
		t.Fatalf("Terminate = %v, %v; want a clean stop", clean, err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a got TERM")

	if err := p.Relaunch(ctx, "a", sleeper("b")); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "b up")
	if p.Launches("a") != 3 || !strings.Contains(strings.Join(p.Cmd("a").Args, " "), "b up") {
		t.Fatalf("after relaunch: launches %d cmd %v", p.Launches("a"), p.Cmd("a"))
	}
	if exits := p.Exits(); len(exits) != 0 {
		t.Fatalf("exits recorded for processes Procs stopped: %v", exits)
	}
	if err := p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if len(p.Names()) != 0 {
		t.Fatalf("StopNode left %v", p.Names())
	}
}

func TestProcsUnexpectedExit(t *testing.T) {
	ctx := context.Background()
	p, n := newTestProcs(t)
	var mu sync.Mutex
	var seen []ProcExit
	p.OnExit = func(e ProcExit) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, e)
	}
	p.TailLines = 2
	if err := p.Launch(ctx, "dies", n, Command("sh", "-c", "echo one; echo two; echo panic: boom; exit 3")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the exit", func() bool { return len(p.Exits()) == 1 })
	e := p.Exits()[0]
	if e.Name != "dies" || e.Node != "n0" || e.Code != 3 || e.Launch != 1 {
		t.Fatalf("exit = %+v", e)
	}
	if e.Tail != "two\npanic: boom" {
		t.Fatalf("tail = %q, want the last two lines", e.Tail)
	}
	mu.Lock()
	if len(seen) != 1 || seen[0].Code != 3 {
		t.Fatalf("OnExit saw %v", seen)
	}
	mu.Unlock()
	if p.Running("dies") {
		t.Fatal("running after it exited")
	}
	// The slot can be started again, and its first launch's log is kept.
	if err := p.Relaunch(ctx, "dies", sleeper("again")); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "dies"), "again up")
	if err := p.Crash(ctx, "dies"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(p.Exits()); got != 1 {
		t.Fatalf("a crash was recorded as an exit: %d exits", got)
	}
}

// A process outlives the context it was launched or restarted under: a
// supervisor restarts one under a context that ends long before the job.
func TestProcsOutliveLaunchContext(t *testing.T) {
	p, n := newTestProcs(t)
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Launch(ctx, "a", n, sleeper("a")); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a up")
	cancel()
	time.Sleep(300 * time.Millisecond)
	if !p.Running("a") || len(p.Exits()) != 0 {
		t.Fatalf("the process died with its launch's context: running %v, exits %v", p.Running("a"), p.Exits())
	}
	if err := p.Crash(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	if err := p.Restart(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(300 * time.Millisecond)
	if !p.Running("a") {
		t.Fatal("the process died with its restart's context")
	}
	// A launch under a context already done fails, and leaves nothing.
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Launch(done, "b", n, sleeper("b")); err == nil {
		t.Fatal("a launch under a done context succeeded")
	}
	if p.Running("b") {
		t.Fatal("a failed launch left a process running")
	}
	if err := p.StopNode(context.Background(), n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

func TestProcsUnknown(t *testing.T) {
	ctx := context.Background()
	p, _ := newTestProcs(t)
	if err := p.Restart(ctx, "x"); err == nil {
		t.Fatal("Restart of an unknown process succeeded")
	}
	if err := p.Pause(ctx, "x"); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Pause of an unknown process: %v", err)
	}
	if _, err := p.Terminate(ctx, "x", StopPolicy{Signal: syscall.SIGTERM}); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Terminate of an unknown process: %v", err)
	}
	if p.Running("x") || p.Node("x") != nil || p.Launches("x") != 0 {
		t.Fatal("an unknown process has state")
	}
}
