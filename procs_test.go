//go:build unix

package torx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// lossyBackend is a LocalBackend whose node can drop out of reach of a
// process's handle -- the process runs on, and the handle can neither see
// it exit nor kill it, as an ssh session whose connection dropped cannot
// -- and whose tail can be held up.
type lossyBackend struct {
	Backend
	// tailGate, if not nil, holds every tail until it is closed.
	tailGate chan struct{}
	// started, if not nil, is called with each process as its launch goes
	// through, before the launch hears that it has.
	started func(*lossyProc)

	mu    sync.Mutex
	procs []*lossyProc
}

func (b *lossyBackend) Stream(ctx context.Context, cmd Cmd) (Process, error) {
	// A process out of reach is not killed through its context either.
	tp, err := b.Backend.Stream(context.WithoutCancel(ctx), cmd)
	if err != nil {
		return nil, err
	}
	lp := &lossyProc{Process: tp, lost: make(chan struct{})}
	b.mu.Lock()
	b.procs = append(b.procs, lp)
	b.mu.Unlock()
	if b.started != nil {
		b.started(lp)
	}
	return lp, nil
}

// launched is how many processes the backend has started.
func (b *lossyBackend) launched() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.procs)
}

func (b *lossyBackend) Exec(ctx context.Context, cmd Cmd) (ExecResult, error) {
	if cmd.Path == "tail" && b.tailGate != nil {
		<-b.tailGate
	}
	return b.Backend.Exec(ctx, cmd)
}

// last is the process launched last.
func (b *lossyBackend) last() *lossyProc {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.procs[len(b.procs)-1]
}

type lossyProc struct {
	Process
	lost chan struct{} // closed once the node is out of reach
	// unkillable makes the kill fail and nothing else: the node can still
	// see the process, and cannot kill it.
	unkillable atomic.Bool
}

func (p *lossyProc) isLost() bool {
	select {
	case <-p.lost:
		return true
	default:
		return false
	}
}

func (p *lossyProc) alive() bool {
	return syscall.Kill(p.Process.(*procStream).cmd.Process.Pid, 0) == nil
}

func (p *lossyProc) Wait(ctx context.Context) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-p.lost:
			cancel()
		case <-ctx.Done():
		}
	}()
	code, err := p.Process.Wait(ctx)
	if p.isLost() {
		return -1, Wrap(ErrBackend, "lossy: wait", errors.New("the session ended without reporting the command's exit"))
	}
	return code, err
}

func (p *lossyProc) Close() error {
	if p.isLost() {
		return Wrap(ErrBackend, "lossy: kill", errors.New("the node is out of reach"))
	}
	if p.unkillable.Load() {
		return Wrap(ErrBackend, "lossy: kill", errors.New("the kill could not be carried out"))
	}
	return p.Process.Close()
}

func newLossyProcs(t *testing.T) (*Procs, *Node, *lossyBackend) {
	t.Helper()
	b := &lossyBackend{Backend: LocalBackend{}}
	n := NewNode(NodeConfig{
		Name:    "n0",
		Backend: b,
		Scratch: MakeScratch(t.TempDir(), "n0"),
		Ports:   NewPortAllocator(""),
	})
	svc := NewServiceBase("svc", Homogeneous(1, NodeSpec{}), nil)
	svc.SetCapturePolicy(CaptureRotate)
	if err := n.Mkdir(context.Background(), n.ServiceScratch("svc").Root); err != nil {
		t.Fatal(err)
	}
	// What a test left out of reach is killed for real.
	t.Cleanup(func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, lp := range b.procs {
			_ = lp.Process.Close()
		}
	})
	return NewProcs(svc), n, b
}

// A process its node lost track of and could not kill may still run: the
// slot does not start another beside it, and StopNode does not call the
// node stopped.
func TestProcsLostProcess(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	if err := p.Launch(ctx, "a", n, sleeper("a")); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a up")
	lp := b.last()
	close(lp.lost)
	waitFor(t, "the exit", func() bool { return len(p.Exits()) == 1 })
	if e := p.Exits()[0]; e.Code != -1 || e.Err == "" || e.Tail != "a up" {
		t.Fatalf("exit = %+v, want a wait that failed", e)
	}
	if p.Running("a") {
		t.Fatal("running, as far as Procs can tell, after the node lost it")
	}
	if !lp.alive() {
		t.Fatal("the process did not outlive the handle that lost it")
	}
	err := p.Restart(ctx, "a")
	if !errors.Is(err, ErrBackend) || !strings.Contains(err.Error(), "may still run") {
		t.Fatalf("Restart over a process that may still run: %v", err)
	}
	if err := p.Relaunch(ctx, "a", sleeper("b")); !errors.Is(err, ErrBackend) {
		t.Fatalf("Relaunch over a process that may still run: %v", err)
	}
	if got := p.Launches("a"); got != 1 {
		t.Fatalf("%d launches, want the one", got)
	}
	err = p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second})
	if !errors.Is(err, ErrBackend) {
		t.Fatalf("StopNode after a kill that could not be carried out: %v", err)
	}
	if len(p.Names()) != 0 {
		t.Fatalf("StopNode left %v", p.Names())
	}
}

// A process being stopped runs until the stop is done with it: a Restart
// within its grace period waits, and starts nothing beside it.
func TestProcsRestartWaitsForStop(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	// It acts on SIGTERM only once the test lets it, so the test, not the
	// clock, says how long it is within its grace period.
	release := filepath.Join(t.TempDir(), "release")
	slow := Command("sh", "-c", `trap 'while [ ! -e "$0" ]; do sleep 0.02; done; exit 0' TERM; echo slow up; while :; do sleep 0.05; done`, release)
	if err := p.Launch(ctx, "a", n, slow); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "slow up")
	old := b.last()
	stopped := make(chan error, 1)
	go func() {
		clean, err := p.Terminate(ctx, "a", StopPolicy{Signal: syscall.SIGTERM, Grace: time.Minute})
		if err == nil && !clean {
			err = errors.New("the process was killed, not stopped")
		}
		stopped <- err
	}()
	waitFor(t, "the stop to begin", func() bool { return !p.Running("a") })

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err := p.Restart(short, "a")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Restart within the grace period: %v, want it to wait its context out", err)
	}
	if !old.alive() || p.Launches("a") != 1 {
		t.Fatalf("within the grace period: alive %v, launches %d", old.alive(), p.Launches("a"))
	}

	relaunched := make(chan error, 1)
	go func() { relaunched <- p.Relaunch(ctx, "a", sleeper("b")) }()
	select {
	case err := <-relaunched:
		t.Fatalf("Relaunch returned %v within the grace period", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := os.WriteFile(release, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := <-relaunched; err != nil {
		t.Fatal(err)
	}
	if old.alive() {
		t.Fatal("Relaunch started a process beside the one still being stopped")
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "b up")
	if !p.Running("a") || p.Launches("a") != 2 {
		t.Fatalf("after the relaunch: running %v, launches %d", p.Running("a"), p.Launches("a"))
	}
	if err := p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

// The record of an exit holds the log of the launch that exited, however
// soon a supervisor starts the next: the slot is not free until the log
// has been read.
func TestProcsExitRecordedBeforeRestart(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	b.tailGate = make(chan struct{})
	if err := p.Launch(ctx, "a", n, Command("sh", "-c", "echo original dying; exit 3")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the exit", func() bool { return !p.Running("a") })
	relaunched := make(chan error, 1)
	go func() { relaunched <- p.Relaunch(ctx, "a", sleeper("replacement")) }()
	select {
	case err := <-relaunched:
		t.Fatalf("Relaunch returned %v before the exit was recorded", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(b.tailGate)
	if err := <-relaunched; err != nil {
		t.Fatal(err)
	}
	exits := p.Exits()
	if len(exits) != 1 || exits[0].Launch != 1 || exits[0].Code != 3 || exits[0].Tail != "original dying" {
		t.Fatalf("exits = %+v, want the first launch's, with its own log", exits)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "replacement up")
	if err := p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

// StopNode waits for a process already on its way out, and reports the
// kill of it that failed.
func TestProcsStopNodeWaitsForExit(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	b.tailGate = make(chan struct{})
	if err := p.Launch(ctx, "a", n, sleeper("a")); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "a up")
	close(b.last().lost)
	waitFor(t, "the exit", func() bool { return !p.Running("a") })
	stopped := make(chan error, 1)
	go func() { stopped <- p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}) }()
	select {
	case err := <-stopped:
		t.Fatalf("StopNode returned %v while the exit was being recorded", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(b.tailGate)
	if err := <-stopped; !errors.Is(err, ErrBackend) {
		t.Fatalf("StopNode over an exit whose kill could not be carried out: %v", err)
	}
	if len(p.Exits()) != 1 || len(p.Names()) != 0 {
		t.Fatalf("exits %v, names %v", p.Exits(), p.Names())
	}
}

// What StopNode stops stays stopped: a process that exits as its node is
// stopped is not started again behind StopNode's back, by an OnExit that
// restarts what exits or by a Restart that was waiting for it to be gone.
func TestProcsStopNodeClosesSlots(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	b.tailGate = make(chan struct{})
	relaunched := make(chan error, 1)
	p.OnExit = func(e ProcExit) { relaunched <- p.Relaunch(ctx, e.Name, sleeper("replacement")) }
	if err := p.Launch(ctx, "a", n, Command("sh", "-c", "echo dying; exit 3")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the exit", func() bool { return !p.Running("a") })
	restarted := make(chan error, 1)
	go func() { restarted <- p.Restart(ctx, "a") }()
	stopped := make(chan error, 1)
	go func() { stopped <- p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}) }()
	waitFor(t, "StopNode to take the slot", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.slots["a"].closing
	})
	// The exit settles under StopNode: OnExit hears of it, and the Restart
	// that waited for it wakes.
	close(b.tailGate)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := <-relaunched; err == nil {
		t.Fatal("OnExit started the process again as its node was stopped")
	}
	if err := <-restarted; err == nil {
		t.Fatal("a Restart that was waiting started the process again as its node was stopped")
	}
	if got := b.launched(); got != 1 || len(p.Names()) != 0 {
		t.Fatalf("%d processes launched, names %v; want the one, forgotten", got, p.Names())
	}
}

// A launch whose context ends as it goes through is called off and its
// process killed. One that cannot be killed may still run: its name stays
// taken, and StopNode does not call the node stopped.
func TestProcsLaunchCalledOff(t *testing.T) {
	bg := context.Background()
	p, n, b := newLossyProcs(t)

	ctx, cancel := context.WithCancel(bg)
	b.started = func(*lossyProc) { cancel() }
	err := p.Launch(ctx, "gone", n, sleeper("gone"))
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrBackend) {
		t.Fatalf("a launch called off: %v, want its context's error alone", err)
	}
	if b.last().alive() || p.Node("gone") != nil {
		t.Fatalf("a launch called off left a process (alive %v) or a slot (%v)", b.last().alive(), p.Names())
	}

	ctx, cancel = context.WithCancel(bg)
	b.started = func(lp *lossyProc) {
		close(lp.lost)
		cancel()
	}
	err = p.Launch(ctx, "a", n, sleeper("a"))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrBackend) {
		t.Fatalf("a launch called off whose kill failed: %v, want both errors", err)
	}
	b.started = nil
	if !b.last().alive() {
		t.Fatal("the process did not outlive the kill that failed")
	}
	if p.Running("a") || p.Node("a") != n {
		t.Fatalf("after the launch: running %v, node %v; want a slot, not running", p.Running("a"), p.Node("a"))
	}
	if err := p.Launch(bg, "a", n, sleeper("b")); err == nil {
		t.Fatal("a second Launch of the name started a process beside one that may still run")
	}
	if err := p.Restart(bg, "a"); !errors.Is(err, ErrBackend) {
		t.Fatalf("Restart over a process that may still run: %v", err)
	}
	if got := b.launched(); got != 2 {
		t.Fatalf("%d processes launched, want the two called off", got)
	}
	err = p.StopNode(bg, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second})
	if !errors.Is(err, ErrBackend) {
		t.Fatalf("StopNode after a kill that could not be carried out: %v", err)
	}
}

// A stop that did not go as asked still ends in the kill, and only a kill
// that failed leaves a process that may run: a stop whose context ended is
// an error to its caller, and the slot starts again.
func TestProcsStopCutShort(t *testing.T) {
	ctx := context.Background()
	p, n, b := newLossyProcs(t)
	stubborn := Command("sh", "-c", `trap '' TERM; echo stubborn up; while :; do sleep 0.05; done`)
	if err := p.Launch(ctx, "a", n, stubborn); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "stubborn up")
	old := b.last()
	short, cancel := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() {
		_, err := p.Terminate(short, "a", StopPolicy{Signal: syscall.SIGTERM, Grace: time.Minute})
		stopped <- err
	}()
	waitFor(t, "the stop to begin", func() bool { return !p.Running("a") })
	cancel()
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Fatalf("a stop whose context ended: %v", err)
	}
	if old.alive() {
		t.Fatal("a stop cut short left its process running")
	}
	if err := p.Restart(ctx, "a"); err != nil {
		t.Fatalf("Restart after a stop cut short, its process killed: %v", err)
	}
	waitForLog(t, p.svc.CapturePath(n, "a"), "stubborn up")

	// The kill is what fails: the process may still run.
	b.last().unkillable.Store(true)
	_, err := p.Terminate(ctx, "a", StopPolicy{Signal: syscall.SIGTERM, Grace: 100 * time.Millisecond})
	if !errors.Is(err, ErrBackend) {
		t.Fatalf("a stop whose kill failed: %v", err)
	}
	if !b.last().alive() {
		t.Fatal("the process did not outlive the kill that failed")
	}
	if err := p.Restart(ctx, "a"); !errors.Is(err, ErrBackend) || !strings.Contains(err.Error(), "may still run") {
		t.Fatalf("Restart over a process that may still run: %v", err)
	}
	if err := p.StopNode(ctx, n, StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second}); !errors.Is(err, ErrBackend) {
		t.Fatalf("StopNode after a kill that could not be carried out: %v", err)
	}
}
