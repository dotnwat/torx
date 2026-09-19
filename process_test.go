//go:build unix

package torx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// ignoringTERM is a command that ignores SIGTERM, records its pid, and loops:
// only a SIGKILL stops it, so a graceful stop of it must time out and kill.
func ignoringTERM(pidfile string) Cmd {
	return Command("sh", "-c", "trap '' TERM; echo $$ > "+pidfile+"; while true; do sleep 0.1; done")
}

// exitingOnTERM is a command that exits with code on SIGTERM once it has
// printed ready, which a test reads before signalling so the signal cannot
// arrive before the trap is installed.
func exitingOnTERM(code int) Cmd {
	return Command("sh", "-c", "trap 'exit "+strconv.Itoa(code)+"' TERM; echo ready; while true; do sleep 0.1; done")
}

// readLine reads one line from p, failing the test if none arrives.
func readLine(t *testing.T, p Process) string {
	t.Helper()
	line, err := bufio.NewReader(p).ReadString('\n')
	if err != nil {
		t.Fatalf("reading the command's output: %v", err)
	}
	return strings.TrimSpace(line)
}

// waitForPidfile returns the pid a command wrote to pidfile once it appears.
func waitForPidfile(t *testing.T, pidfile string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(pidfile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pidfile did not appear")
	return 0
}

// processAlive reports whether pid names a live process; signal 0 checks for
// existence without delivering anything. A zombie still counts as alive.
func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func eventuallyDead(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return !processAlive(pid)
}

func TestShutdownGraceful(t *testing.T) {
	var b LocalBackend
	p, err := b.Stream(context.Background(), exitingOnTERM(0))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer p.Close()
	if got := readLine(t, p); got != "ready" {
		t.Fatalf("first line = %q, want ready", got)
	}

	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, 5*time.Second)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if code != 0 {
		t.Errorf("exit status = %d, want 0", code)
	}
}

// TestShutdownKillsSurvivingChildren checks that a graceful stop still kills
// what the command left behind: a child that outlives it, here a sleep the
// shell backgrounded with its output redirected so it holds no pipe. The
// command's own exit is reaped by the wait, but the group lives on while the
// child does, and Close must kill it all the same.
func TestShutdownKillsSurvivingChildren(t *testing.T) {
	var b LocalBackend
	childfile := filepath.Join(t.TempDir(), "child")
	p, err := b.Stream(context.Background(), Command("sh", "-c",
		"trap 'exit 0' TERM; sleep 30 >/dev/null 2>&1 & echo $! > "+childfile+"; echo ready; while true; do sleep 0.1; done"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := readLine(t, p); got != "ready" {
		t.Fatalf("first line = %q, want ready", got)
	}
	child := waitForPidfile(t, childfile)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })

	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, 5*time.Second)
	if err != nil || code != 0 {
		t.Fatalf("Shutdown = (%d, %v), want (0, nil)", code, err)
	}
	if !eventuallyDead(child, 2*time.Second) {
		t.Errorf("child %d survived Shutdown: Close did not kill the group once the command had been reaped", child)
	}
}

// TestShutdownKillsAfterGrace checks the escalation: a command that ignores the
// signal is killed when the grace period ends, the outcome says so, and nothing
// of the command survives.
func TestShutdownKillsAfterGrace(t *testing.T) {
	var b LocalBackend
	pidfile := filepath.Join(t.TempDir(), "pid")
	p, err := b.Stream(context.Background(), ignoringTERM(pidfile))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer p.Close()
	pid := waitForPidfile(t, pidfile)

	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, 200*time.Millisecond)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Shutdown err = %v, want ErrShutdownTimeout", err)
	}
	if code != -1 {
		t.Errorf("exit status = %d, want -1 (no status once killed)", code)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Errorf("pid %d survived Shutdown: the close did not kill it", pid)
	}
	// After the fact, Wait reports how the command died.
	if code, err := p.Wait(context.Background()); err != nil || code != -1 {
		t.Errorf("Wait after Shutdown = (%d, %v), want (-1, nil): killed by a signal", code, err)
	}
}

// TestShutdownOfExitedCommand checks that a command already gone counts as
// stopped: the signal is refused, but its exit status is reported, not the
// refusal.
func TestShutdownOfExitedCommand(t *testing.T) {
	var b LocalBackend
	p, err := b.Stream(context.Background(), Command("sh", "-c", "exit 4"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer p.Close()
	// Reap it first, so the signal is certainly refused rather than delivered to
	// a zombie; the outcome must be the same either way.
	if code, err := p.Wait(context.Background()); err != nil || code != 4 {
		t.Fatalf("Wait = (%d, %v), want (4, nil)", code, err)
	}

	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, time.Second)
	if err != nil {
		t.Fatalf("Shutdown of an exited command: %v", err)
	}
	if code != 4 {
		t.Errorf("exit status = %d, want 4", code)
	}
}

// TestShutdownHonorsContext checks that the caller's context cuts the wait
// short ahead of the grace period, reported as the context's error rather than
// a shutdown timeout, and that the command is still closed.
func TestShutdownHonorsContext(t *testing.T) {
	var b LocalBackend
	pidfile := filepath.Join(t.TempDir(), "pid")
	p, err := b.Stream(context.Background(), ignoringTERM(pidfile))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer p.Close()
	pid := waitForPidfile(t, pidfile)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = Shutdown(ctx, p, syscall.SIGTERM, time.Minute)
	if !errors.Is(err, ErrBackend) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown err = %v, want ErrBackend wrapping the context's deadline", err)
	}
	if errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("Shutdown reported a grace-period timeout for a wait its caller cut short: %v", err)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Errorf("pid %d survived Shutdown: the close did not kill it", pid)
	}
}

// scriptedProcess is a Process whose outcomes are given, for the branches of
// Shutdown that no real command can be made to take on cue.
type scriptedProcess struct {
	sigErr, waitErr, closeErr error
}

func (p *scriptedProcess) Read([]byte) (int, error)                { return 0, io.EOF }
func (p *scriptedProcess) Close() error                            { return p.closeErr }
func (p *scriptedProcess) Signal(context.Context, os.Signal) error { return p.sigErr }
func (p *scriptedProcess) Wait(context.Context) (int, error) {
	if p.waitErr != nil {
		return -1, p.waitErr
	}
	return 0, nil
}

// TestShutdownPreservesWaitError checks that a wait which failed on its own --
// the transport lost track of the command -- is reported as that failure, not
// as a grace-period timeout: the grace period never ran out, and a service that
// tolerates a timeout (a stubborn process it only logs) must not be made to
// tolerate a broken backend.
func TestShutdownPreservesWaitError(t *testing.T) {
	lost := errors.New("connection lost")
	p := &scriptedProcess{waitErr: Wrap(ErrBackend, "backend: wait", lost)}
	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, time.Minute)
	if code != -1 || !errors.Is(err, lost) {
		t.Errorf("Shutdown = (%d, %v), want (-1, the wait's own error)", code, err)
	}
	if errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("Shutdown reported a grace-period timeout for a wait that failed before the grace period was up: %v", err)
	}
}

// TestShutdownReportsCloseFailureOverWaitError checks the precedence when both
// the wait and the close fail: the kill that could not be carried out is the
// answer, since the command may still be running.
func TestShutdownReportsCloseFailureOverWaitError(t *testing.T) {
	killFailed := errors.New("kill failed")
	p := &scriptedProcess{
		waitErr:  Wrap(ErrBackend, "backend: wait", errors.New("connection lost")),
		closeErr: Wrap(ErrBackend, "backend: kill", killFailed),
	}
	code, err := Shutdown(context.Background(), p, syscall.SIGTERM, time.Minute)
	if code != -1 || !errors.Is(err, killFailed) {
		t.Errorf("Shutdown = (%d, %v), want (-1, the close's error)", code, err)
	}
}
