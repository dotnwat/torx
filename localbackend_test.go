package torx

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalBackendExec(t *testing.T) {
	var b LocalBackend
	res, err := b.Exec(context.Background(), Command("sh", "-c", "echo out; echo err >&2"))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	if strings.TrimSpace(string(res.Stdout)) != "out" {
		t.Errorf("Stdout = %q, want out", res.Stdout)
	}
	if strings.TrimSpace(string(res.Stderr)) != "err" {
		t.Errorf("Stderr = %q, want err", res.Stderr)
	}
}

func TestLocalBackendExecEnv(t *testing.T) {
	var b LocalBackend
	res, err := b.Exec(context.Background(), Cmd{
		Path: "sh",
		Args: []string{"-c", "echo $FOO"},
		Env:  []string{"FOO=bar"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != "bar" {
		t.Errorf("Stdout = %q, want bar", res.Stdout)
	}
}

func TestLocalBackendExecDir(t *testing.T) {
	var b LocalBackend
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "marker"), []byte("here"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	res, err := b.Exec(context.Background(), Cmd{Path: "sh", Args: []string{"-c", "cat marker"}, Dir: dir})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != "here" {
		t.Errorf("Dir not applied: Stdout = %q", res.Stdout)
	}
}

func TestLocalBackendExecExitCode(t *testing.T) {
	var b LocalBackend
	res, err := b.Exec(context.Background(), Command("sh", "-c", "exit 3"))
	if err != nil {
		t.Fatalf("a non-zero exit must not be an error: %v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", res.ExitCode)
	}
}

func TestLocalBackendExecNotFound(t *testing.T) {
	var b LocalBackend
	_, err := b.Exec(context.Background(), Command("torx-nonexistent-binary-xyz"))
	if !errors.Is(err, ErrBackend) {
		t.Errorf("err = %v, want ErrBackend", err)
	}
}

func TestLocalBackendExecCancel(t *testing.T) {
	var b LocalBackend
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := b.Exec(ctx, Command("sh", "-c", "sleep 5"))
	if !errors.Is(err, ErrBackend) {
		t.Errorf("err = %v, want ErrBackend", err)
	}
}

func TestLocalBackendStream(t *testing.T) {
	var b LocalBackend
	rc, err := b.Stream(context.Background(), Command("sh", "-c", "echo line1; echo line2"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer rc.Close()

	var lines []string
	sc := bufio.NewScanner(rc)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) != 2 || lines[0] != "line1" || lines[1] != "line2" {
		t.Errorf("streamed lines = %v, want [line1 line2]", lines)
	}
}

func TestLocalBackendFileOps(t *testing.T) {
	var b LocalBackend
	ctx := context.Background()
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "file.txt") // nested: needs Mkdir first

	if ok, err := b.Exists(ctx, p); err != nil || ok {
		t.Errorf("Exists(missing) = (%v, %v), want (false, nil)", ok, err)
	}
	if err := b.Mkdir(ctx, filepath.Dir(p)); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := b.WriteFile(ctx, p, []byte("hello")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if ok, err := b.Exists(ctx, p); err != nil || !ok {
		t.Errorf("Exists = (%v, %v), want (true, nil)", ok, err)
	}
	if got, err := b.ReadFile(ctx, p); err != nil || string(got) != "hello" {
		t.Errorf("ReadFile = (%q, %v), want (hello, nil)", got, err)
	}

	dst := filepath.Join(dir, "copy.txt")
	if err := b.Put(ctx, p, dst); err != nil {
		t.Fatalf("Put: %v", err)
	}
	back := filepath.Join(dir, "got.txt")
	if err := b.Get(ctx, dst, back); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := b.ReadFile(ctx, back); string(got) != "hello" {
		t.Errorf("round-tripped copy = %q, want hello", got)
	}

	if err := b.Rm(ctx, dir); err != nil {
		t.Fatalf("Rm: %v", err)
	}
	if ok, _ := b.Exists(ctx, p); ok {
		t.Errorf("Exists after Rm = true, want false")
	}
	if err := b.Rm(ctx, dir); err != nil {
		t.Errorf("Rm of an absent path should be a no-op, got %v", err)
	}

	if _, err := b.ReadFile(ctx, filepath.Join(dir, "nope")); !errors.Is(err, ErrBackend) {
		t.Errorf("ReadFile(missing) err = %v, want ErrBackend", err)
	}
}

func TestLocalBackendSignal(t *testing.T) {
	var b LocalBackend
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	if err := b.Signal(context.Background(), c.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	if err := c.Wait(); err == nil {
		t.Errorf("process survived SIGKILL (Wait returned nil)")
	}
}

func TestLocalBackendSignalRejectsUnsafePID(t *testing.T) {
	var b LocalBackend
	// os.FindProcess never fails on Unix, so without a guard these would reach
	// kill(2) and hit the caller's process group (0), every signalable process
	// (-1), init (1), or an arbitrary group (< -1).
	for _, pid := range []int{0, 1, -1, -1000} {
		err := b.Signal(context.Background(), pid, syscall.SIGKILL)
		if err == nil {
			t.Errorf("Signal(pid=%d) = nil, want a refusal", pid)
			continue
		}
		if !errors.Is(err, ErrBackend) {
			t.Errorf("Signal(pid=%d) err = %v, want ErrBackend", pid, err)
		}
	}
}

// TestLocalBackendStreamSignalAndWait checks the handle's signal and wait: the
// signal reaches the command, Wait reports the exit status it chose, and once
// the command has been reaped a further signal is refused rather than sent to
// a pid the kernel may have reused.
func TestLocalBackendStreamSignalAndWait(t *testing.T) {
	var b LocalBackend
	p, err := b.Stream(context.Background(), exitingOnTERM(3))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer p.Close()
	if got := readLine(t, p); got != "ready" {
		t.Fatalf("first line = %q, want ready", got)
	}

	if err := p.Signal(context.Background(), syscall.SIGTERM); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := p.Wait(ctx); err != nil || code != 3 {
		t.Fatalf("Wait = (%d, %v), want (3, nil)", code, err)
	}
	// A second Wait reports the same exit.
	if code, err := p.Wait(ctx); err != nil || code != 3 {
		t.Errorf("second Wait = (%d, %v), want (3, nil)", code, err)
	}
	err = p.Signal(context.Background(), syscall.SIGTERM)
	if !errors.Is(err, ErrBackend) || !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after exit = %v, want ErrBackend wrapping os.ErrProcessDone", err)
	}
	// Close after the exit is clean, and repeatable.
	for range 2 {
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
}

// TestLocalBackendStreamWaitHonorsContext checks that Wait returns when its
// context is done, leaving the command running for Close to kill, and that a
// Wait after the Close reports the kill.
func TestLocalBackendStreamWaitHonorsContext(t *testing.T) {
	var b LocalBackend
	dir := t.TempDir()
	pidfile, childfile := filepath.Join(dir, "pid"), filepath.Join(dir, "child")
	// The command ignores SIGTERM and holds a child, so only Close's group kill
	// ends it -- and the child going with it shows the group was killed.
	p, err := b.Stream(context.Background(), Command("sh", "-c",
		"trap '' TERM; echo $$ > "+pidfile+"; sleep 30 & echo $! > "+childfile+"; wait"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	pid, child := waitForPidfile(t, pidfile), waitForPidfile(t, childfile)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = p.Wait(ctx)
	if !errors.Is(err, ErrBackend) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait err = %v, want ErrBackend wrapping the deadline", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("Wait took %v: it ignored its context", time.Since(start))
	}
	if !processAlive(pid) {
		t.Fatalf("pid %d is gone: a Wait cut short must not stop the command", pid)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Errorf("pid %d survived Close", pid)
	}
	if code, err := p.Wait(context.Background()); err != nil || code != -1 {
		t.Errorf("Wait after Close = (%d, %v), want (-1, nil): killed by a signal", code, err)
	}
	if !eventuallyDead(child, 2*time.Second) {
		t.Errorf("child %d of pid %d survived Close: the group was not killed", child, pid)
	}
}
