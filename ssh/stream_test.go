package ssh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

func TestStreamReadsOutputThenCloses(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", "echo hello; sleep 30"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(line) != "hello" {
		t.Errorf("stream output = %q, want hello", strings.TrimSpace(line))
	}
	if err := stream.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

// TestStreamFailsWhenRemoteExitsBeforeMarker is the regression for the marker
// handshake hang: a remote that accepts the exec but dies before printing the
// pgid marker must make Stream return an error, not block forever. It relies on
// the EOF-delivering goroutine starting before the marker read.
func TestStreamFailsWhenRemoteExitsBeforeMarker(t *testing.T) {
	s := buildTestServer(t)
	s.silentExec = true
	go s.serve()

	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)

	type result struct {
		rc  io.ReadCloser
		err error
	}
	ch := make(chan result, 1)
	go func() {
		rc, err := b.Stream(context.Background(), torx.Command("true"))
		ch <- result{rc, err}
	}()
	select {
	case r := <-ch:
		if r.err == nil {
			_ = r.rc.Close()
			t.Fatal("Stream returned a healthy handle when the remote exited before the marker")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stream hung waiting for a marker the remote never sent")
	}
}

// TestStreamMarkerReadHonorsContext checks that a remote which accepts the exec
// but never emits the marker cannot wedge Stream: the marker read is bounded by
// the context deadline.
func TestStreamMarkerReadHonorsContext(t *testing.T) {
	s := buildTestServer(t)
	s.stallExec = true
	go s.serve()

	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		rc, err := b.Stream(ctx, torx.Command("true"))
		if rc != nil {
			_ = rc.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Stream should fail when the marker never arrives before the deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stream hung: the marker read ignored the context deadline")
	}
}

// TestStreamMarkerReadHonorsCancelWithoutDeadline is the regression for the
// deadline-free cancellation gap: the operator's stop context has no deadline, so
// a marker read that only installed one on ctx.Deadline() would block forever
// once cancelled. Cancelling must unblock the read promptly.
func TestStreamMarkerReadHonorsCancelWithoutDeadline(t *testing.T) {
	s := buildTestServer(t)
	s.stallExec = true
	go s.serve()

	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		rc, err := b.Stream(ctx, torx.Command("true"))
		if rc != nil {
			_ = rc.Close()
		}
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the marker read block
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Stream should fail once the cancelled context unblocks the marker read")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stream hung: the marker read ignored a deadline-free cancellation")
	}
}

func TestStreamHonorsDir(t *testing.T) {
	b := dialBackend(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "marker"), []byte("in-dir\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	// cat resolves "marker" relative to the working directory, so it prints the
	// file's contents only if Stream honored Cmd.Dir. With exec applied to the cd
	// builtin the command never ran at all, and the stream would carry no output.
	stream, err := b.Stream(context.Background(), torx.Cmd{Path: "cat", Args: []string{"marker"}, Dir: dir})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	line, _ := bufio.NewReader(stream).ReadString('\n')
	if strings.TrimSpace(line) != "in-dir" {
		t.Errorf("stream output = %q, want in-dir: Cmd.Dir was not honored", strings.TrimSpace(line))
	}
}

func TestStreamHonorsDirAndEnv(t *testing.T) {
	b := dialBackend(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "marker"), []byte("here\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	// Exercises the whole cd + exec + env chain at once: the command runs in Dir
	// (cat marker) with FOO set from Env.
	stream, err := b.Stream(context.Background(), torx.Cmd{
		Path: "sh", Args: []string{"-c", "cat marker; echo $FOO"},
		Dir: dir, Env: []string{"FOO=bar"},
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	r := bufio.NewReader(stream)
	fromDir, _ := r.ReadString('\n')
	fromEnv, _ := r.ReadString('\n')
	if strings.TrimSpace(fromDir) != "here" {
		t.Errorf("first line = %q, want here (Cmd.Dir)", strings.TrimSpace(fromDir))
	}
	if strings.TrimSpace(fromEnv) != "bar" {
		t.Errorf("second line = %q, want bar (Cmd.Env)", strings.TrimSpace(fromEnv))
	}
}

// TestStreamForwardsStdin is the regression for Stream silently dropping
// Cmd.Stdin: Exec and the LocalBackend honor it, so StartCaptured's contract
// requires Stream to as well. cat echoes its stdin, which the wrapper feeds to
// the exec'd command; the stream carries it back after the pgid marker line.
func TestStreamForwardsStdin(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), torx.Cmd{Path: "cat", Stdin: []byte("round-trip\n")})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.TrimSpace(line) != "round-trip" {
		t.Errorf("stream output = %q, want round-trip: Cmd.Stdin was not forwarded", strings.TrimSpace(line))
	}
}

func TestStreamCloseKillsSignalIgnoringGroup(t *testing.T) {
	b := dialBackend(t)
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "pid")
	// Ignore SIGHUP and SIGTERM, record the pid, then loop forever: only a
	// SIGKILL delivered to the whole process group can stop this.
	script := fmt.Sprintf("trap '' HUP TERM; echo $$ > %s; while true; do sleep 1; done", pidfile)

	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", script))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	pid := waitForPid(t, pidfile)
	if !processAlive(pid) {
		t.Fatalf("service pid %d should be running", pid)
	}

	if err := stream.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Errorf("service pid %d survived Close: teardown did not kill the group", pid)
	}
}

// TestStreamCloseSurfacesKillFailure checks that Close reports a remote kill it
// could not deliver, instead of returning success while the service may still be
// running. The node is made unreachable before Close, so the kill cannot run.
func TestStreamCloseSurfacesKillFailure(t *testing.T) {
	s := newTestServer(t)
	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)

	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", "sleep 30"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// Drop the connection and stop the listener so Close's fresh session to run the
	// remote kill cannot be established.
	b.close()
	_ = s.ln.Close()

	if err := stream.Close(); err == nil {
		t.Error("Close reported success though the remote kill could not be delivered")
	}
}

// TestStreamContextCancelTerminatesCommand checks that cancelling the context
// that started a stream tears the remote command down, matching the LocalBackend
// -- so a cancelled run does not leave services running on the node. Close is not
// called; the cancellation alone must do it.
func TestStreamContextCancelTerminatesCommand(t *testing.T) {
	b := dialBackend(t)
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "pid")
	// Ignore SIGHUP and SIGTERM and loop, so only a SIGKILL to the whole group
	// stops it -- which is what the context watcher must deliver.
	script := fmt.Sprintf("trap '' HUP TERM; echo $$ > %s; while true; do sleep 1; done", pidfile)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := b.Stream(ctx, torx.Command("sh", "-c", script))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	pid := waitForPid(t, pidfile)
	if !processAlive(pid) {
		t.Fatalf("service pid %d should be running", pid)
	}

	cancel() // no Close: cancelling the context alone must tear the command down
	if !eventuallyDead(pid, 3*time.Second) {
		t.Errorf("service pid %d survived context cancellation", pid)
	}
}

func waitForPid(t *testing.T, pidfile string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
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
// existence without delivering anything.
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
