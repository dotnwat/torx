package ssh

import (
	"bufio"
	"context"
	"errors"
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

// TestStreamKillsGroupWhenLoginShellForks covers a login shell that forks the
// command instead of exec'ing it (dash, Debian's /bin/sh): the wrapper is then
// not the session leader and must move the command into a group of its own, so
// Close still kills the whole tree.
func TestStreamKillsGroupWhenLoginShellForks(t *testing.T) {
	b := dialBackendWith(t, func(s *testServer) { s.forkDepth = 1 })
	assertCloseKillsService(t, b)
}

// TestStreamIsolatesNestedWrappers covers a forking login shell under a forking
// ForceCommand or audit wrapper: the session is still the command's own, and
// Stream must run it and kill it on Close, however many layers sit above it.
func TestStreamIsolatesNestedWrappers(t *testing.T) {
	b := dialBackendWith(t, func(s *testServer) { s.forkDepth = 2 })
	assertCloseKillsService(t, b)
}

// TestStreamIsolatesCommandInSharedGroup covers an sshd that does not start
// commands in a new session (Dropbear's shape): the command begins in the
// server's own process group -- here, this test binary's. The wrapper must move
// it into a group of its own, so Close kills the service and nothing else; the
// server is still serving afterwards, and so is this process.
func TestStreamIsolatesCommandInSharedGroup(t *testing.T) {
	b := dialBackendWith(t, func(s *testServer) { s.sharedGroup = true })
	assertCloseKillsService(t, b)
	if _, err := b.Exec(context.Background(), torx.Command("sh", "-c", ":")); err != nil {
		t.Fatalf("server no longer serving after Close: %v", err)
	}
}

// TestStreamIsolatesWithPerl forces the perl fallback by hiding setsid: on a
// Linux host the util-linux tool would otherwise always win, leaving the path
// macOS depends on untested there.
func TestStreamIsolatesWithPerl(t *testing.T) {
	path := toolsPATH(t, "sh", "cat", "tr", "ps", "sleep", "perl")
	b := dialBackendWith(t, func(s *testServer) { s.forkDepth = 1; s.path = path })
	assertCloseKillsService(t, b)
}

// TestStreamRefusesWhenIsolationImpossible is the guard: a command that begins
// in the server's group, on a node with neither setsid nor perl, cannot be given
// a group of its own, and killing the shared group would kill the sshd (here,
// this test binary). Stream must fail before the command runs, say what would
// fix it, and leave the server serving.
func TestStreamRefusesWhenIsolationImpossible(t *testing.T) {
	path := toolsPATH(t, "sh", "cat", "tr", "ps")
	b := dialBackendWith(t, func(s *testServer) { s.sharedGroup = true; s.path = path })
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")

	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", "echo > "+marker+"; sleep 30"))
	if err == nil {
		_ = stream.Close()
		t.Fatal("Stream ran a command it could not isolate from the server's process group")
	}
	if !strings.Contains(err.Error(), "shares process group") || !strings.Contains(err.Error(), "setsid") {
		t.Errorf("error does not explain the refusal: %v", err)
	}
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Error("the command ran despite the refusal")
	}
	if _, err := b.Exec(context.Background(), torx.Command("sh", "-c", ":")); err != nil {
		t.Fatalf("server no longer serving after refusal: %v", err)
	}
}

// assertCloseKillsService streams a service that ignores SIGHUP and SIGTERM and
// records its pid, then checks that Close kills it: only a SIGKILL to the whole
// process group can, so the service surviving means the wrapper reported a
// group the service was not in.
func assertCloseKillsService(t *testing.T, b *backend) {
	t.Helper()
	pidfile := filepath.Join(t.TempDir(), "pid")
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
		t.Errorf("service pid %d survived Close: teardown did not kill its group", pid)
	}
}

// TestStreamCloseSurfacesKillFailure checks that Close reports a remote kill it
// could not deliver, instead of returning success while the service may still be
// running. The node is made unreachable before Close, so the kill cannot run.
// The lost connection ends the session first, without any word on the command:
// that must not pass for its exit, which would have Close skip the kill and
// Signal refuse to try -- while the command runs on.
func TestStreamCloseSurfacesKillFailure(t *testing.T) {
	s := newTestServer(t)
	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	pidfile := filepath.Join(t.TempDir(), "pid")
	script := fmt.Sprintf("trap '' HUP TERM; echo $$ > %s; while true; do sleep 1; done", pidfile)

	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", script))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	pid := waitForPid(t, pidfile)
	// The kill under test cannot reach it, so the loop is ended by hand: it
	// leads its group (the wrapper exec'd it in place), so its pid is the pgid.
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })

	// Drop the connection and stop the listener so Close's fresh session to run the
	// remote kill cannot be established, then wait for the session to end.
	b.close()
	_ = s.ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := stream.Wait(ctx); !errors.Is(err, torx.ErrBackend) {
		t.Fatalf("Wait after the connection dropped = %v, want ErrBackend: the session ended without the exit", err)
	}
	if err := stream.Signal(context.Background(), syscall.SIGTERM); errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after the connection dropped = %v: a lost session was taken for the command's exit", err)
	}
	if err := stream.Close(); err == nil {
		t.Error("Close reported success though the remote kill could not be delivered")
	}
	if !processAlive(pid) {
		t.Errorf("service pid %d is gone: nothing should have been able to kill it", pid)
	}
}

// TestStreamCloseAwaitsExitReport checks that Close, having delivered the kill,
// lets the node report the command's death before it closes the session, so a
// Wait after the Close can say how the command died. The server holds the
// report back, as an sshd slower to reap than the client is to close would: a
// close sent straight after the kill would reach it first, and the report
// would be discarded with the channel.
func TestStreamCloseAwaitsExitReport(t *testing.T) {
	b := dialBackendWith(t, func(s *testServer) { s.exitDelay = 300 * time.Millisecond })
	pidfile := filepath.Join(t.TempDir(), "pid")
	script := fmt.Sprintf("trap '' HUP TERM; echo $$ > %s; while true; do sleep 1; done", pidfile)
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", script))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	pid := waitForPid(t, pidfile)

	if err := stream.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Fatalf("service pid %d survived Close", pid)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := stream.Wait(ctx); err != nil || code != -1 {
		t.Errorf("Wait after Close = (%d, %v), want (-1, nil): the session was closed before the node reported the kill", code, err)
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

// trapsTERM is a command that traps SIGTERM, announces it, and exits cleanly,
// after first saying it is ready: only the program itself can do that, so a
// signal that reached a shell around it instead would show as a missing
// announcement and a signal death.
var trapsTERM = torx.Command("sh", "-c", "trap 'echo got-term; exit 0' TERM; echo ready; while true; do sleep 0.1; done")

// TestStreamSignalReachesCommand checks that the handle's Signal is delivered
// to the command the wrapper exec'd, and that Wait then reports the exit the
// command chose.
func TestStreamSignalReachesCommand(t *testing.T) {
	assertSignalReachesCommand(t, dialBackend(t))
}

// TestStreamSignalReachesCommandBehindForkingShell covers the shapes where the
// wrapper has to make a process group of its own: the pid it reports must still
// be the command's, not that of the shell setsid or perl started.
func TestStreamSignalReachesCommandBehindForkingShell(t *testing.T) {
	assertSignalReachesCommand(t, dialBackendWith(t, func(s *testServer) { s.forkDepth = 1 }))
}

func assertSignalReachesCommand(t *testing.T, b *backend) {
	t.Helper()
	stream, err := b.Stream(context.Background(), trapsTERM)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	r := bufio.NewReader(stream)
	if line, _ := r.ReadString('\n'); strings.TrimSpace(line) != "ready" {
		t.Fatalf("first line = %q, want ready", strings.TrimSpace(line))
	}

	if err := stream.Signal(context.Background(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}
	if line, _ := r.ReadString('\n'); strings.TrimSpace(line) != "got-term" {
		t.Errorf("line after the signal = %q, want got-term: the signal did not reach the command", strings.TrimSpace(line))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := stream.Wait(ctx); err != nil || code != 0 {
		t.Errorf("Wait = (%d, %v), want (0, nil)", code, err)
	}
}

// TestStreamShutdownGraceful runs the graceful stop end to end over ssh: the
// signal, the wait, and the close, with the command's own exit reported.
func TestStreamShutdownGraceful(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), trapsTERM)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if line, _ := bufio.NewReader(stream).ReadString('\n'); strings.TrimSpace(line) != "ready" {
		t.Fatalf("first line = %q, want ready", strings.TrimSpace(line))
	}
	code, err := torx.Shutdown(context.Background(), stream, syscall.SIGTERM, 5*time.Second)
	if err != nil || code != 0 {
		t.Errorf("Shutdown = (%d, %v), want (0, nil)", code, err)
	}
}

// TestStreamWaitReportsExitStatus checks that Wait carries the remote exit
// status back, and that a command which has exited can no longer be signalled:
// sshd has reaped it, so the pid may be someone else's by now.
func TestStreamWaitReportsExitStatus(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", "exit 7"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := stream.Wait(ctx); err != nil || code != 7 {
		t.Fatalf("Wait = (%d, %v), want (7, nil)", code, err)
	}
	err = stream.Signal(context.Background(), syscall.SIGTERM)
	if !errors.Is(err, torx.ErrBackend) || !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after exit = %v, want ErrBackend wrapping os.ErrProcessDone", err)
	}
}

func TestStreamWaitHonorsContext(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), torx.Command("sleep", "30"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = stream.Wait(ctx)
	if !errors.Is(err, torx.ErrBackend) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait err = %v, want ErrBackend wrapping the deadline", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("Wait took %v: it ignored its context", time.Since(start))
	}
}

// TestStreamShutdownKillsSurvivingChildren checks that a graceful stop still
// kills what the command left behind: a child that outlives it, here a sleep
// the shell backgrounded with its output redirected so it holds no session
// pipe. The command's own exit ends the session, but not the group, and Close
// must kill the group all the same.
func TestStreamShutdownKillsSurvivingChildren(t *testing.T) {
	b := dialBackend(t)
	childfile := filepath.Join(t.TempDir(), "child")
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c",
		"trap 'exit 0' TERM; sleep 30 >/dev/null 2>&1 & echo $! > "+childfile+"; echo ready; while true; do sleep 0.1; done"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if line, _ := bufio.NewReader(stream).ReadString('\n'); strings.TrimSpace(line) != "ready" {
		t.Fatalf("first line = %q, want ready", strings.TrimSpace(line))
	}
	child := waitForPid(t, childfile)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })

	code, err := torx.Shutdown(context.Background(), stream, syscall.SIGTERM, 5*time.Second)
	if err != nil || code != 0 {
		t.Fatalf("Shutdown = (%d, %v), want (0, nil)", code, err)
	}
	if !eventuallyDead(child, 2*time.Second) {
		t.Errorf("child %d survived Shutdown: Close did not kill the group once the command had exited", child)
	}
}

// TestStreamWaitObservesExitBehindHeldOutput checks that the command's exit
// counts as soon as the node reports it, even while a child that inherited its
// output pipes keeps the session open: sshd reports the exit on reaping the
// command and closes the session only once the pipes drain, and a wait that
// took the session's end for the exit would turn this clean, prompt exit into
// a shutdown timeout. The stream is drained throughout, as a capture would, so
// it is the pipes on the node that hold the session, not this side.
func TestStreamWaitObservesExitBehindHeldOutput(t *testing.T) {
	b := dialBackend(t)
	childfile := filepath.Join(t.TempDir(), "child")
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c",
		"trap 'exit 0' TERM; sleep 30 & echo $! > "+childfile+"; echo ready; while true; do sleep 0.1; done"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	r := bufio.NewReader(stream)
	if line, _ := r.ReadString('\n'); strings.TrimSpace(line) != "ready" {
		t.Fatalf("first line = %q, want ready", strings.TrimSpace(line))
	}
	go func() { _, _ = io.Copy(io.Discard, r) }()
	child := waitForPid(t, childfile)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })

	code, err := torx.Shutdown(context.Background(), stream, syscall.SIGTERM, 500*time.Millisecond)
	if err != nil || code != 0 {
		t.Fatalf("Shutdown = (%d, %v), want (0, nil): the exit was not observed while the child held the output", code, err)
	}
	if !eventuallyDead(child, 2*time.Second) {
		t.Errorf("child %d survived Shutdown: Close did not kill the group", child)
	}
}

// TestStreamShutdownKillsAfterGrace runs the escalation over ssh: a command that
// ignores the signal is killed when the grace period ends, and Wait then reports
// the signal death as -1 -- sshd reports it with an exit-signal reply, which
// crypto/ssh would otherwise render as 128 plus the signal number.
func TestStreamShutdownKillsAfterGrace(t *testing.T) {
	b := dialBackend(t)
	pidfile := filepath.Join(t.TempDir(), "pid")
	script := fmt.Sprintf("trap '' TERM; echo $$ > %s; while true; do sleep 0.1; done", pidfile)
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", script))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	pid := waitForPid(t, pidfile)

	code, err := torx.Shutdown(context.Background(), stream, syscall.SIGTERM, 200*time.Millisecond)
	if !errors.Is(err, torx.ErrShutdownTimeout) {
		t.Fatalf("Shutdown err = %v, want ErrShutdownTimeout", err)
	}
	if code != -1 {
		t.Errorf("exit status = %d, want -1 (no status once killed)", code)
	}
	if !eventuallyDead(pid, 2*time.Second) {
		t.Errorf("pid %d survived Shutdown: the close did not kill it", pid)
	}
	if code, err := stream.Wait(context.Background()); err != nil || code != -1 {
		t.Errorf("Wait after Shutdown = (%d, %v), want (-1, nil): killed by a signal", code, err)
	}
}

// TestStreamWaitReportsSignalDeath checks that a command the node reports as
// killed by a signal gets the -1 the Process contract promises, as the
// LocalBackend reports it, not the 128 plus the signal number crypto/ssh makes
// of the exit-signal reply.
func TestStreamWaitReportsSignalDeath(t *testing.T) {
	b := dialBackend(t)
	stream, err := b.Stream(context.Background(), torx.Command("sh", "-c", "kill -KILL $$"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := stream.Wait(ctx); err != nil || code != -1 {
		t.Errorf("Wait = (%d, %v), want (-1, nil): the node reported a signal death", code, err)
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
