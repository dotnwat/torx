package ssh

import (
	"bufio"
	"context"
	"fmt"
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
