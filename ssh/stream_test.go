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
