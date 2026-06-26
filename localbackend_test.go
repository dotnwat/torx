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
