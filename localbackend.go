//go:build unix

package torx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// LocalBackend runs commands as local subprocesses and treats node paths as
// local filesystem paths. It is the v1 backend: no SSH and no remote agent, so
// the whole framework runs against localhost with nothing installed.
type LocalBackend struct{}

var _ Backend = LocalBackend{}

// localWaitDelay bounds how long Wait blocks for a command's I/O after the
// process is cancelled, a backstop in case a process escapes the killed group.
const localWaitDelay = 2 * time.Second

// minSignalablePID is the lowest pid Signal will target. A pid below 2 is never
// a specific process to signal: 1 is init, 0 is the caller's process group, and
// negatives select a process group or every process. Rejecting them keeps a
// stale or malformed pid from turning a signal into a group- or system-wide one.
const minSignalablePID = 2

func (b LocalBackend) command(ctx context.Context, cmd Cmd) *exec.Cmd {
	c := exec.CommandContext(ctx, cmd.Path, cmd.Args...)
	c.Dir = cmd.Dir
	if len(cmd.Env) > 0 {
		c.Env = append(os.Environ(), cmd.Env...)
	}
	if cmd.Stdin != nil {
		c.Stdin = bytes.NewReader(cmd.Stdin)
	}
	// Run each command in its own process group so cancellation kills the whole
	// tree, not just the direct child: a shell's grandchildren would otherwise
	// keep the output pipes open and block Wait (a sleep under sh -c would hold
	// Wait for its full duration). WaitDelay bounds that wait as a backstop.
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
		return nil
	}
	c.WaitDelay = localWaitDelay
	return c
}

func (b LocalBackend) Exec(ctx context.Context, cmd Cmd) (ExecResult, error) {
	c := b.command(ctx, cmd)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr

	err := c.Run()
	res := ExecResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return res, Wrap(ErrBackend, "backend: exec "+cmd.Path, ctxErr)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			res.ExitCode = exit.ExitCode()
			return res, nil
		}
		return res, Wrap(ErrBackend, "backend: exec "+cmd.Path, err)
	}
	return res, nil
}

func (b LocalBackend) Stream(ctx context.Context, cmd Cmd) (io.ReadCloser, error) {
	c := b.command(ctx, cmd)
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, Wrap(ErrBackend, "backend: stream "+cmd.Path, err)
	}
	c.Stdout = pw
	c.Stderr = pw
	if err := c.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		return nil, Wrap(ErrBackend, "backend: stream "+cmd.Path, err)
	}
	// The child holds its own dup of the write end; close the parent's copy so
	// the reader sees EOF once the child exits.
	_ = pw.Close()
	return &procStream{cmd: c, r: pr}, nil
}

// procStream streams a running command's output and terminates it on Close.
type procStream struct {
	cmd *exec.Cmd
	r   *os.File
}

func (s *procStream) Read(p []byte) (int, error) {
	return s.r.Read(p)
}

func (s *procStream) Close() error {
	if s.cmd.Process != nil {
		// Kill the whole process group, matching how the command was started.
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	}
	err := s.r.Close()
	_ = s.cmd.Wait()
	return err
}

func (b LocalBackend) Put(ctx context.Context, localPath, nodePath string) error {
	return copyFile(localPath, nodePath)
}

func (b LocalBackend) Get(ctx context.Context, nodePath, localPath string) error {
	return copyFile(nodePath, localPath)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return Wrap(ErrBackend, "backend: copy", err)
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return Wrap(ErrBackend, "backend: copy", err)
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return Wrap(ErrBackend, "backend: copy", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return Wrap(ErrBackend, "backend: copy", err)
	}
	if err := out.Close(); err != nil {
		return Wrap(ErrBackend, "backend: copy", err)
	}
	return nil
}

func (b LocalBackend) ReadFile(ctx context.Context, path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, Wrap(ErrBackend, "backend: read file", err)
	}
	return data, nil
}

func (b LocalBackend) WriteFile(ctx context.Context, path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return Wrap(ErrBackend, "backend: write file", err)
	}
	return nil
}

func (b LocalBackend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, Wrap(ErrBackend, "backend: exists", err)
}

func (b LocalBackend) Mkdir(ctx context.Context, path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return Wrap(ErrBackend, "backend: mkdir", err)
	}
	return nil
}

func (b LocalBackend) Rm(ctx context.Context, path string) error {
	if err := os.RemoveAll(path); err != nil {
		return Wrap(ErrBackend, "backend: rm", err)
	}
	return nil
}

func (b LocalBackend) Signal(ctx context.Context, pid int, sig os.Signal) error {
	if pid < minSignalablePID {
		// os.FindProcess never fails on Unix, so a non-positive pid would reach
		// kill(2) verbatim: 0 targets the caller's whole process group, -1 every
		// process the user may signal, and any value below -1 an arbitrary group.
		// Refuse anything that is not a specific process, the same boundary the ssh
		// backend applies to process-group ids. Signal 0 (a liveness probe) stays
		// available for a valid pid.
		return Wrap(ErrBackend, "backend: signal", fmt.Errorf("refusing to signal unsafe pid %d", pid))
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return Wrap(ErrBackend, "backend: signal", err)
	}
	if err := p.Signal(sig); err != nil {
		return Wrap(ErrBackend, "backend: signal", err)
	}
	return nil
}
