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
	"sync"
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
		if exit, ok := errors.AsType[*exec.ExitError](err); ok {
			res.ExitCode = exit.ExitCode()
			return res, nil
		}
		return res, Wrap(ErrBackend, "backend: exec "+cmd.Path, err)
	}
	return res, nil
}

func (b LocalBackend) Stream(ctx context.Context, cmd Cmd) (Process, error) {
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
	return &procStream{cmd: c, r: pr, done: make(chan struct{})}, nil
}

// procStream is the Process a LocalBackend's Stream returns: it streams the
// command's output, signals the command, waits for it, and terminates it on
// Close.
//
// The command is reaped lazily, by the first Wait or Close, not the moment it
// exits. Until then an exited command stays a zombie, which pins its pid and
// its process-group id: a Signal cannot reach a process that inherited the pid,
// and Close's group kill cannot hit a group that inherited the id. Once reaped,
// os.Process refuses further signals, but Close still kills the group: the
// leader's exit says nothing about its children, and the group lives on while
// any of them does. Only once the group has emptied could its id name another
// group, and then only after the pid space has wrapped around in between; a
// caller that closes soon after its wait, as Shutdown does, keeps that window
// negligible.
type procStream struct {
	cmd *exec.Cmd
	r   *os.File

	reapOnce sync.Once
	done     chan struct{} // closed once the command has been reaped
	waitErr  error         // cmd.Wait's result, valid once done is closed

	closeOnce sync.Once
	closeErr  error
}

func (s *procStream) Read(p []byte) (int, error) {
	return s.r.Read(p)
}

// Signal sends sig to the command. os.Process delivers it to the exact process
// started -- through a pidfd where the platform has one -- and refuses once the
// command has been reaped.
func (s *procStream) Signal(ctx context.Context, sig os.Signal) error {
	if err := s.cmd.Process.Signal(sig); err != nil {
		return Wrap(ErrBackend, "backend: signal", err)
	}
	return nil
}

// Wait blocks until the command has exited or ctx is done. The first call
// starts the reaper; a call cut short by ctx leaves it running, so a later Wait
// or Close still observes the exit.
func (s *procStream) Wait(ctx context.Context) (int, error) {
	s.reap()
	select {
	case <-s.done:
	case <-ctx.Done():
		select {
		case <-s.done:
			// Exited as ctx ran out: the status is the better answer.
		default:
			return -1, Wrap(ErrBackend, "backend: wait", ctx.Err())
		}
	}
	if s.cmd.ProcessState == nil {
		return -1, Wrap(ErrBackend, "backend: wait", s.waitErr)
	}
	// ExitCode is the exit code, or -1 for a command terminated by a signal.
	return s.cmd.ProcessState.ExitCode(), nil
}

// reap starts reaping the command, once; done is closed when it has been.
func (s *procStream) reap() {
	s.reapOnce.Do(func() {
		go func() {
			s.waitErr = s.cmd.Wait()
			close(s.done)
		}()
	})
}

// Close kills the command's whole process group, matching how the command was
// started, and reaps it. The group is killed whether or not the command has
// already exited and been reaped: its children may outlive it, and the group
// lives on while any of them does. A group already gone makes the kill fail
// with ESRCH, which is success for teardown.
func (s *procStream) Close() error {
	s.closeOnce.Do(func() {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		s.closeErr = s.r.Close()
		s.reap()
		<-s.done
	})
	return s.closeErr
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
	defer func() { _ = in.Close() }()
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
