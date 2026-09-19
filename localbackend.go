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

// localWaitDelay bounds how long Exec waits for a command's I/O once the
// command has exited or been cancelled, a backstop in case a process escapes
// the killed group and holds the pipes.
const localWaitDelay = 2 * time.Second

// minSignalablePID is the lowest pid Signal will target. A pid below 2 is never
// a specific process to signal: 1 is init, 0 is the caller's process group, and
// negatives select a process group or every process. Rejecting them keeps a
// stale or malformed pid from turning a signal into a group- or system-wide one.
const minSignalablePID = 2

// configure applies cmd's directory and environment to c and puts the command
// in a process group of its own, so that a kill of the group takes the whole
// tree and not just the direct child: a shell's grandchildren would otherwise
// outlive it and keep its output pipes open (a sleep under sh -c would hold
// Exec for its full duration). Stdin and cancellation are left to the caller.
// Exec hands exec a reader and lets Wait cover the copy along with the output,
// and runs the command under its context so that exec kills it; Stream feeds
// a pipe of its own so the copy cannot hold up the exit report, and watches
// the context itself so that the kill outlives the reap (see Stream).
func configure(c *exec.Cmd, cmd Cmd) {
	c.Dir = cmd.Dir
	if len(cmd.Env) > 0 {
		c.Env = append(os.Environ(), cmd.Env...)
	}
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killGroup SIGKILLs the process group led by pid, the one configure gave the
// command. A group already gone makes the kill fail with ESRCH, which is
// success for a teardown, so the error is not reported.
func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

func (b LocalBackend) Exec(ctx context.Context, cmd Cmd) (ExecResult, error) {
	c := exec.CommandContext(ctx, cmd.Path, cmd.Args...)
	configure(c, cmd)
	// Cancellation kills the whole group, and WaitDelay bounds how long Run
	// then waits for the output copies as a backstop, in case a process
	// escaped the group and still holds the pipes.
	c.Cancel = func() error {
		killGroup(c.Process.Pid)
		return nil
	}
	c.WaitDelay = localWaitDelay
	if cmd.Stdin != nil {
		c.Stdin = bytes.NewReader(cmd.Stdin)
	}
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
	// The command is built without exec's own context watcher: that watcher
	// ends with the reap, once the leader has been waited for, while the
	// stream's promise -- cancelling ctx kills the command as Close would --
	// has to hold until Close, for the children the leader may have left in
	// the group. The stream watches ctx itself, below.
	c := exec.Command(cmd.Path, cmd.Args...)
	configure(c, cmd)
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, Wrap(ErrBackend, "backend: stream "+cmd.Path, err)
	}
	c.Stdout = pw
	c.Stderr = pw
	// Feed stdin through a pipe of our own rather than handing exec a reader.
	// exec copies a reader through a goroutine that Wait waits for, up to
	// WaitDelay, and a child that inherited stdin without reading it holds
	// that copy past the command's exit once the input outgrows the pipe: the
	// exit would go unreported until WaitDelay ran out, and a graceful stop
	// would time out on a command that exited promptly. An *os.File is wired
	// to the child directly, with nothing for Wait to wait on, so the copy
	// runs on its own and Close ends it, as the ssh backend's teardown does.
	var stdinR, stdinW *os.File
	if cmd.Stdin != nil {
		stdinR, stdinW, err = os.Pipe()
		if err != nil {
			_ = pw.Close()
			_ = pr.Close()
			return nil, Wrap(ErrBackend, "backend: stream "+cmd.Path, err)
		}
		c.Stdin = stdinR
	}
	if err := c.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		if stdinR != nil {
			_ = stdinR.Close()
			_ = stdinW.Close()
		}
		return nil, Wrap(ErrBackend, "backend: stream "+cmd.Path, err)
	}
	// The child holds its own dups of the pipe ends it was given; close the
	// parent's copies so the reader sees EOF once the child exits and the
	// child sees EOF once the input has been written.
	_ = pw.Close()
	if stdinR != nil {
		_ = stdinR.Close()
		go func() {
			_, _ = io.Copy(stdinW, bytes.NewReader(cmd.Stdin))
			_ = stdinW.Close()
		}()
	}
	s := &procStream{cmd: c, r: pr, stdinW: stdinW, done: make(chan struct{})}
	// Match the ssh backend: the watcher and Close share one teardown, run at
	// most once, and Close stops the watcher.
	s.stopWatch = context.AfterFunc(ctx, func() { _ = s.teardown() })
	return s, nil
}

// procStream is the Process a LocalBackend's Stream returns: it streams the
// command's output, signals the command, waits for it, and terminates it on
// Close. Cancelling the context that started the stream terminates it the
// same way, through the shared teardown.
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
	cmd       *exec.Cmd
	r         *os.File
	stdinW    *os.File // the input pipe's write end, nil without Cmd.Stdin
	stopWatch func() bool

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
// started, and reaps it.
func (s *procStream) Close() error {
	s.stopWatch()
	return s.teardown()
}

// teardown kills the command's process group and reaps the command exactly
// once, whether it is Close or the context watcher that reaches it first. The
// group is killed whether or not the command has already exited and been
// reaped: its children may outlive it, and the group lives on while any of
// them does. Closing the input pipe ends a stdin copy still blocked on it --
// one held up by a process that escaped the group -- rather than leaking the
// goroutine.
func (s *procStream) teardown() error {
	s.closeOnce.Do(func() {
		killGroup(s.cmd.Process.Pid)
		s.closeErr = s.r.Close()
		if s.stdinW != nil {
			_ = s.stdinW.Close()
		}
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
