//go:build unix

package torx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

// workerGracePeriod is how long a cancelled worker has to exit after SIGTERM
// before it is force-killed.
const workerGracePeriod = 10 * time.Second

// workerTagEnv names the environment entry that tags a worker, and every
// process it starts, with a tag of the launch's own.
const workerTagEnv = "TORX_WORKER_TAG"

// eventDrainGrace bounds how long Launch keeps draining the worker's event pipe
// after the worker has exited. The drain normally ends at EOF the instant the
// worker's write end closes; this grace only bites if a stray inherited copy of
// that write end lingers, in which case the driver stops waiting rather than
// hang forever.
const eventDrainGrace = 5 * time.Second

// SelfExecLauncher runs each job in a worker subprocess by re-executing this
// binary in worker mode. It is the production launcher: a panic, hang, exit, or
// leaked goroutine in job code is confined to the worker process. The assignment
// is written to the worker's stdin and its event stream is read from a dedicated
// inherited pipe (fd 3), leaving the worker's stdout and stderr for human logs.
type SelfExecLauncher struct {
	// Path and Args locate the worker; both default to re-executing this binary
	// with a "worker" argument. Env adds environment entries for the worker.
	Path string
	Args []string
	Env  []string
}

var _ WorkerLauncher = SelfExecLauncher{}

// Launch spawns a worker, sends it the assignment, forwards its events to sink,
// and returns the job's result.
func (l SelfExecLauncher) Launch(ctx context.Context, a Assignment, sink EventSink) (JobResult, error) {
	path := l.Path
	if path == "" {
		path = os.Args[0]
	}
	args := l.Args
	if args == nil {
		args = []string{"worker"}
	}

	// Every process the worker starts inherits its tag, so that whatever
	// it leaves running -- dying abruptly, killed for memory, say, before its
	// teardown, or simply not stopping something -- can be found and killed
	// once it is gone.
	tag := strconv.FormatUint(RandomSeed(), 36)
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(append(os.Environ(), l.Env...), workerTagEnv+"="+tag)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Own process group so cancellation reaches the whole worker tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// Ask the worker to stop gracefully so it can run teardown; WaitDelay
		// escalates to a kill if it does not exit in time.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		return nil
	}
	cmd.WaitDelay = workerGracePeriod

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return JobResult{}, fmt.Errorf("driver: worker stdin: %w", err)
	}
	eventR, eventW, err := os.Pipe()
	if err != nil {
		return JobResult{}, fmt.Errorf("driver: worker pipe: %w", err)
	}
	cmd.ExtraFiles = []*os.File{eventW} // the child sees this as fd 3

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = eventW.Close()
		_ = eventR.Close()
		return JobResult{}, fmt.Errorf("driver: start worker: %w", err)
	}
	_ = eventW.Close() // the child holds its own copy; close ours so reads see EOF

	go func() {
		_ = EncodeAssignment(stdin, a)
		_ = stdin.Close()
	}()

	// Reap the worker concurrently with draining its event pipe. The drain ends
	// naturally at EOF when the worker exits and its write end closes; as a
	// backstop against a stray inherited copy of that write end keeping the pipe
	// open, force the read end closed once the worker is gone and a short grace
	// has elapsed, so a leaked fd can never wedge the driver.
	drainDone := make(chan struct{})
	waitCh := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		select {
		case <-drainDone:
		case <-time.After(eventDrainGrace):
			_ = eventR.Close()
		}
		waitCh <- err
	}()

	var result JobResult
	haveResult := false
	mr := NewMessageReader(eventR)
	for {
		m, err := mr.Read()
		if err != nil {
			break
		}
		switch {
		case m.Result != nil:
			result = *m.Result
			haveResult = true
		case m.Event != nil:
			sink.Emit(*m.Event)
		}
	}
	close(drainDone)
	_ = eventR.Close()
	waitErr := <-waitCh
	if n, err := killTagged(tag); n > 0 || err != nil {
		msg := fmt.Sprintf("driver: killed %d processes the worker left running", n)
		if err != nil {
			msg += ": " + err.Error()
		}
		sink.Emit(Event{Kind: EventLog, Time: time.Now(), Source: variantID(a.JobID, a.Params), Component: "driver", Level: "warn", Message: msg})
	}

	if !haveResult {
		// The worker died before reporting a result, so its teardown never
		// completed: mark the node dirty so the driver quarantines it. waitErr is
		// nil when the worker exited zero without ever sending a result.
		err := errors.New("driver: worker produced no result")
		if waitErr != nil {
			err = fmt.Errorf("driver: worker produced no result: %w", waitErr)
		}
		res := failResult(variantID(a.JobID, a.Params), a.Params, err)
		res.Dirty = true
		return res, nil
	}
	return result, nil
}
