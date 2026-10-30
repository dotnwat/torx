//go:build unix

package torx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// workerGracePeriod is how long a cancelled worker has to exit after SIGTERM
// before it is sent SIGQUIT, for a dump of its goroutines' stacks, and then,
// defaultDumpGrace later, force-killed.
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

// outputCloseGrace bounds how long Launch waits for the copies of the
// worker's output to end once their pipes are closed. A copy ends at once
// then, unless it is blocked writing to a console no one is reading, which
// the driver's own writes would block on too: Launch leaves it to finish
// when the console is read again, rather than wait on it.
const outputCloseGrace = time.Second

// SelfExecLauncher runs each job in a worker subprocess by re-executing this
// binary in worker mode. It is the production launcher: a panic, hang, exit, or
// leaked goroutine in job code is confined to the worker process. The assignment
// is written to the worker's stdin and its event stream is read from a dedicated
// inherited pipe (fd 3), leaving the worker's stdout and stderr for human logs.
type SelfExecLauncher struct {
	// Path and Args locate the worker; both default to re-executing this binary
	// with a "worker" argument. The process Path names is the one the
	// launcher signals, SIGQUIT for a dump of its stacks included: a Path
	// that wraps the worker in another program leaves the dump to that
	// program. Env adds environment entries for the worker.
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
	// The worker's own output -- what the Go runtime prints when it panics,
	// is sent SIGQUIT, or detects a data race -- goes to the driver's, and,
	// when the run persists results, to worker.log in the variant's
	// directory as well, so that it stays with the run it explains.
	var out *workerOutput
	var outErr error // why the worker's output is not kept, if it is not
	if dir, err := jobResultsDir(a.Session.ResultsDir, variantID(a.JobID, a.Params)); err == nil && dir != "" {
		if o, err := newWorkerOutput(filepath.Join(dir, workerLogFile)); err != nil {
			outErr = err
		} else {
			out = o
			cmd.Stdout, cmd.Stderr = o.outW, o.errW
			defer o.close()
		}
	}
	// Own process group so cancellation reaches the whole worker tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	exited := make(chan struct{})
	cmd.Cancel = func() error {
		// Ask the worker to stop gracefully so it can run teardown; WaitDelay
		// escalates to a kill if it does not exit in time.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		// A worker still running once its grace is up is stuck: have it
		// print every goroutine's stack, into worker.log, before the kill
		// leaves no trace of where. The grace is the teardown's in full; the
		// dump gets a short grace of its own before the kill, as Stop gives
		// a command's.
		go func() {
			t := time.NewTimer(workerGracePeriod)
			defer t.Stop()
			select {
			case <-exited:
			case <-t.C:
				_ = cmd.Process.Signal(syscall.SIGQUIT)
			}
		}()
		return nil
	}
	cmd.WaitDelay = workerGracePeriod + defaultDumpGrace

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
	out.started()

	go func() {
		_ = EncodeAssignment(stdin, a)
		_ = stdin.Close()
	}()

	// Reap the worker concurrently with draining its event pipe. The drain ends
	// naturally at EOF when the worker exits and its write end closes; as a
	// backstop against a stray inherited copy of that write end keeping the pipe
	// open, force the read end closed once the worker is gone and a short grace
	// has elapsed, so a leaked fd can never wedge the driver. The worker's
	// output pipes end the same way.
	drainDone := make(chan struct{})
	type reaped struct {
		err     error
		killed  int
		killErr error
	}
	waitCh := make(chan reaped, 1)
	go func() {
		err := cmd.Wait()
		close(exited)
		// Whatever the worker left running is a leftover now that it is gone:
		// kill it before the drains, since a leftover that inherited the
		// worker's output is what would hold them open.
		killed, killErr := killTagged(tag)
		dctx, dcancel := context.WithTimeout(context.Background(), eventDrainGrace)
		defer dcancel()
		select {
		case <-drainDone:
		case <-dctx.Done():
			_ = eventR.Close()
		}
		out.drain(dctx)
		waitCh <- reaped{err, killed, killErr}
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
	r := <-waitCh
	waitErr := r.err
	if r.killed > 0 || r.killErr != nil {
		msg := fmt.Sprintf("driver: killed %d processes the worker left running", r.killed)
		if r.killErr != nil {
			msg += ": " + r.killErr.Error()
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
		result = failResult(variantID(a.JobID, a.Params), a.Params, err)
		result.Dirty = true
	}
	// The worker's output not kept is a slice of the results tree not
	// persisted, which the result says, as it says of its own files.
	if outErr == nil {
		outErr = out.failure()
	}
	if outErr != nil {
		msg := fmt.Sprintf("driver: %s: %v", workerLogFile, outErr)
		if result.PersistErr != "" {
			msg = result.PersistErr + "; " + msg
		}
		result.PersistErr = msg
	}
	return result, nil
}

// workerOutput carries a worker's standard output and error to the driver's
// and to worker.log in the variant's directory. The worker writes to pipes
// the driver copies out itself, rather than to writers exec would copy to
// for it: exec's copying has Wait last until every holder of the pipes has
// closed them, so a process the worker left behind with its output would
// hold the driver up for the whole of WaitDelay. Here the worker's exit
// ends the wait, and the copying ends at EOF, or when drain gives up on it.
type workerOutput struct {
	file       *os.File
	outR, errR *os.File // the driver's ends
	outW, errW *os.File // the worker's ends, which the driver holds until it has started
	outC, errC *os.File // the console, duplicated
	copying    bool
	done       chan struct{} // closed once both copies have ended

	mu  sync.Mutex
	err error // the first failure to write worker.log
}

func newWorkerOutput(path string) (*workerOutput, error) {
	// Truncate what an earlier run into the same directory left; append, so
	// that the two copies interleave whole writes.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &workerOutput{file: f, done: make(chan struct{})}
	ok := false
	defer func() {
		if !ok {
			w.close()
		}
	}()
	if w.outR, w.outW, err = os.Pipe(); err != nil {
		return nil, err
	}
	if w.errR, w.errW, err = os.Pipe(); err != nil {
		return nil, err
	}
	if w.outC, err = dupConsole(os.Stdout); err != nil {
		return nil, fmt.Errorf("stdout: %w", err)
	}
	if w.errC, err = dupConsole(os.Stderr); err != nil {
		return nil, fmt.Errorf("stderr: %w", err)
	}
	ok = true
	return w, nil
}

// dupConsole duplicates one of the driver's standard streams for a copy to
// write to. A write to os.Stdout or os.Stderr that finds its pipe closed
// ends the driver, by the os package's rule for those two files alone; a
// duplicate of the descriptor is not held to it, so a console that is gone
// costs the copy to it and nothing more.
func dupConsole(f *os.File) (*os.File, error) {
	// Hold the fork lock, as the os package does, so that no child forked
	// meanwhile inherits the descriptor before it is marked close-on-exec.
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	fd, err := syscall.Dup(int(f.Fd()))
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), f.Name()), nil
}

// started begins the copying once the worker has started: the driver lets
// go of the worker's ends, so each pipe ends when the worker's tree has.
func (w *workerOutput) started() {
	if w == nil {
		return
	}
	_ = w.outW.Close()
	_ = w.errW.Close()
	w.copying = true
	var wg sync.WaitGroup
	for _, c := range []struct{ r, console *os.File }{{w.outR, w.outC}, {w.errR, w.errC}} {
		wg.Go(func() {
			_, _ = io.Copy(tee{w, c.console}, c.r)
		})
	}
	go func() {
		wg.Wait()
		close(w.done)
	}()
}

// drain waits for the copying to end, and ends it when ctx does first: a
// process the worker left with its output is holding the pipes open, and
// what it has to say is not the worker's.
func (w *workerOutput) drain(ctx context.Context) {
	if w == nil || !w.copying {
		return
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		_ = w.outR.Close()
		_ = w.errR.Close()
	}
}

// close releases everything, and leaves no file behind a worker that
// printed nothing, as most do. A copy still blocked on the console after
// outputCloseGrace is left to end on its own, and the file is closed under
// it.
func (w *workerOutput) close() {
	for _, f := range []*os.File{w.outW, w.errW, w.outR, w.errR} {
		if f != nil {
			_ = f.Close()
		}
	}
	ended := !w.copying
	if w.copying {
		select {
		case <-w.done:
			ended = true
		case <-time.After(outputCloseGrace):
		}
	}
	if fi, err := w.file.Stat(); ended && err == nil && fi.Size() == 0 {
		_ = os.Remove(w.file.Name())
	}
	_ = w.file.Close()
	for _, f := range []*os.File{w.outC, w.errC} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// failure is why worker.log is incomplete, if it is.
func (w *workerOutput) failure() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// tee writes to worker.log and to the console, each whether or not the
// other could be written, and never fails: a copy that stopped would leave
// the worker blocked on a full pipe, and the log is the copy meant to
// outlast a console that is gone.
type tee struct {
	w       *workerOutput
	console io.Writer
}

func (t tee) Write(p []byte) (int, error) {
	if _, err := t.w.file.Write(p); err != nil {
		t.w.mu.Lock()
		if t.w.err == nil {
			t.w.err = err
		}
		t.w.mu.Unlock()
	}
	_, _ = t.console.Write(p)
	return len(p), nil
}
