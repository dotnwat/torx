package torx

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMain lets the test binary stand in for a worker or the driver so tests can
// exercise the real self-exec entry points. A self-exec'd worker is launched as
// "<bin> worker" (matching the real Main); TORX_TEST_WORKER is an explicit
// alternative some tests set. TORX_TEST_DRIVER drives the driver entry point in
// a subprocess, taking its flags from the environment. Worker mode is checked
// first so a driver-spawned worker never falls through to the test runner.
func TestMain(m *testing.M) {
	if (len(os.Args) > 1 && os.Args[1] == "worker") || os.Getenv("TORX_TEST_WORKER") == "1" {
		os.Exit(workerMain())
	}
	if os.Getenv("TORX_TEST_DRIVER") == "1" {
		os.Exit(driverMain(strings.Fields(os.Getenv("TORX_TEST_DRIVER_ARGS"))))
	}
	os.Exit(m.Run())
}

func TestSelfExecLauncher(t *testing.T) {
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	var sink InMemoryEventSink

	res, err := launcher.Launch(context.Background(), Assignment{JobID: "wtest.pass"}, &sink)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status != StatusPass {
		t.Errorf("status = %v, want PASS", res.Status)
	}
	if res.Summary != "ok" {
		t.Errorf("summary = %q, want ok", res.Summary)
	}

	ran := false
	for _, e := range sink.Events() {
		if e.Kind == EventLog && e.Message == "ran" {
			ran = true
		}
	}
	if !ran {
		t.Errorf("'ran' event was not forwarded from the worker; got %+v", sink.Events())
	}
}

func TestSelfExecLauncherWritesJobArtifact(t *testing.T) {
	// The worker process owns the job's results directory and writes the
	// job's artifact there itself; nothing crosses the event pipe. The file is
	// in place once Launch returns, whichever launcher ran the job.
	runDir := t.TempDir()
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	res, err := launcher.Launch(context.Background(),
		Assignment{JobID: "wtest.artifact", Session: SessionConfig{ResultsDir: runDir}}, discardSink{})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status != StatusPass || res.PersistErr != "" {
		t.Fatalf("result = %+v, want PASS with nothing unpersisted", res)
	}
	got, err := os.ReadFile(filepath.Join(runDir, "wtest.artifact", "report.txt"))
	if err != nil || string(got) != "report" {
		t.Errorf("report.txt = %q (%v), want the job's content", got, err)
	}
}

func TestSelfExecLauncherJobFailure(t *testing.T) {
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	res, err := launcher.Launch(context.Background(), Assignment{JobID: "wtest.fail"}, discardSink{})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status != StatusFail {
		t.Errorf("status = %v, want FAIL", res.Status)
	}
}

func init() {
	Register("wtest.orphan", func() Job { return &wOrphanJob{} })
}

// wOrphanJob spawns a process that outlives the worker in its own process group,
// so neither the worker exiting nor a group SIGKILL reaps it. If the worker's
// event pipe (fd 3) leaked into that process, the driver's reader would never
// see EOF. The pid is recorded so the test can reap the orphan.
type wOrphanJob struct{ JobBase }

func (*wOrphanJob) Declare(*JobContext) {}
func (*wOrphanJob) Run(_ context.Context, jc *JobContext) error {
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	if pidfile := jc.Params.String("pidfile", ""); pidfile != "" {
		_ = os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
	}
	// Deliberately neither wait for nor kill the child: leave it orphaned.
	return nil
}

// TestSelfExecLauncherDoesNotLeakEventPipe is the regression for the fd-3 leak:
// a worker whose child outlives it must not keep the driver's event-pipe reader
// blocked. Launch must return once the worker exits, even though the orphan is
// still alive.
func TestSelfExecLauncherDoesNotLeakEventPipe(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "pid")
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	done := make(chan JobResult, 1)
	go func() {
		res, err := launcher.Launch(context.Background(),
			Assignment{JobID: "wtest.orphan", Params: Params{"pidfile": pidfile}}, discardSink{})
		if err != nil {
			t.Errorf("Launch: %v", err)
		}
		done <- res
	}()

	// The deadline is deliberately below eventDrainGrace: with fd 3 no longer
	// leaked, EOF arrives the instant the worker exits and Launch returns almost
	// immediately. A regression would instead rely on the force-close backstop
	// and take at least eventDrainGrace, tripping this deadline.
	select {
	case res := <-done:
		if res.Status != StatusPass {
			t.Errorf("status = %v, want PASS", res.Status)
		}
	case <-time.After(eventDrainGrace - 2*time.Second):
		t.Fatal("Launch did not return promptly: the orphaned child still holds event pipe fd 3")
	}
}

func init() {
	Register("wtest.signal", func() Job { return &wSignalJob{} })
}

// wSignalJob blocks until its context is cancelled, recording start and teardown
// through marker files named in the environment. A parent test uses those
// markers to observe that a signalled driver cancels the run and tears the job
// down, rather than dying instantly and orphaning the worker.
type wSignalJob struct{ JobBase }

func (*wSignalJob) Declare(jc *JobContext) {
	jc.Register(newFakeServiceSpec("sig", new([]string), 1))
}

func (*wSignalJob) Run(ctx context.Context, _ *JobContext) error {
	touch(os.Getenv("TORX_TEST_STARTED_MARKER"))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(60 * time.Second):
		// Safety valve: a leaked worker (the regression this guards against) must
		// not block a machine indefinitely.
		return nil
	}
}

func (*wSignalJob) Teardown(_ context.Context, _ *JobContext) error {
	touch(os.Getenv("TORX_TEST_TEARDOWN_MARKER"))
	return nil
}

func touch(path string) {
	if path != "" {
		_ = os.WriteFile(path, []byte("x"), 0o644)
	}
}

// TestDriverMainSignalTearsDown is the regression for the missing driver signal
// handler: a SIGINT to the driver must cancel the run so the worker tears down,
// instead of the driver terminating immediately and orphaning the worker. The
// teardown marker only appears if the cancellation propagated all the way down.
func TestDriverMainSignalTearsDown(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	dir := t.TempDir()
	started := filepath.Join(dir, "started")
	teardown := filepath.Join(dir, "teardown")

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"TORX_TEST_DRIVER=1",
		"TORX_TEST_DRIVER_ARGS=-results-dir "+filepath.Join(dir, "results")+" wtest.signal",
		"TORX_TEST_STARTED_MARKER="+started,
		"TORX_TEST_TEARDOWN_MARKER="+teardown,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	// Own process group so the SIGINT below reaches only the driver, not the test
	// process running it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start driver: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("driver output:\n%s", out.String())
		}
	})

	// Wait until the job is actually blocking before signalling, so the test does
	// not depend on scheduling timing.
	waitForFile(t, started)

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal driver: %v", err)
	}

	waitForFile(t, teardown)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %s did not appear within the deadline", filepath.Base(path))
}

func init() {
	Register("wtest.stderr", func() Job { return &wStderrJob{} })
	Register("wtest.stuck", func() Job { return &wStuckJob{} })
}

// wStderrJob writes to the worker's standard error itself, as the Go runtime
// does when it panics or finds a data race.
type wStderrJob struct{ JobBase }

func (*wStderrJob) Declare(*JobContext) {}
func (*wStderrJob) Run(context.Context, *JobContext) error {
	_, err := os.Stderr.WriteString("straight to fd 2\n")
	return err
}

// wStuckJob ignores its context: a job stuck where cancellation cannot
// reach it.
type wStuckJob struct{ JobBase }

func (*wStuckJob) Declare(*JobContext) {}
func (*wStuckJob) Run(context.Context, *JobContext) error {
	stuckForAnHour()
	return nil
}

func stuckForAnHour() { time.Sleep(time.Hour) }

func TestSelfExecLauncherKeepsWorkerOutput(t *testing.T) {
	runDir := t.TempDir()
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	res, err := launcher.Launch(context.Background(),
		Assignment{JobID: "wtest.stderr", Session: SessionConfig{ResultsDir: runDir}}, discardSink{})
	if err != nil || res.Status != StatusPass {
		t.Fatalf("Launch: %+v, %v", res, err)
	}
	got, err := os.ReadFile(filepath.Join(runDir, "wtest.stderr", workerLogFile))
	if err != nil || !strings.Contains(string(got), "straight to fd 2") {
		t.Errorf("%s = %q (%v), want what the worker wrote", workerLogFile, got, err)
	}

	// A run into the same directory again (-run-dir) starts the file over,
	// as it does result.json.
	if _, err := launcher.Launch(context.Background(),
		Assignment{JobID: "wtest.stderr", Session: SessionConfig{ResultsDir: runDir}}, discardSink{}); err != nil {
		t.Fatal(err)
	}
	if again, err := os.ReadFile(filepath.Join(runDir, "wtest.stderr", workerLogFile)); err != nil || string(again) != string(got) {
		t.Errorf("%s after a second run = %q (%v), want the second run's output alone", workerLogFile, again, err)
	}

	// A worker that prints nothing leaves no file.
	if _, err := launcher.Launch(context.Background(), Assignment{JobID: "wtest.artifact", Session: SessionConfig{ResultsDir: runDir}}, discardSink{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "wtest.artifact", workerLogFile)); !os.IsNotExist(err) {
		t.Errorf("a quiet worker left %s: %v", workerLogFile, err)
	}
}

// A worker that does not exit when its job is cancelled leaves the stacks
// of its goroutines in worker.log before it is killed.
func TestSelfExecLauncherDumpsAStuckWorker(t *testing.T) {
	runDir := t.TempDir()
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	res, err := launcher.Launch(ctx, Assignment{JobID: "wtest.stuck", Session: SessionConfig{ResultsDir: runDir}}, discardSink{})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status == StatusPass {
		t.Errorf("a stuck job passed")
	}
	// The dump comes once the grace is up, and the worker exits on it;
	// the kill, defaultDumpGrace later, is the bound either way.
	if took := time.Since(start); took > time.Second+workerGracePeriod+defaultDumpGrace+2*time.Second {
		t.Errorf("Launch took %v", took)
	}
	got, err := os.ReadFile(filepath.Join(runDir, "wtest.stuck", workerLogFile))
	if err != nil || !strings.Contains(string(got), "stuckForAnHour") {
		t.Errorf("%s (%v) does not show where the worker was stuck:\n%s", workerLogFile, err, got)
	}
}

func init() {
	Register("wtest.inherit", func() Job { return &wInheritJob{} })
}

// wInheritJob leaves a process running with the worker's own standard
// output and error, as a job that starts a helper the way a shell would
// and forgets it does.
type wInheritJob struct{ JobBase }

func (*wInheritJob) Declare(*JobContext) {}
func (*wInheritJob) Run(_ context.Context, jc *JobContext) error {
	cmd := exec.Command("sleep", "300")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	if pidfile := jc.Params.String("pidfile", ""); pidfile != "" {
		_ = os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
	}
	return nil
}

// A process the worker leaves holding its output must not hold the driver
// up once the worker has exited: Launch returns with the result as soon as
// the leftover is killed, not after a grace for the pipes to close.
func TestSelfExecLauncherOutputOutlivedByAChild(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "pid")
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	runDir := t.TempDir()
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	start := time.Now()
	res, err := launcher.Launch(context.Background(),
		Assignment{JobID: "wtest.inherit", Params: Params{"pidfile": pidfile}, Session: SessionConfig{ResultsDir: runDir}}, discardSink{})
	if err != nil || res.Status != StatusPass {
		t.Fatalf("Launch: %+v, %v", res, err)
	}
	// Where leftovers cannot be found and killed (not Linux), the drain
	// gives up on the pipes after eventDrainGrace; nowhere does the wait
	// run to WaitDelay.
	if took := time.Since(start); took > eventDrainGrace+2*time.Second {
		t.Errorf("Launch took %v with a child holding the worker's output", took)
	}
	if _, err := os.Stat(filepath.Join(runDir, "wtest.inherit", workerLogFile)); !os.IsNotExist(err) {
		t.Errorf("a quiet worker left %s: %v", workerLogFile, err)
	}
}

// A worker.log that cannot be written is a slice of the results tree not
// persisted, and the result says so.
func TestSelfExecLauncherReportsWorkerLogFailure(t *testing.T) {
	runDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(runDir, "wtest.stderr", workerLogFile), 0o755); err != nil {
		t.Fatal(err)
	}
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	res, err := launcher.Launch(context.Background(),
		Assignment{JobID: "wtest.stderr", Session: SessionConfig{ResultsDir: runDir}}, discardSink{})
	if err != nil || res.Status != StatusPass {
		t.Fatalf("Launch: %+v, %v", res, err)
	}
	if !strings.Contains(res.PersistErr, workerLogFile) {
		t.Errorf("PersistErr = %q, want the failure to keep %s", res.PersistErr, workerLogFile)
	}
}

// A console that is gone -- the driver's output piped to a reader that has
// quit -- costs the copy to it and nothing more: the driver, which carries
// the worker's output to it, does not die of the broken pipe.
func TestDriverOutlivesItsConsole(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"TORX_TEST_DRIVER=1",
		"TORX_TEST_DRIVER_ARGS=-results-dir "+filepath.Join(dir, "results")+" wtest.stderr",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = w
	if err := cmd.Start(); err != nil {
		t.Fatalf("start driver: %v", err)
	}
	// The reader of the driver's standard error quits before the worker
	// has anything to say to it.
	_ = w.Close()
	_ = r.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("driver: %v\n%s", err, out.String())
	}
	// The worker's output went through the driver to the console that was
	// gone, and to worker.log, which has it.
	got, err := os.ReadFile(filepath.Join(dir, "results", "latest", "wtest.stderr", workerLogFile))
	if err != nil || !strings.Contains(string(got), "straight to fd 2") {
		t.Errorf("%s = %q (%v), want what the worker wrote", workerLogFile, got, err)
	}
}

// A copy blocked writing to a console no one reads does not hold Launch:
// once the worker's pipes are closed, close waits outputCloseGrace for the
// copies and no longer.
func TestWorkerOutputGivesUpOnAConsoleNoOneReads(t *testing.T) {
	w, err := newWorkerOutput(filepath.Join(t.TempDir(), workerLogFile))
	if err != nil {
		t.Fatal(err)
	}
	// A console whose pipe is full and whose reader reads no more.
	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Close(fds[0]) }()
	_ = syscall.SetNonblock(fds[1], true)
	for junk := make([]byte, 4096); ; {
		if _, err := syscall.Write(fds[1], junk); err != nil {
			break
		}
	}
	_ = syscall.SetNonblock(fds[1], false)
	_ = w.outC.Close()
	w.outC = os.NewFile(uintptr(fds[1]), "console")
	// The worker's end of its standard output, which it writes to and lets
	// go of.
	wfd, err := syscall.Dup(int(w.outW.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	worker := os.NewFile(uintptr(wfd), "worker stdout")
	w.started()
	if _, err := worker.WriteString("hello\n"); err != nil {
		t.Fatal(err)
	}
	_ = worker.Close()
	// The copy reaches worker.log before it blocks on the console.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if b, _ := os.ReadFile(w.file.Name()); strings.Contains(string(b), "hello") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy did not reach worker.log")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	w.drain(ctx)
	w.close()
	if took := time.Since(start); took > outputCloseGrace+2*time.Second {
		t.Errorf("drain and close took %v with the console unread", took)
	}
}
