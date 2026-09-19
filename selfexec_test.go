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
