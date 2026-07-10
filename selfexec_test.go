package torx

import (
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

// TestMain lets the test binary stand in for a worker: when re-executed with
// TORX_TEST_WORKER set, it runs worker mode and exits, so SelfExecLauncher can
// spawn a real worker subprocess from the registered test jobs.
func TestMain(m *testing.M) {
	if os.Getenv("TORX_TEST_WORKER") == "1" {
		os.Exit(workerMain())
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
