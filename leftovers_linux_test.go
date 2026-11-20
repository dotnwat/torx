//go:build linux

package torx

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func init() {
	Register("wtest.leftover", func() Job { return &wLeftoverJob{} })
}

// wLeftoverJob starts a process in a group of its own, which neither the
// worker's exit nor a kill of its group reaches, records its pid, and
// leaves it running; with param die, it then kills its own worker, as the
// kernel does one that runs out of memory, before any teardown.
type wLeftoverJob struct{ JobBase }

func (*wLeftoverJob) Declare(*JobContext) {}
func (*wLeftoverJob) Run(_ context.Context, jc *JobContext) error {
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := os.WriteFile(jc.Params.String("pidfile", ""), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		return err
	}
	if jc.Params.Bool("die", false) {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}
	return nil
}

// alive reports whether pid is a process that has not exited: a zombie, its
// exit not yet collected by its new parent, has.
func alive(pid int) bool {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// The state follows the command name, which is in parentheses.
	i := strings.LastIndexByte(string(stat), ')')
	return i < 0 || len(stat) <= i+2 || stat[i+2] != 'Z'
}

// TestSelfExecLauncherKillsLeftovers: whatever a worker leaves running --
// orphaned by a worker that exits, or by one killed outright -- is gone
// once Launch returns, and the driver says so: to the sink, and in the
// job's trace, after what the worker wrote there.
func TestSelfExecLauncherKillsLeftovers(t *testing.T) {
	for _, die := range []bool{false, true} {
		t.Run("die="+strconv.FormatBool(die), func(t *testing.T) {
			pidfile := filepath.Join(t.TempDir(), "pid")
			runDir := t.TempDir()
			var sink InMemoryEventSink
			launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
			a := Assignment{JobID: "wtest.leftover", Params: Params{"pidfile": pidfile, "die": die}, Session: SessionConfig{ResultsDir: runDir}}
			res, err := launcher.Launch(context.Background(), a, &sink)
			if err != nil {
				t.Fatalf("Launch: %v", err)
			}
			data, err := os.ReadFile(pidfile)
			if err != nil {
				t.Fatalf("the job recorded no pid: %v", err)
			}
			pid, _ := strconv.Atoi(string(data))
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if die && res.Status != StatusFail {
				t.Errorf("a worker killed outright: status %v, want FAIL", res.Status)
			}
			deadline := time.Now().Add(5 * time.Second)
			for alive(pid) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if alive(pid) {
				t.Fatalf("the worker's leftover process %d is still running", pid)
			}
			said := false
			for _, e := range sink.Events() {
				if e.Level == "warn" && strings.Contains(e.Message, "killed 1 processes the worker left running") {
					said = true
				}
			}
			if !said {
				t.Errorf("the driver did not say it killed the leftover; events %+v", sink.Events())
			}
			if res.PersistErr != "" {
				t.Errorf("PersistErr = %q", res.PersistErr)
			}
			// A worker killed outright wrote no verdict; the driver's own
			// comes first of what it adds.
			want := []string{"bound 0 node(s)", "killed 1 processes the worker left running"}
			if die {
				want = []string{"bound 0 node(s)", "worker produced no result: signal: killed", "killed 1 processes the worker left running"}
			}
			events, human := traceOf(t, runDir, variantID(a.JobID, a.Params))
			for name, trace := range map[string]string{eventsFile: events, testLogFile: human} {
				rest := trace
				for _, w := range want {
					i := strings.Index(rest, w)
					if i < 0 {
						t.Errorf("%s does not say %q, in its turn:\n%s", name, w, trace)
						break
					}
					rest = rest[i+len(w):]
				}
			}
		})
	}
}

func TestHasEnvEntry(t *testing.T) {
	env := []byte("A=1\x00TORX_WORKER_TAG=abc\x00B=2\x00")
	for want, ok := range map[string]bool{
		"TORX_WORKER_TAG=abc": true, "TORX_WORKER_TAG=ab": false, "A=1": true, "B=2": true, "TORX_WORKER_TAG=abcd": false,
	} {
		if hasEnvEntry(env, []byte(want)) != ok {
			t.Errorf("hasEnvEntry(%q) = %v", want, !ok)
		}
	}
	if _, err := killTagged("no-process-has-this-tag"); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Errorf("killTagged of an unused tag: %v", err)
	}
}
