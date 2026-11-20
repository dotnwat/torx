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
// once Launch returns, and the driver says so.
func TestSelfExecLauncherKillsLeftovers(t *testing.T) {
	for _, die := range []bool{false, true} {
		t.Run("die="+strconv.FormatBool(die), func(t *testing.T) {
			pidfile := filepath.Join(t.TempDir(), "pid")
			var sink InMemoryEventSink
			launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
			res, err := launcher.Launch(context.Background(),
				Assignment{JobID: "wtest.leftover", Params: Params{"pidfile": pidfile, "die": die}}, &sink)
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

func TestKillTaggedWith(t *testing.T) {
	const tag = "test-tag"
	writeProc := func(procDir string, pid int, env string) {
		t.Helper()
		dir := filepath.Join(procDir, strconv.Itoa(pid))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "environ"), []byte(env), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("counts distinct pids across passes", func(t *testing.T) {
		procDir := t.TempDir()
		writeProc(procDir, 100, workerTagEnv+"="+tag+"\x00")
		calls := 0
		killed, err := killTaggedWith(procDir, tag, 1, func(pid int) error {
			calls++
			if calls == 3 {
				_ = os.RemoveAll(filepath.Join(procDir, strconv.Itoa(pid)))
			}
			return nil
		}, time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("killTaggedWith: %v", err)
		}
		if killed != 1 {
			t.Errorf("killed = %d, want 1 distinct process (kill called %d times)", killed, calls)
		}
	})

	t.Run("EPERM on one pid does not abort killing others", func(t *testing.T) {
		procDir := t.TempDir()
		writeProc(procDir, 100, workerTagEnv+"="+tag+"\x00")
		writeProc(procDir, 200, workerTagEnv+"="+tag+"\x00")
		var got200 bool
		start := time.Now()
		killed, err := killTaggedWith(procDir, tag, 1, func(pid int) error {
			if pid == 100 {
				return syscall.EPERM
			}
			if pid == 200 {
				got200 = true
				_ = os.RemoveAll(filepath.Join(procDir, strconv.Itoa(pid)))
				return nil
			}
			return syscall.ESRCH
		}, 2*time.Second, time.Millisecond)
		if !errors.Is(err, syscall.EPERM) {
			t.Fatalf("err = %v, want EPERM", err)
		}
		if !got200 || killed != 1 {
			t.Errorf("got200=%v killed=%d, want true and 1", got200, killed)
		}
		if time.Since(start) > time.Second {
			t.Errorf("spent %v retrying unkillable pid", time.Since(start))
		}
	})

	t.Run("times out when killable pid never disappears", func(t *testing.T) {
		procDir := t.TempDir()
		writeProc(procDir, 100, workerTagEnv+"="+tag+"\x00")
		killed, err := killTaggedWith(procDir, tag, 1, func(int) error { return nil }, 25*time.Millisecond, 5*time.Millisecond)
		if err == nil {
			t.Fatal("expected timeout error when process never disappears")
		}
		if killed != 1 {
			t.Errorf("killed = %d, want 1", killed)
		}
	})

	t.Run("rescans after ESRCH to catch forked child", func(t *testing.T) {
		procDir := t.TempDir()
		writeProc(procDir, 100, workerTagEnv+"="+tag+"\x00")
		var killedChild bool
		killed, err := killTaggedWith(procDir, tag, 1, func(pid int) error {
			switch pid {
			case 100:
				// Parent forked child 101 after ReadDir and exited before kill.
				_ = os.RemoveAll(filepath.Join(procDir, "100"))
				writeProc(procDir, 101, workerTagEnv+"="+tag+"\x00")
				return syscall.ESRCH
			case 101:
				killedChild = true
				_ = os.RemoveAll(filepath.Join(procDir, "101"))
				return nil
			default:
				return syscall.ESRCH
			}
		}, time.Second, time.Millisecond)
		if err != nil {
			t.Fatalf("killTaggedWith: %v", err)
		}
		if !killedChild || killed != 1 {
			t.Errorf("killedChild=%v killed=%d, want true and 1", killedChild, killed)
		}
	})
}
