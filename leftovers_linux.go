//go:build linux

package torx

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"
)

const (
	// killTaggedTimeout bounds how long killTagged keeps scanning /proc for
	// SIGKILLed leftovers to finish exiting and for any processes they forked
	// to be caught. A large process freeing memory or finishing disk I/O after
	// SIGKILL can take well over a few hundred milliseconds to leave /proc.
	killTaggedTimeout  = 2 * time.Second
	killTaggedInterval = 10 * time.Millisecond
)

// killTagged kills every process whose environment carries the entry
// workerTagEnv=tag -- every process a worker started, and every process
// those started in turn, wherever they were reparented -- and reports how
// many distinct processes it killed. It reads each process's environment as
// it was at exec, from /proc, and goes round again until none that could be
// signalled remains, so that a process forked while it worked does not escape.
// A kill error on one process (e.g. EPERM) is recorded without abandoning the
// rest of the scan.
func killTagged(tag string) (int, error) {
	return killTaggedWith("/proc", tag, os.Getpid(), func(pid int) error {
		return syscall.Kill(pid, syscall.SIGKILL)
	}, killTaggedTimeout, killTaggedInterval)
}

func killTaggedWith(procDir, tag string, self int, kill func(int) error, timeout, interval time.Duration) (int, error) {
	want := []byte(workerTagEnv + "=" + tag)
	killed := map[int]struct{}{}
	failed := map[int]error{}
	deadline := time.Now().Add(timeout)
	for {
		entries, err := os.ReadDir(procDir)
		if err != nil {
			return len(killed), joinKillErrors(failed, err)
		}
		pending := 0
		for _, e := range entries {
			pid, err := strconv.Atoi(e.Name())
			if err != nil || pid == self || pid < minSignalablePID {
				continue
			}
			if _, unkillable := failed[pid]; unkillable {
				continue
			}
			env, err := os.ReadFile(filepath.Join(procDir, e.Name(), "environ"))
			if err != nil || !hasEnvEntry(env, want) {
				continue // gone, a zombie, or another user's
			}
			if err := kill(pid); err == nil {
				killed[pid] = struct{}{}
				pending++
			} else if errors.Is(err, syscall.ESRCH) {
				// The process was present in this /proc snapshot and exited
				// before SIGKILL reached it; rescan in case it forked a child
				// after ReadDir.
				pending++
			} else {
				failed[pid] = err
			}
		}
		if pending == 0 {
			return len(killed), joinKillErrors(failed, nil)
		}
		if !time.Now().Before(deadline) {
			return len(killed), joinKillErrors(failed, errors.New("torx: processes the worker left did not exit in time"))
		}
		time.Sleep(interval)
	}
}

func joinKillErrors(failed map[int]error, extra error) error {
	if len(failed) == 0 {
		return extra
	}
	errs := make([]error, 0, len(failed)+1)
	for _, pid := range slices.Sorted(maps.Keys(failed)) {
		errs = append(errs, fmt.Errorf("pid %d: %w", pid, failed[pid]))
	}
	if extra != nil {
		errs = append(errs, extra)
	}
	return errors.Join(errs...)
}

// hasEnvEntry reports whether env, NUL-separated entries as
// /proc/<pid>/environ holds them, has the entry want.
func hasEnvEntry(env, want []byte) bool {
	for entry := range bytes.SplitSeq(env, []byte{0}) {
		if bytes.Equal(entry, want) {
			return true
		}
	}
	return false
}
