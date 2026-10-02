//go:build linux

package torx

import (
	"bytes"
	"errors"
	"os"
	"strconv"
	"syscall"
	"time"
)

// killTagged kills every process whose environment carries the entry
// workerTagEnv=tag -- every process a worker started, and every process
// those started in turn, wherever they were reparented -- and reports how
// many it killed. It reads each process's environment as it was at exec,
// from /proc, and goes round again until it finds none, so that a process
// forked while it worked does not escape.
func killTagged(tag string) (int, error) {
	want := []byte(workerTagEnv + "=" + tag)
	self := os.Getpid()
	killed := 0
	for range 20 {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return killed, err
		}
		found := 0
		for _, e := range entries {
			pid, err := strconv.Atoi(e.Name())
			if err != nil || pid == self || pid < minSignalablePID {
				continue
			}
			env, err := os.ReadFile("/proc/" + e.Name() + "/environ")
			if err != nil || !hasEnvEntry(env, want) {
				continue // gone, a zombie, or another user's
			}
			found++
			if err := syscall.Kill(pid, syscall.SIGKILL); err == nil {
				killed++
			} else if !errors.Is(err, syscall.ESRCH) {
				return killed, err
			}
		}
		if found == 0 {
			return killed, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return killed, errors.New("torx: processes the worker left keep reappearing")
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
