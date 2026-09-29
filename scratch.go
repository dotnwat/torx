//go:build unix

package torx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// scratchRoot is where runs keep their local nodes' scratch, under the
// temporary directory.
const scratchRoot = "torx"

// runScratch is the directory under which a run's local nodes keep their
// scratch: one of the run's own, $TMPDIR/torx/run-XXXX, so that runs going at
// once on one host never share a node's directory, where one run's clean
// would delete what another's services are writing. The run holds a lock on
// it for as long as it lives and removes it at the end. A directory whose lock
// no process holds belongs to a run that ended without removing it -- one that
// was killed -- and the next run to start removes it.
type runScratch struct {
	dir  string
	lock *os.File
}

// newRunScratch removes the directories of runs that are gone and makes this
// run's.
func newRunScratch() (*runScratch, error) {
	root := filepath.Join(os.TempDir(), scratchRoot)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("scratch: %w", err)
	}
	reapScratch(root)
	dir, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return nil, fmt.Errorf("scratch: %w", err)
	}
	lock, err := lockScratch(dir, true)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("scratch: %w", err)
	}
	return &runScratch{dir: dir, lock: lock}, nil
}

// Close removes the run's directory and releases it.
func (s *runScratch) Close() {
	_ = os.RemoveAll(s.dir)
	_ = s.lock.Close()
}

// lockScratch takes the lock of the run directory dir, creating its lock
// file when create is set, and fails with syscall.EWOULDBLOCK when a live run
// holds it. The lock lasts until the file is closed, or its process ends.
func lockScratch(dir string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), flags, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// reapScratch removes the run directories under root whose runs are gone:
// their lock file is there and no process holds it. A directory without a
// lock file may belong to a run that has only just made it, and is left.
func reapScratch(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "run-") {
			continue
		}
		dir := filepath.Join(root, e.Name())
		lock, err := lockScratch(dir, false)
		if err != nil {
			continue // a live run's, or one without a lock file yet
		}
		_ = os.RemoveAll(dir)
		_ = lock.Close()
	}
}
