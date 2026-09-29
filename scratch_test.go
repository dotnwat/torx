//go:build unix

package torx

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRunScratchIsTheRunsOwnAndReapsTheGone(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	root := filepath.Join(os.TempDir(), scratchRoot)

	// A run that is gone left its directory and lock file behind; another
	// was killed between making its directory and locking it.
	gone := filepath.Join(root, "run-gone")
	unlocked := filepath.Join(root, "run-unlocked")
	for _, d := range []string{gone, unlocked} {
		if err := os.MkdirAll(filepath.Join(d, "node-0"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(gone, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	a, err := newRunScratch()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{gone, unlocked} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("%s is still there (%v)", d, err)
		}
	}

	// A second run at the same time gets a directory of its own, and does
	// not reap the first's, whose lock the first holds.
	b, err := newRunScratch()
	if err != nil {
		t.Fatal(err)
	}
	if a.dir == b.dir || filepath.Dir(a.dir) != root || filepath.Dir(b.dir) != root {
		t.Fatalf("run directories %s and %s, want two under %s", a.dir, b.dir, root)
	}
	if _, err := os.Stat(a.dir); err != nil {
		t.Fatalf("the second run reaped the first's directory: %v", err)
	}

	b.Close()
	if _, err := os.Stat(b.dir); !os.IsNotExist(err) {
		t.Errorf("a closed run's directory is still there (%v)", err)
	}
	a.Close()
}

// TestRunScratchUnderConcurrentStarts has runs start at once, each reaping
// the directories of runs that are gone as it makes its own: no run may
// reap a directory another is still making, or return one that is gone.
func TestRunScratchUnderConcurrentStarts(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	fail := func(s string) {
		mu.Lock()
		failures = append(failures, s)
		mu.Unlock()
	}
	for range 16 {
		wg.Go(func() {
			for range 200 {
				rs, err := newRunScratch()
				if err != nil {
					fail(err.Error())
					continue
				}
				if _, err := os.Stat(filepath.Join(rs.dir, ".lock")); err != nil {
					fail("returned a reaped directory: " + err.Error())
				}
				rs.Close()
			}
		})
	}
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("%d of 3200 starts failed; first: %s", len(failures), failures[0])
	}
}
