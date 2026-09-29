//go:build unix

package torx

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunScratchIsTheRunsOwnAndReapsTheGone(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	root := filepath.Join(os.TempDir(), scratchRoot)

	// A run that is gone left its directory and lock file behind; a run
	// that has only just made its directory has no lock file yet.
	gone := filepath.Join(root, "run-gone")
	fresh := filepath.Join(root, "run-fresh")
	for _, d := range []string{gone, fresh} {
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
	if _, err := os.Stat(gone); !os.IsNotExist(err) {
		t.Errorf("the gone run's directory is still there (%v)", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("the fresh run's directory was removed: %v", err)
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
