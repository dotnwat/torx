package torx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestShouldCollect(t *testing.T) {
	onPass := Artifact{CollectOnPass: true}
	onFailOnly := Artifact{CollectOnPass: false}

	if !ShouldCollect(onPass, true) {
		t.Errorf("CollectOnPass artifact should be collected on pass")
	}
	if ShouldCollect(onFailOnly, true) {
		t.Errorf("non-default artifact should not be collected on pass")
	}
	if !ShouldCollect(onFailOnly, false) {
		t.Errorf("everything should be collected on failure")
	}
	if !ShouldCollect(onPass, false) {
		t.Errorf("CollectOnPass artifact should be collected on failure")
	}
}

func TestCollect(t *testing.T) {
	ctx := context.Background()
	nodeDir := t.TempDir() // stands in for the node's filesystem
	if err := os.WriteFile(filepath.Join(nodeDir, "always.log"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nodeDir, "ondemand.log"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	n := testNode(t, "n0") // LocalBackend: node paths are local paths
	artifacts := []Artifact{
		{Name: "always.log", Path: filepath.Join(nodeDir, "always.log"), CollectOnPass: true},
		{Name: "ondemand.log", Path: filepath.Join(nodeDir, "ondemand.log"), CollectOnPass: false},
	}

	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }

	// On success only the CollectOnPass artifact is gathered.
	passDir := filepath.Join(t.TempDir(), "pass")
	if err := Collect(ctx, n, artifacts, true, passDir); err != nil {
		t.Fatalf("Collect(pass): %v", err)
	}
	if !exists(filepath.Join(passDir, "always.log")) {
		t.Errorf("always.log not collected on pass")
	}
	if exists(filepath.Join(passDir, "ondemand.log")) {
		t.Errorf("ondemand.log collected on pass, want skipped")
	}

	// On failure everything is gathered.
	failDir := filepath.Join(t.TempDir(), "fail")
	if err := Collect(ctx, n, artifacts, false, failDir); err != nil {
		t.Fatalf("Collect(fail): %v", err)
	}
	if !exists(filepath.Join(failDir, "always.log")) || !exists(filepath.Join(failDir, "ondemand.log")) {
		t.Errorf("not all artifacts collected on failure")
	}
}

// TestCollectDir collects a directory as a gzipped tar of it, on failure
// only by default, and leaves nothing of the archive behind on the node.
func TestCollectDir(t *testing.T) {
	ctx := context.Background()
	data := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(filepath.Join(data, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"meta": "m", "wal/seg-1": "w"} {
		if err := os.WriteFile(filepath.Join(data, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	n := testNode(t, "n0")
	arts := []Artifact{{Name: "data.tar.gz", Path: data + "/", Dir: true}}

	passDir := filepath.Join(t.TempDir(), "pass")
	if err := Collect(ctx, n, arts, true, passDir); err != nil {
		t.Fatalf("Collect(pass): %v", err)
	}
	if _, err := os.Stat(filepath.Join(passDir, "data.tar.gz")); err == nil {
		t.Errorf("directory collected on pass, want skipped")
	}

	failDir := filepath.Join(t.TempDir(), "fail")
	if err := Collect(ctx, n, arts, false, failDir); err != nil {
		t.Fatalf("Collect(fail): %v", err)
	}
	out := extract(t, n, filepath.Join(failDir, "data.tar.gz"))
	for name, want := range map[string]string{"data/meta": "m", "data/wal/seg-1": "w"} {
		got, err := os.ReadFile(filepath.Join(out, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
	left, err := filepath.Glob(filepath.Join(n.Scratch().Root, ".artifact-*"))
	if err != nil || len(left) > 0 {
		t.Errorf("archive left on the node: %v %v", left, err)
	}

	missing := []Artifact{{Name: "gone.tar.gz", Path: filepath.Join(t.TempDir(), "gone"), Dir: true}}
	if err := Collect(ctx, n, missing, false, filepath.Join(t.TempDir(), "x")); err == nil {
		t.Errorf("Collect of a missing directory succeeded")
	}
}

// extract unpacks a collected archive into a fresh directory and returns it.
func extract(t *testing.T, n *Node, archive string) string {
	t.Helper()
	out := t.TempDir()
	if res, err := n.Exec(context.Background(), Command("tar", "-C", out, "-xzf", archive)); err != nil || res.ExitCode != 0 {
		t.Fatalf("extract %s: %v %s", archive, err, res.Stderr)
	}
	return out
}

// TestCollectDirThroughSymlink collects the directory a path names through
// a symlink, under the link's own name, and keeps the links inside it as
// links.
func TestCollectDirThroughSymlink(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	real := filepath.Join(base, "v3")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "meta"), []byte("m"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("meta", filepath.Join(real, "alias")); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "current")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	n := testNode(t, "n0")
	dest := t.TempDir()
	if err := Collect(ctx, n, []Artifact{{Name: "current.tar.gz", Path: link, Dir: true}}, false, dest); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := extract(t, n, filepath.Join(dest, "current.tar.gz"))
	if got, err := os.ReadFile(filepath.Join(out, "current", "meta")); err != nil || string(got) != "m" {
		t.Errorf("current/meta = %q, %v; want the directory behind the link", got, err)
	}
	if target, err := os.Readlink(filepath.Join(out, "current", "alias")); err != nil || target != "meta" {
		t.Errorf("current/alias -> %q, %v; want the link kept, to meta", target, err)
	}
}

// TestCollectDirOddNames collects a directory whose name begins with a dash,
// which tar would read as an option, and one named with the characters that
// mean something to the renaming of its members.
func TestCollectDirOddNames(t *testing.T) {
	ctx := context.Background()
	n := testNode(t, "n0")
	for _, name := range []string{"-wal", "a,b&c"} {
		dir := filepath.Join(t.TempDir(), name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		dest := t.TempDir()
		if err := Collect(ctx, n, []Artifact{{Name: "d.tar.gz", Path: dir, Dir: true}}, false, dest); err != nil {
			t.Fatalf("Collect %q: %v", name, err)
		}
		out := extract(t, n, filepath.Join(dest, "d.tar.gz"))
		if got, err := os.ReadFile(filepath.Join(out, name, "f")); err != nil || string(got) != "x" {
			t.Errorf("%s/f = %q, %v; want x", name, got, err)
		}
	}
}

// TestCollectDirChanging collects a directory that changes as tar reads it,
// which tar reports with exit status 1 after writing the archive all the
// same: the node's scratch directory, where the archive itself grows.
func TestCollectDirChanging(t *testing.T) {
	ctx := context.Background()
	n := testNode(t, "n0")
	root := n.Scratch().Root
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "marker"), []byte("k"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if err := Collect(ctx, n, []Artifact{{Name: "scratch.tar.gz", Path: root, Dir: true}}, false, dest); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := extract(t, n, filepath.Join(dest, "scratch.tar.gz"))
	if got, err := os.ReadFile(filepath.Join(out, filepath.Base(root), "marker")); err != nil || string(got) != "k" {
		t.Errorf("marker = %q, %v; want the archive kept despite the change", got, err)
	}
}

func TestFinalizers(t *testing.T) {
	var log []string
	var f Finalizers
	f.Add(func(context.Context) error { log = append(log, "first"); return nil })
	f.Add(func(context.Context) error { log = append(log, "second"); return errors.New("boom") })
	f.Add(func(context.Context) error { log = append(log, "third"); return nil })

	err := f.Run(context.Background())
	if !slices.Equal(log, []string{"third", "second", "first"}) {
		t.Errorf("run order = %v, want [third second first]", log)
	}
	if err == nil {
		t.Errorf("Run err = nil, want the failing finalizer's error")
	}

	// The stack is emptied: a second Run does nothing.
	log = nil
	if err := f.Run(context.Background()); err != nil || len(log) != 0 {
		t.Errorf("second Run = (%v, %v calls), want (nil, 0)", err, len(log))
	}
}
