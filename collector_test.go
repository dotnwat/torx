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
	out := t.TempDir()
	if res, err := n.Exec(ctx, Command("tar", "-C", out, "-xzf", filepath.Join(failDir, "data.tar.gz"))); err != nil || res.ExitCode != 0 {
		t.Fatalf("extract: %v %s", err, res.Stderr)
	}
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
