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
	n := testNode("n0") // LocalBackend: node paths are local paths
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
