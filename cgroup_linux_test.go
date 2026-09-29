//go:build linux

package torx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeCgroup is a directory with the two files the gate touches, reporting
// itself frozen as soon as asked.
func fakeCgroup(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for file, content := range map[string]string{"cgroup.freeze": "0", "cgroup.events": "populated 1\nfrozen 1\n"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCgroupGateHoldsStartsWhileFrozen(t *testing.T) {
	dir := fakeCgroup(t)
	g := gateFor(dir)
	ctx := context.Background()
	if err := g.freeze(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "cgroup.freeze")); string(b) != "1" {
		t.Fatalf("cgroup.freeze is %q after freeze, want 1", b)
	}
	// A start with a deadline gives up; one without waits for the thaw.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := g.enter(short); err == nil {
		t.Fatal("a start entered a frozen cgroup")
	}
	entered := make(chan func(), 1)
	go func() {
		leave, err := g.enter(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		entered <- leave
	}()
	select {
	case <-entered:
		t.Fatal("a start entered before the thaw")
	case <-time.After(50 * time.Millisecond):
	}
	if err := g.thaw(dir); err != nil {
		t.Fatal(err)
	}
	var leave func()
	select {
	case leave = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting start did not enter after the thaw")
	}
	// A freeze waits for the start in flight.
	froze := make(chan error, 1)
	go func() { froze <- g.freeze(ctx, dir) }()
	select {
	case err := <-froze:
		t.Fatalf("the freeze did not wait for the start in flight (%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	leave()
	select {
	case err := <-froze:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the freeze did not go ahead once the start was done")
	}
	if err := g.thaw(dir); err != nil {
		t.Fatal(err)
	}
}

// TestCgroupGateThawDuringAPendingFreeze is a thaw that comes while a freeze
// waits for a start in flight: the freeze must not freeze the cgroup after
// the thaw has let starts through, or a start would fork into a frozen
// cgroup and hang the process.
func TestCgroupGateThawDuringAPendingFreeze(t *testing.T) {
	dir := fakeCgroup(t)
	g := gateFor(dir)
	ctx := context.Background()
	leave, err := g.enter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	froze := make(chan error, 1)
	go func() { froze <- g.freeze(ctx, dir) }()
	pending := func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.frozen
	}
	for deadline := time.Now().Add(5 * time.Second); !pending(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the freeze never closed the gate")
		}
	}
	thawed := make(chan error, 1)
	go func() { thawed <- g.thaw(dir) }()
	time.Sleep(50 * time.Millisecond) // the thaw runs now, or waits for the freeze
	leave()
	for _, done := range []chan error{froze, thawed} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the freeze or the thaw did not finish")
		}
	}
	// The thaw came last: the cgroup is thawed, as the gate says it is.
	if b, _ := os.ReadFile(filepath.Join(dir, "cgroup.freeze")); string(b) != "0" {
		t.Fatalf("cgroup.freeze is %q after a freeze and then a thaw, want 0", b)
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	leave, err = g.enter(short)
	if err != nil {
		t.Fatalf("a start after the thaw: %v", err)
	}
	leave()
}
