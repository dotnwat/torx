//go:build unix

package litestream

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

// slowTail is a LocalBackend whose tail waits for gate: the round trip to
// a node that the record of an exit takes.
type slowTail struct {
	torx.Backend
	gate chan struct{}
}

func (b slowTail) Exec(ctx context.Context, cmd torx.Cmd) (torx.ExecResult, error) {
	if cmd.Path == "tail" {
		<-b.gate
	}
	return b.Backend.Exec(ctx, cmd)
}

func TestNotRunning(t *testing.T) {
	ctx := context.Background()
	s := New("ls", 1)
	gate := make(chan struct{})
	n := torx.NewNode(torx.NodeConfig{
		Name:    "n0",
		Backend: slowTail{torx.LocalBackend{}, gate},
		Scratch: torx.MakeScratch(t.TempDir(), "n0"),
		Ports:   torx.NewPortAllocator(""),
	})
	if err := s.StartNode(ctx, n); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.StopNode(context.Background(), n) })

	// A process that exits is not running at once, and its exit is
	// recorded once its log has been read: asked in between, notRunning
	// waits for the record.
	if err := s.Launch(ctx, "app", n, torx.Command("sh", "-c", "echo boom; exit 7")); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); s.Running("app"); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the process did not exit")
		}
	}
	if exits := s.Exits(); len(exits) != 0 {
		t.Fatalf("the exit was recorded before its log was read: %v", exits)
	}
	time.AfterFunc(100*time.Millisecond, func() { close(gate) })
	err := s.notRunning(ctx, "app")
	if err == nil || !strings.Contains(err.Error(), "app exited with status 7") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("a process that exited: %v, want its status and its log", err)
	}

	// A launch that was crashed leaves no record, and the exit of the
	// launch before it is not its own.
	if err := s.Relaunch(ctx, "app", torx.Command("sh", "-c", "while :; do sleep 0.05; done")); err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := s.notRunning(short, "app"); !errors.Is(err, torx.ErrNotRunning) {
		t.Fatalf("a process that was crashed: %v, want ErrNotRunning", err)
	}

	// A process there never was is not waited for.
	start := time.Now()
	if err := s.notRunning(ctx, "nope"); !errors.Is(err, torx.ErrNotRunning) || time.Since(start) > exitGrace/2 {
		t.Fatalf("an unknown process: %v after %v", err, time.Since(start))
	}
}
