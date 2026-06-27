package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotnwat/torx"
)

// TestMain lets the test binary stand in for the demo binary's other roles, so
// the slice runs end to end from a single executable: "worker" hands off to
// torx.Main's worker mode (the driver spawns a real worker subprocess), and
// "echo-server" runs the server EchoService launches on a node.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "worker":
			torx.Main() // dispatches to worker mode and exits
		case "echo-server":
			os.Exit(echoServerMain(os.Args[2:]))
		}
	}
	os.Exit(m.Run())
}

func TestEchoEndToEnd(t *testing.T) {
	reqs, err := torx.Discover("demo.echo")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("discovered %d jobs, want 1 (demo.echo)", len(reqs))
	}

	// SelfExecLauncher re-executes this test binary in worker mode, which then
	// launches the echo server as a third process, so the run goes through the
	// full driver/worker/service path.
	root := t.TempDir()
	res := torx.Run(context.Background(), demoPool(t, 1), torx.SelfExecLauncher{}, reqs,
		torx.RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("echo suite failed:\n%s", res.Render())
	}
	if res.Jobs[0].Summary == "" {
		t.Errorf("expected a result summary from the echo job, got none")
	}

	// The echo server's captured output is collected into the results tree.
	target, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest symlink: %v", err)
	}
	logPath := filepath.Join(root, target, "demo.echo", "echo", "node-0", "stdout.log")
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("collected echo log missing at %s: %v", logPath, err)
	}
	if !strings.Contains(string(log), "echo server listening") {
		t.Errorf("collected echo log lacks the server's startup output:\n%s", log)
	}
}

// demoPool builds a pool of n local nodes rooted in the test's temp directory.
func demoPool(t *testing.T, n int) *torx.Pool {
	t.Helper()
	ports := torx.NewPortAllocator("")
	base := t.TempDir()
	nodes := make([]*torx.Node, n)
	for i := range nodes {
		name := fmt.Sprintf("node-%d", i)
		nodes[i] = torx.NewNode(torx.NodeConfig{
			Name:    name,
			Backend: torx.LocalBackend{},
			Scratch: torx.MakeScratch(base, name),
			Ports:   ports,
		})
	}
	return torx.NewPool(nodes)
}
