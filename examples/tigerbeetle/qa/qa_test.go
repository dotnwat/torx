//go:build unix

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/dotnwat/torx"
)

// TestMain lets the test binary stand in for the suite binary's worker role,
// so the end-to-end test runs the real driver/worker split.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		torx.Main()
	}
	os.Exit(m.Run())
}

// TestSmokeEndToEnd runs tigerbeetle.smoke through the driver on three local
// nodes, when a tigerbeetle binary is on PATH. Its data files, three of over
// a gigabyte, go under the test's temporary directory: point TMPDIR at a
// disk if /tmp is a small tmpfs.
func TestSmokeEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("tigerbeetle"); err != nil {
		t.Skip("tigerbeetle is not on PATH; install a release from https://github.com/tigerbeetle/tigerbeetle/releases to run the end-to-end test")
	}
	reqs, err := torx.Discover(`tigerbeetle\.smoke`)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	ports := torx.NewPortAllocator("")
	base := t.TempDir()
	var nodes []*torx.Node
	for i := range 3 {
		name := fmt.Sprintf("node-%d", i)
		nodes = append(nodes, torx.NewNode(torx.NodeConfig{
			Name: name, Backend: torx.LocalBackend{}, Scratch: torx.MakeScratch(base, name), Ports: ports,
		}))
	}
	res := torx.Run(context.Background(), torx.NewPool(nodes), torx.SelfExecLauncher{}, reqs, torx.RunOptions{})
	if !res.Ok() {
		t.Fatalf("smoke failed:\n%s", res.Render())
	}
}
