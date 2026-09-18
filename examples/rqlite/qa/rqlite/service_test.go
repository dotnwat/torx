//go:build unix

package rqlite

import (
	"fmt"
	"slices"
	"testing"

	"github.com/dotnwat/torx"
)

func TestCommandLineBindsBroadlyAndAdvertisesHost(t *testing.T) {
	seed := commandLine("node-0", "10.0.0.5", 4001, 4002, nil, "/scratch/node-0/rqlite/data")
	want := []string{
		"-node-id", "node-0",
		"-http-addr", "0.0.0.0:4001", "-http-adv-addr", "10.0.0.5:4001",
		"-raft-addr", "0.0.0.0:4002", "-raft-adv-addr", "10.0.0.5:4002",
		"/scratch/node-0/rqlite/data",
	}
	if !slices.Equal(seed, want) {
		t.Errorf("seed argv = %q\nwant %q", seed, want)
	}
	if slices.Contains(seed, "-join") {
		t.Error("seed argv joins; a seed must bootstrap alone")
	}

	joiner := commandLine("node-2", "10.0.0.7", 4001, 4002, []string{"10.0.0.5:4002", "10.0.0.6:4002"}, "/d")
	if i := slices.Index(joiner, "-join"); i < 0 || joiner[i+1] != "10.0.0.5:4002,10.0.0.6:4002" {
		t.Errorf("joiner argv lacks the comma-joined peers: %q", joiner)
	}
	if joiner[len(joiner)-1] != "/d" {
		t.Errorf("data directory must be the final positional argument, got %q", joiner)
	}
}

// bound returns a service bound to n local nodes with no processes started.
func bound(t *testing.T, n int) (*Service, []*torx.Node) {
	t.Helper()
	nodes := make([]*torx.Node, n)
	for i := range nodes {
		name := fmt.Sprintf("node-%d", i)
		nodes[i] = torx.NewNode(torx.NodeConfig{
			Name:    name,
			Backend: torx.LocalBackend{},
			Scratch: torx.MakeScratch(t.TempDir(), name),
			Ports:   torx.NewPortAllocator(""),
		})
	}
	s := New("rqlite", n)
	s.Bind(nodes)
	return s, nodes
}

func TestPeersAreTheOtherMembersInNodeOrder(t *testing.T) {
	s, nodes := bound(t, 3)
	// Members exist for node-0 and node-1, as when node-2 is about to start.
	s.members["node-0"] = &member{httpPort: 4001, raftPort: 4002}
	s.members["node-1"] = &member{httpPort: 4003, raftPort: 4004}

	if got := s.peersLocked(nodes[0]); !slices.Equal(got, []string{"127.0.0.1:4004"}) {
		t.Errorf("peers of node-0 = %q, want only node-1", got)
	}
	if got := s.peersLocked(nodes[2]); !slices.Equal(got, []string{"127.0.0.1:4002", "127.0.0.1:4004"}) {
		t.Errorf("peers of node-2 = %q, want node-0 then node-1", got)
	}
	// A member whose process is down still counts: its ports are its identity.
	if got := s.peersLocked(nodes[1]); !slices.Equal(got, []string{"127.0.0.1:4002"}) {
		t.Errorf("peers of node-1 = %q, want node-0", got)
	}
}

func TestAddrAndClientFollowMembership(t *testing.T) {
	s, nodes := bound(t, 2)
	if got := s.Addr(nodes[0]); got != "" {
		t.Errorf("Addr before start = %q, want empty", got)
	}
	s.members["node-0"] = &member{httpPort: 4001, raftPort: 4002}
	if got := s.Addr(nodes[0]); got != "127.0.0.1:4001" {
		t.Errorf("Addr = %q, want 127.0.0.1:4001", got)
	}
	if got := s.Client(nodes[0]).URL("/readyz"); got != "http://127.0.0.1:4001/readyz" {
		t.Errorf("Client URL = %q", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("Client for an unstarted node did not panic")
		}
	}()
	s.Client(nodes[1])
}

func TestCrashAndRestartRequireAProcessState(t *testing.T) {
	s, nodes := bound(t, 1)
	ctx := t.Context()
	if err := s.Crash(ctx, nodes[0]); err == nil {
		t.Error("Crash of a never-started node succeeded")
	}
	if err := s.Restart(ctx, nodes[0]); err == nil {
		t.Error("Restart of a never-started node succeeded")
	}
	// StopNode of a never-started node is a no-op: the framework calls it
	// before every start to establish a known state.
	if err := s.StopNode(ctx, nodes[0]); err != nil {
		t.Errorf("StopNode of a never-started node: %v", err)
	}
}
