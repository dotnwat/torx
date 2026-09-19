//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotnwat/torx"
)

// TestMain lets the test binary stand in for the suite binary's worker role,
// so the end-to-end test runs the real driver/worker split: the driver in the
// test process spawns this binary again with "worker" for each job.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		torx.Main()
	}
	os.Exit(m.Run())
}

// requiredEnv, when set, turns a missing rqlited from a skip into a failure.
// CI's rqlite job sets it, so the end-to-end path is exercised on every push
// and a missing binary there is a broken job, not a quiet skip.
const requiredEnv = "TORX_RQLITE_REQUIRED"

// requireRqlited skips the test when rqlited is not on PATH -- the suite
// resolves it from there by name -- unless requiredEnv says it must be.
func requireRqlited(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rqlited"); err == nil {
		return
	}
	msg := "rqlited is not on PATH; install rqlite (brew install rqlite, or a release from https://github.com/rqlite/rqlite/releases) to run the end-to-end test"
	if os.Getenv(requiredEnv) != "" {
		t.Fatalf("%s (%s is set)", msg, requiredEnv)
	}
	t.Skip(msg)
}

// largestJob is the node demand of the biggest job here, which sizes the
// local pool. A job that grows past it fails allocation loudly.
const largestJob = failoverNodes

func TestSuiteEndToEnd(t *testing.T) {
	requireRqlited(t)
	reqs, err := torx.Discover()
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	root := t.TempDir()
	res := torx.Run(context.Background(), localPool(t, largestJob), torx.SelfExecLauncher{}, reqs,
		torx.RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("suite failed:\n%s\n%s", res.Render(), failedJobLogs(root, res))
	}
	byID := map[string]torx.JobResult{}
	for _, r := range res.Jobs {
		if r.Summary == "" {
			t.Errorf("%s: no summary", r.ID)
		}
		byID[r.ID] = r
	}
	for _, id := range []string{"rqlite.smoke", "rqlite.failover", `rqlite.cluster[level="none",nodes=3]`} {
		if _, ok := byID[id]; !ok {
			t.Errorf("no result for %s; ran %v", id, res.Jobs)
		}
	}

	// The crashed leader ran twice, and both incarnations' output reached the
	// results tree: the service's rotate policy has StartCaptured move the first
	// log aside before the second start instead of truncating it.
	var data struct {
		Crashed string `json:"crashed"`
	}
	if err := json.Unmarshal(byID["rqlite.failover"].Data, &data); err != nil || data.Crashed == "" {
		t.Fatalf("failover result data %s: %v", byID["rqlite.failover"].Data, err)
	}
	run, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest symlink: %v", err)
	}
	for _, name := range []string{"stdout.1.log", "stdout.log"} {
		p := filepath.Join(root, run, "rqlite.failover", serviceName, data.Crashed, name)
		if info, err := os.Stat(p); err != nil || info.Size() == 0 {
			t.Errorf("collected log %s missing or empty: %v", p, err)
		}
	}
}

// failedJobLogs returns the collected test_log of every job that did not
// pass, so a failure -- in CI especially, where the temporary results tree is
// gone by the time anyone looks -- shows which wait or check gave out.
func failedJobLogs(root string, res torx.SuiteResult) string {
	run, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		return "no run directory: " + err.Error()
	}
	var b strings.Builder
	for _, r := range res.Jobs {
		if r.Status == torx.StatusPass {
			continue
		}
		log, err := os.ReadFile(filepath.Join(root, run, r.ID, "test_log"))
		if err != nil {
			fmt.Fprintf(&b, "=== %s: no test_log: %v\n", r.ID, err)
			continue
		}
		fmt.Fprintf(&b, "=== %s test_log ===\n%s\n", r.ID, log)
	}
	return b.String()
}

// localPool builds n local nodes rooted in the test's temp directory, sharing
// one port allocator as torx.Main's own local pool does.
func localPool(t *testing.T, n int) *torx.Pool {
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
