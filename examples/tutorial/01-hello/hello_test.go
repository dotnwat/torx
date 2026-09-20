//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func TestSuiteEndToEnd(t *testing.T) {
	reqs, err := torx.Discover()
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	root := t.TempDir()
	res := torx.Run(context.Background(), localPool(t, 1), torx.SelfExecLauncher{}, reqs,
		torx.RunOptions{ResultsDir: root})

	// One job fails on purpose, so the suite as a whole does not pass.
	if res.Ok() {
		t.Fatalf("suite passed, but hello.fail should have failed:\n%s", res.Render())
	}
	byID := map[string]torx.JobResult{}
	for _, r := range res.Jobs {
		byID[r.ID] = r
	}
	if r := byID["hello.pass"]; r.Status != torx.StatusPass || r.Summary == "" {
		t.Errorf("hello.pass: status %s, summary %q; want PASS with a summary", r.Status, r.Summary)
	}
	if r := byID["hello.fail"]; r.Status != torx.StatusFail || r.Error == nil || r.Error.Message != "this job always fails" {
		t.Errorf("hello.fail: status %s, error %+v; want FAIL with the job's error", r.Status, r.Error)
	}

	// Each job has a directory in the results tree with its result.json, and
	// the failure's error is recorded there too.
	run, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest symlink: %v", err)
	}
	var recorded struct {
		Status string `json:"status"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	data, err := os.ReadFile(filepath.Join(root, run, "hello.fail", "result.json"))
	if err != nil {
		t.Fatalf("result.json: %v", err)
	}
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatalf("result.json: %v", err)
	}
	if recorded.Status != "FAIL" || recorded.Error == nil || recorded.Error.Message != "this job always fails" {
		t.Errorf("hello.fail/result.json records %s", data)
	}
	if _, err := os.Stat(filepath.Join(root, run, "hello.pass", "test_log")); err != nil {
		t.Errorf("hello.pass/test_log: %v", err)
	}
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
