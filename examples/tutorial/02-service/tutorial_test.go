//go:build unix

package main

import (
	"context"
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
// test process spawns this binary again with "worker" for each job, and the
// worker launches kvd on its node as a third process.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		torx.Main()
	}
	os.Exit(m.Run())
}

func TestSmokeEndToEnd(t *testing.T) {
	installKVD(t)
	reqs, err := torx.Discover("kv.smoke")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	root := t.TempDir()
	res := torx.Run(context.Background(), localPool(t, 1), torx.SelfExecLauncher{}, reqs,
		torx.RunOptions{ResultsDir: root})
	if !res.Ok() {
		t.Fatalf("suite failed:\n%s\n%s", res.Render(), failedJobLogs(root, res))
	}
	if res.Jobs[0].Summary == "" {
		t.Errorf("kv.smoke: no summary")
	}

	// kvd's captured output was collected into the results tree, under the
	// service's directory and the node's.
	run, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest symlink: %v", err)
	}
	logPath := filepath.Join(root, run, "kv.smoke", serviceName, "node-0", "stdout.log")
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("collected kvd log missing at %s: %v", logPath, err)
	}
	for _, want := range []string{"listening on", "shutting down", "stopped"} {
		if !strings.Contains(string(log), want) {
			t.Errorf("collected kvd log lacks %q:\n%s", want, log)
		}
	}
}

// installKVD builds the tutorial's server into a temporary directory and
// puts that directory on PATH for the test and the workers it spawns. The
// service resolves kvd from PATH by name, so this is the test's version of
// the chore a launcher does for a real run.
func installKVD(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	cmd := exec.Command("go", "build", "-o", filepath.Join(bin, "kvd"), "../kvd")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build kvd: %v", err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
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
		dir := filepath.Join(root, run, r.ID)
		log, err := os.ReadFile(filepath.Join(dir, "test_log"))
		if err != nil {
			fmt.Fprintf(&b, "=== %s: no test_log: %v\n", r.ID, err)
			continue
		}
		fmt.Fprintf(&b, "=== %s test_log ===\n%s\n", r.ID, log)
		// The services' captured output, collected under <service>/<node>/.
		services, _ := os.ReadDir(dir)
		for _, svc := range services {
			if !svc.IsDir() {
				continue
			}
			nodes, _ := os.ReadDir(filepath.Join(dir, svc.Name()))
			for _, n := range nodes {
				files, _ := os.ReadDir(filepath.Join(dir, svc.Name(), n.Name()))
				for _, f := range files {
					if !strings.HasPrefix(f.Name(), "stdout") {
						continue
					}
					out, _ := os.ReadFile(filepath.Join(dir, svc.Name(), n.Name(), f.Name()))
					fmt.Fprintf(&b, "=== %s %s/%s/%s ===\n%s\n", r.ID, svc.Name(), n.Name(), f.Name(), out)
				}
			}
		}
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
