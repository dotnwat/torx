//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestProjectName(t *testing.T) {
	got := projectName("/results/tutorial/2026-09-19T23-30-02Z-612568634")
	if want := "torx-tutorial-2026-09-19t23-30-02z-612568634"; got != want {
		t.Errorf("projectName = %q, want %q", got, want)
	}
	if got := projectName("/tmp/run dir.1"); got != "torx-tutorial-run-dir-1" {
		t.Errorf("projectName with unsafe characters = %q", got)
	}
}

func TestWithPath(t *testing.T) {
	env := withPath([]string{"HOME=/h", "PATH=/usr/bin", "SHELL=/bin/sh"}, "/bin:/usr/bin")
	if strings.Join(env, " ") != "HOME=/h SHELL=/bin/sh PATH=/bin:/usr/bin" {
		t.Errorf("withPath = %v", env)
	}
}

// TestWriteKeys checks the generated files fit together: the client key
// parses, its public half is what the nodes authorize, and the host key is
// what known_hosts holds for every node name.
func TestWriteKeys(t *testing.T) {
	dir := t.TempDir()
	if err := writeKeys(dir, []string{"n0", "n1"}); err != nil {
		t.Fatal(err)
	}
	read := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	signer, err := ssh.ParsePrivateKey(read(clientKeyFile))
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	if !bytes.Equal(read(authorizedKeysFile), ssh.MarshalAuthorizedKey(signer.PublicKey())) {
		t.Errorf("authorized_keys is not the client's public key")
	}
	if info, err := os.Stat(filepath.Join(dir, clientKeyFile)); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("client key mode %v, want 0600", info.Mode().Perm())
	}
	hostSigner, err := ssh.ParsePrivateKey(read(hostKeyFile))
	if err != nil {
		t.Fatalf("host key: %v", err)
	}
	known := string(read(knownHostsFile))
	hostPub := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey())))
	if !strings.HasPrefix(known, "n0,n1 ") || !strings.Contains(known, hostPub) {
		t.Errorf("known_hosts %q does not name both nodes with the host key", known)
	}
}

// The end-to-end tests run the launcher as a subprocess from the repository
// root, as a person would, and read the run directory it announces.

func TestLauncherLocal(t *testing.T) {
	root := t.TempDir()
	runDir := launch(t, "-results-dir", root, `kv\.smoke`, `kv\.graceful`)
	checkRun(t, runDir, "local", []string{"kv.smoke", "kv.graceful"})
}

// requiredEnv, when set, turns an unusable docker into a failure instead of
// a skip. CI's tutorial job sets it, so the docker path is exercised on
// every push and a missing docker there is a broken job, not a quiet skip.
const requiredEnv = "TORX_DOCKER_REQUIRED"

func TestLauncherDocker(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		msg := "docker is not usable (docker info failed); the docker backend test needs a running docker engine"
		if os.Getenv(requiredEnv) != "" {
			t.Fatalf("%s (%s is set)", msg, requiredEnv)
		}
		t.Skip(msg)
	}
	root := t.TempDir()
	// The two-node benchmark variant makes the load generators on n1 and n2
	// reach the server on n0 across containers.
	runDir := launch(t, "-backend", "docker", "-results-dir", root, `kv\.smoke`, `kv\.bench\[clients=4,nodes=2`)
	checkRun(t, runDir, "docker", []string{"kv.smoke", "kv.bench[clients=4,nodes=2,seconds=2]"})
	if loads := filesNamed(t, filepath.Join(runDir, "kv.bench[clients=4,nodes=2,seconds=2]"), "load-", ".json"); len(loads) != 2 {
		t.Errorf("load artifacts %v, want one per load node", loads)
	}
	// The run directory records how the nodes were provisioned and reached.
	for _, name := range []string{"docker/compose.yaml", "docker/Dockerfile", "docker/manifest.json", "keys/known_hosts", "bin/kvd"} {
		if _, err := os.Stat(filepath.Join(runDir, name)); err != nil {
			t.Errorf("run directory: %v", err)
		}
	}
}

// launch runs the launcher with args from the repository root and returns
// the run directory it announced, failing the test if the launcher did.
func launch(t *testing.T, args ...string) string {
	t.Helper()
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", append([]string{"run", "./" + stepDir + "/launcher"}, args...)...)
	cmd.Dir = repo
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("launcher %v: %v\n%s", args, err, stdout.String())
	}
	first, _, _ := strings.Cut(stdout.String(), "\n")
	runDir, ok := strings.CutPrefix(first, "run directory: ")
	if !ok {
		t.Fatalf("launcher's first line %q is not the run directory", first)
	}
	return runDir
}

// checkRun checks a run directory: the invocation names the backend, every
// job passed, and the server's captured output was collected from whichever
// node it ran on.
func checkRun(t *testing.T, runDir, backend string, jobs []string) {
	t.Helper()
	var inv invocation
	data, err := os.ReadFile(filepath.Join(runDir, invocationFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &inv); err != nil || inv.Backend != backend {
		t.Errorf("invocation %s: %v; want backend %s", data, err, backend)
	}
	for _, id := range jobs {
		var res struct {
			Status string `json:"status"`
		}
		data, err := os.ReadFile(filepath.Join(runDir, id, "result.json"))
		if err != nil {
			t.Errorf("%s: %v", id, err)
			continue
		}
		if err := json.Unmarshal(data, &res); err != nil || res.Status != "PASS" {
			t.Errorf("%s: %s", id, data)
		}
		nodes := filesNamed(t, filepath.Join(runDir, id, "kvd"), "", "")
		if len(nodes) != 1 {
			t.Errorf("%s: kvd ran on %v, want one node", id, nodes)
			continue
		}
		log, err := os.ReadFile(filepath.Join(runDir, id, "kvd", nodes[0], "stdout.log"))
		if err != nil || !bytes.Contains(log, []byte("listening on")) {
			t.Errorf("%s: collected kvd log from %s: %v\n%s", id, nodes[0], err, log)
		}
	}
}

// filesNamed lists the entries of dir whose names have the given prefix and
// suffix; a missing dir is simply empty.
func filesNamed(t *testing.T, dir, prefix, suffix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), suffix) {
			names = append(names, e.Name())
		}
	}
	return names
}
