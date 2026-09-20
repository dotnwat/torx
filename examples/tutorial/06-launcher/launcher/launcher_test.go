//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

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

// TestLauncherRelativeResultsDir gives the launcher a -results-dir relative
// to its working directory. The run directory goes onto the suite's PATH and
// into compose's flags, both of which need it absolute, so the launcher has
// to resolve it rather than pass it through.
func TestLauncherRelativeResultsDir(t *testing.T) {
	root := t.TempDir()
	rel, err := filepath.Rel(repoDir(t), root)
	if err != nil {
		t.Fatal(err)
	}
	runDir := launch(t, "-results-dir", rel, `kv\.smoke`)
	if !strings.HasPrefix(runDir, root+string(filepath.Separator)) {
		t.Fatalf("run directory %q is not under %s", runDir, root)
	}
	checkRun(t, runDir, "local", []string{"kv.smoke"})
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

// TestDockerTeardownOnSignal interrupts the docker backend at the two points
// where a signal used to end the launcher without a teardown: while compose
// is provisioning the nodes, and while the suite is running in the driver.
// The launcher runs against a stub docker on PATH that records every
// invocation and blocks where compose would, so the test needs no docker.
func TestDockerTeardownOnSignal(t *testing.T) {
	launcher := buildLauncher(t)
	stub := stubDocker(t)
	for _, tc := range []struct {
		name   string
		block  string                // the compose subcommand the stub blocks in
		signal func(*exec.Cmd) error // how the launcher is interrupted there
		want   []string              // the compose subcommands run, in order
	}{
		// Ctrl-C at a terminal reaches the whole foreground process group,
		// the launcher and the compose command in it alike.
		{"interrupt during up", "up", func(cmd *exec.Cmd) error {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
		}, []string{"up", "down"}},
		// A supervisor, or a CI runner cancelling the job, terminates the
		// launcher alone; it is the launcher that must pass that on.
		{"sigterm during run", "run", func(cmd *exec.Cmd) error {
			return cmd.Process.Signal(syscall.SIGTERM)
		}, []string{"up", "run", "down"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			log := filepath.Join(t.TempDir(), "docker.log")
			cmd := exec.CommandContext(ctx, launcher, "-backend", "docker", "-results-dir", t.TempDir(), `kv\.smoke`)
			cmd.Dir = repoDir(t)
			cmd.Env = append(os.Environ(),
				"PATH="+stub+string(os.PathListSeparator)+os.Getenv("PATH"),
				"TORX_STUB_LOG="+log, "TORX_STUB_BLOCK="+tc.block)
			// A process group of its own, so the test can signal the launcher
			// and its children the way a terminal does.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.WaitDelay = 10 * time.Second
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			// Interrupt once the stub is blocked where compose would be.
			for !slices.Contains(composeSubcommands(t, log), tc.block) {
				if ctx.Err() != nil {
					t.Fatalf("the launcher never reached compose %s\n%s", tc.block, stderr.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err := tc.signal(cmd); err != nil {
				t.Fatal(err)
			}
			// The launcher fails the run rather than dying of the signal ...
			var exit *exec.ExitError
			if err := cmd.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("launcher: %v, want exit status 1\n%s", err, stderr.String())
			}
			// ... after asking compose to tear the project down, and nothing
			// else, once interrupted.
			lines := composeSubcommands(t, log)
			if !slices.Equal(lines, tc.want) {
				t.Errorf("compose subcommands %q, want %q\n%s", lines, tc.want, stderr.String())
			}
			logged, _ := os.ReadFile(log)
			if !strings.Contains(string(logged), " down --remove-orphans --rmi local") {
				t.Errorf("the teardown does not remove this run's images:\n%s", logged)
			}
		})
	}
}

// stubDocker writes a stand-in docker into a directory for PATH and returns
// the directory. The stub appends every invocation to $TORX_STUB_LOG,
// answers "docker version" with this machine's architecture, and blocks in
// the compose subcommand named by $TORX_STUB_BLOCK until signalled.
func stubDocker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
printf '%s\n' "$*" >>"$TORX_STUB_LOG"
case "$1" in
version) echo ` + runtime.GOARCH + ` ;;
compose)
	shift
	while [ "${1#-}" != "$1" ]; do shift 2; done
	if [ "$1" = "$TORX_STUB_BLOCK" ]; then exec sleep 60; fi
	;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// composeSubcommands reads the stub's log and returns the subcommand of each
// compose invocation in it, skipping compose's global flags, which each take
// a value.
func composeSubcommands(t *testing.T, log string) []string {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var subs []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || f[0] != "compose" {
			continue
		}
		f = f[1:]
		for len(f) > 1 && strings.HasPrefix(f[0], "-") {
			f = f[2:]
		}
		if len(f) > 0 {
			subs = append(subs, f[0])
		}
	}
	return subs
}

// repoDir is the repository root, which the launcher must be run from.
func repoDir(t *testing.T) string {
	t.Helper()
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// buildLauncher builds the launcher for the tests that signal it: "go run"
// does not pass every signal on to the program it runs.
func buildLauncher(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "launcher")
	cmd := exec.Command("go", "build", "-o", bin, "./"+stepDir+"/launcher")
	cmd.Dir = repoDir(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build launcher: %v\n%s", err, out)
	}
	return bin
}

// launch runs the launcher with args from the repository root and returns
// the run directory it announced, failing the test if the launcher did.
func launch(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "./" + stepDir + "/launcher"}, args...)...)
	cmd.Dir = repoDir(t)
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
