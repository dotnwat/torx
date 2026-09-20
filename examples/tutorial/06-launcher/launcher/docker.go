//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// dockerDir is the directory of docker files in this step, copied into
	// each run directory so the run records exactly what provisioned it.
	dockerDir = "docker"
	// composeFile and manifestFile live in dockerDir. The manifest is
	// static: compose gives the nodes fixed names on its network, and the
	// keys it points at have fixed names too.
	composeFile  = "compose.yaml"
	manifestFile = "manifest.json"
	// keysDir is where the ssh keys are written, under the run directory.
	keysDir = "keys"
	// containerRunDir is where the run directory is mounted in the driver
	// container; compose.yaml mounts it there.
	containerRunDir = "/torx/run"
	// composeGrace is how long a cancelled compose command has to return
	// after being signalled before it is killed: long enough for the suite
	// in the driver to stop its jobs and services and exit.
	composeGrace = 30 * time.Second
	// teardownTimeout bounds the compose down that ends every run.
	teardownTimeout = 2 * time.Minute
)

// nodeNames are the node services in compose.yaml, and the names the manifest
// and the ssh host keys use for them.
var nodeNames = []string{"n0", "n1", "n2"}

// runDocker provisions the nodes with docker compose, runs the suite in the
// driver container, and tears the containers down. Everything the compose
// project needs is in the run directory: the docker files copied from
// stepDir, the cross-compiled binaries, and freshly generated keys. The
// suite's exit status is returned; a provisioning failure is 1.
//
// Why a driver container: on Docker Desktop (macOS), containers on a bridge
// network are not reachable from the host, so a suite running on the host
// could not dial the nodes. Running it in a container on the same network
// is also what a CI runner or a bastion host looks like: the suite runs
// where it can reach the nodes.
func runDocker(ctx context.Context, stepDir, runDir string, argv []string) int {
	// An interrupt or a SIGTERM cancels ctx instead of ending the process,
	// so whatever compose has created by then is still torn down. The
	// compose command in the foreground is cancelled by forwarding the
	// signal to it: compose abandons provisioning, or passes it on to the
	// suite in the driver, which stops its jobs and services and exits, and
	// that exit status is reported as usual. A terminal's Ctrl-C reaches
	// compose directly as well; the suite then sees a second signal, which
	// it ignores.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := prepareDocker(stepDir, runDir); err != nil {
		return die(err)
	}
	project := projectName(runDir)
	compose := func(ctx context.Context, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "docker", append([]string{
			"compose",
			"-f", filepath.Join(runDir, dockerDir, composeFile),
			"--project-directory", runDir,
			"-p", project,
		}, args...)...)
		cmd.Dir = runDir
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = composeGrace
		// Results the driver writes should belong to whoever ran the launcher,
		// which matters on Linux, where a bind mount keeps container-side
		// ownership.
		cmd.Env = append(os.Environ(), "TORX_UID="+strconv.Itoa(os.Getuid()), "TORX_GID="+strconv.Itoa(os.Getgid()))
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd
	}
	// The teardown removes the containers, the network, and the node images
	// compose built for this project. It runs on a context of its own,
	// bounded but not cancelled along with ctx: it is the one thing an
	// interrupted run must still finish.
	teardown := func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
		defer cancel()
		fmt.Fprintln(os.Stderr, "launcher: stopping the nodes")
		if err := compose(ctx, "down", "--remove-orphans", "--rmi", "local").Run(); err != nil {
			fmt.Fprintln(os.Stderr, "launcher: docker compose down:", err)
		}
	}

	// Build the node image and start the nodes, waiting until each one's
	// sshd answers (the healthcheck in compose.yaml).
	fmt.Fprintf(os.Stderr, "launcher: starting %d nodes (compose project %s)\n", len(nodeNames), project)
	if err := compose(ctx, append([]string{"up", "--build", "--detach", "--wait"}, nodeNames...)...).Run(); err != nil {
		teardown()
		return die(interrupted(ctx, fmt.Errorf("docker compose up: %w", err)))
	}
	defer teardown()

	// Run the suite in the driver container. Its exit status is ours; for a
	// run stopped by a signal it is compose's own, which is non-zero too.
	err := compose(ctx, append([]string{"run", "--rm", "driver"}, argv...)...).Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit) && exit.ExitCode() >= 0:
		return exit.ExitCode()
	default:
		// compose died of a signal, or did not start.
		return die(interrupted(ctx, fmt.Errorf("docker compose run: %w", err)))
	}
}

// interrupted returns a plain "interrupted" in place of err once ctx has
// been cancelled, when the signal, not what compose reported on its way out,
// is what happened to the run.
func interrupted(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return errors.New("interrupted")
	}
	return err
}

// prepareDocker copies the docker files into the run directory and writes
// the keys beside them.
func prepareDocker(stepDir, runDir string) error {
	src := filepath.Join(stepDir, dockerDir)
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	dst := filepath.Join(runDir, dockerDir)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := copyFile(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name()), 0o644); err != nil {
			return err
		}
	}
	return writeKeys(filepath.Join(runDir, keysDir), nodeNames)
}

// projectName derives the compose project name from the run directory, so
// concurrent runs do not share containers or a network. Compose allows
// lowercase letters, digits, hyphens, and underscores.
func projectName(runDir string) string {
	base := strings.ToLower(filepath.Base(runDir))
	var b strings.Builder
	b.WriteString("torx-tutorial-")
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}
