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
	if err := prepareDocker(stepDir, runDir); err != nil {
		return die(err)
	}
	project := projectName(runDir)
	compose := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "docker", append([]string{
			"compose",
			"-f", filepath.Join(runDir, dockerDir, composeFile),
			"--project-directory", runDir,
			"-p", project,
		}, args...)...)
		cmd.Dir = runDir
		// Results the driver writes should belong to whoever ran the launcher,
		// which matters on Linux, where a bind mount keeps container-side
		// ownership.
		cmd.Env = append(os.Environ(), "TORX_UID="+strconv.Itoa(os.Getuid()), "TORX_GID="+strconv.Itoa(os.Getgid()))
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd
	}

	// Build the node image and start the nodes, waiting until each one's
	// sshd answers (the healthcheck in compose.yaml).
	fmt.Fprintf(os.Stderr, "launcher: starting %d nodes (compose project %s)\n", len(nodeNames), project)
	if err := compose(append([]string{"up", "--build", "--detach", "--wait"}, nodeNames...)...).Run(); err != nil {
		_ = compose("down", "--remove-orphans").Run()
		return die(fmt.Errorf("docker compose up: %w", err))
	}
	defer func() {
		fmt.Fprintln(os.Stderr, "launcher: stopping the nodes")
		if err := compose("down", "--remove-orphans").Run(); err != nil {
			fmt.Fprintln(os.Stderr, "launcher: docker compose down:", err)
		}
	}()

	// Run the suite in the driver container. An interrupt reaches docker
	// compose in the foreground as well as this process; ignoring it here
	// lets compose stop the driver and return, so the teardown above still
	// runs, and the suite's cancelled run is what is reported.
	signal.Ignore(os.Interrupt)
	defer signal.Reset(os.Interrupt)
	err := compose(append([]string{"run", "--rm", "driver"}, argv...)...).Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit):
		return exit.ExitCode()
	default:
		return die(fmt.Errorf("docker compose run: %w", err))
	}
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
