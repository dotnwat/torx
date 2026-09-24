//go:build unix

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// modulePath is the torx module; the launcher runs only from inside it.
const modulePath = "github.com/dotnwat/torx"

// target is the platform the binaries are built for: the host's for the
// local backend, and Linux on the docker engine's architecture for docker,
// which on a Mac is not the host's platform at all. A suite is one static
// binary, so cross-compiling it is all "deploying" it takes.
type target struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

// buildTarget picks the target for backend, asking the docker engine for its
// architecture when the nodes are containers.
func buildTarget(backend string) (target, error) {
	if backend != "docker" {
		return target{OS: runtime.GOOS, Arch: runtime.GOARCH}, nil
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return target{}, fmt.Errorf("the docker backend needs docker on PATH (Docker's CLI, or Podman's installed as docker): %w", err)
	}
	info, err := dockerInfo()
	if err != nil {
		return target{}, err
	}
	var arch string
	if info.Host != nil {
		arch = info.Host.Arch
	} else {
		// Docker's info spells the architecture as uname does (x86_64); its
		// version spells it as Go does.
		out, err := exec.Command("docker", "version", "--format", "{{.Server.Arch}}").Output()
		if err != nil {
			return target{}, fmt.Errorf("docker version: %w (is the docker engine running?)", err)
		}
		arch = strings.TrimSpace(string(out))
	}
	if arch == "" {
		return target{}, fmt.Errorf("the docker engine reported no architecture")
	}
	return target{OS: "linux", Arch: arch}, nil
}

// gitInfo identifies the launching tree. SHA is empty when the tree is not a
// git checkout or git is unavailable.
type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// repoRoot locates the torx repository through the go tool, so the launcher
// builds the suite of the tree it is run from and refuses to run elsewhere.
func repoRoot() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}} {{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("locate the repository: %w; run from inside the torx repository", err)
	}
	path, dir, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if path != modulePath || dir == "" {
		return "", fmt.Errorf("the current module is %q, not %s; run from inside the torx repository", path, modulePath)
	}
	return dir, nil
}

// gitIdentity captures the tree's commit and whether it has uncommitted
// changes, best-effort: a tree that is not a checkout yields an empty SHA.
func gitIdentity(repo string) gitInfo {
	sha, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		return gitInfo{}
	}
	status, err := exec.Command("git", "-C", repo, "status", "--porcelain").Output()
	if err != nil {
		return gitInfo{}
	}
	return gitInfo{SHA: strings.TrimSpace(string(sha)), Dirty: strings.TrimSpace(string(status)) != ""}
}

// goBuild builds pkg from repo into out for tgt, statically (no cgo) so the
// binary runs in a container with no C library. The build's output goes to
// standard error so standard output stays clean for the run-directory line.
func goBuild(repo, pkg, out string, tgt target) error {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "GOOS="+tgt.OS, "GOARCH="+tgt.Arch, "CGO_ENABLED=0")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build %s: %w", pkg, err)
	}
	return nil
}

// installFile copies src to dst as an executable, creating dst's directory.
func installFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return copyFile(src, dst, 0o755)
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339)
}
