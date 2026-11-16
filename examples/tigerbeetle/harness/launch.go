//go:build unix

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// invocation is the record of one launch, written to the run directory before
// the suite starts.
type invocation struct {
	Argv        []string        `json:"argv"`
	Created     string          `json:"created"`
	Git         gitInfo         `json:"git"`
	TigerBeetle tigerbeetleInfo `json:"tigerbeetle"`
	Suite       string          `json:"suite"`
	SuiteArgv   []string        `json:"suite_argv"`
	Scratch     string          `json:"scratch"`
	JobPatterns []string        `json:"job_patterns"`
	Params      string          `json:"params,omitempty"`
}

type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// tigerbeetleInfo is the binary under test, as given, and what `version
// --verbose` says of it: its release, commit, build mode, and whether it
// was built with extra assertions (process.verify).
type tigerbeetleInfo struct {
	Path    string `json:"path"`
	Version string `json:"version"`
}

// moduleRoot locates this example's module through the go tool, so the
// harness builds the suite of the tree it is run from.
func moduleRoot() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}} {{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("locate the module: %w; run from examples/tigerbeetle", err)
	}
	path, dir, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if path != modulePath || dir == "" {
		return "", fmt.Errorf("the current module is %q, not %s; run from examples/tigerbeetle", path, modulePath)
	}
	return dir, nil
}

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

func goBuild(module, out string) error {
	cmd := exec.Command("go", "build", "-o", out, suitePackage)
	cmd.Dir = module
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build %s: %w", suitePackage, err)
	}
	return nil
}

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

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339) }
