//go:build unix

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// modulePath is the torx module; the harness runs only from inside it.
	modulePath = "github.com/dotnwat/torx"
	// suitePackage is the suite, relative to the repository root.
	suitePackage = "./examples/rqlite/qa"
	// suiteBinary is the built suite's name inside a run directory's bin/.
	suiteBinary = "qa"
	// defaultResultsDir is the results root, relative to the repository root.
	defaultResultsDir = "results/rqlite"
	// invocationFile is the record the harness writes into each run directory.
	invocationFile = "invocation.json"
	// paramsFile is the archived name of a -params file, whatever it was called.
	paramsFile = "params.json"

	// rqlitedName is the server binary the suite resolves from PATH, and
	// requiredMajor the rqlite major version the suite is written against:
	// cluster join semantics have changed across majors.
	rqlitedName   = "rqlited"
	requiredMajor = 10
)

// invocation is the record of one launch, written to the run directory before
// the suite starts.
type invocation struct {
	Argv        []string    `json:"argv"`         // the harness's own command line
	Created     string      `json:"created"`      // RFC 3339, UTC
	Backend     string      `json:"backend"`      // where the nodes came from; always local here
	Git         gitInfo     `json:"git"`          // identity of the tree the suite was built from
	Rqlited     rqlitedInfo `json:"rqlited"`      // the system under test the nodes resolved
	Suite       string      `json:"suite"`        // the suite binary, inside the run directory
	SuiteArgv   []string    `json:"suite_argv"`   // exactly what was exec'd
	JobPatterns []string    `json:"job_patterns"` // the job selection, as given
	Params      string      `json:"params,omitempty"`
}

// gitInfo identifies the launching tree. SHA is empty when the tree is not a
// git checkout or git is unavailable.
type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// rqlitedInfo is the server binary the suite will resolve, as found on PATH.
type rqlitedInfo struct {
	Path    string `json:"path"`
	Version string `json:"version"` // as reported by -version, e.g. v10.3.5
}

// repoRoot locates the torx repository through the go tool, so the harness
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

// findRqlited resolves rqlited from PATH and checks its major version.
func findRqlited() (rqlitedInfo, error) {
	path, err := exec.LookPath(rqlitedName)
	if err != nil {
		return rqlitedInfo{}, fmt.Errorf("%s is not on PATH: install rqlite (brew install rqlite, or a release from https://github.com/rqlite/rqlite/releases) and retry", rqlitedName)
	}
	// rqlited prints its version line on standard error.
	out, err := exec.Command(path, "-version").CombinedOutput()
	if err != nil {
		return rqlitedInfo{}, fmt.Errorf("%s -version: %w", path, err)
	}
	version, major, err := parseVersion(string(out))
	if err != nil {
		return rqlitedInfo{}, fmt.Errorf("%s: %w", path, err)
	}
	if major != requiredMajor {
		return rqlitedInfo{}, fmt.Errorf("%s is %s; the suite is written against rqlite v%d", path, version, requiredMajor)
	}
	return rqlitedInfo{Path: path, Version: version}, nil
}

// parseVersion reads the version and its major out of rqlited's -version
// line, e.g. "rqlited v10.3.5 darwin arm64 go1.27.1 sqlite3.53.4 (...)".
func parseVersion(line string) (version string, major int, err error) {
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != rqlitedName {
		return "", 0, fmt.Errorf("unrecognized -version output %q", strings.TrimSpace(line))
	}
	version = fields[1]
	num, _, _ := strings.Cut(strings.TrimPrefix(version, "v"), ".")
	major, err = strconv.Atoi(num)
	if err != nil || !strings.HasPrefix(version, "v") {
		return "", 0, fmt.Errorf("unrecognized version %q in -version output", version)
	}
	return version, major, nil
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

// goBuild builds the suite from repo into out, with the build's output on
// standard error so standard output stays clean for the run-directory line.
func goBuild(repo, out string) error {
	cmd := exec.Command("go", "build", "-o", out, suitePackage)
	cmd.Dir = repo
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build %s: %w", suitePackage, err)
	}
	return nil
}

// makeRunDir creates a unique run directory under root and repoints
// root/latest at it, the way torx itself does in -results-dir mode: a UTC
// timestamp names the run, a random suffix keeps two runs started in the same
// second apart, and the latest link is a best-effort convenience swapped in
// by rename so it is never seen half-written.
func makeRunDir(root string) (string, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	stamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	dir, err := os.MkdirTemp(root, stamp+"-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return "", err
	}
	name := filepath.Base(dir)
	tmp := filepath.Join(root, ".latest."+name)
	_ = os.Remove(tmp)
	if err := os.Symlink(name, tmp); err == nil {
		if err := os.Rename(tmp, filepath.Join(root, "latest")); err != nil {
			_ = os.Remove(tmp)
		}
	}
	return dir, nil
}

// archiveParams copies the -params file verbatim into runDir under a fixed
// name and returns the copy's path.
func archiveParams(src, runDir string) (string, error) {
	dst := filepath.Join(runDir, paramsFile)
	if err := copyFile(src, dst, 0o644); err != nil {
		return "", fmt.Errorf("archive params: %w", err)
	}
	return dst, nil
}

// installFile copies the built suite to dst as an executable, creating dst's
// directory.
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
