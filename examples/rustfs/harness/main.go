//go:build unix

// Command harness launches the RustFS suite: it builds the suite, puts the
// rustfs binary under test where the suite's nodes resolve it, mints the
// run directory and records the invocation in it, and execs the suite with
// -run-dir, so the suite's exit status is the harness's.
//
// Which build is under test is the harness's to say, since hunting for bugs
// means running more than one: a release, a preview, or a build of main
// with debug assertions. -rustfs names the binary; it is linked (or, across
// filesystems, copied) into the run directory's bin/, which goes first on
// the suite's PATH, so the run directory keeps exactly what ran. Without
// it, the rustfs on PATH runs.
//
// The nodes' scratch -- every server's drives -- defaults to the run
// directory's scratch/, emptied as each job ends, so that concurrent runs
// never share a node's directory and a system whose /tmp is a small tmpfs
// is not filled. -scratch moves it.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the harness's own output a caller may rely
// on; everything else goes to standard error.
//
// Usage, from anywhere inside the torx repository:
//
//	go run ./examples/rustfs/harness [-rustfs PATH] [-netns] [-cgroups] [-params FILE] [-seed N] [-nodes N] [-parallel N] [JOB_REGEX ...]
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
)

const (
	suitePackage      = "./examples/rustfs/qa"
	suiteBinary       = "qa"
	defaultResultsDir = "results/rustfs" // relative to the torx repository
	invocationFile    = "invocation.json"
	paramsFile        = "params.json"
	binaryName        = "rustfs"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("harness", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: harness [flags] [JOB_REGEX ...]\n\nLaunch the RustFS suite on a local pool. Flags:\n")
		fs.PrintDefaults()
	}
	binary := fs.String("rustfs", "", "the rustfs binary under test (default: the one on PATH)")
	resultsDir := fs.String("results-dir", "", "root to create the run directory under (default: "+defaultResultsDir+" in the repository)")
	scratch := fs.String("scratch", "", "directory for the nodes' scratch, drives included (default: scratch/ in the run directory)")
	paramsFlag := fs.String("params", "", "parameter override file for the suite, archived in the run directory")
	nodes := fs.Int("nodes", 0, "local pool size (0 sizes it to the largest job)")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	netns := fs.Bool("netns", false, "give each node a network of its own, for the network faults, and limited tmpfs drives, for disk-full")
	cgroups := fs.Bool("cgroups", false, "give each node a cgroup of its own, for the resource faults")
	seed := fs.String("seed", "", "the run seed, to repeat a run")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	patterns := fs.Args()

	repo, err := repoRoot()
	if err != nil {
		return die(err)
	}
	if *paramsFlag != "" {
		if _, err := os.Stat(*paramsFlag); err != nil {
			return die(fmt.Errorf("params file: %w", err))
		}
	}
	if *binary == "" {
		if *binary, err = exec.LookPath(binaryName); err != nil {
			return die(fmt.Errorf("%s is not on PATH; install a release from https://github.com/rustfs/rustfs/releases or name a build with -rustfs", binaryName))
		}
	}
	if *binary, err = filepath.Abs(*binary); err != nil {
		return die(err)
	}
	version, err := exec.Command(*binary, "--version").Output()
	if err != nil {
		return die(fmt.Errorf("%s --version: %w", *binary, err))
	}
	fmt.Fprintf(os.Stderr, "harness: %s: %s\n", *binary, firstLine(string(version)))

	build, err := os.MkdirTemp("", "torx-rustfs-harness-")
	if err != nil {
		return die(err)
	}
	defer func() { _ = os.RemoveAll(build) }()
	built := filepath.Join(build, suiteBinary)
	if err := goBuild(repo, built); err != nil {
		return die(err)
	}

	root := *resultsDir
	if root == "" {
		root = filepath.Join(repo, defaultResultsDir)
	}
	runDir, err := torx.MakeRunDir(root)
	if err != nil {
		return die(fmt.Errorf("run directory: %w", err))
	}
	bin := filepath.Join(runDir, "bin")
	suite := filepath.Join(bin, suiteBinary)
	if err := installFile(built, suite); err != nil {
		return die(err)
	}
	if err := linkFile(*binary, filepath.Join(bin, binaryName)); err != nil {
		return die(err)
	}
	if *scratch == "" {
		*scratch = filepath.Join(runDir, "scratch")
	}
	if err := os.MkdirAll(*scratch, 0o755); err != nil {
		return die(err)
	}

	argv := []string{suite, "-run-dir", runDir, "-parallel", strconv.Itoa(*parallel)}
	if *nodes > 0 {
		argv = append(argv, "-nodes", strconv.Itoa(*nodes))
	}
	if *netns {
		argv = append(argv, "-netns")
	}
	if *cgroups {
		argv = append(argv, "-cgroups")
	}
	if *seed != "" {
		argv = append(argv, "-seed", *seed)
	}
	inv := invocation{
		Argv: os.Args, Created: time.Now().UTC().Format(time.RFC3339), Git: gitIdentity(repo),
		RustFS: rustfsInfo{Path: *binary, Version: strings.TrimSpace(string(version))},
		Suite:  suite, Scratch: *scratch, JobPatterns: patterns,
	}
	if *paramsFlag != "" {
		archived := filepath.Join(runDir, paramsFile)
		if err := copyFile(*paramsFlag, archived, 0o644); err != nil {
			return die(fmt.Errorf("archive params: %w", err))
		}
		inv.Params = paramsFile
		argv = append(argv, "-params", archived)
	}
	argv = append(argv, patterns...)
	inv.SuiteArgv = argv
	if err := writeJSON(filepath.Join(runDir, invocationFile), inv); err != nil {
		return die(err)
	}

	env := setEnv(os.Environ(), "PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	env = setEnv(env, "TMPDIR", *scratch)
	_ = os.RemoveAll(build)
	fmt.Printf("run directory: %s\n", runDir)
	if err := syscall.Exec(suite, argv, env); err != nil {
		return die(fmt.Errorf("exec %s: %w", suite, err))
	}
	return 0
}

// invocation is the record of one launch, written to the run directory before
// the suite starts.
type invocation struct {
	Argv        []string   `json:"argv"`
	Created     string     `json:"created"`
	Git         gitInfo    `json:"git"`
	RustFS      rustfsInfo `json:"rustfs"`
	Suite       string     `json:"suite"`
	SuiteArgv   []string   `json:"suite_argv"`
	Scratch     string     `json:"scratch"`
	JobPatterns []string   `json:"job_patterns"`
	Params      string     `json:"params,omitempty"`
}

type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// rustfsInfo is the binary under test, as given, and what `--version` says
// of it: its release, commit, and build profile.
type rustfsInfo struct {
	Path    string `json:"path"`
	Version string `json:"version"`
}

// repoRoot locates the torx repository through the go tool, so the harness
// builds the suite of the tree it is run from.
func repoRoot() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}} {{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("locate the module: %w; run from inside the torx repository", err)
	}
	path, dir, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if path != "github.com/dotnwat/torx" || dir == "" {
		return "", fmt.Errorf("the current module is %q, not github.com/dotnwat/torx; run from inside the torx repository", path)
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

func installFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return copyFile(src, dst, 0o755)
}

// linkFile puts src at dst with a hard link, which costs nothing however
// large the binary, or a copy where src is on another filesystem.
func linkFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Link(src, dst); err == nil || !errors.Is(err, syscall.EXDEV) {
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

// setEnv sets key in env to value, replacing it rather than appending a
// second entry: a Go program reads the first of two, and the suite is one.
func setEnv(env []string, key, value string) []string {
	out := slices.DeleteFunc(slices.Clone(env), func(kv string) bool { return strings.HasPrefix(kv, key+"=") })
	return append(out, key+"="+value)
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

func die(err error) int {
	fmt.Fprintln(os.Stderr, "harness:", err)
	return 1
}
