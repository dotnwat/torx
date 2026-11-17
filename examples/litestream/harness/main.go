//go:build unix

// Command harness launches the Litestream suite: it builds the suite and
// lsapp, builds Litestream from a checkout (or takes a binary already
// built), puts both in the run directory's bin/, which goes first on the
// suite's PATH so the run directory keeps exactly what ran, mints the run
// directory and records the invocation in it -- the Litestream under test
// and its version -- and execs the suite with -run-dir, so the suite's exit
// status is the harness's.
//
// -litestream names a Litestream checkout to build, or a binary; the default
// is the checkout README.md says how to clone. -race builds Litestream with
// the race detector.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the harness's own output a caller may rely
// on; everything else goes to standard error.
//
// Usage, from anywhere inside the example's module (examples/litestream):
//
//	go run ./harness [-litestream PATH] [-race] [-netns] [-cgroups] [-params FILE] [-seed N] [-nodes N] [-parallel N] [JOB_REGEX ...]
package main

import (
	"encoding/json"
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
	modulePath        = "github.com/dotnwat/torx/examples/litestream"
	suitePackage      = "./qa"
	appPackage        = "./app"
	defaultResultsDir = "results/litestream" // relative to the torx repository
	invocationFile    = "invocation.json"
	paramsFile        = "params.json"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("harness", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: harness [flags] [JOB_REGEX ...]\n\nLaunch the Litestream suite on a local pool. Flags:\n")
		fs.PrintDefaults()
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return die(err)
	}
	under := fs.String("litestream", filepath.Join(cache, "torx-litestream", "src", "litestream"), "a Litestream checkout to build, or a litestream binary")
	race := fs.Bool("race", false, "build Litestream with the race detector")
	resultsDir := fs.String("results-dir", "", "root to create the run directory under (default: "+defaultResultsDir+" in the repository)")
	scratch := fs.String("scratch", "", "directory for the nodes' scratch (default: scratch/ in the run directory)")
	paramsFlag := fs.String("params", "", "parameter override file for the suite, archived in the run directory")
	nodes := fs.Int("nodes", 0, "local pool size (0 sizes it to the largest job)")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	netns := fs.Bool("netns", false, "give each node a network of its own")
	cgroups := fs.Bool("cgroups", false, "give each node a cgroup of its own")
	seed := fs.String("seed", "", "the run seed, to repeat a run")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	patterns := fs.Args()

	module, err := moduleRoot()
	if err != nil {
		return die(err)
	}
	repo := filepath.Dir(filepath.Dir(module))
	if *paramsFlag != "" {
		if _, err := os.Stat(*paramsFlag); err != nil {
			return die(fmt.Errorf("params file: %w", err))
		}
	}
	if *under, err = filepath.Abs(*under); err != nil {
		return die(err)
	}

	build, err := os.MkdirTemp("", "torx-litestream-harness-")
	if err != nil {
		return die(err)
	}
	defer func() { _ = os.RemoveAll(build) }()
	info := litestreamInfo{Path: *under, Race: *race}
	binary := *under
	if fi, err := os.Stat(*under); err != nil {
		return die(fmt.Errorf("-litestream: %w; README.md says how to clone Litestream", err))
	} else if fi.IsDir() {
		info.Source = *under
		info.Commit = gitIdentity(*under)
		binary = filepath.Join(build, "litestream")
		var flags []string
		if *race {
			flags = append(flags, "-race")
		}
		if err := goBuild(*under, binary, "./cmd/litestream", flags...); err != nil {
			return die(err)
		}
	}
	if info.Version, err = version(binary); err != nil {
		return die(err)
	}
	fmt.Fprintf(os.Stderr, "harness: litestream %s (%s)\n", info.Version, *under)

	built := map[string]string{"qa": suitePackage, "lsapp": appPackage}
	for name, pkg := range built {
		if err := goBuild(module, filepath.Join(build, name), pkg); err != nil {
			return die(err)
		}
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
	for _, name := range []string{"qa", "lsapp"} {
		if err := copyFile(filepath.Join(build, name), filepath.Join(bin, name), 0o755); err != nil {
			return die(err)
		}
	}
	if err := copyFile(binary, filepath.Join(bin, "litestream"), 0o755); err != nil {
		return die(err)
	}
	suite := filepath.Join(bin, "qa")
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
		Litestream: info, Suite: suite, Scratch: *scratch, JobPatterns: patterns,
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

// version is what the binary says its version is.
func version(binary string) (string, error) {
	out, err := exec.Command(binary, "version").Output()
	if err != nil {
		return "", fmt.Errorf("%s version: %w", binary, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// invocation is the record of one launch, written to the run directory before
// the suite starts.
type invocation struct {
	Argv        []string       `json:"argv"`
	Created     string         `json:"created"`
	Git         gitInfo        `json:"git"`
	Litestream  litestreamInfo `json:"litestream"`
	Suite       string         `json:"suite"`
	SuiteArgv   []string       `json:"suite_argv"`
	Scratch     string         `json:"scratch"`
	JobPatterns []string       `json:"job_patterns"`
	Params      string         `json:"params,omitempty"`
}

type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// litestreamInfo is the Litestream under test.
type litestreamInfo struct {
	Path    string  `json:"path"`
	Source  string  `json:"source,omitempty"`
	Commit  gitInfo `json:"commit,omitzero"`
	Race    bool    `json:"race,omitempty"`
	Version string  `json:"version"`
}

// moduleRoot locates the example's module through the go tool, so the
// harness builds the suite of the tree it is run from.
func moduleRoot() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}} {{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("locate the module: %w; run from inside examples/litestream", err)
	}
	path, dir, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if path != modulePath || dir == "" {
		return "", fmt.Errorf("the current module is %q, not %s; run from inside examples/litestream", path, modulePath)
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

func goBuild(dir, out, pkg string, flags ...string) error {
	args := append(append([]string{"build"}, flags...), "-o", out, pkg)
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build %s in %s: %w", pkg, dir, err)
	}
	return nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
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

func die(err error) int {
	fmt.Fprintln(os.Stderr, "harness:", err)
	return 1
}
