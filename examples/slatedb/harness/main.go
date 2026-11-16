//go:build unix

// Command harness launches the SlateDB suite: it builds the suite and
// slatedb-node, the program that runs SlateDB in each process of the
// database, puts slatedb-node where the suite's nodes resolve it, mints the
// run directory and records the invocation in it, and execs the suite with
// -run-dir, so the suite's exit status is the harness's.
//
// SlateDB is a library, so the build under test is the slatedb-node built
// against it. -slatedb names a SlateDB checkout to build against -- main,
// or a branch with a fix; without it the node builds against the release
// its Cargo.toml names. -node names a slatedb-node already built instead.
// Either way the binary is linked into the run directory's bin/, which
// goes first on the suite's PATH, so the run directory keeps exactly what
// ran.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the harness's own output a caller may rely
// on; everything else goes to standard error.
//
// Usage, from anywhere inside the torx repository:
//
//	go run ./examples/slatedb/harness [-slatedb DIR | -node PATH] [-netns] [-cgroups] [-params FILE] [-seed N] [-nodes N] [-parallel N] [JOB_REGEX ...]
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
	suitePackage      = "./examples/slatedb/qa"
	suiteBinary       = "qa"
	nodeManifest      = "examples/slatedb/node/Cargo.toml"
	defaultResultsDir = "results/slatedb" // relative to the torx repository
	invocationFile    = "invocation.json"
	paramsFile        = "params.json"
	binaryName        = "slatedb-node"
	slatedbGit        = "https://github.com/slatedb/slatedb"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("harness", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: harness [flags] [JOB_REGEX ...]\n\nLaunch the SlateDB suite on a local pool. Flags:\n")
		fs.PrintDefaults()
	}
	src := fs.String("slatedb", "", "a SlateDB checkout to build slatedb-node against (default: the release node/Cargo.toml names)")
	binary := fs.String("node", "", "a slatedb-node already built, instead of building one")
	targetDir := fs.String("target-dir", "", "cargo's target directory (default: $XDG_CACHE_HOME/torx-slatedb/target)")
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
	if *src != "" && *binary != "" {
		return die(errors.New("-slatedb and -node exclude each other"))
	}

	repo, err := repoRoot()
	if err != nil {
		return die(err)
	}
	if *paramsFlag != "" {
		if _, err := os.Stat(*paramsFlag); err != nil {
			return die(fmt.Errorf("params file: %w", err))
		}
	}
	under := nodeInfo{Source: "release in " + nodeManifest}
	if *binary == "" {
		if *targetDir == "" {
			cache, err := os.UserCacheDir()
			if err != nil {
				return die(err)
			}
			*targetDir = filepath.Join(cache, "torx-slatedb", "target")
		}
		if *src != "" {
			if *src, err = filepath.Abs(*src); err != nil {
				return die(err)
			}
			under.Source = *src
			under.Commit = gitIdentity(*src)
		}
		if *binary, err = cargoBuild(repo, *src, *targetDir); err != nil {
			return die(err)
		}
	} else {
		under.Source = "prebuilt"
	}
	if *binary, err = filepath.Abs(*binary); err != nil {
		return die(err)
	}
	under.Path = *binary
	fmt.Fprintf(os.Stderr, "harness: slatedb-node %s (%s)\n", *binary, under.Source)

	build, err := os.MkdirTemp("", "torx-slatedb-harness-")
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
	if err := copyFile(*binary, filepath.Join(bin, binaryName), 0o755); err != nil {
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
		Node: under, Suite: suite, Scratch: *scratch, JobPatterns: patterns,
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

// cargoBuild builds slatedb-node, against the checkout src if set, and
// returns the binary's path.
func cargoBuild(repo, src, targetDir string) (string, error) {
	cargo, err := exec.LookPath("cargo")
	if err != nil {
		home, _ := os.UserHomeDir()
		cargo = filepath.Join(home, ".cargo", "bin", "cargo")
		if _, serr := os.Stat(cargo); serr != nil {
			return "", fmt.Errorf("cargo is not on PATH; install Rust (https://rustup.rs) or name a build with -node")
		}
	}
	args := []string{"build", "--release", "--manifest-path", filepath.Join(repo, nodeManifest)}
	// SlateDB's Db::begin and Db::snapshot became sync after 0.17.0; the
	// release node builds against has the async ones.
	asyncBegin := true
	if src != "" {
		b, err := os.ReadFile(filepath.Join(src, "slatedb", "src", "db.rs"))
		if err != nil {
			return "", fmt.Errorf("-slatedb %s: %w", src, err)
		}
		asyncBegin = strings.Contains(string(b), "pub async fn begin(")
	}
	if asyncBegin {
		args = append(args, "--features", "async-api")
	}
	if src != "" {
		for _, crate := range []string{"slatedb", "slatedb-common"} {
			args = append(args, "--config", fmt.Sprintf("patch.%q.%s.path=%q", slatedbGit, crate, filepath.Join(src, crate)))
		}
	}
	cmd := exec.Command(cargo, args...)
	cmd.Env = setEnv(os.Environ(), "CARGO_TARGET_DIR", targetDir)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	fmt.Fprintf(os.Stderr, "harness: %s\n", strings.Join(cmd.Args, " "))
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("cargo build: %w", err)
	}
	return filepath.Join(targetDir, "release", binaryName), nil
}

// invocation is the record of one launch, written to the run directory before
// the suite starts.
type invocation struct {
	Argv        []string `json:"argv"`
	Created     string   `json:"created"`
	Git         gitInfo  `json:"git"`
	Node        nodeInfo `json:"slatedb_node"`
	Suite       string   `json:"suite"`
	SuiteArgv   []string `json:"suite_argv"`
	Scratch     string   `json:"scratch"`
	JobPatterns []string `json:"job_patterns"`
	Params      string   `json:"params,omitempty"`
}

type gitInfo struct {
	SHA   string `json:"sha,omitempty"`
	Dirty bool   `json:"dirty"`
}

// nodeInfo is the slatedb-node under test: where it came from, and the
// commit of the SlateDB checkout it was built against, if one was named.
type nodeInfo struct {
	Path   string  `json:"path"`
	Source string  `json:"source"`
	Commit gitInfo `json:"commit"`
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
