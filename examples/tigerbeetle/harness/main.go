//go:build unix

// Command harness launches the TigerBeetle suite: it builds the suite, puts
// the tigerbeetle binary under test where the suite's nodes resolve it, mints
// the run directory and records the invocation in it, and execs the suite
// with -run-dir, so the suite's exit status is the harness's.
//
// Which build is under test is the harness's to say, since hunting for bugs
// means running more than one: a release, the release's debug build (extra
// assertions, stack traces), or a build of main. -tigerbeetle names the
// binary; it is copied into the run directory's bin/, which goes first on
// the suite's PATH, so the run directory keeps exactly what ran. Without it,
// the tigerbeetle on PATH runs.
//
// The nodes' scratch -- a data file of over a gigabyte per replica --
// defaults to the run directory's scratch/, emptied as each job ends, so
// that concurrent runs never share a node's directory and a system whose /tmp
// is a small tmpfs is not filled. -scratch moves it.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the harness's own output a caller may rely
// on; everything else goes to standard error.
//
// Usage, from examples/tigerbeetle:
//
//	go run ./harness [-tigerbeetle PATH] [-netns] [-cgroups] [-params FILE] [-seed N] [-nodes N] [-parallel N] [JOB_REGEX ...]
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/dotnwat/torx"
)

const (
	modulePath        = "github.com/dotnwat/torx/examples/tigerbeetle"
	suitePackage      = "./qa"
	suiteBinary       = "qa"
	defaultResultsDir = "results/tigerbeetle" // relative to the torx repository
	invocationFile    = "invocation.json"
	paramsFile        = "params.json"
	binaryName        = "tigerbeetle"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	fs := flag.NewFlagSet("harness", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: harness [flags] [JOB_REGEX ...]\n\nLaunch the TigerBeetle suite on a local pool. Flags:\n")
		fs.PrintDefaults()
	}
	binary := fs.String("tigerbeetle", "", "the tigerbeetle binary under test (default: the one on PATH)")
	resultsDir := fs.String("results-dir", "", "root to create the run directory under (default: "+defaultResultsDir+" in the repository)")
	scratch := fs.String("scratch", "", "directory for the nodes' scratch, data files included (default: scratch/ in the run directory)")
	paramsFlag := fs.String("params", "", "parameter override file for the suite, archived in the run directory")
	nodes := fs.Int("nodes", 0, "local pool size (0 sizes it to the largest job)")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	netns := fs.Bool("netns", false, "give each node a network of its own, for the network faults, and a limited tmpfs data directory, for disk-full")
	cgroups := fs.Bool("cgroups", false, "give each node a cgroup of its own, for the resource faults")
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
	if *binary == "" {
		if *binary, err = exec.LookPath(binaryName); err != nil {
			return die(fmt.Errorf("%s is not on PATH; install a release from https://github.com/tigerbeetle/tigerbeetle/releases or name a build with -tigerbeetle", binaryName))
		}
	}
	version, err := exec.Command(*binary, "version", "--verbose").Output()
	if err != nil {
		return die(fmt.Errorf("%s version: %w", *binary, err))
	}
	fmt.Fprintf(os.Stderr, "harness: %s: %s\n", *binary, firstLine(string(version)))

	build, err := os.MkdirTemp("", "torx-tigerbeetle-harness-")
	if err != nil {
		return die(err)
	}
	defer func() { _ = os.RemoveAll(build) }()
	built := filepath.Join(build, suiteBinary)
	if err := goBuild(module, built); err != nil {
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
	if err := installFile(*binary, filepath.Join(bin, binaryName)); err != nil {
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
		Argv: os.Args, Created: nowUTC(), Git: gitIdentity(repo),
		TigerBeetle: tigerbeetleInfo{Path: *binary, Version: strings.TrimSpace(string(version))},
		Suite:       suite, Scratch: *scratch, JobPatterns: patterns,
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

	env := append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+*scratch)
	_ = os.RemoveAll(build)
	fmt.Printf("run directory: %s\n", runDir)
	if err := syscall.Exec(suite, argv, env); err != nil {
		return die(fmt.Errorf("exec %s: %w", suite, err))
	}
	return 0
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

func die(err error) int {
	fmt.Fprintln(os.Stderr, "harness:", err)
	return 1
}
