//go:build unix

// Command harness launches the rqlite suite the way a deployment of torx
// launches any suite: through a front end that owns what a suite cannot own
// itself.
//
// torx deliberately stops at running jobs against nodes it is handed. It does
// not build the suite, provision the system under test, or decide where
// results go, because every project answers those differently. This harness
// is one such answer, kept to the minimum a real one needs:
//
//   - build the suite binary from the current tree;
//   - check that rqlited, which the service resolves from PATH by name, is
//     installed and of the major version the suite is written against;
//   - mint the run directory, through torx so it is named and linked the way
//     the suite's own -results-dir mode would, and record the invocation in
//     it -- argv, git identity, the suite binary and its exact arguments, the
//     rqlited found, a verbatim copy of any -params file -- before the suite
//     starts, so a results tree always says what produced it, even for a run
//     that was interrupted;
//   - exec the suite with -run-dir pointing at that directory, so the suite's
//     exit status is the harness's.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the harness's own output a caller may rely
// on; everything else goes to standard error.
//
// The pool is local: nodes are subprocesses of this host, which is where a
// learning example belongs. A harness for remote nodes would provision them
// (containers, cloud instances) and pass the suite a -pool manifest instead;
// the suite would not change.
//
// Usage, from anywhere inside the torx repository:
//
//	go run ./examples/rqlite/harness [-results-dir DIR] [-params FILE] [-nodes N] [-parallel N] [JOB_REGEX ...]
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/dotnwat/torx"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("harness", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: harness [flags] [JOB_REGEX ...]\n\nLaunch the rqlite suite on a local pool. Flags:\n")
		fs.PrintDefaults()
	}
	resultsDir := fs.String("results-dir", "", "root to create the run directory under (default: "+defaultResultsDir+" in the repository)")
	paramsFile := fs.String("params", "", "parameter override file for the suite, archived in the run directory")
	nodes := fs.Int("nodes", 0, "local pool size (0 sizes it to the largest job)")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	netns := fs.Bool("netns", false, "give each node a network namespace of its own, so jobs can inject network faults (Linux)")
	seed := fs.String("seed", "", "run seed, to repeat a run's random choices (default: the suite draws one)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	patterns := fs.Args()

	// Inputs are checked before anything is built or created, so a bad
	// invocation leaves nothing behind.
	repo, err := repoRoot()
	if err != nil {
		return die(err)
	}
	if *paramsFile != "" {
		if _, err := os.Stat(*paramsFile); err != nil {
			return die(fmt.Errorf("params file: %w", err))
		}
	}
	rq, err := findRqlited()
	if err != nil {
		return die(err)
	}
	fmt.Fprintf(os.Stderr, "harness: %s %s\n", rq.Path, rq.Version)
	identity := gitIdentity(repo)

	// The suite is built into a throwaway directory and moved into the run
	// directory once that exists, so a failed build mints no run directory and
	// a run directory always holds the exact binary that ran in it.
	build, err := os.MkdirTemp("", "torx-rqlite-harness-")
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
	suite := filepath.Join(runDir, "bin", suiteBinary)
	if err := installFile(built, suite); err != nil {
		return die(err)
	}

	argv := []string{suite, "-run-dir", runDir, "-parallel", strconv.Itoa(*parallel)}
	if *nodes > 0 {
		argv = append(argv, "-nodes", strconv.Itoa(*nodes))
	}
	backend := "local"
	if *netns {
		argv = append(argv, "-netns")
		backend = "local-netns"
	}
	if *seed != "" {
		argv = append(argv, "-seed", *seed)
	}
	inv := invocation{
		Argv:        os.Args,
		Created:     nowUTC(),
		Backend:     backend,
		Git:         identity,
		Rqlited:     rq,
		Suite:       suite,
		JobPatterns: patterns,
	}
	if *paramsFile != "" {
		// The copy is what the suite reads, so it is exactly the input that ran.
		archived, err := archiveParams(*paramsFile, runDir)
		if err != nil {
			return die(err)
		}
		inv.Params = filepath.Base(archived)
		argv = append(argv, "-params", archived)
	}
	argv = append(argv, patterns...)
	inv.SuiteArgv = argv
	if err := writeJSON(filepath.Join(runDir, invocationFile), inv); err != nil {
		return die(err)
	}

	// Exec replaces this process, so nothing deferred runs past this point:
	// the build directory is removed here, and the suite's exit status is ours.
	_ = os.RemoveAll(build)
	fmt.Printf("run directory: %s\n", runDir)
	if err := syscall.Exec(suite, argv, os.Environ()); err != nil {
		return die(fmt.Errorf("exec %s: %w", suite, err))
	}
	return 0
}

func die(err error) int {
	fmt.Fprintln(os.Stderr, "harness:", err)
	return 1
}
