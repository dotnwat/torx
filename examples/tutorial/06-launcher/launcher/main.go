//go:build unix

// Command launcher runs the tutorial suite the way a project runs any torx
// suite: through a front end that owns what a suite cannot own itself.
//
// torx deliberately stops at running jobs against nodes it is handed. It
// does not build the suite, put the system under test on the nodes,
// provision the nodes, or decide where results go, because every project
// answers those differently. This launcher is one answer, kept to what a
// real one needs:
//
//   - build kvd and the suite from the current tree, for the platform the
//     nodes run on;
//   - mint the run directory, through torx so it is named and linked the
//     way the suite's own -results-dir mode would, and record the invocation
//     in it (argv, git identity, the binaries and exactly how the suite
//     was run, a verbatim copy of any -params file) before the suite
//     starts, so a results tree always says what produced it, even for a
//     run that was interrupted;
//   - prepare the nodes: for the local backend, that is putting kvd on the
//     PATH the suite's subprocesses inherit; for the docker backend, it is
//     building a node image with kvd and an sshd in it, starting three
//     containers from it, and generating the keys the suite logs in with;
//   - run the suite with -run-dir pointing at the run directory, exec'd in
//     place for the local backend and in a container beside the nodes for
//     the docker backend, so its exit status is the launcher's;
//   - and for docker, tear the containers down afterwards.
//
// The line "run directory: <path>" on standard output, printed before the
// suite starts, is the one line of the launcher's own output a caller may
// rely on; everything else goes to standard error.
//
// Usage, from anywhere inside the torx repository:
//
//	go run ./examples/tutorial/06-launcher/launcher [-backend local|docker] [-results-dir DIR] [-params FILE] [-nodes N] [-parallel N] [JOB_REGEX ...]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/dotnwat/torx"
)

// NEW in step 6: the launcher. Everything in this directory, and in
// ../docker, is new.

const (
	// suitePackage and kvdPackage are what gets built, relative to the
	// repository root; stepDir is this step, holding the docker files.
	suitePackage = "./examples/tutorial/06-launcher/suite"
	kvdPackage   = "./examples/tutorial/kvd"
	stepDir      = "examples/tutorial/06-launcher"
	// defaultResultsDir is the results root, relative to the repository root.
	defaultResultsDir = "results/tutorial"
	// invocationFile is the record the launcher writes into each run
	// directory, and paramsFile the archived name of a -params file.
	invocationFile = "invocation.json"
	paramsFile     = "params.json"
)

// invocation is the record of one launch, written to the run directory
// before the suite starts.
type invocation struct {
	Argv        []string `json:"argv"`         // the launcher's own command line
	Created     string   `json:"created"`      // RFC 3339, UTC
	Backend     string   `json:"backend"`      // where the nodes came from
	Target      target   `json:"target"`       // the platform the binaries were built for
	Git         gitInfo  `json:"git"`          // identity of the tree the suite was built from
	Suite       string   `json:"suite"`        // the suite binary, inside the run directory
	SuiteArgv   []string `json:"suite_argv"`   // exactly how the suite was run
	JobPatterns []string `json:"job_patterns"` // the job selection, as given
	Params      string   `json:"params,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("launcher", flag.ContinueOnError)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "usage: launcher [flags] [JOB_REGEX ...]\n\nLaunch the tutorial suite. Flags:\n")
		fs.PrintDefaults()
	}
	backend := fs.String("backend", "local", `where the nodes come from: "local" (subprocesses of this host) or "docker" (containers reached over ssh)`)
	resultsDir := fs.String("results-dir", "", "root to create the run directory under (default: "+defaultResultsDir+" in the repository)")
	paramsPath := fs.String("params", "", "parameter override file for the suite, archived in the run directory")
	nodes := fs.Int("nodes", 0, "local pool size (0 sizes it to the largest job); the docker pool is the three nodes in docker/compose.yaml")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	patterns := fs.Args()
	if *backend != "local" && *backend != "docker" {
		return die(fmt.Errorf("-backend must be local or docker, not %q", *backend))
	}

	// Inputs are checked before anything is built or created, so a bad
	// invocation leaves nothing behind.
	repo, err := repoRoot()
	if err != nil {
		return die(err)
	}
	if *paramsPath != "" {
		if _, err := os.Stat(*paramsPath); err != nil {
			return die(fmt.Errorf("params file: %w", err))
		}
	}
	// The run directory is spelled absolutely wherever it goes: onto the
	// exec'd suite's PATH, where Go refuses a relative entry, and into
	// compose's flags, which compose resolves from inside the run directory.
	// So a relative -results-dir is resolved against the launcher's
	// working directory before anything is derived from it.
	root := *resultsDir
	if root == "" {
		root = filepath.Join(repo, defaultResultsDir)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return die(fmt.Errorf("results directory: %w", err))
	}
	tgt, err := buildTarget(*backend)
	if err != nil {
		return die(err)
	}
	fmt.Fprintf(os.Stderr, "launcher: building for %s/%s\n", tgt.OS, tgt.Arch)

	// The binaries are built into a throwaway directory and moved into the
	// run directory once that exists, so a failed build mints no run directory
	// and a run directory always holds the exact binaries that ran in it.
	build, err := os.MkdirTemp("", "torx-tutorial-launcher-")
	if err != nil {
		return die(err)
	}
	defer func() { _ = os.RemoveAll(build) }()
	for _, b := range []struct{ pkg, name string }{{kvdPackage, "kvd"}, {suitePackage, "suite"}} {
		if err := goBuild(repo, b.pkg, filepath.Join(build, b.name), tgt); err != nil {
			return die(err)
		}
	}

	runDir, err := torx.MakeRunDir(root)
	if err != nil {
		return die(fmt.Errorf("run directory: %w", err))
	}
	bin := filepath.Join(runDir, "bin")
	for _, name := range []string{"kvd", "suite"} {
		if err := installFile(filepath.Join(build, name), filepath.Join(bin, name)); err != nil {
			return die(err)
		}
	}

	// The suite sees the run directory at mount: its own path for the local
	// backend, and the path it is bind-mounted at in the driver container for
	// docker. Every path handed to the suite is spelled from there.
	mount := runDir
	if *backend == "docker" {
		mount = containerRunDir
	}
	suite := filepath.Join(mount, "bin", "suite")
	argv := []string{suite, "-run-dir", mount, "-parallel", strconv.Itoa(*parallel)}
	if *backend == "docker" {
		argv = append(argv, "-pool", filepath.Join(mount, dockerDir, manifestFile))
	} else if *nodes > 0 {
		argv = append(argv, "-nodes", strconv.Itoa(*nodes))
	}
	inv := invocation{
		Argv:        os.Args,
		Created:     nowUTC(),
		Backend:     *backend,
		Target:      tgt,
		Git:         gitIdentity(repo),
		Suite:       filepath.Join(runDir, "bin", "suite"),
		JobPatterns: patterns,
	}
	if *paramsPath != "" {
		// The copy is what the suite reads, so it is exactly the input that ran.
		if err := copyFile(*paramsPath, filepath.Join(runDir, paramsFile), 0o644); err != nil {
			return die(fmt.Errorf("archive params: %w", err))
		}
		inv.Params = paramsFile
		argv = append(argv, "-params", filepath.Join(mount, paramsFile))
	}
	argv = append(argv, patterns...)
	inv.SuiteArgv = argv
	if err := writeJSON(filepath.Join(runDir, invocationFile), inv); err != nil {
		return die(err)
	}
	fmt.Printf("run directory: %s\n", runDir)

	if *backend == "docker" {
		_ = os.RemoveAll(build)
		return runDocker(context.Background(), filepath.Join(repo, stepDir), runDir, argv)
	}
	// The local pool's nodes are subprocesses of the suite, so putting kvd on
	// the suite's PATH puts it on every node's. Exec replaces this process,
	// so nothing deferred runs past this point: the build directory is
	// removed here, and the suite's exit status is ours.
	_ = os.RemoveAll(build)
	env := withPath(os.Environ(), bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := syscall.Exec(argv[0], argv, env); err != nil {
		return die(fmt.Errorf("exec %s: %w", argv[0], err))
	}
	return 0
}

// withPath returns env with PATH set to path. Exec passes the environment
// verbatim, so an appended second PATH would lose to the first; the existing
// one is replaced instead.
func withPath(env []string, path string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, "PATH=") {
			out = append(out, kv)
		}
	}
	return append(out, "PATH="+path)
}

func die(err error) int {
	fmt.Fprintln(os.Stderr, "launcher:", err)
	return 1
}
