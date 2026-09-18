//go:build unix

package torx

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

// Main is the entry point a suite binary calls. With a "worker" first argument it
// runs a single job from its pipes and exits; otherwise it runs as the driver,
// discovering the matching jobs (positional arguments select by id), scheduling
// them onto a local pool, printing the summary, and exiting non-zero if any job
// failed.
//
// A suite's main is just:
//
//	func main() { torx.Main() }
func Main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		os.Exit(workerMain())
	}
	os.Exit(driverMain(os.Args[1:]))
}

// workerMain runs one job: it reads the assignment from stdin and writes its
// event stream to the inherited pipe on fd 3. SIGTERM cancels the job's context
// so teardown runs before the worker exits.
func workerMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	events := os.NewFile(3, "torx-events")
	if events == nil {
		fmt.Fprintln(os.Stderr, "torx worker: no event pipe on fd 3")
		return 1
	}
	defer events.Close()
	// The driver handed us this pipe via ExtraFiles, which clears close-on-exec;
	// restore it so the services this worker spawns do not inherit it. A service
	// that kept the write end open would stop the driver's reader from ever
	// seeing EOF, wedging the whole run after the worker exits.
	syscall.CloseOnExec(int(events.Fd()))

	if err := RunWorker(ctx, os.Stdin, events); err != nil {
		fmt.Fprintln(os.Stderr, "torx worker:", err)
		return 1
	}
	return 0
}

func driverMain(args []string) int {
	fs := flag.NewFlagSet("torx", flag.ContinueOnError)
	nodes := fs.Int("nodes", 0, "local nodes in the pool (0 sizes to the largest job)")
	poolFile := fs.String("pool", "", "build the pool from a node manifest (JSON) instead of local nodes")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	resultsPath := fs.String("results", "", "write newline-delimited JSON results to this file")
	resultsDir := fs.String("results-dir", "results", "write the per-run results tree under this directory (empty to disable)")
	runDir := fs.String("run-dir", "", "write results into exactly this pre-created directory (mutually exclusive with -results-dir)")
	paramsFile := fs.String("params", "", "JSON file of parameter overrides replacing the named jobs' compiled-in variants")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// -run-dir names the run directory itself; -results-dir a root to mint one
	// under. Passing both is a contradiction, caught here where "set" is
	// distinguishable from -results-dir's non-empty default.
	if *runDir != "" {
		resultsDirSet := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "results-dir" {
				resultsDirSet = true
			}
		})
		if resultsDirSet {
			fmt.Fprintln(os.Stderr, "torx: -run-dir and -results-dir are mutually exclusive")
			return 2
		}
		*resultsDir = ""
	}

	var overrides ParamsOverrides
	if *paramsFile != "" {
		var err error
		if overrides, err = LoadParamsOverrides(*paramsFile); err != nil {
			fmt.Fprintln(os.Stderr, "torx:", err)
			return 2
		}
	}

	// Positional arguments select jobs by id (regular expressions); with none,
	// every registered job runs.
	requests, err := DiscoverWith(overrides, fs.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx:", err)
		return 2
	}
	if len(requests) == 0 {
		fmt.Fprintln(os.Stderr, "torx: no jobs matched")
		return 1
	}

	pool, err := selectPool(*poolFile, *nodes, requests)
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx:", err)
		return 2
	}

	reporters := []Reporter{ConsoleReporter{W: os.Stdout}}
	if *resultsPath != "" {
		f, err := os.Create(*resultsPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "torx:", err)
			return 2
		}
		defer f.Close()
		reporters = append(reporters, NewJSONReporter(f))
	}

	// Ctrl-C (SIGINT) or a SIGTERM cancels the run so workers are torn down and
	// partial results are still written, instead of orphaning workers and their
	// services. Run's cancellation path SIGTERMs each worker's process group,
	// which the worker turns into teardown before it exits. A cancelled run is
	// reported as not Ok, so the exit below is non-zero.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res := Run(ctx, pool, SelfExecLauncher{}, requests,
		RunOptions{MaxParallel: *parallel, Reporters: reporters, ResultsDir: *resultsDir, RunDir: *runDir})
	if !res.Ok() {
		return 1
	}
	return 0
}

// selectPool builds the pool a run executes against: from a manifest file when
// poolFile is set, otherwise a pool of local nodes sized to nodes, or to the
// largest job when nodes is not positive.
func selectPool(poolFile string, nodes int, requests []JobRequest) (*Pool, error) {
	if poolFile != "" {
		m, err := LoadManifest(poolFile)
		if err != nil {
			return nil, err
		}
		return PoolFromManifest(m)
	}
	size := nodes
	if size <= 0 {
		size = maxDemand(requests)
	}
	return localPool(size), nil
}

// maxDemand returns the largest node demand among the requests, at least 1.
func maxDemand(requests []JobRequest) int {
	max := 1
	for _, req := range requests {
		if spec, err := sizeJob(req); err == nil && spec.Size() > max {
			max = spec.Size()
		}
	}
	return max
}

// localPool builds a pool of n local nodes sharing one port allocator.
func localPool(n int) *Pool {
	ports := NewPortAllocator("")
	base := filepath.Join(os.TempDir(), "torx")
	nodes := make([]*Node, n)
	for i := range nodes {
		name := fmt.Sprintf("node-%d", i)
		nodes[i] = NewNode(NodeConfig{
			Name:       name,
			Backend:    LocalBackend{},
			Descriptor: BackendDescriptor{Kind: "local"},
			Scratch:    MakeScratch(base, name),
			Ports:      ports,
		})
	}
	return NewPool(nodes)
}
