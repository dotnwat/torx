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

	if err := RunWorker(ctx, os.Stdin, events); err != nil {
		fmt.Fprintln(os.Stderr, "torx worker:", err)
		return 1
	}
	return 0
}

func driverMain(args []string) int {
	fs := flag.NewFlagSet("torx", flag.ContinueOnError)
	nodes := fs.Int("nodes", 0, "local nodes in the pool (0 sizes to the largest job)")
	parallel := fs.Int("parallel", 1, "maximum concurrent jobs")
	resultsPath := fs.String("results", "", "write newline-delimited JSON results to this file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// Positional arguments select jobs by id (regular expressions); with none,
	// every registered job runs.
	requests, err := Discover(fs.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx:", err)
		return 2
	}
	if len(requests) == 0 {
		fmt.Fprintln(os.Stderr, "torx: no jobs matched")
		return 1
	}

	size := *nodes
	if size <= 0 {
		size = maxDemand(requests)
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

	res := Run(context.Background(), localPool(size), SelfExecLauncher{}, requests,
		RunOptions{MaxParallel: *parallel, Reporters: reporters})
	if !res.Ok() {
		return 1
	}
	return 0
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
			Name:    name,
			Backend: LocalBackend{},
			Scratch: MakeScratch(base, name),
			Ports:   ports,
		})
	}
	return NewPool(nodes)
}
