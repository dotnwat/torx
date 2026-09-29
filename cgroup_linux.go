//go:build linux

// Control groups for the local pool: nodes whose resources a job can take
// away.
//
// Under -cgroups each local node runs every command in a cgroup (v2) of its
// own, so a job can freeze a node outright, starve it of CPU, press on its
// memory, or slow its disk -- the gray failures of a machine that is up but
// barely working -- with the resfault package. No root is needed: the
// driver re-executes itself in a systemd user scope with Delegate=yes, a
// cgroup subtree it owns, and arranges it as
//
//	<scope>/driver    the driver, its workers, and their commands
//	<scope>/node-<i>  node i's commands
//
// with the cpu, io, memory, and pids controllers enabled for the children,
// as the scope's own delegation allows. A command enters its node's cgroup as
// it is created (clone3's CLONE_INTO_CGROUP), so no process of a node ever
// runs outside it. The scope, and every cgroup in it, is removed by systemd
// once the run's last process exits.

package torx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// scopeEnv marks a driver already re-executed into its scope.
	scopeEnv = "TORX_CGROUP_SCOPE"
	// cgroupRootEnv holds the scope's path once the driver has arranged
	// it, so a driver re-executed again -- into the -netns lab -- finds it
	// rather than arranging it twice.
	cgroupRootEnv = "TORX_CGROUP_ROOT"
	cgroupFS      = "/sys/fs/cgroup"
)

// cgroupControllers are the controllers enabled for the nodes' cgroups,
// each only if the scope has it.
var cgroupControllers = []string{"cpu", "io", "memory", "pids"}

func inScope() bool { return os.Getenv(scopeEnv) != "" }

// enterScope re-executes this program, with the same arguments, in a
// systemd user scope that delegates its cgroup to it, and returns the exit
// status it should exit with: the re-executed driver's. systemd-run --scope
// runs the program as its own process, in the foreground, so signals reach
// it as they would the driver itself.
func enterScope() int {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx: -cgroups:", err)
		return 2
	}
	args := append([]string{"--user", "--scope", "--quiet", "--collect", "-p", "Delegate=yes", "--", self}, os.Args[1:]...)
	cmd := exec.Command("systemd-run", args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	// A scope's process is systemd-run's own child, so it inherits this
	// environment.
	cmd.Env = append(os.Environ(), scopeEnv+"=1")
	// systemd-run execs the program in its own place, which keeps this: should
	// this process die without passing a signal on, the run is told to stop
	// as by an interrupt, rather than carrying on orphaned.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "torx: -cgroups: cannot start a systemd user scope: %v\n", err)
		return 2
	}
	go func() {
		for sig := range sigs {
			_ = cmd.Process.Signal(sig)
		}
	}()
	err = cmd.Wait()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx: -cgroups:", err)
		return 2
	}
	return 0
}

// ownCgroup is the path of the cgroup this process is in.
func ownCgroup() (string, error) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(b)) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "0::"); ok {
			return filepath.Join(cgroupFS, rest), nil
		}
	}
	return "", errors.New("not in a cgroup v2 hierarchy")
}

// prepareCgroupRoot arranges the scope this driver runs in for nodes'
// cgroups: every process in it moves to <scope>/driver, since a cgroup that
// hands controllers to its children may hold no processes itself, and the
// controllers are enabled for the children. It returns the scope's path.
func prepareCgroupRoot() (string, error) {
	if root := os.Getenv(cgroupRootEnv); root != "" {
		return root, nil
	}
	root, err := ownCgroup()
	if err != nil {
		return "", fmt.Errorf("cgroups: %w", err)
	}
	driver := filepath.Join(root, "driver")
	if err := os.Mkdir(driver, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("cgroups: %w (the driver's cgroup must be delegated to it; run under -cgroups)", err)
	}
	procs, err := os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil {
		return "", fmt.Errorf("cgroups: %w", err)
	}
	for pid := range strings.FieldsSeq(string(procs)) {
		if err := os.WriteFile(filepath.Join(driver, "cgroup.procs"), []byte(pid), 0); err != nil && !errors.Is(err, syscall.ESRCH) {
			return "", fmt.Errorf("cgroups: moving %s into %s: %w", pid, driver, err)
		}
	}
	have, err := os.ReadFile(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return "", fmt.Errorf("cgroups: %w", err)
	}
	available := strings.Fields(string(have))
	var enable []string
	for _, c := range cgroupControllers {
		for _, a := range available {
			if a == c {
				enable = append(enable, "+"+c)
			}
		}
	}
	if len(enable) > 0 {
		if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte(strings.Join(enable, " ")), 0); err != nil {
			return "", fmt.Errorf("cgroups: enabling %s: %w", strings.Join(enable, " "), err)
		}
	}
	if err := os.Setenv(cgroupRootEnv, root); err != nil {
		return "", err
	}
	return root, nil
}

// nodeCgroup creates node's cgroup under root and returns its path.
func nodeCgroup(root, node string) (string, error) {
	dir := filepath.Join(root, node)
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("cgroups: %w", err)
	}
	return dir, nil
}

// resetCgroup lifts whatever a job left on the cgroup at path -- a freeze, a
// cap on CPU, memory, or a device's bandwidth -- so the next job to use the
// node finds it as the run made it. A limit whose controller the cgroup does
// not have is not there to lift.
func resetCgroup(path string) error {
	set := func(file, value string) error {
		err := os.WriteFile(filepath.Join(path, file), []byte(value), 0)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	errs := []error{
		set("cgroup.freeze", "0"),
		set("cpu.max", "max"),
		set("memory.high", "max"),
		set("memory.max", "max"),
	}
	limits, err := os.ReadFile(filepath.Join(path, "io.max"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	for line := range strings.Lines(string(limits)) {
		if dev, _, ok := strings.Cut(strings.TrimSpace(line), " "); ok {
			errs = append(errs, set("io.max", dev+" rbps=max wbps=max riops=max wiops=max"))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("cgroups: resetting %s: %w", path, err)
	}
	return nil
}

// cgroupGate orders the commands this process starts into a cgroup against
// its freezes: a start waits while the cgroup is frozen, and a freeze waits
// for the starts in flight to exec.
type cgroupGate struct {
	mu       sync.Mutex
	idle     *sync.Cond // broadcast when starting drops to zero
	starting int
	frozen   bool
	thawed   chan struct{} // closed by the thaw that ends the current freeze
}

var gates sync.Map // cgroup path -> *cgroupGate

func gateFor(path string) *cgroupGate {
	if g, ok := gates.Load(path); ok {
		return g.(*cgroupGate)
	}
	g := &cgroupGate{}
	g.idle = sync.NewCond(&g.mu)
	actual, _ := gates.LoadOrStore(path, g)
	return actual.(*cgroupGate)
}

// enter waits until the cgroup is not frozen, or ctx is done, and counts a
// start in; the function it returns counts the start out.
func (g *cgroupGate) enter(ctx context.Context) (func(), error) {
	g.mu.Lock()
	for g.frozen {
		thawed := g.thawed
		g.mu.Unlock()
		select {
		case <-thawed:
		case <-ctx.Done():
			return nil, fmt.Errorf("cgroups: waiting for a frozen node to thaw: %w", ctx.Err())
		}
		g.mu.Lock()
	}
	g.starting++
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		if g.starting--; g.starting == 0 {
			g.idle.Broadcast()
		}
		g.mu.Unlock()
	}, nil
}

// freeze freezes the cgroup at path once no start is in flight, and waits
// until the kernel reports it frozen.
func (g *cgroupGate) freeze(ctx context.Context, path string) error {
	g.mu.Lock()
	if !g.frozen {
		g.frozen = true
		g.thawed = make(chan struct{})
	}
	for g.starting > 0 {
		g.idle.Wait()
	}
	err := os.WriteFile(filepath.Join(path, "cgroup.freeze"), []byte("1"), 0)
	g.mu.Unlock()
	if err != nil {
		_ = g.thaw(path)
		return fmt.Errorf("cgroups: freezing %s: %w", path, err)
	}
	for {
		events, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
		if err != nil {
			return fmt.Errorf("cgroups: %w", err)
		}
		if strings.Contains(string(events), "frozen 1") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cgroups: waiting for %s to freeze: %w", path, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// thaw thaws the cgroup at path and lets the starts waiting on it go.
func (g *cgroupGate) thaw(path string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := os.WriteFile(filepath.Join(path, "cgroup.freeze"), []byte("0"), 0); err != nil {
		return fmt.Errorf("cgroups: thawing %s: %w", path, err)
	}
	if g.frozen {
		g.frozen = false
		close(g.thawed)
	}
	return nil
}

// intoCgroup has a command started with attr begin life in the cgroup at
// path. It returns a function to call once the command has started, which
// releases what it held open.
func intoCgroup(attr *syscall.SysProcAttr, path string) (func(), error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("cgroups: %w", err)
	}
	attr.UseCgroupFD = true
	attr.CgroupFD = fd
	return func() { _ = syscall.Close(fd) }, nil
}
