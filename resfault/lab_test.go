//go:build linux

package resfault

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

// The lab test runs this test binary as a torx suite under -cgroups, whose
// local nodes each run in a cgroup of their own. TestMain sends the driver
// and worker invocations to torx.

const (
	// requiredEnv turns the lab test's skip, on a host that cannot give
	// nodes cgroups, into a failure.
	requiredEnv = "RESFAULT_LAB_REQUIRED"
	suiteEnv    = "RESFAULT_TEST_SUITE"
)

func TestMain(m *testing.M) {
	if (len(os.Args) > 1 && os.Args[1] == "worker") || os.Getenv(suiteEnv) != "" {
		torx.Main()
	}
	os.Exit(m.Run())
}

func init() {
	torx.Register("resfault.lab", func() torx.Job { return &labJob{} })
}

// nothing is a service that asks for one node and runs nothing on it.
type nothing struct{ *torx.ServiceBase }

func newNothing() *nothing {
	s := &nothing{}
	s.ServiceBase = torx.NewServiceBase("nothing", torx.Homogeneous(1, torx.NodeSpec{}), nil)
	return s
}

func (*nothing) Start(context.Context) error { return nil }
func (*nothing) Stop(context.Context) error  { return nil }
func (*nothing) Clean(context.Context) error { return nil }
func (*nothing) Wait(context.Context) error  { return nil }

// labJob freezes a node and thaws it, caps its CPU, and throttles its disk,
// checking what a process on the node sees of each, and resets it.
type labJob struct {
	torx.JobBase
	s *nothing
}

func (j *labJob) Declare(jc *torx.JobContext) {
	j.s = newNothing()
	jc.Register(j.s)
}

func (j *labJob) Run(ctx context.Context, jc *torx.JobContext) error {
	n := j.s.Nodes()[0]
	root := n.Scratch().Root
	if err := n.Mkdir(ctx, root); err != nil {
		return err
	}
	defer func() { _ = n.Rm(context.WithoutCancel(ctx), root) }()
	if err := Check(n); err != nil {
		return err
	}
	cg, _ := cgroupOf(n)
	// Every command on the node runs in its cgroup.
	res, err := n.Exec(ctx, torx.Command("cat", "/proc/self/cgroup"))
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(strings.TrimPrefix(string(res.Stdout), "0::")); !strings.HasSuffix(cg, got) {
		return fmt.Errorf("a command on the node ran in %s, want %s", got, cg)
	}
	if err := freezeThaw(ctx, n, root); err != nil {
		return err
	}
	if err := cpu(ctx, n, cg); err != nil {
		return err
	}
	if err := disk(ctx, jc, n, root); err != nil {
		return err
	}
	if err := LimitMemory(n, 64<<20); err != nil {
		return err
	}
	if err := Reset(ctx, n); err != nil {
		return err
	}
	for file, want := range map[string]string{"cgroup.freeze": "0", "cpu.max": "max 100000", "memory.high": "max"} {
		b, err := os.ReadFile(filepath.Join(cg, file))
		if err != nil {
			return err
		}
		if got := strings.TrimSpace(string(b)); got != want {
			return fmt.Errorf("after Reset, %s is %q, want %q", file, got, want)
		}
	}
	return nil
}

// freezeThaw runs a counter on the node, freezes the node, and checks the
// counter stands still until the thaw.
func freezeThaw(ctx context.Context, n *torx.Node, root string) error {
	counter := filepath.Join(root, "counter")
	p, err := n.Stream(ctx, torx.Command("sh", "-c", `i=0; while :; do i=$((i+1)); echo $i > "$1"; sleep 0.01; done`, "sh", counter))
	if err != nil {
		return err
	}
	defer p.Close()
	read := func() (string, error) {
		b, err := os.ReadFile(counter)
		return strings.TrimSpace(string(b)), err
	}
	moving := func(want bool, state string) error {
		a, err := read()
		if err != nil {
			return err
		}
		time.Sleep(300 * time.Millisecond)
		b, err := read()
		if err != nil {
			return err
		}
		if (a != b) != want {
			return fmt.Errorf("on a %s node the counter went from %q to %q in 300ms", state, a, b)
		}
		return nil
	}
	if err := torx.WaitUntil(ctx, func(context.Context) (bool, error) {
		_, err := read()
		return err == nil, nil
	}, 10*time.Millisecond); err != nil {
		return err
	}
	if err := moving(true, "running"); err != nil {
		return err
	}
	if err := Freeze(ctx, n); err != nil {
		return err
	}
	if err := moving(false, "frozen"); err != nil {
		return err
	}
	// A command started on the frozen node waits to start, with the rest of
	// the process running on -- a collection included, which stops every
	// thread -- until the thaw, or until its context ends.
	late := make(chan error, 1)
	go func() {
		_, err := n.Exec(ctx, torx.Command("true"))
		late <- err
	}()
	for range 3 {
		runtime.GC()
	}
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = n.Exec(short, torx.Command("true"))
	cancel()
	if err == nil {
		return errors.New("a command with a short deadline ran on a frozen node")
	}
	select {
	case err := <-late:
		return errors.Join(errors.New("a command ran on a frozen node before the thaw"), err)
	default:
	}
	if err := Thaw(ctx, n); err != nil {
		return err
	}
	select {
	case err := <-late:
		if err != nil {
			return fmt.Errorf("the command waiting for the thaw: %w", err)
		}
	case <-time.After(10 * time.Second):
		return errors.New("the command waiting for the thaw did not run after it")
	}
	return moving(true, "thawed")
}

// cpu caps the node at a twentieth of a CPU and checks a busy loop gets
// about that.
func cpu(ctx context.Context, n *torx.Node, cg string) error {
	if err := LimitCPU(n, 0.05); err != nil {
		return err
	}
	before, err := usage(cg)
	if err != nil {
		return err
	}
	if _, err := n.Exec(ctx, torx.Command("timeout", "1", "sh", "-c", "while :; do :; done")); err != nil {
		return err
	}
	after, err := usage(cg)
	if err != nil {
		return err
	}
	if used := after - before; used > 300*time.Millisecond {
		return fmt.Errorf("a busy second on a node capped at 5%% of a CPU used %v of it", used)
	}
	return LimitCPU(n, 0)
}

// usage is the CPU time the cgroup's processes have used.
func usage(cg string) (time.Duration, error) {
	b, err := os.ReadFile(filepath.Join(cg, "cpu.stat"))
	if err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "usage_usec "); ok {
			us, err := strconv.ParseInt(v, 10, 64)
			return time.Duration(us) * time.Microsecond, err
		}
	}
	return 0, errors.New("cpu.stat has no usage_usec")
}

// disk throttles the node's writes to the disk under root and checks a
// direct write of 4MiB at 2MiB/s takes most of two seconds. A root on no
// block device, a tmpfs, cannot be throttled; that is logged and skipped.
func disk(ctx context.Context, jc *torx.JobContext, n *torx.Node, root string) error {
	// The data is random, since a filesystem that compresses (btrfs mounted
	// with compress) would write zeros to the device as next to nothing.
	src := filepath.Join(root, "random")
	res, err := n.Exec(ctx, torx.Command("dd", "if=/dev/urandom", "of="+src, "bs=256k", "count=16"))
	if err != nil {
		return fmt.Errorf("dd: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("dd: exit %d: %s", res.ExitCode, res.Stderr)
	}
	err = ThrottleIO(n, root, IOLimit{WriteBPS: 2 << 20})
	if err != nil && strings.Contains(err.Error(), "not a block device") {
		jc.Log("info", "no disk throttle: "+err.Error())
		return nil
	}
	if err != nil {
		return err
	}
	start := time.Now()
	res, err = n.Exec(ctx, torx.Command("dd", "if="+src, "of="+filepath.Join(root, "direct"), "bs=256k", "count=16", "oflag=direct"))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("dd: %s", res.Stderr)
	}
	if took := time.Since(start); took < 1200*time.Millisecond {
		return fmt.Errorf("a direct 4MiB write at 2MiB/s took %v", took)
	}
	return ThrottleIO(n, root, IOLimit{})
}

// labUsable says why this host cannot give nodes cgroups, or nil if it can.
func labUsable() error {
	for _, tool := range []string{"systemd-run", "sh", "timeout", "dd", "cat"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("%s is not installed", tool)
		}
	}
	out, err := exec.Command("systemd-run", "--user", "--scope", "--quiet", "--collect", "-p", "Delegate=yes", "--",
		"sh", "-c", `cat "/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/cgroup.controllers"`).CombinedOutput()
	if err != nil {
		return fmt.Errorf("no systemd user scope with a delegated cgroup here (%w: %s)", err, strings.TrimSpace(string(out)))
	}
	for _, want := range []string{"cpu", "io", "memory"} {
		if !strings.Contains(string(out), want) {
			return fmt.Errorf("a delegated user scope here has no %s controller (it has %q)", want, strings.TrimSpace(string(out)))
		}
	}
	return nil
}

// TestFaultsInALab runs the lab job through the real driver and worker under
// -cgroups.
func TestFaultsInALab(t *testing.T) {
	if err := labUsable(); err != nil {
		if os.Getenv(requiredEnv) != "" {
			t.Fatalf("%v (%s is set)", err, requiredEnv)
		}
		t.Skip(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, args := range [][]string{{"-cgroups"}, {"-cgroups", "-netns"}} {
		cmd := exec.CommandContext(ctx, self, append(args, "-results-dir", "", "resfault.lab")...)
		cmd.Env = append(os.Environ(), suiteEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("suite under %v failed (%v):\n%s", args, err, out)
		}
		if !strings.Contains(string(out), "1 passed") {
			t.Errorf("suite under %v: output does not report the job passing:\n%s", args, out)
		}
	}
}
