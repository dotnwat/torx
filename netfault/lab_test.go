//go:build linux

package netfault

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

// The end-to-end test runs this test binary as a torx suite under -netns: the
// driver re-executes it into a lab, and its workers run the job below, whose
// nodes run the binary again as a TCP server and as a dialer. TestMain sends
// each of those invocations where it belongs.

const (
	// requiredEnv turns the lab test's skip, on a host that cannot build a
	// lab, into a failure: CI sets it where it has made sure the host can.
	requiredEnv = "NETFAULT_LAB_REQUIRED"
	suiteEnv    = "NETFAULT_TEST_SUITE"
	serveArg    = "netfault-test-serve"
	dialArg     = "netfault-test-dial"
	probePort   = "7000"
)

func TestMain(m *testing.M) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == serveArg:
		serve()
	case len(os.Args) > 2 && os.Args[1] == dialArg:
		os.Exit(dial(os.Args[2]))
	case len(os.Args) > 1 && os.Args[1] == "worker", os.Getenv(suiteEnv) != "":
		torx.Main()
	}
	os.Exit(m.Run())
}

// serve accepts connections on probePort and closes them, forever.
func serve() {
	ln, err := net.Listen("tcp", ":"+probePort)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("ready")
	for {
		c, err := ln.Accept()
		if err != nil {
			os.Exit(1)
		}
		_ = c.Close()
	}
}

// dial exits 0 if a connection to addr completes within a short timeout.
func dial(addr string) int {
	c, err := net.DialTimeout("tcp", net.JoinHostPort(addr, probePort), 500*time.Millisecond)
	if err != nil {
		return 1
	}
	_ = c.Close()
	return 0
}

func init() {
	torx.Register("netfault.lab", func() torx.Job { return &labJob{} })
}

// probes is a service that runs the test binary's TCP server on each of three
// nodes.
type probes struct {
	*torx.ServiceBase
	mu    sync.Mutex
	procs map[string]torx.Process // node name -> its server
}

func newProbes() *probes {
	p := &probes{procs: map[string]torx.Process{}}
	p.ServiceBase = torx.NewServiceBase("probe", torx.Homogeneous(3, torx.NodeSpec{}), p)
	return p
}

func (p *probes) StartNode(ctx context.Context, n *torx.Node) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	proc, err := n.Stream(ctx, torx.Command(self, serveArg))
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.procs[n.Name()] = proc
	p.mu.Unlock()
	return nil
}

func (p *probes) WaitNode(ctx context.Context, n *torx.Node) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return torx.WaitForPort(ctx, net.JoinHostPort(n.Addr(), probePort))
}

func (p *probes) StopNode(_ context.Context, n *torx.Node) error {
	p.mu.Lock()
	proc := p.procs[n.Name()]
	delete(p.procs, n.Name())
	p.mu.Unlock()
	if proc != nil {
		return proc.Close()
	}
	return nil
}

func (p *probes) CleanNode(context.Context, *torx.Node) error { return nil }

// labJob checks what each fault does to the connections between three nodes.
type labJob struct {
	torx.JobBase
	p *probes
}

func (j *labJob) Declare(jc *torx.JobContext) {
	j.p = newProbes()
	jc.Register(j.p)
}

func (j *labJob) Run(ctx context.Context, jc *torx.JobContext) error {
	nodes := j.p.Nodes()
	a, b, c := nodes[0], nodes[1], nodes[2]
	if err := Check(ctx, nodes); err != nil {
		return err
	}
	// reaches reports whether from can open a connection to to.
	reaches := func(from, to *torx.Node) (bool, error) {
		self, err := os.Executable()
		if err != nil {
			return false, err
		}
		res, err := from.Exec(ctx, torx.Command(self, dialArg, to.Addr()))
		return err == nil && res.ExitCode == 0, err
	}
	// expect checks every ordered pair against the pairs that must be cut.
	expect := func(step string, cut ...[2]*torx.Node) error {
		for _, from := range nodes {
			for _, to := range nodes {
				if from == to {
					continue
				}
				want := true
				for _, pair := range cut {
					if pair[0] == from && pair[1] == to {
						want = false
					}
				}
				got, err := reaches(from, to)
				if err != nil {
					return err
				}
				if got != want {
					return fmt.Errorf("%s: %s reaches %s: %t, want %t", step, from.Name(), to.Name(), got, want)
				}
			}
		}
		return nil
	}
	steps := []struct {
		name   string
		inject func() error
		cut    [][2]*torx.Node
	}{
		{"no fault", func() error { return nil }, nil},
		{"isolate a", func() error { return Isolate(ctx, a, nodes) },
			[][2]*torx.Node{{a, b}, {b, a}, {a, c}, {c, a}}},
		{"heal", func() error { return Heal(ctx, nodes...) }, nil},
		{"bridge through b", func() error { return Partition(ctx, []*torx.Node{a, b}, []*torx.Node{b, c}) },
			[][2]*torx.Node{{a, c}, {c, a}}},
		// A one-way block still stalls a connection either way: a
		// connection's replies cross it too. Block replaces only a's rules,
		// so the bridge is healed first.
		{"a drops what b sends", func() error {
			if err := Heal(ctx, nodes...); err != nil {
				return err
			}
			return Block(ctx, a, b)
		},
			[][2]*torx.Node{{a, b}, {b, a}}},
		// A connection's handshake is all small packets, so it crosses a
		// black hole for large ones.
		{"a drops b's large packets", func() error { return Blackhole(ctx, a, 1000, b) }, nil},
		{"heal again", func() error { return Heal(ctx, nodes...) }, nil},
	}
	for _, s := range steps {
		if err := s.inject(); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		if err := expect(s.name, s.cut...); err != nil {
			return err
		}
	}

	// A shape delays a node's packets; the delay shows in a connection's
	// handshake, which waits for a reply.
	if err := SetShape(ctx, a, Shape{Delay: 300 * time.Millisecond}); err != nil {
		return err
	}
	start := time.Now()
	if ok, err := reaches(b, a); err != nil {
		return err
	} else if !ok {
		return errors.New("b cannot reach a delayed a")
	}
	if took := time.Since(start); took < 300*time.Millisecond {
		return fmt.Errorf("a connection to a delayed by 300ms took %v", took)
	}
	if err := Unshape(ctx, a); err != nil {
		return err
	}
	// Unshaping a node without a shape is not an error.
	return Unshape(ctx, a)
}

// labUsable says why this host cannot run a lab, or nil if it can.
func labUsable() error {
	for _, tool := range []string{"ip", "nsenter", "nft", "tc", "sleep"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("%s is not installed", tool)
		}
	}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("this kernel does not let an unprivileged user create a user namespace: %w", err)
	}
	return nil
}

// TestFaultsInALab runs the lab job through the real driver and worker under
// -netns.
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
	cmd := exec.CommandContext(ctx, self, "-netns", "-results-dir", "", "netfault.lab")
	cmd.Env = append(os.Environ(), suiteEnv+"=1")
	out, err := cmd.CombinedOutput()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok || err != nil {
		t.Fatalf("suite under -netns failed (%v, %v):\n%s", err, exit, out)
	}
	if !strings.Contains(string(out), "1 passed") {
		t.Errorf("suite output does not report the job passing:\n%s", out)
	}
}
