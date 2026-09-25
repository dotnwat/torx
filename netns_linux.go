//go:build linux

// A network lab for the local pool: nodes with networks of their own.
//
// The local pool's nodes all share the host's network, so a job cannot cut
// one off from another or slow the link between two: there is no link. Under
// -netns the driver instead gives each local node a network namespace of its
// own, joined to the others by a bridge, with an address of its own, so the
// traffic between nodes crosses interfaces a job can filter (nft) and shape
// (tc netem) -- see the netfault package. No root is needed: the driver
// re-executes itself in a user namespace of its own, where it is root over
// the namespaces it creates and over nothing else, together with a network
// namespace that holds the bridge and a mount namespace kept for faults to
// come. The driver and its workers live in that network namespace, so they
// reach every node at its address, and nothing in the lab reaches the host's
// network or is reached from it.
//
// Each node's namespace is held by a sleep of its own, which dies with the
// driver; the commands a node runs enter the namespace through nsenter (see
// LocalBackend.NetNS), and its files are the host's, under its scratch root
// as for any local node. The lab needs ip (iproute2), nsenter (util-linux),
// and sleep on the host, and a kernel that lets an unprivileged user create
// a user namespace.

package torx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// labEnv marks a driver that is already inside its lab's namespaces.
const labEnv = "TORX_NETNS_LAB"

const (
	labBridge = "torx0"
	// labPrefix is the lab's subnet: the bridge takes the first address and
	// node i the (i+2)th. The lab is sealed off from the host's network, so
	// the subnet cannot clash with one the host uses.
	labPrefix = "10.77.0.0/16"
	// labPortMin and labPortMax bound the ports each node leases. A node's
	// namespace is its own, so its ports are too, and they are leased from a
	// range rather than probed from the driver's namespace.
	labPortMin = 20000
	labPortMax = 30000
)

// inLab reports whether this process is a driver already inside its lab.
func inLab() bool { return os.Getenv(labEnv) != "" }

// enterLab re-executes this program, with the same arguments, inside a new
// user, network, and mount namespace, and returns the exit status it should
// exit with: the re-executed driver's. SIGINT and SIGTERM are passed on to
// it, so an interrupt tears the run down as it would without the lab.
func enterLab() int {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "torx: -netns:", err)
		return 2
	}
	cmd := exec.Command(self, os.Args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), labEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET | syscall.CLONE_NEWNS,
		UidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
		GidMappingsEnableSetgroups: false,
		// Should this process die without passing a signal on -- killed
		// outright -- the run in the lab is told to stop as by an interrupt,
		// rather than carrying on orphaned.
		Pdeathsig: syscall.SIGTERM,
	}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "torx: -netns: cannot create the lab's namespaces: %v\n"+
			"(the kernel must let an unprivileged user create a user namespace; see user.max_user_namespaces, "+
			"and on Ubuntu kernel.apparmor_restrict_unprivileged_userns)\n", err)
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
		fmt.Fprintln(os.Stderr, "torx: -netns:", err)
		return 2
	}
	return 0
}

// localLab is the network a -netns run's nodes live on: the bridge in the
// driver's namespace and a namespace per node, each held by a process.
type localLab struct {
	holders []*exec.Cmd
}

// newLabPool builds a pool of n local nodes, each in a network namespace of
// its own on the lab's bridge. It must run inside the lab (inLab). Close the
// lab to release the namespaces; they die with the driver regardless.
func newLabPool(n int) (*Pool, *localLab, error) {
	lab := &localLab{}
	prefix := netip.MustParsePrefix(labPrefix)
	bridgeAddr := prefix.Addr().Next()
	for _, args := range [][]string{
		{"link", "set", "lo", "up"},
		{"link", "add", labBridge, "type", "bridge"},
		{"addr", "add", netip.PrefixFrom(bridgeAddr, prefix.Bits()).String(), "dev", labBridge},
		{"link", "set", labBridge, "up"},
	} {
		if err := labRun("ip", args...); err != nil {
			return nil, nil, err
		}
	}
	base := filepath.Join(os.TempDir(), "torx")
	nodes := make([]*Node, n)
	addr := bridgeAddr
	for i := range nodes {
		addr = addr.Next()
		if !prefix.Contains(addr) {
			lab.Close()
			return nil, nil, fmt.Errorf("netns: %d nodes do not fit in %s", n, labPrefix)
		}
		ns, err := lab.addNode(i, netip.PrefixFrom(addr, prefix.Bits()))
		if err != nil {
			lab.Close()
			return nil, nil, err
		}
		name := fmt.Sprintf("node-%d", i)
		config, _ := json.Marshal(LocalBackend{NetNS: ns})
		nodes[i] = NewNode(NodeConfig{
			Name:       name,
			Addr:       addr.String(),
			Backend:    LocalBackend{NetNS: ns},
			Descriptor: BackendDescriptor{Kind: "local", Config: config},
			Scratch:    MakeScratch(base, name),
			Ports:      NewRangePortAllocator(labPortMin, labPortMax),
		})
	}
	return NewPool(nodes), lab, nil
}

// addNode creates node i's namespace, held by a sleep, and plugs it into the
// bridge at addr: a veth pair with one end on the bridge and the other in the
// namespace as eth0. It returns the namespace's path.
func (l *localLab) addNode(i int, addr netip.Prefix) (string, error) {
	holder := exec.Command("sleep", "infinity")
	holder.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWNET,
		Setpgid:    true,
		// The namespace must not outlive the driver, however the driver ends.
		Pdeathsig: syscall.SIGKILL,
	}
	if err := holder.Start(); err != nil {
		return "", fmt.Errorf("netns: node %d's namespace: %w", i, err)
	}
	l.holders = append(l.holders, holder)
	pid := strconv.Itoa(holder.Process.Pid)
	ns := "/proc/" + pid + "/ns/net"
	veth := fmt.Sprintf("tv%d", i)
	for _, step := range [][]string{
		{"ip", "link", "add", veth, "type", "veth", "peer", "name", "eth0", "netns", pid},
		{"ip", "link", "set", veth, "master", labBridge, "up"},
		{"nsenter", "--net=" + ns, "ip", "link", "set", "lo", "up"},
		{"nsenter", "--net=" + ns, "ip", "addr", "add", addr.String(), "dev", "eth0"},
		{"nsenter", "--net=" + ns, "ip", "link", "set", "eth0", "up"},
	} {
		if err := labRun(step[0], step[1:]...); err != nil {
			return "", fmt.Errorf("netns: node %d: %w", i, err)
		}
	}
	return ns, nil
}

// Close ends the processes holding the nodes' namespaces, which releases
// them.
func (l *localLab) Close() {
	for _, h := range l.holders {
		_ = syscall.Kill(-h.Process.Pid, syscall.SIGKILL)
		_ = h.Wait()
	}
	l.holders = nil
}

// labRun runs one setup command, failing with its output.
func labRun(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
