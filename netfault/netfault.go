//go:build unix

// Package netfault injects network faults between torx nodes: partitions,
// one-way blocks, and delay and loss on a node's outgoing packets.
//
// It acts on each node through the node's own backend, running nft
// (nftables) and tc (netem) there with n.Exec, so it works wherever a node's
// commands run with privilege over its network: the nodes of a -netns run,
// each root in a network namespace of its own; a container with
// CAP_NET_ADMIN; a host reached as root. Rules name peers by address
// (Node.Addr), so the nodes it acts on must have distinct IP addresses -- the
// plain local pool, where every node is the loopback, cannot be partitioned.
// Check says whether a set of nodes can take faults.
//
// A partition drops packets rather than refusing them, as a failed switch or
// a cut cable would: a connection across it neither completes nor fails
// until a timeout says so. Clients outside the nodes -- the job itself, in
// the driver's namespace under -netns -- are not cut off by any rule here.
package netfault

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dotnwat/torx"
)

// table is the nftables table the package owns on each node. Every rule it
// installs lives there, so healing a node is dropping the table.
const table = "torx_netfault"

// Check reports whether faults can be injected among nodes: each has an IP
// address no other has, and nft and tc run on each with the privilege they
// need.
func Check(ctx context.Context, nodes []*torx.Node) error {
	if _, err := addrs(nodes); err != nil {
		return err
	}
	for _, n := range nodes {
		if err := run(ctx, n, nil, "nft", "list", "tables"); err != nil {
			return err
		}
		if err := run(ctx, n, nil, "tc", "qdisc", "show"); err != nil {
			return err
		}
	}
	return nil
}

// Partition splits nodes into groups: each node in a group drops every packet
// to or from a node it shares no group with, and passes the rest. Overlapping
// groups express a bridge -- a node in two groups reaches both, while the two
// do not reach each other -- and a single group of one node against the rest
// isolates it. Every node in a group has its rules replaced, including any
// Block; a node in no group is left as it is.
func Partition(ctx context.Context, groups ...[]*torx.Node) error {
	var all []*torx.Node
	for _, g := range groups {
		for _, n := range g {
			if !slices.Contains(all, n) {
				all = append(all, n)
			}
		}
	}
	ips, err := addrs(all)
	if err != nil {
		return err
	}
	shares := func(a, b *torx.Node) bool {
		for _, g := range groups {
			if slices.Contains(g, a) && slices.Contains(g, b) {
				return true
			}
		}
		return false
	}
	return each(all, func(n *torx.Node) error {
		var cut []netip.Addr
		for _, p := range all {
			if p != n && !shares(n, p) {
				cut = append(cut, ips[p])
			}
		}
		return apply(ctx, n, rules{dropFrom: cut, dropTo: cut})
	})
}

// Isolate cuts n off from every other node in nodes, in both directions.
func Isolate(ctx context.Context, n *torx.Node, nodes []*torx.Node) error {
	var rest []*torx.Node
	for _, p := range nodes {
		if p != n {
			rest = append(rest, p)
		}
	}
	return Partition(ctx, []*torx.Node{n}, rest)
}

// Block has to drop every packet from each node in from, in that direction
// only: to still sends to them and they receive it, but nothing they send
// reaches it -- replies included, so a connection either way stalls. It
// replaces to's rules; the nodes in from are not touched.
func Block(ctx context.Context, to *torx.Node, from ...*torx.Node) error {
	ips, err := addrs(append([]*torx.Node{to}, from...))
	if err != nil {
		return err
	}
	var cut []netip.Addr
	for _, p := range from {
		cut = append(cut, ips[p])
	}
	return apply(ctx, to, rules{dropFrom: cut})
}

// Blackhole has to drop every packet of at least size bytes from each node in
// from, while smaller ones get through: an MTU black hole, where a link
// passes connection setup and short messages but loses anything large. A
// stream that sends a large segment stalls behind it, retransmitting it into
// the void, while a connection that only ever sends short messages -- a
// heartbeat, say -- carries on. It replaces to's rules; the nodes in from are
// not touched.
func Blackhole(ctx context.Context, to *torx.Node, size int, from ...*torx.Node) error {
	ips, err := addrs(append([]*torx.Node{to}, from...))
	if err != nil {
		return err
	}
	r := rules{minLength: size}
	for _, p := range from {
		r.dropFrom = append(r.dropFrom, ips[p])
	}
	return apply(ctx, to, r)
}

// Heal removes every rule the package installed on each node, so it reaches
// and is reached by every peer again. Shaping is separate; see Unshape.
func Heal(ctx context.Context, nodes ...*torx.Node) error {
	return each(nodes, func(n *torx.Node) error { return apply(ctx, n, rules{}) })
}

// Shape describes what a node does to the packets it sends: hold each for
// Delay, give or take Jitter, and drop Loss percent of them.
type Shape struct {
	Delay  time.Duration
	Jitter time.Duration
	Loss   float64
}

// SetShape applies s to every packet n sends, on the interface that holds
// n's address, replacing any shape it had.
func SetShape(ctx context.Context, n *torx.Node, s Shape) error {
	dev, err := iface(ctx, n)
	if err != nil {
		return err
	}
	args := []string{"qdisc", "replace", "dev", dev, "root", "netem"}
	if s.Delay > 0 {
		args = append(args, "delay", micros(s.Delay))
		if s.Jitter > 0 {
			args = append(args, micros(s.Jitter))
		}
	}
	if s.Loss > 0 {
		args = append(args, "loss", strconv.FormatFloat(s.Loss, 'f', -1, 64)+"%")
	}
	return run(ctx, n, nil, "tc", args...)
}

// Unshape removes n's shape, if it has one.
func Unshape(ctx context.Context, n *torx.Node) error {
	dev, err := iface(ctx, n)
	if err != nil {
		return err
	}
	res, err := n.Exec(ctx, torx.Command("tc", "qdisc", "del", "dev", dev, "root"))
	if err != nil {
		return fmt.Errorf("netfault: %s: tc: %w", n.Name(), err)
	}
	// With no shape there is no root qdisc of ours to delete, which tc
	// reports as an error; that is the state asked for.
	if res.ExitCode != 0 && !noQdisc(res.Stderr) {
		return fmt.Errorf("netfault: %s: tc qdisc del: exit %d: %s", n.Name(), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// rules is what a node drops: packets from the dropFrom addresses and to the
// dropTo addresses, of at least minLength bytes when it is positive. The zero
// value drops nothing.
type rules struct {
	dropFrom, dropTo []netip.Addr
	minLength        int
}

// script renders r as an nft script that replaces the package's table with
// one holding r, atomically: nft applies a script as one transaction. The
// table is declared before it is deleted so the delete never fails on a node
// that has none.
func (r rules) script() string {
	var b strings.Builder
	fmt.Fprintf(&b, "table inet %s\ndelete table inet %s\n", table, table)
	if len(r.dropFrom) == 0 && len(r.dropTo) == 0 {
		return b.String()
	}
	fmt.Fprintf(&b, "table inet %s {\n", table)
	fmt.Fprintf(&b, "\tchain input {\n\t\ttype filter hook input priority 0; policy accept;\n")
	writeDrops(&b, "saddr", r.dropFrom, r.minLength)
	fmt.Fprintf(&b, "\t}\n\tchain output {\n\t\ttype filter hook output priority 0; policy accept;\n")
	writeDrops(&b, "daddr", r.dropTo, r.minLength)
	fmt.Fprintf(&b, "\t}\n}\n")
	return b.String()
}

// writeDrops writes rules dropping packets whose field (saddr or daddr) is
// one of addrs, and whose length is at least minLength when it is positive, a
// rule per address family.
func writeDrops(b *strings.Builder, field string, addrs []netip.Addr, minLength int) {
	length := ""
	if minLength > 0 {
		length = fmt.Sprintf(" meta length >= %d", minLength)
	}
	var v4, v6 []string
	for _, a := range addrs {
		if a.Is4() {
			v4 = append(v4, a.String())
		} else {
			v6 = append(v6, a.String())
		}
	}
	if len(v4) > 0 {
		fmt.Fprintf(b, "\t\tip %s { %s }%s drop\n", field, strings.Join(v4, ", "), length)
	}
	if len(v6) > 0 {
		fmt.Fprintf(b, "\t\tip6 %s { %s }%s drop\n", field, strings.Join(v6, ", "), length)
	}
}

// apply replaces n's rules with r.
func apply(ctx context.Context, n *torx.Node, r rules) error {
	return run(ctx, n, []byte(r.script()), "nft", "-f", "-")
}

// addrs maps each node to its IP address, refusing a node whose address is
// not an IP or is another node's too: a rule by address cannot tell such
// nodes apart.
func addrs(nodes []*torx.Node) (map[*torx.Node]netip.Addr, error) {
	out := make(map[*torx.Node]netip.Addr, len(nodes))
	owner := map[netip.Addr]*torx.Node{}
	for _, n := range nodes {
		a, err := netip.ParseAddr(n.Addr())
		if err != nil {
			return nil, fmt.Errorf("netfault: %s's address %q is not an IP address", n.Name(), n.Addr())
		}
		a = a.Unmap()
		if o, dup := owner[a]; dup && o != n {
			return nil, fmt.Errorf("netfault: %s and %s share the address %s, so no rule can tell them apart (run under -netns for nodes with addresses of their own)", o.Name(), n.Name(), a)
		}
		owner[a] = n
		out[n] = a
	}
	return out, nil
}

// iface returns the name of n's interface that holds n's address.
func iface(ctx context.Context, n *torx.Node) (string, error) {
	res, err := n.Exec(ctx, torx.Command("ip", "-o", "addr", "show"))
	if err != nil {
		return "", fmt.Errorf("netfault: %s: ip: %w", n.Name(), err)
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("netfault: %s: ip addr show: exit %d: %s", n.Name(), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	dev, ok := ifaceOf(res.Stdout, n.Addr())
	if !ok {
		return "", fmt.Errorf("netfault: %s: no interface holds its address %s", n.Name(), n.Addr())
	}
	return dev, nil
}

// ifaceOf finds the interface holding addr in the output of "ip -o addr
// show", whose lines read "2: eth0    inet 10.77.0.3/16 brd ...".
func ifaceOf(out []byte, addr string) (string, bool) {
	want, err := netip.ParseAddr(addr)
	if err != nil {
		return "", false
	}
	for line := range bytes.Lines(out) {
		f := strings.Fields(string(line))
		if len(f) < 4 || (f[2] != "inet" && f[2] != "inet6") {
			continue
		}
		p, err := netip.ParsePrefix(f[3])
		if err == nil && p.Addr() == want.Unmap() {
			return strings.TrimSuffix(f[1], ":"), true
		}
	}
	return "", false
}

// noQdisc reports whether tc's complaint is that there was no qdisc to
// delete.
func noQdisc(stderr []byte) bool {
	s := string(stderr)
	return strings.Contains(s, "handle of zero") || strings.Contains(s, "No such file or directory")
}

// micros renders d for tc, which takes microseconds with a "us" suffix.
func micros(d time.Duration) string {
	return strconv.FormatInt(d.Microseconds(), 10) + "us"
}

// run runs name args on n, with stdin when it is not nil, and fails on a
// non-zero exit with what the command said.
func run(ctx context.Context, n *torx.Node, stdin []byte, name string, args ...string) error {
	cmd := torx.Command(name, args...)
	cmd.Stdin = stdin
	res, err := n.Exec(ctx, cmd)
	if err != nil {
		return fmt.Errorf("netfault: %s: %s: %w", n.Name(), name, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("netfault: %s: %s %s: exit %d: %s", n.Name(), name, strings.Join(args, " "), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// each runs fn on every node, returning every failure.
func each(nodes []*torx.Node, fn func(*torx.Node) error) error {
	var errs []error
	for _, n := range nodes {
		errs = append(errs, fn(n))
	}
	return errors.Join(errs...)
}
