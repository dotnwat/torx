//go:build unix

package netfault

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

func node(name, addr string) *torx.Node {
	return torx.NewNode(torx.NodeConfig{Name: name, Addr: addr, Backend: torx.LocalBackend{}})
}

func TestScriptOfNoRulesOnlyDropsTheTable(t *testing.T) {
	got := rules{}.script()
	want := "table inet torx_netfault\ndelete table inet torx_netfault\n"
	if got != want {
		t.Errorf("script = %q, want %q", got, want)
	}
}

func TestScriptDropsEachFamilyBothWays(t *testing.T) {
	r := rules{
		dropFrom: []netip.Addr{netip.MustParseAddr("10.77.0.3"), netip.MustParseAddr("fd00::3")},
		dropTo:   []netip.Addr{netip.MustParseAddr("10.77.0.4")},
	}
	s := r.script()
	for _, want := range []string{
		"delete table inet torx_netfault\n", // replaces what was there
		"type filter hook input priority 0; policy accept;",
		"ip saddr { 10.77.0.3 } drop",
		"ip6 saddr { fd00::3 } drop",
		"ip daddr { 10.77.0.4 } drop",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("script lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "ip6 daddr") {
		t.Errorf("script drops IPv6 output it was not asked to:\n%s", s)
	}
}

func TestScriptBlackholeDropsOnlyLargePackets(t *testing.T) {
	s := rules{dropFrom: []netip.Addr{netip.MustParseAddr("10.77.0.3")}, minLength: 256}.script()
	if !strings.Contains(s, "ip saddr { 10.77.0.3 } meta length >= 256 drop") {
		t.Errorf("script does not drop by length:\n%s", s)
	}
}

func TestAddrsRefusesNodesItCannotTellApart(t *testing.T) {
	if _, err := addrs([]*torx.Node{node("a", "127.0.0.1"), node("b", "127.0.0.1")}); err == nil {
		t.Error("two nodes on one address were accepted")
	}
	if _, err := addrs([]*torx.Node{node("a", "db.example")}); err == nil {
		t.Error("a hostname was accepted as an address")
	}
	if _, err := addrs([]*torx.Node{node("a", "10.77.0.2"), node("b", "10.77.0.3")}); err != nil {
		t.Errorf("distinct addresses refused: %v", err)
	}
}

func TestPartitionRefusesSharedAddressesBeforeActing(t *testing.T) {
	a, b := node("a", "127.0.0.1"), node("b", "127.0.0.1")
	if err := Partition(context.Background(), []*torx.Node{a}, []*torx.Node{b}); err == nil {
		t.Error("a partition of nodes sharing an address was accepted")
	}
}

func TestIfaceOfFindsTheInterfaceHoldingAnAddress(t *testing.T) {
	out := []byte(`1: lo    inet 127.0.0.1/8 scope host lo\       valid_lft forever preferred_lft forever
2: eth0    inet 10.77.0.3/16 scope global eth0\       valid_lft forever preferred_lft forever
2: eth0    inet6 fe80::1/64 scope link \       valid_lft forever preferred_lft forever
`)
	if dev, ok := ifaceOf(out, "10.77.0.3"); !ok || dev != "eth0" {
		t.Errorf("ifaceOf = %q, %v; want eth0", dev, ok)
	}
	if _, ok := ifaceOf(out, "10.77.0.9"); ok {
		t.Error("ifaceOf found an address no interface holds")
	}
}

func TestMicros(t *testing.T) {
	if got := micros(150 * time.Millisecond); got != "150000us" {
		t.Errorf("micros = %q", got)
	}
}
