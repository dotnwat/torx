//go:build unix

package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

func TestResolveClusterParamsFillsDefaults(t *testing.T) {
	got, err := resolveClusterParams(torx.Params{})
	if err != nil {
		t.Fatalf("resolve empty: %v", err)
	}
	if got.Int(paramNodes, 0) != defaultNodes || got.String(paramLevel, "") != defaultLevel {
		t.Errorf("resolved empty set = %v, want nodes=%d level=%s", got, defaultNodes, defaultLevel)
	}
	// A partial set and the full set canonicalize identically: one variant, one id.
	partial, _ := resolveClusterParams(torx.Params{paramLevel: rqlite.LevelStrong})
	full, _ := resolveClusterParams(torx.Params{paramLevel: rqlite.LevelStrong, paramNodes: defaultNodes})
	if partial.Int(paramNodes, 0) != full.Int(paramNodes, 0) || partial.String(paramLevel, "") != full.String(paramLevel, "") {
		t.Errorf("partial %v and full %v resolved differently", partial, full)
	}
}

func TestResolveClusterParamsAcceptsJSONNumbers(t *testing.T) {
	// A -params file arrives through JSON, where every number is a float64.
	got, err := resolveClusterParams(torx.Params{paramNodes: float64(5)})
	if err != nil {
		t.Fatalf("resolve float64 5: %v", err)
	}
	if n, ok := got[paramNodes].(int); !ok || n != 5 {
		t.Errorf("nodes resolved to %v (%T), want int 5", got[paramNodes], got[paramNodes])
	}
}

func TestResolveClusterParamsRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		in   torx.Params
		want string
	}{
		{"unknown key", torx.Params{"node": 3}, "unknown parameter"},
		{"unknown level", torx.Params{paramLevel: "bogus"}, "must be one of"},
		{"level not a string", torx.Params{paramLevel: 1}, "must be one of"},
		{"zero nodes", torx.Params{paramNodes: 0}, "must be an integer"},
		{"too many nodes", torx.Params{paramNodes: maxNodes + 1}, "must be an integer"},
		{"fractional nodes", torx.Params{paramNodes: 2.5}, "must be an integer"},
		{"nodes not a number", torx.Params{paramNodes: "3"}, "must be an integer"},
	}
	for _, c := range cases {
		_, err := resolveClusterParams(c.in)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error = %v, want one containing %q", c.name, err, c.want)
		}
	}
}

func TestClusterVariantsResolveCanonically(t *testing.T) {
	// A request carries the base id and its resolved parameters; the variant
	// id the driver reports is derived from those. The compiled-in matrix
	// yields one variant per level, each with both dimensions filled in.
	reqs, err := torx.Discover("rqlite.cluster")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(reqs) != len(rqlite.Levels()) {
		t.Fatalf("discovered %d cluster variants, want %d", len(reqs), len(rqlite.Levels()))
	}
	levels := map[string]bool{}
	for _, r := range reqs {
		if r.ID != "rqlite.cluster" {
			t.Errorf("request id = %q, want the base id", r.ID)
		}
		if n := r.Params.Int(paramNodes, 0); n != defaultNodes {
			t.Errorf("variant %v has nodes=%d, want %d", r.Params, n, defaultNodes)
		}
		levels[r.Params.String(paramLevel, "")] = true
	}
	for _, l := range rqlite.Levels() {
		if !levels[l] {
			t.Errorf("no variant for level %s; got %v", l, levels)
		}
	}

	// An external override replaces the matrix and passes through the same
	// resolution, so a partial entry comes out with its defaults filled in.
	ov, err := torx.ParseParamsOverrides([]byte(`{"rqlite.cluster": {"configs": [{"level": "strong"}, {"nodes": 5}]}}`))
	if err != nil {
		t.Fatalf("parse overrides: %v", err)
	}
	reqs, err = torx.DiscoverWith(ov, "rqlite.cluster")
	if err != nil {
		t.Fatalf("discover with overrides: %v", err)
	}
	var got []string
	for _, r := range reqs {
		got = append(got, fmt.Sprintf("%s/%d", r.Params.String(paramLevel, "?"), r.Params.Int(paramNodes, 0)))
	}
	if want := "strong/3 weak/5"; strings.Join(got, " ") != want {
		t.Errorf("override variants = %v, want %s", got, want)
	}
}

func TestCheckMembership(t *testing.T) {
	healthy := []rqlite.NodeInfo{
		{ID: "node-0", Voter: true, Reachable: true, Leader: true},
		{ID: "node-1", Voter: true, Reachable: true},
		{ID: "node-2", Voter: true, Reachable: true},
	}
	if err := checkMembership(healthy, 3); err != nil {
		t.Errorf("healthy view rejected: %v", err)
	}
	cases := []struct {
		name string
		view []rqlite.NodeInfo
		want int
		msg  string
	}{
		{"wrong size", healthy, 2, "3 members, want 2"},
		{"no leader", []rqlite.NodeInfo{{ID: "a", Voter: true, Reachable: true}}, 1, "0 leaders"},
		{"unreachable", []rqlite.NodeInfo{{ID: "a", Voter: true, Leader: true}}, 1, "unreachable"},
		{"non-voter", []rqlite.NodeInfo{{ID: "a", Reachable: true, Leader: true}}, 1, "not a voter"},
	}
	for _, c := range cases {
		err := checkMembership(c.view, c.want)
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: error = %v, want one containing %q", c.name, err, c.msg)
		}
	}
}
