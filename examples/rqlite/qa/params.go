//go:build unix

package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// Parameters of rqlite.cluster.
const (
	paramNodes = "nodes" // cluster size
	paramLevel = "level" // read consistency level the follower is read at

	defaultNodes = 3
	maxNodes     = 9
	defaultLevel = rqlite.LevelWeak
)

// resolveClusterParams canonicalizes one parameter set of rqlite.cluster:
// defaults filled in, values checked, unknown keys refused. Discovery runs it
// on every variant, from the compiled-in matrix and from a -params file
// alike, so {level:"weak"} and {level:"weak",nodes:3} are one variant with
// one id, a mistyped key fails instead of silently running the default, and
// an unknown level -- which rqlite itself accepts and serves at its default --
// fails before any node is allocated.
func resolveClusterParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{paramNodes: defaultNodes, paramLevel: defaultLevel}
	for k, v := range p {
		switch k {
		case paramNodes:
			n, ok := intValue(v)
			if !ok || n < 1 || n > maxNodes {
				return nil, fmt.Errorf("%s must be an integer in [1, %d], got %v", paramNodes, maxNodes, v)
			}
			out[paramNodes] = n
		case paramLevel:
			s, ok := v.(string)
			if !ok || !rqlite.ValidLevel(s) {
				return nil, fmt.Errorf("%s must be one of %s, got %v", paramLevel, strings.Join(rqlite.Levels(), ", "), v)
			}
			out[paramLevel] = s
		default:
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	return out, nil
}

// intValue decodes an integer parameter, which arrives as int from a Go
// matrix and as float64 from JSON. A fractional value is not an integer.
func intValue(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n != math.Trunc(n) {
			return 0, false
		}
		return int(n), true
	}
	return 0, false
}
