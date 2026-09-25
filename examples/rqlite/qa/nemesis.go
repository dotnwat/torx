//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// faultWeights names the faults the nemesis injects and how often it picks
// each, relative to the others. The -leader variants aim at whichever node
// leads when the fault is injected, since that is where a fault does the most
// damage; the others pick any node.
var faultWeights = map[string]int{
	"crash":          2, // SIGKILL a node; restart it after the hold
	"crash-leader":   3,
	"crash-majority": 1, // SIGKILL a majority at once, so the cluster loses quorum
	"stop":           1, // SIGTERM a node, which must exit cleanly; restart it after the hold
	"stop-leader":    1,
	"pause":          2, // SIGSTOP a node; SIGCONT it after the hold
	"pause-leader":   3,
	"snapshot":       2, // have a node snapshot now
	"reap":           1, // have a node reap its snapshot store now
	"stepdown":       1, // have the leader hand leadership over
}

// validFault reports whether name is a fault the nemesis knows.
func validFault(name string) bool {
	_, ok := faultWeights[name]
	return ok
}

const (
	nemesisQuietMin = 500 * time.Millisecond // between one fault's heal and the next fault
	nemesisQuietMax = 2 * time.Second
	nemesisHoldMin  = 500 * time.Millisecond // how long a fault lasts before it is healed
	nemesisHoldMax  = 6 * time.Second
	nemesisOpBound  = 10 * time.Second // an API call the nemesis makes
)

// nemesis injects one fault at a time into the cluster until its context is
// done, holding each for a random time and then healing it, and records every
// fault and heal in the history.
type nemesis struct {
	db     *rqlite.Service
	h      *History
	jc     *torx.JobContext
	rng    *rand.Rand
	faults []string

	anomalies []Anomaly // stops that did not go cleanly
}

func (m *nemesis) run(ctx context.Context) {
	for {
		if !sleep(ctx, m.between(nemesisQuietMin, nemesisQuietMax)) {
			return
		}
		fault := m.pick()
		op := Op{Process: "nemesis", F: fault, Start: m.h.Now()}
		// The run's end stops the nemesis between faults, never inside one: a
		// graceful stop cut short would leave the node in doubt.
		heal, targets, err := m.inject(context.WithoutCancel(ctx), fault)
		op.End = m.h.Now()
		op.Node = strings.Join(targets, ",")
		op.Outcome = Ok
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		m.h.Add(op)
		m.jc.Log("info", fmt.Sprintf("nemesis: %s %s %s", fault, op.Node, op.Err))
		if heal == nil {
			continue
		}
		sleep(ctx, m.between(nemesisHoldMin, nemesisHoldMax))
		// A heal runs even once the run is over: the fault must not outlast it.
		hctx := context.WithoutCancel(ctx)
		op = Op{Process: "nemesis", F: "heal-" + fault, Node: op.Node, Start: m.h.Now()}
		err = heal(hctx)
		op.End = m.h.Now()
		op.Outcome = Ok
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		m.h.Add(op)
		m.jc.Log("info", fmt.Sprintf("nemesis: healed %s %s %s", fault, op.Node, op.Err))
	}
}

// pick draws a fault by weight.
func (m *nemesis) pick() string {
	total := 0
	for _, f := range m.faults {
		total += faultWeights[f]
	}
	r := m.rng.IntN(total)
	for _, f := range m.faults {
		if r -= faultWeights[f]; r < 0 {
			return f
		}
	}
	return m.faults[len(m.faults)-1]
}

// between draws a duration uniformly from [lo, hi).
func (m *nemesis) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(m.rng.Int64N(int64(hi-lo)))
}

// inject applies fault and returns how to heal it (nil for an instant fault)
// and the nodes it hit.
func (m *nemesis) inject(ctx context.Context, fault string) (func(context.Context) error, []string, error) {
	switch fault {
	case "crash", "crash-leader":
		n, err := m.target(ctx, fault == "crash-leader")
		if err != nil {
			return nil, nil, err
		}
		if err := m.db.Crash(ctx, n); err != nil {
			return nil, names(n), err
		}
		return func(ctx context.Context) error { return m.db.Restart(ctx, n) }, names(n), nil

	case "crash-majority":
		up := m.responsive()
		quorum := len(m.db.Nodes())/2 + 1
		if len(up) < quorum {
			return nil, nil, errNoTarget
		}
		m.rng.Shuffle(len(up), func(i, j int) { up[i], up[j] = up[j], up[i] })
		victims := up[:quorum]
		for _, n := range victims {
			if err := m.db.Crash(ctx, n); err != nil {
				return nil, names(victims...), err
			}
		}
		return func(ctx context.Context) error {
			for _, n := range victims {
				if err := m.db.Restart(ctx, n); err != nil {
					return err
				}
			}
			return nil
		}, names(victims...), nil

	case "stop", "stop-leader":
		n, err := m.target(ctx, fault == "stop-leader")
		if err != nil {
			return nil, nil, err
		}
		start := time.Now()
		err = m.db.Shutdown(ctx, n)
		if took := time.Since(start); err == nil && took > slowStop {
			m.anomalies = append(m.anomalies, Anomaly{Kind: "slow-shutdown", Severity: sevWarn,
				Detail: fmt.Sprintf("%s took %s to exit on SIGTERM", n.Name(), took.Round(time.Millisecond))})
		}
		if err != nil {
			// rqlite stops on SIGTERM within the grace period; one that had to
			// be killed is a finding, and its goroutine dump is in its log. One
			// that exited non-zero is worth a look, not a failure: a node
			// stopped while it is still joining exits 1, saying the join was
			// cancelled. The node is down either way.
			kind, sev := "unclean-shutdown", sevWarn
			if errors.Is(err, torx.ErrShutdownTimeout) {
				kind, sev = "shutdown-hang", sevError
			}
			m.anomalies = append(m.anomalies, Anomaly{Kind: kind, Severity: sev,
				Detail: fmt.Sprintf("%s after %s: %v", n.Name(), time.Since(start).Round(time.Millisecond), err)})
		}
		return func(ctx context.Context) error { return m.db.Restart(ctx, n) }, names(n), nil

	case "pause", "pause-leader":
		n, err := m.target(ctx, fault == "pause-leader")
		if err != nil {
			return nil, nil, err
		}
		if err := m.db.Pause(ctx, n); err != nil {
			return nil, names(n), err
		}
		return func(ctx context.Context) error { return m.db.Resume(ctx, n) }, names(n), nil

	case "snapshot", "reap", "stepdown":
		n, err := m.target(ctx, false)
		if err != nil {
			return nil, nil, err
		}
		cctx, cancel := context.WithTimeout(ctx, nemesisOpBound)
		defer cancel()
		c := m.db.Client(n)
		switch fault {
		case "snapshot":
			err = c.Snapshot(cctx)
		case "reap":
			err = c.Reap(cctx)
		default:
			err = c.Stepdown(cctx, true, "")
		}
		return nil, names(n), err
	}
	return nil, nil, fmt.Errorf("unknown fault %q", fault)
}

// target picks the node a fault hits: the leader when leader is set and one
// is known, otherwise any responsive node.
func (m *nemesis) target(ctx context.Context, leader bool) (*torx.Node, error) {
	if leader {
		lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		l, err := m.db.Leader(lctx)
		cancel()
		if err == nil {
			return l, nil
		}
	}
	up := m.responsive()
	if len(up) == 0 {
		return nil, errNoTarget
	}
	return up[m.rng.IntN(len(up))], nil
}

// responsive lists the nodes running and not paused.
func (m *nemesis) responsive() []*torx.Node {
	var up []*torx.Node
	for _, n := range m.db.Nodes() {
		if m.db.Running(n) && !m.db.Paused(n) {
			up = append(up, n)
		}
	}
	return up
}

// healAll ends every fault still in effect -- resuming paused nodes and
// restarting stopped ones, including any that exited on their own -- and
// waits until every node is ready and caught up.
func (m *nemesis) healAll(ctx context.Context) error {
	for _, n := range m.db.Nodes() {
		if m.db.Paused(n) {
			if err := m.db.Resume(ctx, n); err != nil {
				return err
			}
		}
		if !m.db.Running(n) {
			if err := m.db.Restart(ctx, n); err != nil {
				return err
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, healTimeout)
	defer cancel()
	for _, n := range m.db.Nodes() {
		if err := m.db.WaitNode(ctx, n); err != nil {
			return fmt.Errorf("%s not ready: %w", n.Name(), err)
		}
	}
	if _, err := m.db.AwaitLeader(ctx); err != nil {
		return err
	}
	for _, n := range m.db.Nodes() {
		if err := m.db.WaitSynced(ctx, n); err != nil {
			return fmt.Errorf("%s not synced: %w", n.Name(), err)
		}
	}
	return nil
}

// sleep waits for d or until ctx is done, reporting whether it slept the whole
// time.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func names(nodes ...*torx.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Name()
	}
	return out
}

// chaosConfig is the rqlited configuration a chaos variant runs with: flag
// name to value, holding only the flags drawn away from their defaults.
type chaosConfig map[string]string

// configSpace lists, per flag, the values a variant draws from; "" keeps
// rqlite's default. The choices lean toward the settings that put rqlite's
// less-trodden paths under the faults: snapshots every few entries instead of
// every few thousand, checks for one every fraction of a second, a WAL
// threshold that snapshots on nearly every write, and VACUUMs running
// alongside them.
var configSpace = map[string][]string{
	"raft-snap":               {"", "16", "128", "1024"},
	"raft-snap-int":           {"", "100ms", "1s"},
	"raft-snap-wal-size":      {"", "0", "4096", "65536"},
	"auto-vacuum-int":         {"", "1s", "5s"},
	"auto-optimize-int":       {"", "1s"},
	"raft-shutdown-stepdown":  {"", "false"},
	"compress-snap-transport": {"", "true"},
	"write-queue-batch-size":  {"", "1", "16"},
	"write-queue-tx":          {"", "true"},
	"raft-election-timeout":   {"", "500ms", "2s"},
}

// drawConfig draws a configuration, one value per flag in a fixed order so a
// seed always draws the same one.
func drawConfig(rng *rand.Rand) chaosConfig {
	c := chaosConfig{}
	for _, flag := range slices.Sorted(maps.Keys(configSpace)) {
		choices := configSpace[flag]
		if v := choices[rng.IntN(len(choices))]; v != "" {
			c[flag] = v
		}
	}
	// Raft requires the heartbeat timeout not exceed the election timeout.
	if et, ok := c["raft-election-timeout"]; ok {
		c["raft-heartbeat-timeout"] = et
	}
	return c
}

// flags renders the configuration as rqlited flags, in a stable order.
func (c chaosConfig) flags() []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(c)) {
		out = append(out, "-"+k+"="+c[k])
	}
	return out
}
