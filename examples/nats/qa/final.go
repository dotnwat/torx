//go:build unix

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
)

// finalRead reads every message of stream from each of its replicas, as
// the stream's leader in turn, once every fault is healed. It returns the
// messages of the replica that led first, and what it found: replicas
// that disagree with it, a stream it could not read.
func (j *chaosJob) finalRead(ctx context.Context, c *nats.Client, stream string) (*final, []anomaly) {
	var si *nats.StreamInfo
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var err error
		si, err = c.StreamInfo(rctx, stream)
		return err == nil && si.Cluster != nil && si.Cluster.Leader != "", nil
	}, time.Second)
	if err != nil {
		return nil, []anomaly{{Kind: "no-final-read", Detail: fmt.Sprintf("%s has no leader to read it from: %v", stream, err)}}
	}
	var bad []anomaly
	var first []msg
	var firstFrom string
	for _, peer := range si.Cluster.Peers() {
		msgs, err := readReplica(ctx, c, stream, peer)
		if err != nil {
			bad = append(bad, anomaly{Kind: "no-final-read", Detail: fmt.Sprintf("reading %s from %s: %v", stream, peer, err)})
			continue
		}
		if firstFrom == "" {
			first, firstFrom = msgs, peer
			continue
		}
		if d := diffMsgs(first, msgs); d != "" {
			bad = append(bad, anomaly{Kind: "replica-divergence", Detail: fmt.Sprintf("%s on %s and on %s differ:\n%s", stream, firstFrom, peer, d)})
		}
	}
	if firstFrom == "" {
		return nil, bad
	}
	return newFinal(stream, first), bad
}

// readReplica moves stream's leadership to server peer, and reads every
// message from it. The leadership must hold for the whole read.
func readReplica(ctx context.Context, c *nats.Client, stream, peer string) ([]msg, error) {
	for attempt := 0; ; attempt++ {
		if err := leadTo(ctx, c, stream, peer); err != nil {
			return nil, err
		}
		msgs, err := readAll(ctx, c, stream)
		if err != nil {
			return nil, err
		}
		si, err := c.StreamInfo(ctx, stream)
		if err == nil && si.Cluster != nil && si.Cluster.Leader == peer {
			return msgs, nil
		}
		if attempt >= 3 {
			return nil, fmt.Errorf("%s's leadership left %s during every read", stream, peer)
		}
	}
}

// leadTo moves stream's leadership to server s.
func leadTo(ctx context.Context, c *nats.Client, stream, s string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var last string
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		si, err := c.StreamInfo(rctx, stream)
		if err == nil && si.Cluster != nil && si.Cluster.Leader == s {
			return true, nil
		}
		if err == nil && si.Cluster != nil {
			last = "leader " + si.Cluster.Leader
		}
		if err := c.StepDown(rctx, stream, s); err != nil {
			last = err.Error()
		}
		return false, nil
	}, time.Second)
	if err != nil {
		return fmt.Errorf("moving %s's leadership to %s: %w (last: %s)", stream, s, err, last)
	}
	return nil
}

// readAll reads every message of stream from its leader, in order.
func readAll(ctx context.Context, c *nats.Client, stream string) ([]msg, error) {
	var out []msg
	seq := uint64(1)
	for {
		var m *nats.StoredMsg
		var err error
		for attempt := range 5 {
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			m, err = c.MsgGet(rctx, stream, nats.MsgGetRequest{Seq: seq, NextFor: ">"})
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
		}
		if err != nil {
			return nil, fmt.Errorf("get from %d: %w", seq, err)
		}
		if m == nil {
			return out, nil
		}
		mm := msg{Seq: m.Seq, Subject: m.Subject, Value: valueOf(m.Header, m.Data)}
		if stream == ctrStream || stream == ctrAggStream {
			mm.Val, mm.Incr = counterVal(m.Data), m.Header.Get("Nats-Incr")
		}
		if stream == schedStream {
			mm.Scheduler, mm.Time = m.Header.Get("Nats-Scheduler"), m.Time
		}
		out = append(out, mm)
		seq = m.Seq + 1
	}
}

// diffMsgs describes how two replicas' messages differ, or is "" if they
// do not.
func diffMsgs(a, b []msg) string {
	am, bm := map[uint64]msg{}, map[uint64]msg{}
	for _, m := range a {
		am[m.Seq] = m
	}
	for _, m := range b {
		bm[m.Seq] = m
	}
	var seqs []uint64
	for s := range am {
		seqs = append(seqs, s)
	}
	for s := range bm {
		if _, ok := am[s]; !ok {
			seqs = append(seqs, s)
		}
	}
	slices.Sort(seqs)
	var lines []string
	n := 0
	for _, s := range seqs {
		x, xok := am[s]
		y, yok := bm[s]
		if xok == yok && x == y {
			continue
		}
		n++
		if len(lines) < 20 {
			lines = append(lines, fmt.Sprintf("  %d: %s vs %s", s, describe(x, xok), describe(y, yok)))
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d of %d sequences differ (%d messages vs %d):\n%s", n, len(seqs), len(a), len(b), strings.Join(lines, "\n"))
}

func describe(m msg, ok bool) string {
	if !ok {
		return "nothing"
	}
	return m.Subject + " " + m.Value
}

// replicaStates is each stream's state on each server that holds a replica
// of it, as the servers' JetStream reports say.
func (j *chaosJob) replicaStates(ctx context.Context) map[string][]nats.ReplicaState {
	out := map[string][]nats.ReplicaState{}
	for _, s := range j.cluster.Procs() {
		rs, err := nats.Replicas(ctx, j.cluster.MonitorURL(s), s, nats.AppAccount)
		if err != nil {
			torx.Logf(ctx, "warn", "reading %s's JetStream report: %v", s, err)
			continue
		}
		for stream, r := range rs {
			out[stream] = append(out[stream], r)
		}
	}
	return out
}

// describeReplicas says how each replica of streams stands, and how its
// copying of its origins does.
func (j *chaosJob) describeReplicas(ctx context.Context, streams []string) string {
	states := j.replicaStates(ctx)
	var b strings.Builder
	for _, stream := range streams {
		for _, r := range states[stream] {
			fmt.Fprintf(&b, "%s on %s (leader %s): %d messages, last %d", stream, r.Server, r.Leader, r.State.Msgs, r.State.LastSeq)
			for _, src := range append([]*nats.SourceInfo{r.Mirror}, r.Sources...) {
				if src == nil {
					continue
				}
				fmt.Fprintf(&b, "; from %s lag %d active %v", src.Name, src.Lag, src.Active)
				if src.Error != nil {
					fmt.Fprintf(&b, " error %v", src.Error)
				}
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}
