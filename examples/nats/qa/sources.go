//go:build unix

package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
)

// Streams that copy others.
const (
	// mirrorStream mirrors regStream: it holds the same messages at the
	// same sequences.
	mirrorStream = "regm"
	// aggStream sources regStream, batchStream, and workStream: it holds
	// each of their messages once, in each one's order.
	aggStream = "agg"
	// ctrAggStream is a counter stream that sources ctrStream: each of its
	// counters ends at the value the origin's does.
	ctrAggStream = "ctragg"
)

// aggOrigins are the streams aggStream sources.
var aggOrigins = []string{regStream, batchStream, workStream}

// copyStreams are the streams that copy others.
var copyStreams = []string{mirrorStream, aggStream, ctrAggStream}

// source is how a stream sources origin: with an ephemeral consumer the
// server makes, or, with durable, through the durable push consumer name
// the job made on the origin.
func source(origin, name string, durable bool) map[string]any {
	s := map[string]any{"name": origin}
	if durable {
		s["consumer"] = map[string]any{"name": name, "deliver_subject": "torx.src." + name}
	}
	return s
}

// createCopies makes the streams that copy others, and, with durable, the
// durable consumers on the origins they copy through, acked by flow
// control.
func (j *chaosJob) createCopies(ctx context.Context, admin *nats.Client) error {
	durable := j.settings["durable_sources"] == true
	type src struct{ origin, name string }
	var srcs []src
	srcs = append(srcs, src{regStream, "regm-src"})
	for _, o := range aggOrigins {
		srcs = append(srcs, src{o, "agg-" + o})
	}
	srcs = append(srcs, src{ctrStream, "ctragg-src"})
	if durable {
		for _, s := range srcs {
			cfg := map[string]any{
				"durable_name": s.name, "deliver_subject": "torx.src." + s.name, "ack_policy": "flow_control",
				"idle_heartbeat": int64(time.Second), "num_replicas": j.replicas,
			}
			err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
				rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				err := admin.Request(rctx, "$JS.API.CONSUMER.CREATE."+s.origin+"."+s.name, map[string]any{"stream_name": s.origin, "config": cfg}, nil)
				return err == nil, nil
			}, time.Second)
			if err != nil {
				return fmt.Errorf("create sourcing consumer %s on %s: %w", s.name, s.origin, err)
			}
		}
	}
	mirror := j.streamConfig(mirrorStream)
	delete(mirror, "subjects")
	mirror["mirror"] = source(regStream, "regm-src", durable)
	agg := j.streamConfig(aggStream)
	delete(agg, "subjects")
	var sources []map[string]any
	for _, o := range aggOrigins {
		sources = append(sources, source(o, "agg-"+o, durable))
	}
	agg["sources"] = sources
	ctrAgg := j.streamConfig(ctrAggStream)
	delete(ctrAgg, "subjects")
	ctrAgg["allow_msg_counter"] = true
	ctrAgg["sources"] = []map[string]any{source(ctrStream, "ctragg-src", durable)}
	for _, cfg := range []map[string]any{mirror, agg, ctrAgg} {
		if err := createStream(ctx, admin, cfg); err != nil {
			return err
		}
	}
	return nil
}

// caughtUp waits for the copies to have copied everything their origins
// hold, and returns what it last saw that had not.
func (j *chaosJob) caughtUp(ctx context.Context, admin *nats.Client) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		var not []string
		info := map[string]*nats.StreamInfo{}
		for _, s := range append([]string{regStream, ctrStream, batchStream, workStream}, copyStreams...) {
			si, err := admin.StreamInfo(ctx, s)
			if err != nil {
				not = append(not, fmt.Sprintf("%s: %v", s, err))
				continue
			}
			info[s] = si
		}
		if len(not) == 0 {
			if m := info[mirrorStream]; m.State.LastSeq != info[regStream].State.LastSeq {
				not = append(not, fmt.Sprintf("%s at %d, %s at %d", mirrorStream, m.State.LastSeq, regStream, info[regStream].State.LastSeq))
			}
			var want uint64
			for _, o := range aggOrigins {
				want += info[o].State.Msgs
			}
			if got := info[aggStream].State.Msgs; got != want {
				not = append(not, fmt.Sprintf("%s holds %d messages, its origins %d", aggStream, got, want))
			}
			// Each increment the counter stream sources is a message of its
			// own there.
			if got, want := info[ctrAggStream].State.Msgs, info[ctrStream].State.Msgs; got != want {
				not = append(not, fmt.Sprintf("%s holds %d messages, %s %d", ctrAggStream, got, ctrStream, want))
			}
		}
		if ctx.Err() == nil {
			last = strings.Join(not, "; ")
		}
		return len(not) == 0, nil
	}, 2*time.Second)
	return last, err == nil
}

// checkMirror holds a mirror to its origin: the same messages at the same
// sequences.
func checkMirror(origin, mirror *final) []anomaly {
	var bad []anomaly
	var diffs []string
	for _, seq := range slices.Sorted(maps.Keys(origin.bySeq)) {
		o, m := origin.bySeq[seq], mirror.bySeq[seq]
		if o.Value != m.Value || o.Subject != m.Subject {
			diffs = append(diffs, fmt.Sprintf("%d: %s %s vs %s %s", seq, o.Subject, o.Value, m.Subject, m.Value))
		}
	}
	for _, seq := range slices.Sorted(maps.Keys(mirror.bySeq)) {
		if _, ok := origin.bySeq[seq]; !ok {
			m := mirror.bySeq[seq]
			diffs = append(diffs, fmt.Sprintf("%d: nothing vs %s %s", seq, m.Subject, m.Value))
		}
	}
	if len(diffs) > 0 {
		bad = append(bad, anomaly{Kind: "mirror-divergence", Detail: fmt.Sprintf("%s and its mirror %s differ at %d sequences:\n%s", origin.stream, mirror.stream, len(diffs), strings.Join(diffs[:min(len(diffs), 20)], "\n"))})
	}
	return bad
}

// checkSources holds a stream that sources others to them: it holds each
// of their messages once, nothing else, and each origin's in its order.
func checkSources(origins []*final, agg *final) []anomaly {
	var bad []anomaly
	from := map[string]*final{}
	for _, o := range origins {
		for v := range o.byValue {
			from[v] = o
		}
	}
	var missing, dups, phantoms []string
	for _, o := range origins {
		var prev uint64 // the aggregate's sequence of this origin's last message so far
		var prevValue string
		for _, seq := range slices.Sorted(maps.Keys(o.bySeq)) {
			v := o.bySeq[seq].Value
			at := agg.byValue[v]
			switch {
			case len(at) == 0:
				missing = append(missing, fmt.Sprintf("%s %d %s", o.stream, seq, v))
				continue
			case len(at) > 1:
				dups = append(dups, fmt.Sprintf("%s %d %s at %v", o.stream, seq, v, at))
			}
			if at[0] < prev {
				bad = append(bad, anomaly{Kind: "source-order", Detail: fmt.Sprintf("%s holds %s's %s at %d, before %s at %d", agg.stream, o.stream, v, at[0], prevValue, prev)})
			}
			prev, prevValue = at[0], v
		}
	}
	for v, at := range agg.byValue {
		if from[v] == nil {
			phantoms = append(phantoms, fmt.Sprintf("%s at %v", v, at))
		}
	}
	slices.Sort(phantoms)
	for kind, list := range map[string][]string{"source-missing": missing, "source-duplicate": dups, "source-phantom": phantoms} {
		if len(list) > 0 {
			bad = append(bad, anomaly{Kind: kind, Detail: fmt.Sprintf("%s: %d messages: %s", agg.stream, len(list), strings.Join(list[:min(len(list), 20)], "; "))})
		}
	}
	return bad
}

// checkCounterSource holds a counter stream that sources another to its
// values: each of its counters ends where the origin's does.
func checkCounterSource(origin, agg *final) []anomaly {
	var diffs []string
	for _, subject := range slices.Sorted(maps.Keys(origin.bySubject)) {
		os, as := origin.bySubject[subject], agg.bySubject[subject]
		want := origin.bySeq[os[len(os)-1]].Val
		var got string
		if len(as) > 0 {
			got = agg.bySeq[as[len(as)-1]].Val
		}
		if got != want {
			diffs = append(diffs, fmt.Sprintf("%s: %s vs %s", subject, want, got))
		}
	}
	if len(diffs) == 0 {
		return nil
	}
	return []anomaly{{Kind: "counter-source", Detail: fmt.Sprintf("%s's counters end elsewhere than %s's: %s", agg.stream, origin.stream, strings.Join(diffs, "; "))}}
}
