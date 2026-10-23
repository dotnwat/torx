//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/dotnwat/torx/netfault"
	gonats "github.com/nats-io/nats.go"
)

// deposedJob is nats.deposed: a stream's leader, cut off from its peers but
// not from the server a client publishes through, refuses the client's
// conditional writes from its stale state, while the leader its peers
// elected stores them: the client is told a write failed that took effect.
//
// Five servers; a stream of three replicas; a client on a server B that
// holds none. The leader L is cut off from every server but B, so B
// forwards each publish to L and to the new leader N alike, and the client
// takes whichever answer comes first. L's refusal of a write conditional on
// N's last sequence -- "wrong last sequence", from the state it has -- comes
// at once; N's ack, only once it has replicated the write.
type deposedJob struct {
	torx.JobBase
	cluster *nats.Cluster
}

func (j *deposedJob) Declare(jc *torx.JobContext) {
	j.cluster = nats.New(clusterName, 5, nats.Config{})
	jc.Register(j.cluster)
}

const deposedStream = "dep"

func (j *deposedJob) Run(ctx context.Context, jc *torx.JobContext) error {
	nodes := j.cluster.Nodes()
	if err := netfault.Check(ctx, nodes); err != nil {
		return fmt.Errorf("nats.deposed partitions the servers: %w", err)
	}
	admin, err := nats.Connect(j.cluster.URLs(), "admin")
	if err != nil {
		return err
	}
	defer admin.Close()
	cfg := map[string]any{"name": deposedStream, "subjects": []string{"dep.>"}, "num_replicas": 3, "storage": "file"}
	if err := createStream(ctx, admin, cfg); err != nil {
		return err
	}
	si, err := admin.StreamInfo(ctx, deposedStream)
	if err != nil {
		return err
	}
	peers := si.Cluster.Peers()
	leader := si.Cluster.Leader
	var bridge string
	for _, s := range j.cluster.Procs() {
		if !slices.Contains(peers, s) {
			bridge = s
			break
		}
	}
	torx.Logf(ctx, "info", "stream %s on %v, leader %s; the client publishes through %s", deposedStream, peers, leader, bridge)

	cl, err := nats.Connect(j.cluster.URL(bridge), "client")
	if err != nil {
		return err
	}
	defer cl.Close()
	publish := func(value string, expect *uint64) (*nats.PubAck, error) {
		hdr := gonats.Header{}
		hdr.Set(idHeader, value)
		if expect != nil {
			hdr.Set("Nats-Expected-Last-Subject-Sequence", strconv.FormatUint(*expect, 10))
		}
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return cl.Publish(rctx, "dep.x", []byte(value), hdr)
	}
	for i := range 10 {
		if _, err := publish(fmt.Sprintf("before-%d", i), nil); err != nil {
			return fmt.Errorf("publish before the partition: %w", err)
		}
	}

	// Cut the leader off from everyone but the bridge.
	var leaderNode, bridgeNode *torx.Node
	var rest []*torx.Node
	for _, n := range nodes {
		switch j.cluster.ByNode(n) {
		case leader:
			leaderNode = n
		case bridge:
			bridgeNode = n
			rest = append(rest, n)
		default:
			rest = append(rest, n)
		}
	}
	if err := netfault.Partition(ctx, []*torx.Node{leaderNode, bridgeNode}, rest); err != nil {
		return err
	}
	defer func() { _ = netfault.Heal(context.WithoutCancel(ctx), nodes...) }()

	// Wait for the peers to elect another leader.
	var elected string
	err = torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		for _, p := range peers {
			if p == leader {
				continue
			}
			rs, err := nats.Replicas(ctx, j.cluster.MonitorURL(p), p, nats.AppAccount)
			if err != nil {
				continue
			}
			if l := rs[deposedStream].Leader; l != "" && l != leader {
				elected = l
				return true, nil
			}
		}
		return false, nil
	}, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("no new leader: %w", err)
	}
	torx.Logf(ctx, "info", "%s elected; %s may not know it yet", elected, leader)

	// Write unconditionally until the new leader acks, then on condition
	// of what it acked, while the old leader may still answer.
	type attempt struct {
		value  string
		expect uint64
		err    error
	}
	var refused []attempt
	var acked int
	partitioned := time.Now()
	var lastRefusal time.Duration
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for i := 0; time.Since(partitioned) < 30*time.Second; i++ {
		<-tick.C
		ack, err := publish(fmt.Sprintf("plain-%d", i), nil)
		if err != nil {
			continue
		}
		// On condition of the subject's last sequence being the one the
		// new leader just acked.
		value := fmt.Sprintf("cas-%d", i)
		e := ack.Seq
		if _, err := publish(value, &e); err != nil {
			refused = append(refused, attempt{value, e, err})
			lastRefusal = time.Since(partitioned)
		} else {
			acked++
		}
	}
	torx.Logf(ctx, "info", "%d conditional writes acked, %d refused, the last refused %v after the new leader was elected", acked, len(refused), lastRefusal.Round(time.Millisecond))

	// Heal, and see which refused writes the stream holds.
	if err := netfault.Heal(ctx, nodes...); err != nil {
		return err
	}
	f, bad := j.finalRead(ctx, admin)
	if f == nil {
		return fmt.Errorf("reading the stream back: %w", bad)
	}
	var lies []string
	for _, a := range refused {
		var api *nats.APIError
		if seqs := f.byValue[a.value]; len(seqs) > 0 && errors.As(a.err, &api) {
			lies = append(lies, fmt.Sprintf("%s on condition of %d: refused with %q, yet the stream holds it at %v", a.value, a.expect, api.Error(), seqs))
		}
	}
	summary := fmt.Sprintf("%d conditional writes refused while %s and %s both led, up to %v after %s was elected; %d of them stored (%d acked)",
		len(refused), leader, elected, lastRefusal.Round(time.Millisecond), elected, len(lies), acked)
	jc.SetSummary(summary)
	if len(lies) > 0 {
		return fmt.Errorf("%s:\n%s", summary, strings.Join(lies[:min(len(lies), 10)], "\n"))
	}
	return nil
}

// finalRead waits for the stream to have one leader all its replicas
// follow, and reads it from that leader.
func (j *deposedJob) finalRead(ctx context.Context, admin *nats.Client) (*final, error) {
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		si, err := admin.StreamInfo(ctx, deposedStream)
		if err != nil || si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			return false, nil
		}
		for _, r := range si.Cluster.Replicas {
			if !r.Current {
				return false, nil
			}
		}
		return true, nil
	}, time.Second)
	if err != nil {
		return nil, err
	}
	msgs, err := readAll(ctx, admin, deposedStream)
	if err != nil {
		return nil, err
	}
	return newFinal(deposedStream, msgs), nil
}
