//go:build unix

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
)

// smokeJob is nats.smoke: a three-server cluster stores messages in a
// replicated stream and every replica, led in turn, serves them back.
type smokeJob struct {
	torx.JobBase
	cluster *nats.Cluster
}

func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.cluster = nats.New(clusterName, 3, nats.Config{})
	jc.Register(j.cluster)
}

func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	c, err := nats.Connect(j.cluster.URLs(), "smoke")
	if err != nil {
		return err
	}
	defer c.Close()
	const stream, n = "smoke", 100
	err = torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := c.CreateStream(rctx, map[string]any{"name": stream, "subjects": []string{"smoke.>"}, "num_replicas": 3, "storage": "file"})
		return err == nil, nil
	}, time.Second)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	for i := range n {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ack, err := c.Publish(rctx, fmt.Sprintf("smoke.%d", i%7), fmt.Appendf(nil, "m%d", i), nil)
		cancel()
		if err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
		if ack.Seq != uint64(i+1) {
			return fmt.Errorf("publish %d: acked at %d", i, ack.Seq)
		}
	}
	si, err := c.StreamInfo(ctx, stream)
	if err != nil {
		return err
	}
	for _, s := range si.Cluster.Peers() {
		if err := leadAndRead(ctx, c, stream, s, n); err != nil {
			return err
		}
	}
	jc.SetSummary(fmt.Sprintf("%d messages read back from each of %v", n, si.Cluster.Peers()))
	return nil
}

// leadAndRead moves stream's leadership to server s and reads every
// message back from it.
func leadAndRead(ctx context.Context, c *nats.Client, stream, s string, n int) error {
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		si, err := c.StreamInfo(rctx, stream)
		if err == nil && si.Cluster != nil && si.Cluster.Leader == s {
			return true, nil
		}
		_ = c.StepDown(rctx, stream, s)
		return false, nil
	}, 500*time.Millisecond)
	if err != nil {
		return fmt.Errorf("moving %s's leadership to %s: %w", stream, s, err)
	}
	for i := range n {
		m, err := c.MsgGet(ctx, stream, nats.MsgGetRequest{Seq: uint64(i + 1)})
		if err != nil {
			return fmt.Errorf("%s: get %d: %w", s, i+1, err)
		}
		if m == nil || string(m.Data) != fmt.Sprintf("m%d", i) {
			return fmt.Errorf("%s: get %d: %+v", s, i+1, m)
		}
	}
	return nil
}
