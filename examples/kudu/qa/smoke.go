//go:build unix

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
)

// smokeJob is kudu.smoke: a cluster of three masters and three tablet
// servers comes up, takes a table, and reads back what was written to it,
// at the timestamps Kudu gave the writes.
type smokeJob struct {
	torx.JobBase
	cluster *kudu.Cluster
	client  *kudu.ClientNode
}

func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.cluster = kudu.New(clusterName, 3, kudu.Config{Masters: 3, DataDirs: 2})
	j.client = kudu.NewClientNode(clientName, j.cluster, 1)
	jc.Register(j.cluster)
	jc.Register(j.client)
}

func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	c := kudu.NewClient(j.client.URL(), "smoke")
	if err := waitTServers(ctx, j.cluster, j.client.Node(), 3); err != nil {
		return err
	}
	res, err := c.CreateTable(ctx, kudu.Table{Name: "kv", HashBuckets: 3, Replicas: 3}, time.Minute)
	if err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	var last int64
	for k := range int64(10) {
		w, err := c.Write(ctx, "kv", "", 10*time.Second, []kudu.Op{{Op: "insert", K: k, V: fmt.Sprint("w", k), P: "p"}})
		if err != nil {
			return err
		}
		if w.Error != "" || len(w.Results) != 1 || w.Results[0].Code != "OK" {
			return fmt.Errorf("insert %d: %+v", k, w)
		}
		last = w.Results[0].TS
	}
	// A snapshot is exclusive of its timestamp: the last write is not in a
	// scan at its own.
	for _, tc := range []struct {
		ts   int64
		want int
	}{{last, 9}, {last + 1, 10}} {
		r, err := c.Read(ctx, kudu.ReadReq{Table: "kv", Mode: "snapshot", TS: tc.ts})
		if err != nil {
			return err
		}
		if r.Error != "" || len(r.Rows) != tc.want {
			return fmt.Errorf("snapshot at %d: want %d rows, got %+v", tc.ts, tc.want, r)
		}
	}
	out, err := kudu.Tool(ctx, j.client.Node(), "cluster", "ksck", j.cluster.MasterAddrs(), "-checksum_scan")
	if err != nil {
		return fmt.Errorf("ksck: %w\n%s", err, out)
	}
	jc.SetSummary("3 masters and 3 tablet servers took a table and read back its writes")
	return nil
}

// waitTServers waits for want tablet servers to have registered with the
// leader master, as `kudu tserver list` run on node from sees them.
func waitTServers(ctx context.Context, c *kudu.Cluster, from *torx.Node, want int) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		out, err := kudu.Tool(ctx, from, "tserver", "list", c.MasterAddrs(), "-format=json")
		if err != nil {
			last = err.Error()
			return false, nil
		}
		last = out
		return countJSONArray(out) >= want, nil
	}, 500*time.Millisecond)
	if err != nil {
		return fmt.Errorf("waiting for %d tablet servers: %w (last: %s)", want, err, last)
	}
	return nil
}
