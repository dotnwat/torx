//go:build unix

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/tutorial/kvd/client"
)

// NEW in step 4: the fault jobs. They inject faults only through the
// service's Crash, Shutdown, and Restart.

// faultKeys is how many keys the fault jobs write before the fault.
const faultKeys = 100

// durabilityJob is kv.durability: writes survive a crash. It writes a set
// of keys, kills kvd without warning, restarts it on the same data
// directory, waits for it to be ready, and reads every key back. The time
// from the restart to readiness is recorded on the result.
type durabilityJob struct {
	torx.JobBase
	db *Service
}

func (j *durabilityJob) Declare(jc *torx.JobContext) {
	j.db = New(serviceName)
	jc.Register(j.db)
}

func (j *durabilityJob) Run(ctx context.Context, jc *torx.JobContext) error {
	want, err := writeKeys(ctx, j.db.Client(), faultKeys)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("wrote %d keys; crashing kvd", len(want)))
	if err := j.db.Crash(ctx); err != nil {
		return err
	}

	start := time.Now()
	if err := j.db.Restart(ctx); err != nil {
		return err
	}
	// Wait is the same readiness wait the framework did before Run: it
	// returns once the new process answers, having replayed its log.
	if err := j.db.Wait(ctx); err != nil {
		return err
	}
	restart := time.Since(start)
	jc.Log("info", fmt.Sprintf("kvd is back at %s after %s", j.db.Addr(), restart.Round(time.Millisecond)))

	// A fresh client: the old one's connections died with the process.
	if err := verifyKeys(ctx, j.db.Client(), want); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("%d keys survived a crash; kvd was back in %s", len(want), restart.Round(time.Millisecond)))
	return jc.Record(map[string]any{"keys": len(want), "restart_ms": restart.Milliseconds()})
}

// gracefulJob is kv.graceful: the stop itself is under test. It writes keys,
// has the service stop kvd the way an operator would and hold it to that --
// an exit on SIGTERM, within the grace period, with status 0 -- then
// restarts it and checks the keys are there. Where the durability job
// checks the data survives the worst case, this one checks the server's
// shutdown path works at all; a server that ignored the signal, or exited
// unclean, would pass the first and fail here.
type gracefulJob struct {
	torx.JobBase
	db *Service
}

func (j *gracefulJob) Declare(jc *torx.JobContext) {
	j.db = New(serviceName)
	jc.Register(j.db)
}

func (j *gracefulJob) Run(ctx context.Context, jc *torx.JobContext) error {
	want, err := writeKeys(ctx, j.db.Client(), faultKeys)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("wrote %d keys; stopping kvd with SIGTERM", len(want)))
	start := time.Now()
	if err := j.db.Shutdown(ctx); err != nil {
		return err
	}
	stop := time.Since(start)
	jc.Log("info", fmt.Sprintf("kvd exited cleanly in %s", stop.Round(time.Millisecond)))

	if err := j.db.Restart(ctx); err != nil {
		return err
	}
	if err := j.db.Wait(ctx); err != nil {
		return err
	}
	if err := verifyKeys(ctx, j.db.Client(), want); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("kvd stopped cleanly in %s and came back with all %d keys", stop.Round(time.Millisecond), len(want)))
	return jc.Record(map[string]any{"keys": len(want), "stop_ms": stop.Milliseconds()})
}

// writeKeys writes n distinct keys and returns what it wrote.
func writeKeys(ctx context.Context, c *client.Client, n int) (map[string]string, error) {
	want := make(map[string]string, n)
	for i := range n {
		key, value := fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i)
		if err := c.Put(ctx, key, value); err != nil {
			return nil, err
		}
		want[key] = value
	}
	return want, nil
}

// verifyKeys reads every key back and fails on the first that is missing or
// wrong.
func verifyKeys(ctx context.Context, c *client.Client, want map[string]string) error {
	for key, value := range want {
		got, err := c.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("after restart: %w", err)
		}
		if got != value {
			return fmt.Errorf("after restart: %s = %q, want %q", key, got, value)
		}
	}
	return nil
}
