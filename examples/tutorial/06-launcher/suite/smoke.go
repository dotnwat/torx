//go:build unix

package main

import (
	"context"
	"fmt"

	"github.com/dotnwat/torx"
)

// smokeJob is kv.smoke: one node, one key written and read back. It is the
// smallest job that proves the binary runs, the service brings it to
// readiness, and the API answers -- the shape every later job elaborates.
type smokeJob struct {
	torx.JobBase
	db *Service
}

// Declare registers the service the job needs. It must be pure: construct
// and register, but do not start anything. The framework calls it once to
// size the job and again, in the worker, to rebuild the job identically.
func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.db = New(serviceName)
	jc.Register(j.db)
}

// Run is the test body. By the time it runs the framework has started every
// declared service and waited for it to be ready.
func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	c := j.db.Client()
	if err := c.Put(ctx, "greeting", "hello, torx"); err != nil {
		return err
	}
	got, err := c.Get(ctx, "greeting")
	if err != nil {
		return err
	}
	if got != "hello, torx" {
		return fmt.Errorf("read back %q, wrote %q", got, "hello, torx")
	}
	st, err := c.Stats(ctx)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("kvd at %s holds %d key(s) after %d put(s) and %d get(s)", j.db.Addr(), st.Keys, st.Puts, st.Gets))
	jc.SetSummary(fmt.Sprintf("kvd at %s answered; wrote and read back %q", j.db.Addr(), got))
	return nil
}
