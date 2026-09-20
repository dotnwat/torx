//go:build unix

// Command 01-hello is the first step of the torx tutorial: a suite with two
// jobs and no service. One passes, one fails, and together they show what a
// suite binary is, how a job is registered and selected, what a run leaves
// in the results tree, and how a failure looks there. README.md walks
// through it.
package main

import (
	"context"
	"errors"

	"github.com/dotnwat/torx"
)

// main hands the process to torx. When you run the binary, it is the driver:
// it discovers the jobs registered below, runs each in a worker subprocess,
// and prints the results. A worker is this same binary, re-executed with
// the "worker" argument.
func main() { torx.Main() }

// Jobs are registered by a stable id in an init function, so that the driver
// can discover them and a worker can rebuild the one it is handed.
func init() {
	torx.Register("hello.pass", func() torx.Job { return &passJob{} })
	torx.Register("hello.fail", func() torx.Job { return &failJob{} })
}

// passJob is the smallest possible job: it declares no services, logs a line,
// records a one-line summary, and returns nil, which is a pass.
type passJob struct{ torx.JobBase }

// Declare is where a job registers the services it needs. This one needs none.
func (*passJob) Declare(*torx.JobContext) {}

// Run is the test body. Return nil to pass, an error to fail.
func (*passJob) Run(ctx context.Context, jc *torx.JobContext) error {
	jc.Log("info", "hello from a torx job")
	jc.SetSummary("nothing to test, and it passed")
	return nil
}

// failJob returns an error, which is how a job fails. The error's text is what
// the results record.
type failJob struct{ torx.JobBase }

func (*failJob) Declare(*torx.JobContext) {}

func (*failJob) Run(ctx context.Context, jc *torx.JobContext) error {
	jc.Log("info", "about to fail on purpose")
	return errors.New("this job always fails")
}
