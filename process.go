//go:build unix

// Process: the handle to a command a backend's Stream started.
//
// A service holds a Process for each long-running command it launches. Through
// it the service reads the command's output, signals it, waits for it to exit,
// and closes it, which kills whatever is left of it and reaps it. Shutdown
// composes those into the usual graceful stop -- a signal, a bounded wait for
// the exit, and then the close -- so a service stops the way an operator would
// and a job can drive a rolling restart or a reload through a real signal.

package torx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Process is a running command started by a backend's Stream. Reading it yields
// the command's combined stdout and stderr. Close kills the command and its
// whole process group and reaps it; it is safe to call more than once, and the
// command is gone once it returns. A Process is safe for concurrent use.
type Process interface {
	io.ReadCloser

	// Signal sends sig to the command itself: the program the backend exec'd,
	// not any shell that launched it. It fails with an error wrapping ErrBackend
	// once the command has exited and been reaped (the cause is then
	// os.ErrProcessDone) or when the signal cannot be delivered.
	Signal(ctx context.Context, sig os.Signal) error

	// Wait blocks until the command has exited or ctx is done, and returns the
	// exit status as the transport reports it: the exit code, or -1 for a
	// command terminated by a signal. A wait cut short by ctx returns an error
	// wrapping ErrBackend and ctx's own error; an exit the transport could not
	// observe returns an error wrapping ErrBackend alone. The command is
	// reaped by the first Wait or Close to reach its exit, and Wait may be
	// called again afterwards -- after a Close, it reports how the command
	// died -- but Close is still required to release the handle.
	Wait(ctx context.Context) (int, error)
}

// Shutdown stops p gracefully: it sends sig, waits up to grace for the command
// to exit, and then closes p, which kills whatever is left and reaps it. It
// returns the command's exit status when the command exited within the grace
// period, whether in response to sig or before it -- a command already gone
// counts as stopped, and its status says how it went. Otherwise the status is
// -1 and the error says why: an error wrapping ErrShutdownTimeout when the
// grace period ran out and the command had to be killed, the signal's own error
// when it could not be delivered to a running command, ctx's error when the
// wait was cut short by the caller, the close's error when the kill could not
// be carried out, or the wait's own error when the transport lost track of the
// command before the grace period was up. A kill that could not be carried
// out is reported ahead of whatever ended the wait, the caller's own
// cancellation included: the command may then still be running, and a caller
// must be able to tell that from a stop that cleaned up. Whatever the
// outcome, p is closed when Shutdown returns.
//
// The signal reaches the command only once it has replaced the shell the
// backend launched it through; stop a command after it has shown it is up (a
// readiness check), not in the moments after its start.
func Shutdown(ctx context.Context, p Process, sig os.Signal, grace time.Duration) (int, error) {
	sigErr := p.Signal(ctx, sig)
	// Wait even when the signal failed: a command that had already exited is
	// what makes Signal fail most often, and its status is the answer then.
	wctx, cancel := context.WithTimeout(ctx, grace)
	code, waitErr := p.Wait(wctx)
	// Note now whether it was the caller's context that ended the wait: the
	// close can take a while (an ssh close waits for the node to report the
	// death), and a caller's deadline that ran out during it must not turn a
	// grace period that ran out into a wait the caller cut short.
	callerErr := ctx.Err()
	cancel()
	closeErr := p.Close()
	switch {
	case waitErr == nil:
		return code, closeErr
	case closeErr != nil:
		// The kill could not be carried out, so the command may still be
		// running: that outranks whatever ended the wait, the caller's own
		// cancellation and a signal that could not be delivered included.
		return -1, closeErr
	case sigErr != nil:
		return -1, sigErr
	case callerErr != nil:
		return -1, waitErr
	case errors.Is(waitErr, context.DeadlineExceeded):
		// The grace period ran out: only wctx's deadline can have done that,
		// as ctx's own error was ruled out above.
		return -1, Wrap(ErrShutdownTimeout, "shutdown", fmt.Errorf("still running %v after %v; killed", sig, grace))
	default:
		// The wait failed on its own before the grace period was up -- the
		// transport lost track of the command -- and the kill went through.
		// That is not a timeout, and a caller that tolerates one (a service
		// that only logs a stubborn process) must not be made to tolerate
		// a broken backend.
		return -1, waitErr
	}
}
