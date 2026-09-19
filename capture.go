//go:build unix

// Capturing a service process's output to a node-local file.
//
// StartCaptured is the simple path for a service that just wants its console
// output preserved: it launches the process with stdout and stderr redirected to
// a file on the node and registers that file for collection, so the framework
// gathers it into the results tree after the job. A service that produces more --
// a --log-file target, a data dump -- adds those with AddArtifact; one that needs
// full control can override Artifacts. Redirection uses a POSIX shell on the
// node, so the captured output lands on the node's own disk rather than streaming
// back through an unread pipe.
//
// A service may launch more than one process on a node over a job -- a failover
// test crashes one and starts another in its place -- and each launch lands in
// the same file. A CapturePolicy says what becomes of the previous process's
// output when that happens.

package torx

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// CapturePolicy says what StartCaptured does with the output of a process it
// launched earlier on the same node when it launches the next one there. It
// applies only within a run: a log left on the node by an earlier run is
// removed before the first launch whatever the policy, so a readiness check
// never matches stale content and a collected artifact is never a prior run's
// output.
type CapturePolicy int

const (
	// CaptureTruncate discards the previous process's output, so stdout.log only
	// ever holds the output of the process launched last. This is the default.
	CaptureTruncate CapturePolicy = iota
	// CaptureRotate keeps the previous process's output: before the k+1-th launch
	// on a node, stdout.log is moved aside as stdout.<k>.log and registered for
	// collection beside the new stdout.log, so the results tree holds every
	// incarnation's output in order -- what a crashed process logged up to its
	// crash next to what its replacement logged after.
	CaptureRotate
)

// captureLog is the name of the file StartCaptured redirects output to.
const captureLog = "stdout.log"

// SetCapturePolicy sets what StartCaptured does with a previous process's
// output when it launches another process on the same node. Set it at
// construction, before any launch; the default is CaptureTruncate.
func (b *ServiceBase) SetCapturePolicy(p CapturePolicy) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.capture = p
}

// StartCaptured starts cmd on n as a long-running process with its combined
// stdout and stderr redirected to a node-local file (stdout.log under the
// service's per-node scratch), registers that file as an artifact to collect, and
// returns the process: Signal and Wait address the program itself, Close kills
// it and reaps it, and Shutdown composes the three into a graceful stop. Its
// reader carries nothing, since the output goes to the file. Env, Dir, and
// Stdin from cmd are applied to the launched process.
//
// The file only ever holds output of the process just started. A log left at
// that path by an earlier run is removed before the first launch on a node,
// so a readiness check that polls it (e.g. WaitForLog) cannot be satisfied by
// stale content and the collected artifact is never a prior run's output. On a
// later launch on the same node, the previous process's output is discarded or
// kept as a separate artifact according to the service's CapturePolicy.
func (b *ServiceBase) StartCaptured(ctx context.Context, n *Node, cmd Cmd) (Process, error) {
	dir := n.ServiceScratch(b.name).Root
	if err := n.Mkdir(ctx, dir); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, captureLog)

	b.mu.Lock()
	launched := b.launches[n.Name()]
	policy := b.capture
	b.mu.Unlock()

	// Clearing the path must complete before the launch: callers may poll the
	// log for readiness as soon as control returns, while the child performs its
	// truncating redirect only once the scheduler runs it. A leftover file would
	// satisfy such a poll in that window, letting the caller proceed (and tear
	// the service down) before the new process has written anything.
	if err := b.clearCaptureLog(ctx, n, dir, launched, policy); err != nil {
		return nil, err
	}

	Logf(ctx, "info", "exec %s on %s", strings.Join(append([]string{cmd.Path}, cmd.Args...), " "), n.Name())

	// exec so the shell is replaced by the service: the handle's Signal reaches
	// it directly and no extra shell process lingers in the group.
	script := "exec " + shJoin(cmd.Path, cmd.Args) + " > " + shQuote(logPath) + " 2>&1"
	handle, err := n.Stream(ctx, Cmd{
		Path:  "sh",
		Args:  []string{"-c", script},
		Env:   cmd.Env,
		Dir:   cmd.Dir,
		Stdin: cmd.Stdin,
	})
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	if b.launches == nil {
		b.launches = make(map[string]int)
	}
	b.launches[n.Name()]++
	b.mu.Unlock()
	if launched == 0 {
		// Every launch on the node writes the same path, so one registration
		// covers them all.
		b.AddArtifact(n, Artifact{Name: captureLog, Path: logPath, CollectOnPass: true})
	}
	return handle, nil
}

// clearCaptureLog makes way for the next launch on n, where launched counts
// the processes launched there so far in this run. Before the first, any log
// at the path is an earlier run's and is removed. Before a later one, the log
// is the previous process's output, which the policy discards or moves aside
// as stdout.<launched>.log and registers for collection. A rotation with no
// log to move -- the previous launch never wrote one -- registers nothing.
func (b *ServiceBase) clearCaptureLog(ctx context.Context, n *Node, dir string, launched int, policy CapturePolicy) error {
	logPath := filepath.Join(dir, captureLog)
	if launched == 0 || policy != CaptureRotate {
		return n.Rm(ctx, logPath)
	}
	exists, err := n.Exists(ctx, logPath)
	if err != nil || !exists {
		return err
	}
	name := fmt.Sprintf("stdout.%d.log", launched)
	rotated := filepath.Join(dir, name)
	// A backend has no rename operation, so the move runs as a command on the
	// node, which has mv wherever it has the sh the launch itself needs.
	res, err := n.Exec(ctx, Command("mv", logPath, rotated))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return Wrap(ErrBackend, "capture: rotate "+logPath,
			fmt.Errorf("mv exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr))))
	}
	Logf(ctx, "info", "rotated %s to %s on %s", captureLog, name, n.Name())
	b.AddArtifact(n, Artifact{Name: name, Path: rotated, CollectOnPass: true})
	return nil
}

// shJoin renders an argv as a single shell word list, each token quoted.
func shJoin(path string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, shQuote(path))
	for _, a := range args {
		parts = append(parts, shQuote(a))
	}
	return strings.Join(parts, " ")
}

// shQuote single-quotes s for safe use in a POSIX shell command, escaping any
// embedded single quotes.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
