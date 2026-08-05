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
package torx

import (
	"context"
	"io"
	"path/filepath"
	"strings"
)

// StartCaptured starts cmd on n as a long-running process with its combined
// stdout and stderr redirected to a node-local file (stdout.log under the
// service's per-node scratch), registers that file as an artifact to collect, and
// returns a handle whose Close stops and reaps the process. Env, Dir, and Stdin
// from cmd are applied to the launched process. A log left at that path by a
// previous incarnation is removed before the process is launched, so the file
// only ever holds output of the process just started: a readiness check that
// polls it (e.g. WaitForLog) cannot be satisfied by stale content, and the
// collected artifact is never a prior run's output.
func (b *ServiceBase) StartCaptured(ctx context.Context, n *Node, cmd Cmd) (io.ReadCloser, error) {
	dir := n.ServiceScratch(b.name).Root
	if err := n.Mkdir(ctx, dir); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "stdout.log")

	// The removal must complete before the launch: callers may poll the log for
	// readiness as soon as control returns, while the child performs its
	// truncating redirect only once the scheduler runs it. A leftover file from
	// a previous incarnation would satisfy such a poll in that window, letting
	// the caller proceed (and tear the service down) before the new process has
	// written anything.
	if err := n.Rm(ctx, logPath); err != nil {
		return nil, err
	}

	Logf(ctx, "info", "exec %s on %s", strings.Join(append([]string{cmd.Path}, cmd.Args...), " "), n.Name())

	// exec so the shell is replaced by the service: signals reach it directly and
	// no extra shell process lingers in the group.
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
	b.AddArtifact(n, Artifact{Name: "stdout.log", Path: logPath, CollectOnPass: true})
	return handle, nil
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
