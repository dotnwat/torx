//go:build unix

// Collecting artifacts and running finalizers on teardown.
//
// An Artifact names a file on a node to gather after a job runs. Collect copies
// the ones that apply -- everything on failure, only those marked CollectOnPass
// on success -- from a node to a local directory, best-effort, so one missing
// file does not stop the rest. Finalizers is a LIFO stack of cleanup callbacks a
// job accumulates and runs on teardown, aggregating their errors the way service
// teardown does.

package torx

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// Artifact names a file on a node to collect after a job runs.
type Artifact struct {
	Name          string // logical name and local filename
	Path          string // path on the node
	CollectOnPass bool   // gather even when the job passed; a failure gathers all
	// Dir says Path is a directory, collected as a gzipped tar of it named
	// Name: the data directory of a server, say, registered so that a
	// failed run keeps the state the server left on disk to examine with
	// the system's own tools. The archive holds the directory under its
	// own name. Archiving runs tar on the node, in its scratch directory.
	Dir bool
}

// Archiver is implemented by a service that exposes artifacts to collect after a
// job runs: log files, captured console output, data dumps -- anything on its
// nodes. ServiceBase implements it from the artifacts registered with AddArtifact
// (including any captured by StartCaptured); a service may override Artifacts to
// compute the set however it needs.
type Archiver interface {
	Artifacts(n *Node) []Artifact
}

// ShouldCollect reports whether a should be gathered given the job outcome.
func ShouldCollect(a Artifact, passed bool) bool {
	return !passed || a.CollectOnPass
}

// Collect copies the applicable artifacts from n into destDir, best-effort: it
// gathers every file it can and returns the aggregated error for those it could
// not. Callers collecting from multiple nodes should give each node its own
// destDir to avoid name collisions.
func Collect(ctx context.Context, n *Node, artifacts []Artifact, passed bool, destDir string) error {
	var wanted []Artifact
	for _, a := range artifacts {
		if ShouldCollect(a, passed) {
			wanted = append(wanted, a)
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return Wrap(ErrService, "collect: mkdir "+destDir, err)
	}

	var errs MultiError
	// Directories are archived in the node's scratch directory, made once
	// for all of them; without it, none of them can be collected.
	dirs := slices.ContainsFunc(wanted, func(a Artifact) bool { return a.Dir })
	if dirs {
		if err := n.Mkdir(ctx, n.Scratch().Root); err != nil {
			errs.Append(Wrap(ErrService, "collect: mkdir "+n.Scratch().Root+" for directory archives", err))
			dirs = false
		}
	}
	for _, a := range wanted {
		switch {
		case !a.Dir:
			errs.Append(n.Get(ctx, a.Path, filepath.Join(destDir, a.Name)))
		case dirs:
			errs.Append(collectDir(ctx, n, a, filepath.Join(destDir, a.Name)))
		}
	}
	return errs.Err()
}

// cleanupTimeout bounds the removal of a directory artifact's archive from
// the node, which runs however the collection ended: a node that has
// stalled must not hold the teardown for good.
const cleanupTimeout = 30 * time.Second

// collectDir archives the directory a names on n into the node's scratch
// directory, copies the archive to dest, and removes it from the node.
// Streaming the archive straight off the node, with no copy in scratch, is
// issue #59.
func collectDir(ctx context.Context, n *Node, a Artifact, dest string) error {
	archive := path.Join(n.Scratch().Root, ".artifact-"+a.Name)
	dir := path.Clean(a.Path)
	// tar runs inside the directory and names the members after it, rather
	// than archiving it from its parent by name, so that a path whose last
	// element is a symlink archives the directory it points to, not the
	// link. The S flag keeps the renaming off symlink targets; the -- keeps
	// a name beginning with a dash from being read as an option.
	rename := "s,^\\.," + sedLiteral(path.Base(dir)) + ",S"
	res, err := n.Exec(ctx, Command("tar", "-C", dir, "-czf", archive, "--transform", rename, "--", "."))
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_ = n.Rm(rctx, archive)
	}()
	if err != nil {
		return Wrap(ErrService, "collect: archive "+a.Path, err)
	}
	switch {
	case res.ExitCode == 1:
		// A file changed as tar read it: a process that outlived its stop
		// still writes to the directory, or the directory is the scratch
		// directory, where the archive itself grows. GNU tar exits 1 for
		// that and writes the archive all the same, which is what a failed
		// run needs kept: note it, and keep it.
		Logf(ctx, "warn", "archiving %s on %s: %s", a.Path, n.Name(), strings.TrimSpace(string(res.Stderr)))
	case res.ExitCode != 0:
		return Wrap(ErrService, "collect: archive "+a.Path, res.ExitError("tar"))
	}
	return n.Get(ctx, archive, dest)
}

// sedLiteral quotes s to stand for itself in a sed s command delimited by
// commas.
func sedLiteral(s string) string {
	return strings.NewReplacer(`\`, `\\`, `,`, `\,`, `&`, `\&`).Replace(s)
}

// Finalizers is a LIFO stack of cleanup callbacks run on teardown. The zero
// value is ready to use and safe for concurrent use.
type Finalizers struct {
	mu  sync.Mutex
	fns []func(context.Context) error
}

// Add pushes a cleanup callback.
func (f *Finalizers) Add(fn func(context.Context) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fns = append(f.fns, fn)
}

// Run executes the callbacks in reverse order of registration, running every one
// even if some fail, and returns the aggregated error. The stack is emptied, so
// a second Run is a no-op.
func (f *Finalizers) Run(ctx context.Context) error {
	f.mu.Lock()
	fns := f.fns
	f.fns = nil
	f.mu.Unlock()

	var errs MultiError
	for _, fn := range slices.Backward(fns) {
		errs.Append(fn(ctx))
	}
	return errs.Err()
}
