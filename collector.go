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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	for _, a := range wanted {
		if a.Dir {
			errs.Append(collectDir(ctx, n, a, filepath.Join(destDir, a.Name)))
			continue
		}
		errs.Append(n.Get(ctx, a.Path, filepath.Join(destDir, a.Name)))
	}
	return errs.Err()
}

// collectDir archives the directory a names on n into the node's scratch
// directory, copies the archive to dest, and removes it from the node.
func collectDir(ctx context.Context, n *Node, a Artifact, dest string) error {
	root := n.Scratch().Root
	if err := n.Mkdir(ctx, root); err != nil {
		return err
	}
	archive := path.Join(root, ".artifact-"+a.Name)
	dir := path.Clean(a.Path)
	res, err := n.Exec(ctx, Command("tar", "-C", path.Dir(dir), "-czf", archive, path.Base(dir)))
	defer func() { _ = n.Rm(context.WithoutCancel(ctx), archive) }()
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return Wrap(ErrService, "collect: archive "+a.Path,
			fmt.Errorf("tar exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr))))
	}
	return n.Get(ctx, archive, dest)
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
