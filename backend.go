// The Backend seam: how torx acts on a single node.
//
// A Backend runs commands, moves files, signals processes, and streams output
// on one node, hiding whether that node is reached by a local subprocess, SSH,
// a container, or anything else. The interface lives in the core because nodes,
// services, and jobs are all expressed in terms of it; concrete backends
// implement it. LocalBackend (the only one in v1) runs everything as local
// subprocesses against the local filesystem -- no SSH, no agent. Heavier
// backends (SSH, Docker, Kubernetes) belong in their own packages so their
// dependencies stay out of the core.
package torx

import (
	"context"
	"io"
	"syscall"
)

// Cmd describes a command to run on a node. Path plus Args is an argv -- no
// shell is involved -- so for shell features run an explicit shell, e.g.
// Command("sh", "-c", script). Env entries ("KEY=VALUE") are added to the node's
// environment, Dir sets the working directory (empty means the backend default),
// and Stdin, if non-nil, is fed to the command.
type Cmd struct {
	Path  string
	Args  []string
	Env   []string
	Dir   string
	Stdin []byte
}

// Command builds a Cmd from an executable and its arguments.
func Command(path string, args ...string) Cmd {
	return Cmd{Path: path, Args: args}
}

// ExecResult is the outcome of a finished command.
type ExecResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}

// Backend acts on a single node. Every method takes a context for cancellation
// and deadlines and returns an error wrapping ErrBackend when the operation
// itself fails. A non-zero command exit is reported in ExecResult.ExitCode, not
// as an error: Exec returns a nil error whenever the command ran to completion,
// leaving the caller to decide whether a given exit code is a failure.
type Backend interface {
	// Exec runs cmd to completion and returns its captured result.
	Exec(ctx context.Context, cmd Cmd) (ExecResult, error)
	// Stream starts cmd and returns its combined stdout and stderr as a stream;
	// closing the reader terminates the command and reaps it.
	Stream(ctx context.Context, cmd Cmd) (io.ReadCloser, error)
	// Put copies a local file to a path on the node.
	Put(ctx context.Context, localPath, nodePath string) error
	// Get copies a file from the node to a local path.
	Get(ctx context.Context, nodePath, localPath string) error
	// ReadFile returns the contents of a file on the node.
	ReadFile(ctx context.Context, path string) ([]byte, error)
	// WriteFile writes data to a file on the node, creating or truncating it.
	WriteFile(ctx context.Context, path string, data []byte) error
	// Exists reports whether a path exists on the node.
	Exists(ctx context.Context, path string) (bool, error)
	// Mkdir creates a directory on the node, including parents (mkdir -p).
	Mkdir(ctx context.Context, path string) error
	// Rm removes a path on the node, recursively and without error if absent
	// (rm -rf).
	Rm(ctx context.Context, path string) error
	// Signal sends sig to the process with the given pid on the node.
	Signal(ctx context.Context, pid int, sig syscall.Signal) error
}
