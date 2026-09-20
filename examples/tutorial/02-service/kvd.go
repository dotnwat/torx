//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/tutorial/kvd/client"
)

// binary is the server, resolved from the node's PATH under this fixed name.
// torx never stages the system under test: whoever prepares a node
// guarantees it is there. For the local pool that is you, by building kvd
// onto your PATH (see README.md); later steps hand the chore to a launcher.
const binary = "kvd"

const (
	// readyTimeout bounds how long kvd may take to report ready after a
	// start. It replays its log before it listens, which takes milliseconds
	// for a fresh one, so a node not ready after this long is stuck, and the
	// wait must fail rather than hang the job.
	readyTimeout = 30 * time.Second
	// stopGrace is how long kvd gets to exit after SIGTERM before it is
	// killed. It finishes the requests in flight and closes its log, well
	// under a second.
	stopGrace = 5 * time.Second
)

// Service runs one kvd on one node.
//
// A service embeds *torx.ServiceBase and implements the four per-node hooks.
// ServiceBase turns them into the Start/Wait/Stop/Clean lifecycle the
// framework drives: before a job it stops and cleans the node and then starts
// it, so kvd always begins from a known state, and after the job it stops it,
// collects what it logged, and cleans up.
type Service struct {
	*torx.ServiceBase

	mu   sync.Mutex
	port int          // the leased port; 0 until the first start
	proc torx.Process // the running kvd; nil when it is not running
}

// New builds a service named name that needs one node. Homogeneous(count,
// spec) is the node demand: the framework sizes the job from it before
// anything runs, and an empty spec matches any node.
func New(name string) *Service {
	s := &Service{}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(1, torx.NodeSpec{}), s)
	return s
}

// StartNode leases a port and launches kvd on n behind it. kvd binds every
// interface, so a client that is not on the node -- the job, which runs in
// the worker process -- can reach it, and the service advertises the node's
// reachable address (Addr) rather than the loopback. StartCaptured runs the
// process with its output redirected to a node-local file that is collected
// into the results tree after the job, and returns the process to signal,
// wait for, and stop.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	if err := preflight(ctx, n); err != nil {
		return err
	}
	port, err := n.AllocatePort()
	if err != nil {
		return err
	}
	cmd := torx.Command(binary, "serve",
		"--listen", "0.0.0.0",
		"--port", strconv.Itoa(port),
		"--dir", s.dataDir(n),
	)
	proc, err := s.StartCaptured(ctx, n, cmd)
	if err != nil {
		n.ReleasePort(port) // it never came up; do not leak the lease
		return err
	}
	s.mu.Lock()
	s.port, s.proc = port, proc
	s.mu.Unlock()
	return nil
}

// WaitNode blocks until kvd answers its readiness endpoint. Readiness is a
// real check, never a sleep: WaitForHTTP polls until the server answers
// below 500, bounded here by readyTimeout for the whole wait.
func (s *Service) WaitNode(ctx context.Context, n *torx.Node) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	return torx.WaitForHTTP(ctx, "http://"+s.Addr()+"/readyz")
}

// StopNode stops kvd the way an operator would: SIGTERM, a grace period to
// exit on its own, and a SIGKILL of its whole process group if it has not.
// A process that had to be killed, or that exited unclean, is noted rather
// than failed: the node is clean either way, and an error from a teardown
// hook marks the node dirty and quarantines it. The port goes back to the
// allocator once the process is down.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	port, proc := s.port, s.proc
	s.port, s.proc = 0, nil
	s.mu.Unlock()
	if proc == nil {
		return nil
	}
	code, err := torx.Shutdown(ctx, proc, syscall.SIGTERM, stopGrace)
	switch {
	case errors.Is(err, torx.ErrShutdownTimeout):
		torx.Logf(ctx, "warn", "kvd on %s did not exit within %v of SIGTERM and was killed", n.Name(), stopGrace)
		err = nil
	case err == nil && code != 0:
		torx.Logf(ctx, "warn", "kvd on %s exited with status %d on SIGTERM", n.Name(), code)
	}
	n.ReleasePort(port)
	return err
}

// CleanNode removes the service's scratch directory on n: kvd's data
// directory and its captured output.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Addr returns the host:port a client dials to reach kvd, or "" before the
// service has started.
func (s *Service) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.port == 0 {
		return ""
	}
	return net.JoinHostPort(s.node().Addr(), strconv.Itoa(s.port))
}

// Client returns a client for kvd's HTTP API.
func (s *Service) Client() *client.Client {
	return client.New(s.Addr())
}

// preflight checks that kvd is on n's PATH by running its version command
// through the node -- n.Exec runs a command to completion and returns what
// it printed and how it exited. Without the check, a node that was not
// prepared fails at the end of the readiness wait with a timeout, and the
// reason is only in the collected log; with it, the job fails at once and
// the error says what to do.
func preflight(ctx context.Context, n *torx.Node) error {
	res, err := n.Exec(ctx, torx.Command(binary, "version"))
	if err != nil {
		return fmt.Errorf("%s is not on the PATH of %s (see README.md): %w", binary, n.Name(), err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("%s version on %s exited %d: %s", binary, n.Name(), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// node is the one node the service runs on. Bind hands it over before Start.
func (s *Service) node() *torx.Node {
	return s.Nodes()[0]
}

// dataDir is where kvd keeps its log on n: under the service's own scratch
// directory, which is disjoint from every other service's on the node.
func (s *Service) dataDir(n *torx.Node) string {
	return n.ServiceScratch(s.Name()).Sub("data")
}
