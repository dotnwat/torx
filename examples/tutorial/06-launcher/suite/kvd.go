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

// Service runs one kvd on one node, and lets a job crash it, stop it
// gracefully, and restart it in place.
//
// A service embeds *torx.ServiceBase and implements the four per-node hooks.
// ServiceBase turns them into the Start/Wait/Stop/Clean lifecycle the
// framework drives: before a job it stops and cleans the node and then starts
// it, so kvd always begins from a known state, and after the job it stops it,
// collects what it logged, and cleans up. Crash, Shutdown, and Restart are
// the service's own methods, for a job to inject faults through; a job never
// reaches past the service to the process.
type Service struct {
	*torx.ServiceBase

	// mu guards port and proc. The framework calls the four hooks one at a
	// time, so they never race with each other, and the earlier steps had no
	// lock. Crash, Shutdown, and Restart are different: a job calls them
	// from Run, which may be using the service from other goroutines at the
	// same time, clients reading Addr while a fault is injected, and the
	// lock keeps the two fields consistent for all of them.
	mu sync.Mutex
	// port is the leased port. It is kvd's address as far as a job is
	// concerned, so it is kept across Crash, Shutdown, and Restart and only
	// released by StopNode, when the framework stops the service.
	port int
	// proc is the running kvd: nil before the first start, between a Crash
	// or Shutdown and the Restart, and after StopNode.
	proc torx.Process
}

// New builds a service named name that needs one node. Homogeneous(count,
// spec) is the node demand: the framework sizes the job from it before
// anything runs, and an empty spec matches any node.
func New(name string) *Service {
	s := &Service{}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(1, torx.NodeSpec{}), s)
	// A Restart launches a second kvd on the node. Keep what the first one
	// logged up to its crash or stop beside what its replacement logs -- as
	// stdout.1.log next to stdout.log in the results tree -- instead of
	// truncating it, which is the default.
	s.SetCapturePolicy(torx.CaptureRotate)
	return s
}

// StartNode leases a port and launches kvd on n behind it. kvd binds every
// interface, so a client that is not on the node can reach it. The job is one,
// since it runs in the worker process, and the service advertises the node's
// reachable address (Addr) rather than the loopback. A launch that fails
// leaves the port leased for StopNode to release, which the framework's
// teardown guarantees.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	if err := preflight(ctx, n); err != nil {
		return err
	}
	port, err := n.AllocatePort()
	if err != nil {
		return err
	}
	// kvd's data log is worth having when a job fails -- it says what the
	// server had actually recorded -- and noise when it passes. An artifact
	// registered without CollectOnPass is gathered only on failure, into the
	// service's directory in the results tree beside the captured output.
	s.AddArtifact(n, torx.Artifact{Name: "kv.log", Path: s.dataDir(n) + "/kv.log"})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.port = port
	return s.launchLocked(ctx, n)
}

// launchLocked starts kvd behind the leased port on the data directory. It is
// the one place a process is created, shared by StartNode and Restart.
// StartCaptured runs the process with its output redirected to a node-local
// file that is collected into the results tree after the job, and returns
// the process to signal, wait for, and stop. The caller holds s.mu.
func (s *Service) launchLocked(ctx context.Context, n *torx.Node) error {
	if s.proc != nil {
		return fmt.Errorf("kvd on %s is already running", n.Name())
	}
	cmd := torx.Command(binary, "serve",
		"--listen", "0.0.0.0",
		"--port", strconv.Itoa(s.port),
		"--dir", s.dataDir(n),
	)
	proc, err := s.StartCaptured(ctx, n, cmd)
	if err != nil {
		return err
	}
	s.proc = proc
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
// hook marks the node dirty and quarantines it. Shutdown is where the same
// stop is an assertion. The port goes back to the allocator once the
// process is down, or at once if a Crash or Shutdown already took it down.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	port, proc := s.port, s.proc
	s.port, s.proc = 0, nil
	s.mu.Unlock()
	var err error
	if proc != nil {
		code, serr := torx.Shutdown(ctx, proc, syscall.SIGTERM, stopGrace)
		switch {
		case errors.Is(serr, torx.ErrShutdownTimeout):
			torx.Logf(ctx, "warn", "kvd on %s did not exit within %v of SIGTERM and was killed", n.Name(), stopGrace)
		case serr != nil:
			err = serr
		case code != 0:
			torx.Logf(ctx, "warn", "kvd on %s exited with status %d on SIGTERM", n.Name(), code)
		}
	}
	if port != 0 {
		n.ReleasePort(port)
	}
	return err
}

// CleanNode removes the service's scratch directory on n: kvd's data
// directory and its captured output.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Crash kills kvd outright: Close sends SIGKILL to its whole process group,
// so none of its shutdown code runs. The port and the data directory are
// kept, so Restart brings back the same server at the same address, with
// the log the crashed one left. A kill that could not be carried out keeps
// the process: Restart refuses to launch beside it, and the teardown's stop
// tries again and fails the job if it cannot, so a node that may still be
// running a stray kvd is quarantined rather than reused.
func (s *Service) Crash(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil {
		return errors.New("kvd is not running")
	}
	if err := s.proc.Close(); err != nil {
		return fmt.Errorf("crashing kvd on %s: %w", s.node().Name(), err)
	}
	s.proc = nil
	return nil
}

// Shutdown stops kvd gracefully and holds it to that: SIGTERM, a bounded
// wait for it to exit on its own, and an exit status of 0. It is the same
// stop StopNode does at teardown, but where StopNode only notes a process
// that had to be killed or exited unclean, here that fails the call -- the
// stop itself is under test. The port and data directory are kept for
// Restart. A process the grace period ran out on was killed and is gone; on
// any other error -- the signal could not be delivered, the wait was cut
// short, the kill did not go through -- it may still be there, so it is
// kept for the teardown to retry, as after a failed Crash.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil {
		return errors.New("kvd is not running")
	}
	name := s.node().Name()
	code, err := torx.Shutdown(ctx, s.proc, syscall.SIGTERM, stopGrace)
	if err != nil {
		if errors.Is(err, torx.ErrShutdownTimeout) {
			s.proc = nil
		}
		return fmt.Errorf("shutting down kvd on %s: %w", name, err)
	}
	s.proc = nil
	if code != 0 {
		return fmt.Errorf("kvd on %s exited with status %d on SIGTERM, want 0", name, code)
	}
	return nil
}

// Restart launches kvd again after a Crash or Shutdown, behind the same port
// and on the same data directory, so it replays what the previous process
// recorded. Follow it with Wait, which blocks until the new process is ready.
func (s *Service) Restart(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.port == 0 {
		return errors.New("kvd has never been started")
	}
	return s.launchLocked(ctx, s.node())
}

// Addr returns the host:port a client dials to reach kvd, or "" before the
// service has started. It is stable across Crash, Shutdown, and Restart.
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
// through the node. n.Exec runs a command to completion and returns what
// it printed and how it exited. Without the check, a node that was not
// prepared fails at the end of the readiness wait with a timeout, and the
// reason is only in the collected log. With it, the job fails at once and
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
