//go:build unix

package rqlite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
)

// binary is the server, resolved from each node's PATH under this fixed name.
// torx never stages the system under test: whoever prepares a node -- the
// harness in the local case, an operator otherwise -- guarantees it is there.
const binary = "rqlited"

const (
	// readyTimeout bounds how long a node may take to report ready after a
	// start or restart. Formation over loopback takes well under a second; a
	// node still not ready after this long is stuck, and the wait must fail
	// rather than hang the job.
	readyTimeout = 60 * time.Second
	// leaderTimeout bounds a wait for a leader. An election after a crash
	// completes within a few election timeouts, one second each by default.
	leaderTimeout = 30 * time.Second
	// probeTimeout bounds each per-member reachability probe behind /nodes,
	// and viewTimeout the whole membership request a leader lookup makes: a
	// member whose request stalls costs the lookup one member's turn, not the
	// caller's whole wait.
	probeTimeout = 2 * time.Second
	viewTimeout  = 5 * time.Second
	// syncTimeout is how long rqlite holds a synced readiness check open
	// before answering 503 for a node still behind the leader. It must stay
	// under the bound torx puts on each readiness probe, or the probe gives
	// up first and the node never reports synced.
	syncTimeout = time.Second
	// stopGrace is how long a node gets to exit after SIGTERM before it is
	// killed. A graceful shutdown -- the leader stepping down, the store
	// snapshotting and closing -- takes well under a second.
	stopGrace = 5 * time.Second
)

// stopPolicy is how the service stops rqlited after grace: SIGTERM, the grace
// period, and, for a process still running after it, SIGQUIT before the kill,
// on which the Go runtime writes every goroutine's stack to the captured log.
// A stop that hangs is a finding, and the stacks are where it hung.
func stopPolicy(grace time.Duration) torx.StopPolicy {
	return torx.StopPolicy{Signal: syscall.SIGTERM, Grace: grace, Dump: syscall.SIGQUIT}
}

// Service runs an rqlite cluster of one rqlited per node.
//
// The first node bootstraps a one-node cluster and each later node joins
// through the nodes started before it. ServiceBase starts nodes one at a
// time in order, but it launches every node before the framework waits on
// any, so a launched predecessor is not a ready one; a joiner gives up after
// a few join attempts, and one that gives up before the seed is ready exits
// for good. StartNode therefore waits for a node's predecessors to be ready
// before launching it, which makes the order a real prerequisite. A node's
// ports are leased when it first starts and kept until the framework stops
// it, not released with its process, because they are its identity to its
// peers. Crash, Shutdown, and Restart replace the process behind the same
// addresses, so a restarted node rejoins as the member that went away rather
// than as a stranger, and the Raft log in its data directory lets it catch
// up.
type Service struct {
	*torx.ServiceBase

	mu      sync.Mutex
	members map[string]*member // node name -> member, from first start to stop
	flags   []string           // extra rqlited flags every launch passes, from SetFlags
	grace   time.Duration      // how long a stop waits for rqlited to exit, from SetStopGrace
	exits   []Exit             // processes that exited without the service stopping them
}

// Exit is an rqlited that exited on its own: not stopped by Crash, Shutdown,
// or the framework's teardown. A server that dies under a test is the most
// basic failure there is, and one a job that injects other faults must not
// mistake for its own doing.
type Exit struct {
	Node string    `json:"node"`
	Code int       `json:"code"` // the exit status, -1 for a signal
	Err  string    `json:"error,omitempty"`
	Time time.Time `json:"time"`
}

// member is a node's place in the cluster: the leased ports that address it
// and the process currently filling that place.
type member struct {
	httpPort int
	raftPort int
	proc     torx.Process // the running rqlited, nil between a Crash or Shutdown and the Restart
	paused   bool         // proc is stopped by SIGSTOP, from Pause until Resume or its end
	stopping bool         // a Shutdown is stopping the member's last process
	// unstopped is the error of a Crash or Shutdown that could not establish
	// its rqlited was gone: the kill could not be carried out, or the transport
	// lost track of the process. The old rqlited may still hold the member's
	// ports and data directory, so Restart refuses, and StopNode fails the
	// teardown with it so the node is quarantined rather than reused.
	unstopped error
}

// New builds a service named name that runs a cluster of nodes rqlited
// processes, one per node.
func New(name string, nodes int) *Service {
	s := &Service{members: map[string]*member{}, grace: stopGrace}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), s)
	// A Restart launches a second rqlited on the node; keep what the crashed one
	// logged up to its crash beside what its replacement logs.
	s.SetCapturePolicy(torx.CaptureRotate)
	return s
}

// SetFlags sets extra rqlited flags, such as "-raft-snap=64", that every
// launch on every node passes, a Restart included. Call it before the service
// starts: a job that draws its configuration at random calls it from Setup.
func (s *Service) SetFlags(flags ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flags = append([]string(nil), flags...)
}

// SetStopGrace sets how long Shutdown and the teardown's stop wait for
// rqlited to exit after SIGTERM before they kill it; stopGrace by default.
func (s *Service) SetStopGrace(grace time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grace = grace
}

// Exits returns the processes that exited without the service stopping them,
// in the order the service noticed.
func (s *Service) Exits() []Exit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Exit(nil), s.exits...)
}

// StartNode waits for the node's predecessors to be ready, leases the node's
// ports on its first start, and launches rqlited behind them. A failed launch
// leaves the member in place for StopNode to release, which the framework's
// teardown guarantees.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	for _, p := range s.predecessors(n) {
		if err := s.WaitNode(ctx, p); err != nil {
			return fmt.Errorf("rqlite: %s cannot join before %s is ready: %w", n.Name(), p.Name(), err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[n.Name()]
	if !ok {
		httpPort, err := n.AllocatePort()
		if err != nil {
			return err
		}
		raftPort, err := n.AllocatePort()
		if err != nil {
			n.ReleasePort(httpPort)
			return err
		}
		m = &member{httpPort: httpPort, raftPort: raftPort}
		s.members[n.Name()] = m
	}
	return s.launchLocked(ctx, n, m)
}

// WaitNode blocks until the node's readiness endpoint reports it serving and
// aware of a leader. rqlite answers 503 until then, which WaitForHTTP treats
// as not ready yet.
func (s *Service) WaitNode(ctx context.Context, n *torx.Node) error {
	return s.waitReady(ctx, n, "/readyz")
}

// WaitSynced blocks until n is ready and has received every log entry the
// leader had committed when the check began: the test that a restarted node
// has caught up with the log, not merely rejoined. Receipt is not
// application -- the entries may still be applying to the node's SQLite
// copy when this returns -- so a read of that copy afterwards must poll for
// what it expects rather than assert it at once.
func (s *Service) WaitSynced(ctx context.Context, n *torx.Node) error {
	return s.waitReady(ctx, n, "/readyz?sync&timeout="+syncTimeout.String())
}

// StopNode stops the node's rqlited, if one is running, and releases its
// ports: the framework stopping a node ends its membership. The stop is the
// graceful one an operator would do -- SIGTERM, on which a leader steps down
// before exiting, then a bounded wait -- so every job's teardown goes through
// rqlite's shutdown path. A process that ignored the signal and had to be
// killed, or that exited unclean, is logged rather than failed: the node is
// clean either way, and a teardown error would mark it dirty and quarantine
// it. Shutdown is where the stop itself is under test. The one stop that does
// fail is an earlier Crash or Shutdown that could not establish its rqlited
// was gone: that node may still be running it, and the teardown must say so.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	m := s.members[n.Name()]
	delete(s.members, n.Name())
	var proc torx.Process
	var paused bool
	if m != nil {
		proc, paused = m.proc, m.paused
		m.proc, m.paused = nil, false
	}
	grace := s.grace
	s.mu.Unlock()
	if m == nil {
		return nil
	}
	err := m.unstopped
	if proc != nil {
		// A paused process cannot act on SIGTERM; let it run to its shutdown.
		if paused {
			_ = proc.Signal(ctx, syscall.SIGCONT)
		}
		code, serr := torx.Stop(ctx, proc, stopPolicy(grace))
		switch {
		case errors.Is(serr, torx.ErrShutdownTimeout):
			torx.Logf(ctx, "warn", "%s did not exit within %v of SIGTERM and was killed", n.Name(), grace)
		case serr != nil:
			err = serr
		case code != 0:
			torx.Logf(ctx, "warn", "%s exited with status %d on SIGTERM", n.Name(), code)
		}
	}
	n.ReleasePort(m.httpPort)
	n.ReleasePort(m.raftPort)
	return err
}

// CleanNode removes the node's scratch directory: the data directory holding
// its SQLite database and Raft log, and the captured output.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Crash kills the node's rqlited outright -- SIGKILL, so no leader stepdown or
// other graceful shutdown runs -- and keeps its ports and data directory, so
// Restart brings the same member back. Shutdown is the graceful counterpart.
// A kill that could not be carried out leaves the member unstopped: the
// process is let go of, but Restart refuses and the teardown fails.
func (s *Service) Crash(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("rqlite: %s is not running", n.Name())
	}
	err := m.proc.Close() // SIGKILL ends a paused process as well
	m.proc, m.paused = nil, false
	if err != nil {
		m.unstopped = fmt.Errorf("rqlite: crashing %s: %w", n.Name(), err)
		return m.unstopped
	}
	return nil
}

// Shutdown stops the node's rqlited gracefully -- SIGTERM, on which a leader
// steps down before exiting -- and keeps its ports and data directory, so
// Restart brings the same member back. Unlike the teardown's stop, this one
// is under test: a process that does not exit within the grace period (it is
// killed then) or exits with a non-zero status fails the call. Those two
// failures leave the node clean, the process being gone either way. Any other
// error -- the signal or the kill could not be delivered, the wait was cut
// short, the transport lost track of the process -- leaves the member
// unstopped: torx.Stop reports a timeout only once its kill went through,
// and past that it does not say whether the process is gone, so it is taken
// to still be there.
//
// The service's lock is not held while the process stops, which can take the
// whole grace period: the rest of the cluster, and the clients reaching it
// through the service, carry on meanwhile, as they would around an operator's
// stop. Restart refuses the member until the stop is done.
func (s *Service) Shutdown(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		s.mu.Unlock()
		return fmt.Errorf("rqlite: %s is not running", n.Name())
	}
	proc, paused, grace := m.proc, m.paused, s.grace
	m.proc, m.paused, m.stopping = nil, false, true
	s.mu.Unlock()

	if paused {
		// A paused process cannot act on SIGTERM. Should the resume fail, the
		// stop's own signal and wait say what state the process is in.
		_ = proc.Signal(ctx, syscall.SIGCONT)
	}
	code, err := torx.Stop(ctx, proc, stopPolicy(grace))

	s.mu.Lock()
	defer s.mu.Unlock()
	m.stopping = false
	if err != nil {
		err = fmt.Errorf("rqlite: shutting down %s: %w", n.Name(), err)
		if !errors.Is(err, torx.ErrShutdownTimeout) {
			m.unstopped = err
		}
		return err
	}
	if code != 0 {
		return fmt.Errorf("rqlite: %s exited with status %d on SIGTERM, want 0", n.Name(), code)
	}
	return nil
}

// Pause stops the node's rqlited in its tracks with SIGSTOP: the process and
// its connections stay, but it does nothing -- no heartbeats, no replies --
// until Resume. To its peers a paused leader is one that went silent, and when
// it resumes it still believes it leads until it hears otherwise: the fault
// that tests whether a deposed leader can serve a stale read or accept a
// write it cannot commit.
func (s *Service) Pause(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGSTOP, true)
}

// Resume continues a node paused by Pause with SIGCONT.
func (s *Service) Resume(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGCONT, false)
}

// signalPause sends sig to the node's running rqlited and records whether it
// is now paused.
func (s *Service) signalPause(ctx context.Context, n *torx.Node, sig syscall.Signal, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("rqlite: %s is not running", n.Name())
	}
	if m.paused == paused {
		return nil
	}
	if err := m.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("rqlite: %v to %s: %w", sig, n.Name(), err)
	}
	m.paused = paused
	return nil
}

// Paused reports whether the node's rqlited is paused.
func (s *Service) Paused(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.paused
}

// Running reports whether the node has an rqlited process, paused or not.
func (s *Service) Running(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.proc != nil
}

// Restart launches rqlited again for a node stopped by Crash or Shutdown,
// behind the same addresses and on the same data directory. Follow it with
// WaitNode or WaitSynced. A member whose stop could not establish its process
// was gone cannot be restarted: the old rqlited may still hold its ports and
// data directory.
func (s *Service) Restart(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil {
		return fmt.Errorf("rqlite: %s has never been started", n.Name())
	}
	if m.unstopped != nil {
		return fmt.Errorf("rqlite: %s cannot restart over a process that may still be running: %w", n.Name(), m.unstopped)
	}
	if m.stopping {
		return fmt.Errorf("rqlite: %s cannot restart while its last process is still stopping", n.Name())
	}
	return s.launchLocked(ctx, n, m)
}

// Addr returns the host:port of the node's HTTP API, or "" for a node the
// service has not started. It is stable across Crash and Restart.
func (s *Service) Addr(n *torx.Node) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[n.Name()]
	if !ok {
		return ""
	}
	return net.JoinHostPort(n.Addr(), strconv.Itoa(m.httpPort))
}

// Client returns a client for the node's HTTP API. It panics for a node the
// service has not started, which is a programming error in the calling job; a
// started node keeps its address through Crash and Restart.
func (s *Service) Client(n *torx.Node) *Client {
	addr := s.Addr(n)
	if addr == "" {
		panic(fmt.Sprintf("rqlite: %s has not been started", n.Name()))
	}
	return NewClient(addr)
}

// Leader returns the node the cluster currently recognizes as leader,
// according to the first running member that reports one. Between a crash
// and the election that follows it, members may still name the old leader,
// or none; the error means no running member reported a leader.
func (s *Service) Leader(ctx context.Context) (*torx.Node, error) {
	for _, n := range s.running() {
		view, err := s.membership(ctx, n)
		if err != nil {
			continue
		}
		if l, ok := LeaderOf(view); ok {
			if ln := s.node(l.ID); ln != nil {
				return ln, nil
			}
		}
	}
	return nil, errors.New("rqlite: no running member reports a leader")
}

// AwaitLeader polls Leader until some member reports one.
func (s *Service) AwaitLeader(ctx context.Context) (*torx.Node, error) {
	return s.awaitLeader(ctx, func(*torx.Node) bool { return true })
}

// AwaitNewLeader polls until a member reports a leader other than old. After
// old is crashed, the survivors keep naming it until their election timeout
// expires and one of them wins the election that follows.
func (s *Service) AwaitNewLeader(ctx context.Context, old *torx.Node) (*torx.Node, error) {
	return s.awaitLeader(ctx, func(n *torx.Node) bool { return n.Name() != old.Name() })
}

// awaitLeader polls Leader, bounded by leaderTimeout, until it reports a node
// accept approves of.
func (s *Service) awaitLeader(ctx context.Context, accept func(*torx.Node) bool) (*torx.Node, error) {
	ctx, cancel := context.WithTimeout(ctx, leaderTimeout)
	defer cancel()
	var leader *torx.Node
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		l, err := s.Leader(ctx)
		if err != nil || !accept(l) {
			return false, nil
		}
		leader = l
		return true, nil
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("rqlite: waiting for a leader: %w", err)
	}
	return leader, nil
}

// membership asks n for its view of the cluster, bounded by viewTimeout.
func (s *Service) membership(ctx context.Context, n *torx.Node) ([]NodeInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, viewTimeout)
	defer cancel()
	return s.Client(n).Nodes(ctx, probeTimeout)
}

// launchLocked starts rqlited for m on n: the one place a process is created,
// shared by StartNode and Restart. The caller holds s.mu.
func (s *Service) launchLocked(ctx context.Context, n *torx.Node, m *member) error {
	if m.proc != nil {
		return fmt.Errorf("rqlite: %s is already running", n.Name())
	}
	args := commandLine(n.Name(), n.Addr(), m.httpPort, m.raftPort, s.peersLocked(n), s.flags, s.dataDir(n))
	proc, err := s.StartCaptured(ctx, n, torx.Command(binary, args...))
	if err != nil {
		return err
	}
	m.proc, m.paused = proc, false
	go s.watch(n, m, proc)
	return nil
}

// watch waits for proc to exit and records the exit if the service did not
// cause it: every stop the service makes takes the process off its member
// first, so a process still on its member when it exits died on its own. The
// member is then left stopped, as a Crash would leave it, so a Restart can
// bring it back. Wait returns once the process is reaped, which the service's
// own stops and the teardown guarantee, so the watcher does not outlive the
// job.
func (s *Service) watch(n *torx.Node, m *member, proc torx.Process) {
	code, err := proc.Wait(context.Background())
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.proc != proc {
		return
	}
	m.proc, m.paused = nil, false
	e := Exit{Node: n.Name(), Code: code, Time: time.Now()}
	if err != nil {
		e.Err = err.Error()
	}
	s.exits = append(s.exits, e)
	_ = proc.Close()
}

// commandLine is the rqlited argv for a member. It binds every interface and
// advertises the node's reachable address, so peers and clients on other
// hosts can dial it while a loopback-only local node works the same way;
// joins through peers when it has any; passes the extra flags; and keeps its
// state under dataDir.
func commandLine(id, host string, httpPort, raftPort int, peers, flags []string, dataDir string) []string {
	args := []string{
		"-node-id", id,
		"-http-addr", net.JoinHostPort("0.0.0.0", strconv.Itoa(httpPort)),
		"-http-adv-addr", net.JoinHostPort(host, strconv.Itoa(httpPort)),
		"-raft-addr", net.JoinHostPort("0.0.0.0", strconv.Itoa(raftPort)),
		"-raft-adv-addr", net.JoinHostPort(host, strconv.Itoa(raftPort)),
	}
	if len(peers) > 0 {
		args = append(args, "-join", strings.Join(peers, ","))
	}
	args = append(args, flags...)
	return append(args, dataDir)
}

// peersLocked lists the Raft addresses of every other member, in node order:
// none for the seed's first start, so it bootstraps alone; the nodes already
// up for each later first start; and the survivors for a restart, which a
// node with state on disk does not need to rejoin but which it checks its
// membership against. The caller holds s.mu.
func (s *Service) peersLocked(self *torx.Node) []string {
	var peers []string
	for _, n := range s.Nodes() {
		if n.Name() == self.Name() {
			continue
		}
		if m, ok := s.members[n.Name()]; ok {
			peers = append(peers, net.JoinHostPort(n.Addr(), strconv.Itoa(m.raftPort)))
		}
	}
	return peers
}

// waitReady polls path on n until it answers below 500, bounded by
// readyTimeout for the whole wait; torx bounds each probe on its own, so a
// probe that stalls costs one attempt rather than the node's readiness window.
func (s *Service) waitReady(ctx context.Context, n *torx.Node, path string) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	return torx.WaitForHTTP(ctx, s.Client(n).URL(path))
}

// predecessors lists the members before n in node order: the nodes n joins
// through on its first start, which must be ready before it is launched. A
// node with no member yet was never started and is not waited for.
func (s *Service) predecessors(n *torx.Node) []*torx.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	var before []*torx.Node
	for _, p := range s.Nodes() {
		if p.Name() == n.Name() {
			break
		}
		if _, ok := s.members[p.Name()]; ok {
			before = append(before, p)
		}
	}
	return before
}

// dataDir is where the node's rqlited keeps its database and Raft log.
func (s *Service) dataDir(n *torx.Node) string {
	return filepath.Join(n.ServiceScratch(s.Name()).Root, "data")
}

// running lists the nodes with a live process that is not paused, in node
// order: the ones that can answer.
func (s *Service) running() []*torx.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	var nodes []*torx.Node
	for _, n := range s.Nodes() {
		if m, ok := s.members[n.Name()]; ok && m.proc != nil && !m.paused {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// node maps an rqlite node id back to the torx node; ids are node names.
func (s *Service) node(id string) *torx.Node {
	for _, n := range s.Nodes() {
		if n.Name() == id {
			return n
		}
	}
	return nil
}
