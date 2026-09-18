//go:build unix

package rqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
)

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
// peers. Crash and Restart replace the process behind the same addresses, so
// a restarted node rejoins as the member that went away rather than as a
// stranger, and the Raft log in its data directory lets it catch up.
type Service struct {
	*torx.ServiceBase

	mu      sync.Mutex
	members map[string]*member // node name -> member, from first start to stop
}

// member is a node's place in the cluster: the leased ports that address it,
// the process currently filling that place, and how many processes have.
type member struct {
	httpPort int
	raftPort int
	proc     io.ReadCloser // the running rqlited, nil between Crash and Restart
	starts   int           // processes launched behind these ports so far
}

// New builds a service named name that runs a cluster of nodes rqlited
// processes, one per node.
func New(name string, nodes int) *Service {
	s := &Service{members: map[string]*member{}}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), s)
	return s
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
	return s.waitReady(ctx, n, "/readyz?sync&timeout=1s")
}

// StopNode terminates the node's rqlited, if one is running, and releases its
// ports: the framework stopping a node ends its membership.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	m := s.members[n.Name()]
	delete(s.members, n.Name())
	s.mu.Unlock()
	if m == nil {
		return nil
	}
	var err error
	if m.proc != nil {
		err = m.proc.Close()
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
// Restart brings the same member back.
func (s *Service) Crash(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("rqlite: %s is not running", n.Name())
	}
	err := m.proc.Close()
	m.proc = nil
	return err
}

// Restart launches rqlited again for a crashed node, behind the same addresses
// and on the same data directory. Follow it with WaitNode or WaitSynced.
func (s *Service) Restart(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil {
		return fmt.Errorf("rqlite: %s has never been started", n.Name())
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
	if m.starts > 0 {
		// StartCaptured truncates stdout.log before launching, so the previous
		// incarnation's output -- what a crashed leader logged up to its crash --
		// is set aside first and collected beside the new log.
		if err := s.rotateLog(ctx, n, m.starts); err != nil {
			return err
		}
	}
	args := commandLine(n.Name(), n.Addr(), m.httpPort, m.raftPort, s.peersLocked(n), s.dataDir(n))
	proc, err := s.StartCaptured(ctx, n, torx.Command(binary, args...))
	if err != nil {
		return err
	}
	m.proc = proc
	m.starts++
	return nil
}

// commandLine is the rqlited argv for a member. It binds every interface and
// advertises the node's reachable address, so peers and clients on other
// hosts can dial it while a loopback-only local node works the same way;
// joins through peers when it has any; and keeps its state under dataDir.
func commandLine(id, host string, httpPort, raftPort int, peers []string, dataDir string) []string {
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

// rotateLog moves the captured stdout.log of the node's previous incarnation
// aside as stdout.<k>.log and registers it for collection. A backend has no
// rename operation, so the move runs as a command on the node.
func (s *Service) rotateLog(ctx context.Context, n *torx.Node, k int) error {
	dir := n.ServiceScratch(s.Name()).Root
	name := fmt.Sprintf("stdout.%d.log", k)
	res, err := n.Exec(ctx, torx.Command("mv", filepath.Join(dir, "stdout.log"), filepath.Join(dir, name)))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("rqlite: rotate log on %s: mv exited %d: %s", n.Name(), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	s.AddArtifact(n, torx.Artifact{Name: name, Path: filepath.Join(dir, name), CollectOnPass: true})
	return nil
}

// waitReady polls path on n until it answers below 500, bounded by readyTimeout.
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

// running lists the nodes with a live process, in node order.
func (s *Service) running() []*torx.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	var nodes []*torx.Node
	for _, n := range s.Nodes() {
		if m, ok := s.members[n.Name()]; ok && m.proc != nil {
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
