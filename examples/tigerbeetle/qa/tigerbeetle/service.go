//go:build unix

// Package tigerbeetle deploys a TigerBeetle cluster onto torx nodes, one
// replica per node, and injects the faults a replica's process can take:
// crashes, pauses, restarts with other flags, and a lost data file recovered
// from the rest of the cluster.
package tigerbeetle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// binary is the server, resolved from each node's PATH under this fixed name.
// torx never stages the system under test: whoever prepares a node puts the
// build under test there first, a release, a debug build, or one of main.
const binary = "tigerbeetle"

const (
	// formatTimeout bounds formatting a data file, which writes its whole
	// write-ahead log: a gigabyte, in about a second on a fast disk.
	formatTimeout = 60 * time.Second
	// recoverTimeout bounds `tigerbeetle recover`, which must hear from the
	// cluster before it can write the data file it is recovering.
	recoverTimeout = 60 * time.Second
	// readyTimeout bounds how long a replica may take to listen after it
	// starts: opening a data file reads its superblock and write-ahead log
	// headers, well under a second.
	readyTimeout = 60 * time.Second
	// stopGrace is how long the teardown lets a replica exit after SIGTERM.
	// A replica has no graceful shutdown to run; the signal ends it.
	stopGrace = 5 * time.Second
)

// Exit statuses TigerBeetle documents for its fatal conditions (vsr.FatalReason):
// a replica that exits with one of these stopped on purpose, on an
// environmental condition such as a full disk, rather than on a bug.
const (
	ExitCLI                       = 1
	ExitNoSpaceLeft               = 2
	ExitManifestNodePoolExhausted = 3
	ExitStorageSizeExceedsLimit   = 4
	ExitStorageSizeWouldExceed    = 5
	ExitForestTablesWouldExceed   = 6
	ExitUnknownVSRCommand         = 7
)

// Service runs a TigerBeetle cluster, one replica per node, replica i on the
// i-th node.
//
// Every replica must be told the address of every other before any starts,
// so the service leases one port on each node when it starts and keeps them
// until it stops: they are the replicas' identities to one another and to
// clients. A replica's data file is formatted at its first start and kept
// across Crash and Restart; Recover replaces it with one recovered from the
// cluster, as an operator does for a lost disk.
type Service struct {
	*torx.ServiceBase
	replicas int

	mu      sync.Mutex
	cluster uint64
	ports   map[string]int     // node name -> the replica's port, from Start to Stop
	members map[string]*member // node name -> member, from first start to stop
	flags   []string           // flags every start passes, from SetFlags
	extra   map[int][]string   // replica index -> flags its starts alone pass
	limit   int64              // the size each data directory is limited to
	exits   []Exit             // processes that exited without the service stopping them
}

// Exit is a replica that exited on its own: not stopped by Crash or the
// teardown. Code is its exit status, -1 when a signal ended it; Replica is
// its index.
type Exit struct {
	Node    string    `json:"node"`
	Replica int       `json:"replica"`
	Code    int       `json:"code"`
	Err     string    `json:"error,omitempty"`
	Time    time.Time `json:"time"`
	// Said is the end of what the process logged, from its panic if it
	// logged one.
	Said string `json:"said,omitempty"`
}

// Fatal reports whether the exit is one of TigerBeetle's deliberate fatal
// stops (a full disk, a storage limit) rather than a crash: a panic on a
// failed assertion ends the process with SIGABRT, which reads as -1.
func (e Exit) Fatal() bool {
	return e.Code >= ExitCLI && e.Code <= ExitUnknownVSRCommand
}

// member is a replica: its index, and the process currently filling it.
type member struct {
	index int
	proc  torx.Process // the running replica, nil between a Crash and the Restart
	// paused is whether proc is stopped by SIGSTOP, from Pause until Resume.
	paused bool
	// unstopped is the error of a Crash that could not establish its
	// process was gone. The old process may still hold the replica's port
	// and data file, so Restart refuses, and the teardown fails with it.
	unstopped error
	// lost is whether the replica's data file is gone or half-recovered: a
	// Recover could not finish, and the replica must recover before it
	// starts again.
	lost bool
}

// New builds a service named name that runs a cluster of replicas replicas.
func New(name string, replicas int) *Service {
	return NewWithStandbys(name, replicas, 0)
}

// NewWithStandbys builds a service whose cluster has standbys as well as
// replicas: members that follow the log, one node each after the
// replicas', without taking part in consensus. Standbys are experimental in
// TigerBeetle.
func NewWithStandbys(name string, replicas, standbys int) *Service {
	s := &Service{replicas: replicas, members: map[string]*member{}, extra: map[int][]string{}}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(replicas+standbys, torx.NodeSpec{}), s)
	// A Restart launches another replica on the node; keep what the one
	// before it logged up to its end.
	s.SetCapturePolicy(torx.CaptureRotate)
	return s
}

// SetCluster sets the cluster id the replicas are formatted with. Call it
// before the service starts.
func (s *Service) SetCluster(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cluster = id
}

// Cluster is the cluster id.
func (s *Service) Cluster() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cluster
}

// SetFlags sets the flags every replica's start passes, before the data
// file, such as "--cache-grid=256MiB". They apply from the next launch.
func (s *Service) SetFlags(flags ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flags = append([]string(nil), flags...)
}

// SetReplicaFlags sets flags that replica i's starts alone pass, after the
// ones SetFlags sets: how a job runs its replicas with configurations that
// differ. They apply from the replica's next launch, so a Crash and Restart
// changes a running replica's configuration.
func (s *Service) SetReplicaFlags(i int, flags ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extra[i] = append([]string(nil), flags...)
}

func (s *Service) startFlagsLocked(i int) []string {
	return append(append([]string(nil), s.flags...), s.extra[i]...)
}

// SetDataLimit limits each node's data directory to size bytes, with a
// filesystem of that size mounted over it (diskfault.Limit) before the
// replica's data file is first formatted, so that a job can fill it. A data
// file starts at over a gigabyte, its write-ahead log and client replies
// written out in full, so the limit must leave room above that for the grid
// to grow into. Call it before the service starts, and only for nodes that
// can take disk faults (diskfault.Check).
func (s *Service) SetDataLimit(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = size
}

// DataDir is the directory holding the replica's data file.
func (s *Service) DataDir(n *torx.Node) string {
	return filepath.Join(n.ServiceScratch(s.Name()).Root, "data")
}

// DataFile is the replica's data file.
func (s *Service) DataFile(n *torx.Node) string {
	return filepath.Join(s.DataDir(n), fmt.Sprintf("replica-%d.tigerbeetle", s.Index(n)))
}

// Replicas is how many of the service's members are active replicas; the
// rest are standbys.
func (s *Service) Replicas() int { return s.replicas }

// Standby reports whether n is a standby.
func (s *Service) Standby(n *torx.Node) bool { return s.Index(n) >= s.replicas }

// Index is the replica index of node n, -1 for a node not in the service.
func (s *Service) Index(n *torx.Node) int {
	for i, m := range s.Nodes() {
		if m.Name() == n.Name() {
			return i
		}
	}
	return -1
}

// Addresses are the replicas' addresses in replica order, as --addresses and
// a client take them. They are empty before the service starts.
func (s *Service) Addresses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addressesLocked()
}

func (s *Service) addressesLocked() []string {
	if len(s.ports) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.Nodes()))
	for _, n := range s.Nodes() {
		out = append(out, net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[n.Name()])))
	}
	return out
}

// Addr is replica n's address.
func (s *Service) Addr(n *torx.Node) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[n.Name()]))
}

// NewClient connects a client to the cluster's replicas; a client knows
// nothing of standbys. Each client is a session of its own, and the cluster
// keeps a bounded number of them (clients_max, 64 in a release build),
// evicting the least recently used beyond that.
func (s *Service) NewClient() (tb.Client, error) {
	QuietClientLog()
	addrs := s.Addresses()
	if len(addrs) == 0 {
		return nil, errors.New("tigerbeetle: the service has not started")
	}
	return tb.NewClient(tb.ToUint128(s.Cluster()), addrs[:s.replicas])
}

// Exits returns the processes that exited without the service stopping
// them, in the order the service noticed.
func (s *Service) Exits() []Exit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Exit(nil), s.exits...)
}

// Start leases every replica's port before any replica starts, since each
// needs all their addresses, and then starts them in order.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	err := s.leaseLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.ServiceBase.Start(ctx)
}

func (s *Service) leaseLocked() error {
	if len(s.ports) > 0 {
		return nil
	}
	s.ports = map[string]int{}
	for _, n := range s.Nodes() {
		p, err := n.AllocatePort()
		if err != nil {
			s.releaseLocked()
			return err
		}
		s.ports[n.Name()] = p
	}
	return nil
}

func (s *Service) releaseLocked() {
	for _, n := range s.Nodes() {
		if p, ok := s.ports[n.Name()]; ok {
			n.ReleasePort(p)
		}
	}
	s.ports = nil
}

// Stop stops every replica and then releases their ports.
func (s *Service) Stop(ctx context.Context) error {
	err := s.ServiceBase.Stop(ctx)
	s.mu.Lock()
	s.releaseLocked()
	s.mu.Unlock()
	return err
}

// StartNode formats the replica's data file, limiting its directory first
// if the service limits them, and launches the replica.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.members[n.Name()]; ok {
		return fmt.Errorf("tigerbeetle: %s is already a member", n.Name())
	}
	if s.limit > 0 {
		if err := diskfault.Limit(ctx, n, s.DataDir(n), s.limit); err != nil {
			return err
		}
	} else if err := n.Mkdir(ctx, s.DataDir(n)); err != nil {
		return err
	}
	m := &member{index: s.Index(n)}
	s.members[n.Name()] = m
	if err := s.formatLocked(ctx, n, m); err != nil {
		return err
	}
	return s.launchLocked(ctx, n, m)
}

// formatLocked creates the replica's data file.
func (s *Service) formatLocked(ctx context.Context, n *torx.Node, m *member) error {
	ctx, cancel := context.WithTimeout(ctx, formatTimeout)
	defer cancel()
	member := "--replica="
	if m.index >= s.replicas {
		member = "--standby="
	}
	return exec(ctx, n, binary, "format",
		"--cluster="+strconv.FormatUint(s.cluster, 10),
		member+strconv.Itoa(m.index),
		"--replica-count="+strconv.Itoa(s.replicas),
		s.DataFile(n))
}

// WaitNode blocks until the replica accepts connections on its port. That
// says the process is up and has opened its data file; whether the cluster
// serves requests is for a client to find out. A replica that exits instead
// -- on a flag it refuses, say -- fails the wait with what it said last.
func (s *Service) WaitNode(ctx context.Context, n *torx.Node) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	addr := s.Addr(n)
	var d net.Dialer
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !s.Running(n) {
			for _, e := range slices.Backward(s.Exits()) {
				if e.Node == n.Name() {
					return false, fmt.Errorf("tigerbeetle: %s exited with status %d before it was ready: %s", n.Name(), e.Code, e.Said)
				}
			}
			return false, fmt.Errorf("tigerbeetle: %s is not running", n.Name())
		}
		dctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		c, err := d.DialContext(dctx, "tcp", addr)
		if err != nil {
			return false, nil
		}
		_ = c.Close()
		return true, nil
	}, 100*time.Millisecond)
}

// StopNode ends the replica's process, if it has one. A replica has no
// graceful shutdown: SIGTERM ends it as SIGKILL would, and the teardown
// does not wait on it for longer than stopGrace. A member whose Crash could
// not establish its process was gone fails the teardown, so the node is
// quarantined rather than reused.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	m := s.members[n.Name()]
	delete(s.members, n.Name())
	var proc torx.Process
	if m != nil {
		proc = m.proc
		m.proc, m.paused = nil, false
	}
	s.mu.Unlock()
	if m == nil {
		return nil
	}
	err := m.unstopped
	if proc != nil {
		if _, serr := torx.Shutdown(ctx, proc, syscall.SIGTERM, stopGrace); serr != nil && !errors.Is(serr, torx.ErrShutdownTimeout) {
			err = errors.Join(err, serr)
		}
	}
	return err
}

// CleanNode removes the node's scratch directory, data file and captured
// output both; a limited data directory is unmounted first.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	limited := s.limit > 0
	s.mu.Unlock()
	if limited {
		if err := diskfault.Unlimit(ctx, n, s.DataDir(n)); err != nil {
			return err
		}
	}
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Crash kills the replica outright and keeps its data file, so Restart
// brings the same replica back.
func (s *Service) Crash(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("tigerbeetle: %s is not running", n.Name())
	}
	err := m.proc.Close() // SIGKILL ends a paused process as well
	m.proc, m.paused = nil, false
	if err != nil {
		m.unstopped = fmt.Errorf("tigerbeetle: crashing %s: %w", n.Name(), err)
		return m.unstopped
	}
	return nil
}

// Restart launches the replica again on the data file it had, with the
// flags set for it now. A replica whose Recover could not finish recovers
// first, as an operator would try again, since a half-recovered data file
// does not start.
func (s *Service) Restart(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.stoppedLocked(n)
	if err != nil {
		return err
	}
	if m.lost {
		return s.recoverLocked(ctx, n, m)
	}
	return s.launchLocked(ctx, n, m)
}

// Recover replaces a stopped replica's data file with one recovered from
// the cluster -- `tigerbeetle recover`, the procedure for a replica whose
// disk was lost -- and launches it. Recovering needs a cluster to hear
// from, so the other replicas must be up. A recovered replica syncs its state
// from the others before it takes part in consensus again.
func (s *Service) Recover(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.stoppedLocked(n)
	if err != nil {
		return err
	}
	return s.recoverLocked(ctx, n, m)
}

// recoverLocked replaces the replica's data file with a recovered one and
// launches it. Until a recovery finishes the replica is lost: its data file
// is gone or half-written, and Restart recovers again.
func (s *Service) recoverLocked(ctx context.Context, n *torx.Node, m *member) error {
	m.lost = true
	if err := n.Rm(ctx, s.DataFile(n)); err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, recoverTimeout)
	defer cancel()
	if err := exec(rctx, n, binary, "recover",
		"--cluster="+strconv.FormatUint(s.cluster, 10),
		"--addresses="+strings.Join(s.addressesLocked(), ","),
		"--replica="+strconv.Itoa(m.index),
		"--replica-count="+strconv.Itoa(s.replicas),
		s.DataFile(n)); err != nil {
		return err
	}
	m.lost = false
	return s.launchLocked(ctx, n, m)
}

func (s *Service) stoppedLocked(n *torx.Node) (*member, error) {
	m := s.members[n.Name()]
	switch {
	case m == nil:
		return nil, fmt.Errorf("tigerbeetle: %s has never been started", n.Name())
	case m.unstopped != nil:
		return nil, fmt.Errorf("tigerbeetle: %s cannot restart over a process that may still be running: %w", n.Name(), m.unstopped)
	case m.proc != nil:
		return nil, fmt.Errorf("tigerbeetle: %s is running", n.Name())
	}
	return m, nil
}

// Pause stops the replica in its tracks with SIGSTOP, until Resume.
func (s *Service) Pause(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGSTOP, true)
}

// Resume continues a replica paused by Pause.
func (s *Service) Resume(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGCONT, false)
}

func (s *Service) signalPause(ctx context.Context, n *torx.Node, sig syscall.Signal, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("tigerbeetle: %s is not running", n.Name())
	}
	if m.paused == paused {
		return nil
	}
	if err := m.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("tigerbeetle: %v to %s: %w", sig, n.Name(), err)
	}
	m.paused = paused
	return nil
}

// Paused reports whether the replica is paused.
func (s *Service) Paused(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.paused
}

// Running reports whether the replica has a process, paused or not.
func (s *Service) Running(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.proc != nil
}

// launchLocked starts the replica's process: the one place a process is
// created, shared by StartNode, Restart, and Recover.
func (s *Service) launchLocked(ctx context.Context, n *torx.Node, m *member) error {
	if m.proc != nil {
		return fmt.Errorf("tigerbeetle: %s is already running", n.Name())
	}
	args := []string{"start", "--addresses=" + strings.Join(s.addressesLocked(), ",")}
	args = append(args, s.startFlagsLocked(m.index)...)
	args = append(args, s.DataFile(n))
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
// first, so a process still on its member when it exits ended on its own.
// The member is left stopped, as a Crash leaves it, so Restart can bring it
// back.
func (s *Service) watch(n *torx.Node, m *member, proc torx.Process) {
	code, err := proc.Wait(context.Background())
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.proc != proc {
		return
	}
	m.proc, m.paused = nil, false
	e := Exit{Node: n.Name(), Replica: m.index, Code: code, Time: time.Now()}
	if err != nil {
		e.Err = err.Error()
	}
	// The lock keeps a Restart from rotating the log away before it is read.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if b, err := s.Log(ctx, n, 16<<10); err == nil {
		e.Said = lastWords(string(b))
	}
	s.exits = append(s.exits, e)
	_ = proc.Close()
}

// lastWords is the part of a log worth quoting for an exit: from the last
// panic on, or else the last few lines.
func lastWords(log string) string {
	if i := strings.LastIndex(log, "panic"); i >= 0 {
		if j := strings.LastIndex(log[:i], "\n"); j >= 0 {
			i = j + 1
		}
		log = log[i:]
		if len(log) > 4096 {
			log = log[:4096]
		}
		return log
	}
	lines := strings.Split(strings.TrimRight(log, "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-5):], "\n")
}

// Log returns up to the last max bytes of what the replica's current or
// last process has logged.
func (s *Service) Log(ctx context.Context, n *torx.Node, max int) ([]byte, error) {
	path := filepath.Join(n.ServiceScratch(s.Name()).Root, "stdout.log")
	res, err := n.Exec(ctx, torx.Command("tail", "-c", strconv.Itoa(max), path))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("tigerbeetle: reading %s's log: %s", n.Name(), strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

// normalView matches a replica's log line on entering normal status, which
// names the view it entered and whether it leads it.
var normalView = regexp.MustCompile(`transition_to_normal_from_[a-z_]+_status: view=(?:\d+\.\.)?(\d+) (primary|backup)`)

// Primary returns the replica that leads the newest view any running
// replica reports having entered, going by their logs: the primary of view
// v is replica v mod the replica count. A replica cut off from the cluster
// keeps reporting the view it last saw, so the newest view any reports is
// the best guess, and a guess is all a nemesis needs.
func (s *Service) Primary(ctx context.Context) (*torx.Node, error) {
	best := -1
	for _, n := range s.Nodes() {
		if !s.Running(n) {
			continue
		}
		// A node whose processes are frozen runs no command until thawed,
		// the tail of its log included; its view is left out.
		lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		b, err := s.Log(lctx, n, 1<<20)
		cancel()
		if err != nil {
			continue
		}
		ms := normalView.FindAllSubmatch(b, -1)
		if len(ms) == 0 {
			continue
		}
		if v, err := strconv.Atoi(string(ms[len(ms)-1][1])); err == nil && v > best {
			best = v
		}
	}
	if best < 0 {
		return nil, errors.New("tigerbeetle: no running replica reports a view")
	}
	return s.Nodes()[best%s.replicas], nil
}

// exec runs name args on n and fails on a non-zero exit with what it said.
func exec(ctx context.Context, n *torx.Node, name string, args ...string) error {
	res, err := n.Exec(ctx, torx.Command(name, args...))
	if err != nil {
		return fmt.Errorf("tigerbeetle: %s: %s %s: %w", n.Name(), name, args[0], err)
	}
	if res.ExitCode != 0 {
		out := strings.TrimSpace(string(res.Stderr) + string(res.Stdout))
		if len(out) > 2000 {
			out = out[len(out)-2000:]
		}
		return fmt.Errorf("tigerbeetle: %s: %s %s: exit %d: %s", n.Name(), name, strings.Join(args, " "), res.ExitCode, out)
	}
	return nil
}
