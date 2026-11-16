//go:build unix

// Package slatedb runs the processes of a SlateDB database on torx nodes --
// writers, readers, a standalone compactor, compaction workers, a garbage
// collector -- each a slatedb-node of its own over one database in an S3
// bucket, and injects the faults a process can take: crashes, pauses, and
// clocks that are skewed or jump.
//
// SlateDB is a library, an LSM tree kept in object storage, so the suite
// runs it in slatedb-node (../../node), a small program that opens the
// database in one role and serves its API over HTTP. Every process of the
// database is one; they share nothing but the bucket.
package slatedb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
)

// binary is slatedb-node, resolved from each node's PATH.
const binary = "slatedb-node"

// The roles a process can take.
const (
	Writer    = "writer"    // opens the database for writes, fencing any writer before it
	Reader    = "reader"    // a DbReader, which reads what the writers made durable
	Compactor = "compactor" // a standalone compactor
	Worker    = "worker"    // a compaction worker, which runs a compactor's jobs
	GC        = "gc"        // a standalone garbage collector
)

const (
	// readyTimeout bounds how long a process may take to open the
	// database: a writer replays the WAL its predecessor left.
	readyTimeout = 120 * time.Second
	// stopGrace is how long a stop lets a process close after SIGTERM
	// before it is killed: a writer's close flushes its memtable.
	stopGrace = 30 * time.Second
)

// Store says where the database's bucket is: the object store's endpoint,
// as the nodes reach it, and the bucket.
type Store struct {
	Endpoint string
	Bucket   string
	// Timeout is the object-store client's request timeout.
	Timeout time.Duration
}

// Spec says how to launch a process.
type Spec struct {
	// Role is one of the roles above.
	Role string
	// DB is the database's path in the bucket.
	DB string
	// Settings is a writer's Settings; Options is a reader's
	// DbReaderOptions, a compactor's CompactorOptions, a worker's
	// CompactionWorkerOptions, or a collector's GarbageCollectorOptions.
	// Either is any value that marshals to SlateDB's JSON for it; nil is
	// SlateDB's defaults.
	Settings any
	Options  any
	// ClockOffset skews the process's clock from the node's.
	ClockOffset time.Duration
	// Seed seeds the process's randomness, if nonzero.
	Seed uint64
	// Env is extra environment for the process: RUST_LOG, for one.
	Env []string
	// MergeAppend installs the node's append merge operator; every process
	// of a database must agree on it.
	MergeAppend bool
	// SegmentPrefixLen splits a writer's or reader's keys into segments
	// (RFC 0024) by their first n bytes; 0 for none.
	SegmentPrefixLen int
}

// Exit is a process that exited without the service stopping it.
type Exit struct {
	Proc string    `json:"proc"`
	Node string    `json:"node"`
	Role string    `json:"role"`
	Code int       `json:"code"`
	Err  string    `json:"error,omitempty"`
	Time time.Time `json:"time"`
	// Said is the end of what the process logged.
	Said string `json:"said,omitempty"`
}

// proc is one process slot: a name, the node it runs on, and its process
// while it runs.
type proc struct {
	name   string
	node   *torx.Node
	port   int
	spec   Spec
	proc   torx.Process
	paused bool
	// unstopped is the error of a crash that could not establish the
	// process was gone; it may still hold the port.
	unstopped error
	launches  int
}

// Service runs a database's processes on its nodes. It starts none by
// itself: a job launches each process by name, on the node it picks, and
// the service keeps it until the job, or the teardown, stops it.
type Service struct {
	*torx.ServiceBase
	store Store

	mu    sync.Mutex
	procs map[string]*proc
	exits []Exit
}

// New builds a service named name over nodes nodes.
func New(name string, nodes int) *Service {
	s := &Service{procs: map[string]*proc{}}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), s)
	// Each process logs to <name>.log on its node; one relaunched keeps
	// the log of the one before it as <name>.<k>.log.
	s.SetCapturePolicy(torx.CaptureRotate)
	return s
}

// SetStore says where the bucket is. Call it before the first Launch.
func (s *Service) SetStore(st Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = st
}

// StartNode makes the node's scratch directory; processes start by Launch.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	return n.Mkdir(ctx, n.ServiceScratch(s.Name()).Root)
}

// WaitNode has nothing to wait for.
func (s *Service) WaitNode(context.Context, *torx.Node) error { return nil }

// StopNode stops every process on the node.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	var mine []*proc
	for _, p := range s.procs {
		if p.node.Name() == n.Name() {
			mine = append(mine, p)
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, p := range mine {
		if _, err := s.Terminate(ctx, p.name); err != nil && !errors.Is(err, errNotRunning) {
			errs = append(errs, err)
		}
		s.mu.Lock()
		if p.unstopped != nil {
			errs = append(errs, p.unstopped)
		}
		n.ReleasePort(p.port)
		delete(s.procs, p.name)
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}

// CleanNode removes the node's scratch directory.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

var errNotRunning = errors.New("slatedb: not running")

// Launch starts a process named name on node n, as spec says. A name is a
// slot: Restart launches the slot's process again, with the spec it was
// last launched with, or Relaunch with another.
func (s *Service) Launch(ctx context.Context, name string, n *torx.Node, spec Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.procs[name]; ok {
		return fmt.Errorf("slatedb: %s exists", name)
	}
	port, err := n.AllocatePort()
	if err != nil {
		return err
	}
	p := &proc{name: name, node: n, port: port, spec: spec}
	s.procs[name] = p
	return s.launchLocked(ctx, p)
}

// Relaunch starts the stopped process name again with another spec.
func (s *Service) Relaunch(ctx context.Context, name string, spec Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.stoppedLocked(name)
	if err != nil {
		return err
	}
	p.spec = spec
	return s.launchLocked(ctx, p)
}

// Restart starts the stopped process name again, as it was last launched.
func (s *Service) Restart(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.stoppedLocked(name)
	if err != nil {
		return err
	}
	return s.launchLocked(ctx, p)
}

func (s *Service) stoppedLocked(name string) (*proc, error) {
	p := s.procs[name]
	switch {
	case p == nil:
		return nil, fmt.Errorf("slatedb: no process %s", name)
	case p.unstopped != nil:
		return nil, fmt.Errorf("slatedb: %s cannot restart over a process that may still run: %w", name, p.unstopped)
	case p.proc != nil:
		return nil, fmt.Errorf("slatedb: %s is running", name)
	}
	return p, nil
}

func (s *Service) launchLocked(ctx context.Context, p *proc) error {
	n := p.node
	dir := path.Join(n.ServiceScratch(s.Name()).Root, p.name)
	if err := n.Mkdir(ctx, dir); err != nil {
		return err
	}
	p.launches++
	args := []string{p.spec.Role,
		"--listen", net.JoinHostPort(n.Addr(), strconv.Itoa(p.port)),
		"--db", p.spec.DB,
		"--clock-offset-ms", strconv.FormatInt(p.spec.ClockOffset.Milliseconds(), 10),
	}
	if p.spec.Seed != 0 {
		args = append(args, "--seed", strconv.FormatUint(p.spec.Seed, 10))
	}
	if p.spec.MergeAppend {
		args = append(args, "--merge-append")
	}
	if p.spec.SegmentPrefixLen > 0 && (p.spec.Role == Writer || p.spec.Role == Reader) {
		args = append(args, "--segment-prefix-len", strconv.Itoa(p.spec.SegmentPrefixLen))
	}
	for flag, v := range map[string]any{"--settings": p.spec.Settings, "--options": p.spec.Options} {
		if v == nil {
			continue
		}
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("slatedb: %s %s: %w", p.name, flag, err)
		}
		f := path.Join(dir, fmt.Sprintf("%s-%d.json", strings.TrimPrefix(flag, "--"), p.launches))
		if err := n.WriteFile(ctx, f, b); err != nil {
			return err
		}
		args = append(args, flag, f)
	}
	cmd := torx.Command(binary, args...)
	cmd.Dir = dir
	timeout := s.store.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	cmd.Env = append([]string{
		"SLATEDB_S3_ENDPOINT=" + s.store.Endpoint,
		"SLATEDB_S3_BUCKET=" + s.store.Bucket,
		// The access key names the process to the object store, so its
		// faults can single it out, and its history says who did what.
		"SLATEDB_S3_ACCESS_KEY=" + p.name,
		"SLATEDB_S3_TIMEOUT_MS=" + strconv.FormatInt(timeout.Milliseconds(), 10),
		"RUST_LOG=info",
		"RUST_BACKTRACE=1",
		"RUST_LIB_BACKTRACE=0", // panics only, not every error
	}, p.spec.Env...)
	tp, err := s.StartCapturedAs(ctx, n, p.name, cmd)
	if err != nil {
		return err
	}
	p.proc, p.paused = tp, false
	go s.watch(p, tp)
	return nil
}

func (s *Service) watch(p *proc, tp torx.Process) {
	code, err := tp.Wait(context.Background())
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.proc != tp {
		return // stopped by the service
	}
	p.proc, p.paused = nil, false
	e := Exit{Proc: p.name, Node: p.node.Name(), Role: p.spec.Role, Code: code, Time: time.Now()}
	if err != nil {
		e.Err = err.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if b, err := s.tail(ctx, p.node, p.name, 16<<10); err == nil {
		e.Said = lastLines(string(b), 12)
	}
	s.exits = append(s.exits, e)
	_ = tp.Close()
}

// Log returns the last max bytes process name has logged.
func (s *Service) Log(ctx context.Context, name string, max int) ([]byte, error) {
	s.mu.Lock()
	p := s.procs[name]
	s.mu.Unlock()
	if p == nil {
		return nil, fmt.Errorf("slatedb: no process %s", name)
	}
	return s.tail(ctx, p.node, name, max)
}

func (s *Service) tail(ctx context.Context, n *torx.Node, name string, max int) ([]byte, error) {
	res, err := n.Exec(ctx, torx.Command("tail", "-c", strconv.Itoa(max), s.CapturePath(n, name)))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("slatedb: reading %s's log: %s", name, strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// Exits returns the processes that exited on their own, in order.
func (s *Service) Exits() []Exit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.exits)
}

// URL is process name's API, "http://host:port".
func (s *Service) URL(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	if p == nil {
		return ""
	}
	return "http://" + net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.port))
}

// Node is the node process name runs on.
func (s *Service) Node(name string) *torx.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.procs[name]; p != nil {
		return p.node
	}
	return nil
}

// LaunchSpec is the spec process name was last launched with.
func (s *Service) LaunchSpec(name string) Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.procs[name]; p != nil {
		return p.spec
	}
	return Spec{}
}

// Procs returns the names of the processes of a role, or of every role if
// role is "", sorted.
func (s *Service) Procs(role string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for name, p := range s.procs {
		if role == "" || p.spec.Role == role {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// Running reports whether process name is running, paused or not.
func (s *Service) Running(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	return p != nil && p.proc != nil
}

// Paused reports whether process name is paused.
func (s *Service) Paused(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	return p != nil && p.paused
}

// WaitReady waits for process name to serve: for a writer or reader, to
// have opened the database.
func (s *Service) WaitReady(ctx context.Context, name string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = readyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := s.URL(name) + "/health"
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !s.Running(name) {
			for _, e := range slices.Backward(s.Exits()) {
				if e.Proc == name {
					return false, fmt.Errorf("slatedb: %s exited with status %d before it was ready:\n%s", name, e.Code, e.Said)
				}
			}
			return false, fmt.Errorf("slatedb: %s: %w", name, errNotRunning)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, nil
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	}, 100*time.Millisecond)
}

// Crash kills process name outright, paused or not.
func (s *Service) Crash(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	if p == nil || p.proc == nil {
		return fmt.Errorf("slatedb: %s: %w", name, errNotRunning)
	}
	err := p.proc.Close() // SIGKILL ends a paused process too
	p.proc, p.paused = nil, false
	if err != nil {
		p.unstopped = fmt.Errorf("slatedb: crashing %s: %w", name, err)
		return p.unstopped
	}
	return nil
}

// Terminate stops process name with SIGTERM, which has it close the
// database, and kills it if it has not exited within the grace. It
// reports whether the process exited on its own.
func (s *Service) Terminate(ctx context.Context, name string) (bool, error) {
	s.mu.Lock()
	p := s.procs[name]
	if p == nil || p.proc == nil {
		s.mu.Unlock()
		return false, fmt.Errorf("slatedb: %s: %w", name, errNotRunning)
	}
	tp, paused := p.proc, p.paused
	p.proc, p.paused = nil, false
	s.mu.Unlock()
	if paused {
		_ = tp.Signal(ctx, syscall.SIGCONT)
	}
	_, err := torx.Shutdown(ctx, tp, syscall.SIGTERM, stopGrace)
	clean := err == nil
	if errors.Is(err, torx.ErrShutdownTimeout) {
		err = nil
	}
	if err != nil {
		s.mu.Lock()
		p.unstopped = fmt.Errorf("slatedb: stopping %s: %w", name, err)
		s.mu.Unlock()
	}
	return clean, err
}

// Pause stops process name with SIGSTOP; Resume continues it.
func (s *Service) Pause(ctx context.Context, name string) error {
	return s.signalPause(ctx, name, syscall.SIGSTOP, true)
}

// Resume continues a paused process.
func (s *Service) Resume(ctx context.Context, name string) error {
	return s.signalPause(ctx, name, syscall.SIGCONT, false)
}

func (s *Service) signalPause(ctx context.Context, name string, sig syscall.Signal, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	if p == nil || p.proc == nil {
		return fmt.Errorf("slatedb: %s: %w", name, errNotRunning)
	}
	if p.paused == paused {
		return nil
	}
	if err := p.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("slatedb: %v to %s: %w", sig, name, err)
	}
	p.paused = paused
	return nil
}

// SetClock moves process name's clock to offset from its node's, at once:
// a clock that jumps.
func (s *Service) SetClock(ctx context.Context, name string, offset time.Duration) error {
	var out map[string]any
	return NewClient(s.URL(name)).call(ctx, "/clock", map[string]any{"offset_ms": offset.Milliseconds()}, &out)
}
