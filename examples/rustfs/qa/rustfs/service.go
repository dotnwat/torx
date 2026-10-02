//go:build unix

// Package rustfs deploys a RustFS cluster onto torx nodes -- one server per
// node, each with drives of its own, all of them one erasure-coded pool --
// and injects the faults a server's process and drives can take: crashes,
// pauses, restarts with other settings, and drives lost or replaced.
package rustfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/rustfs/qa/s3"
)

// binary is the server, resolved from each node's PATH under this fixed
// name. torx never stages the system under test: whoever prepares a node
// puts the build under test there first, a release or a build of main.
const binary = "rustfs"

// The credentials of the cluster's root user.
const (
	AccessKey = "torxadmin"
	SecretKey = "torxsecret"
)

const (
	// readyTimeout bounds how long a server may take to serve after it
	// starts. A server waits for a quorum of the cluster's drives before it
	// serves, so a server started while too many of its peers are down
	// waits until they come back or this runs out.
	readyTimeout = 90 * time.Second
	// stopGrace is how long a stop lets a server exit after SIGTERM before
	// it is killed.
	stopGrace = 10 * time.Second
)

// Service runs a RustFS cluster: one server per node, each serving drives
// drives, the cluster's drives one pool.
//
// Every server must be told every drive's endpoint, its peers' addresses
// included, before any starts, so the service leases one port on each node
// when it starts and keeps them until it stops. A server's drives are
// directories in its node's scratch, kept across Crash and Restart; a drive
// can be wiped, as a replaced disk is.
type Service struct {
	*torx.ServiceBase
	drives int

	mu      sync.Mutex
	ports   map[string]int     // node name -> the server's port, from Start to Stop
	members map[string]*member // node name -> member, from first start to stop
	env     []string           // environment every start passes, from SetEnv
	extra   map[int][]string   // server index -> environment its starts alone pass
	limit   int64              // the size each drive is limited to, 0 for none
	exits   []Exit             // processes that exited without the service stopping them
}

// Exit is a server that exited on its own: not stopped by Crash or a stop.
// Code is its exit status, -1 when a signal ended it.
type Exit struct {
	Node   string    `json:"node"`
	Server int       `json:"server"`
	Code   int       `json:"code"`
	Err    string    `json:"error,omitempty"`
	Time   time.Time `json:"time"`
	// Said is the end of what the process logged, from its panic if it
	// logged one.
	Said string `json:"said,omitempty"`
}

// member is a server: its index, and the process currently running it.
type member struct {
	index int
	proc  torx.Process // the running server, nil between a Crash and the Restart
	// paused is whether proc is stopped by SIGSTOP, from Pause until Resume.
	paused bool
	// unstopped is the error of a Crash that could not establish its
	// process was gone. The old process may still hold the server's port and
	// drives, so Restart refuses, and the teardown fails with it.
	unstopped error
}

// New builds a service named name that runs a cluster of servers servers,
// each with drives drives.
func New(name string, servers, drives int) *Service {
	s := &Service{drives: drives, members: map[string]*member{}, extra: map[int][]string{}}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(servers, torx.NodeSpec{}), s)
	// A Restart launches another server on the node; keep what the one
	// before it logged up to its end.
	s.SetCapturePolicy(torx.CaptureRotate)
	return s
}

// SetEnv sets environment every server's start passes, "KEY=VALUE", on top
// of what the service sets itself. It applies from the next launch.
func (s *Service) SetEnv(env ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.env = slices.Clone(env)
}

// SetServerEnv sets environment that server i's starts alone pass, after
// SetEnv's: how a job runs servers with settings that differ. It applies
// from the server's next launch, so a Crash and Restart changes a running
// server's settings.
func (s *Service) SetServerEnv(i int, env ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extra[i] = slices.Clone(env)
}

// SetDriveLimit limits each drive to size bytes, with a filesystem of that
// size mounted over it (diskfault.Limit) before the server first starts, so
// that a job can fill it -- and so each drive is a device of its own, as
// RustFS expects of a server's drives. Call it before the service starts,
// and only for nodes that can take disk faults (diskfault.Check).
func (s *Service) SetDriveLimit(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = size
}

// Drives is how many drives each server has.
func (s *Service) Drives() int { return s.drives }

// Drive is the directory of server n's drive d.
func (s *Service) Drive(n *torx.Node, d int) string {
	return filepath.Join(n.ServiceScratch(s.Name()).Root, "drive"+strconv.Itoa(d))
}

// Index is the server index of node n, -1 for a node not in the service.
func (s *Service) Index(n *torx.Node) int {
	return slices.IndexFunc(s.Nodes(), func(m *torx.Node) bool { return m.Name() == n.Name() })
}

// Endpoint is server n's S3 endpoint, "http://host:port".
func (s *Service) Endpoint(n *torx.Node) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return "http://" + net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[n.Name()]))
}

// Client returns a client of server n's S3 endpoint, signed as the root
// user.
func (s *Service) Client(n *torx.Node) (*s3.Client, error) {
	return s3.New(s.Endpoint(n), AccessKey, SecretKey)
}

// Exits returns the processes that exited without the service stopping
// them, in the order the service noticed.
func (s *Service) Exits() []Exit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.exits)
}

// Start leases every server's port before any server starts, since each
// needs all their drives' endpoints, and then starts them.
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

// Stop stops every server and then releases their ports.
func (s *Service) Stop(ctx context.Context) error {
	err := s.ServiceBase.Stop(ctx)
	s.mu.Lock()
	s.releaseLocked()
	s.mu.Unlock()
	return err
}

// volumesLocked is RUSTFS_VOLUMES: every drive of every server, listed one
// by one, which RustFS takes as one pool spanning them all.
func (s *Service) volumesLocked() string {
	var vols []string
	for _, n := range s.Nodes() {
		hp := net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[n.Name()]))
		for d := range s.drives {
			vols = append(vols, "http://"+hp+s.Drive(n, d))
		}
	}
	return strings.Join(vols, " ")
}

// StartNode makes the server's drives, limiting them first if the service
// limits them, and launches the server. Every server starts at once, since
// none serves before a quorum of the drives is up; WaitNode waits.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.members[n.Name()]; ok {
		return fmt.Errorf("rustfs: %s is already a member", n.Name())
	}
	for d := range s.drives {
		var err error
		if s.limit > 0 {
			err = diskfault.Limit(ctx, n, s.Drive(n, d), s.limit)
		} else {
			err = n.Mkdir(ctx, s.Drive(n, d))
		}
		if err != nil {
			return err
		}
	}
	m := &member{index: s.Index(n)}
	s.members[n.Name()] = m
	return s.launchLocked(ctx, n, m)
}

// WaitNode blocks until the server says it is ready to serve: its drives
// and the cluster's format are loaded. A server that exits instead fails
// the wait with what it said last.
func (s *Service) WaitNode(ctx context.Context, n *torx.Node) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	return s.waitReady(ctx, n)
}

func (s *Service) waitReady(ctx context.Context, n *torx.Node) error {
	url := s.Endpoint(n) + "/health/ready"
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !s.Running(n) {
			for _, e := range slices.Backward(s.Exits()) {
				if e.Node == n.Name() {
					return false, fmt.Errorf("rustfs: %s exited with status %d before it was ready: %s", n.Name(), e.Code, e.Said)
				}
			}
			return false, fmt.Errorf("rustfs: %s is not running", n.Name())
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
	}, 200*time.Millisecond)
}

// WaitWritable waits, up to timeout, for any server to say the cluster has
// the quorum of drives a write needs (/minio/health/cluster), which a
// server answers for the whole cluster. A server is ready (WaitReady) at a
// read quorum, sooner.
func (s *Service) WaitWritable(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		for _, n := range s.Nodes() {
			if !s.Running(n) || s.Paused(n) {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Endpoint(n)+"/minio/health/cluster", nil)
			if err != nil {
				return false, err
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true, nil
			}
		}
		return false, nil
	}, 250*time.Millisecond)
	if err != nil {
		return fmt.Errorf("rustfs: the cluster has no write quorum: %w", err)
	}
	return nil
}

// WaitReady waits, up to timeout, for a running server to say it is ready.
func (s *Service) WaitReady(ctx context.Context, n *torx.Node, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.waitReady(ctx, n)
}

// StopNode stops the server, if it has one: SIGTERM, and SIGKILL after
// stopGrace. A member whose Crash could not establish its process was gone
// fails the teardown, so the node is quarantined rather than reused.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	m := s.members[n.Name()]
	delete(s.members, n.Name())
	var proc torx.Process
	if m != nil {
		proc = m.proc
		m.proc = nil
	}
	paused := m != nil && m.paused
	s.mu.Unlock()
	if m == nil {
		return nil
	}
	err := m.unstopped
	if proc != nil {
		if paused {
			_ = proc.Signal(ctx, syscall.SIGCONT)
		}
		if _, serr := torx.Shutdown(ctx, proc, syscall.SIGTERM, stopGrace); serr != nil && !errors.Is(serr, torx.ErrShutdownTimeout) {
			err = errors.Join(err, serr)
		}
	}
	return err
}

// CleanNode removes the node's scratch directory, drives and captured
// output both; limited drives are unmounted first.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	limited := s.limit > 0
	s.mu.Unlock()
	if limited {
		for d := range s.drives {
			if err := diskfault.Unlimit(ctx, n, s.Drive(n, d)); err != nil {
				return err
			}
		}
	}
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Crash kills the server outright and keeps its drives, so Restart brings
// the same server back.
func (s *Service) Crash(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("rustfs: %s is not running", n.Name())
	}
	err := m.proc.Close() // SIGKILL ends a paused process as well
	m.proc, m.paused = nil, false
	if err != nil {
		m.unstopped = fmt.Errorf("rustfs: crashing %s: %w", n.Name(), err)
		return m.unstopped
	}
	return nil
}

// Terminate stops the server as an operator would, with SIGTERM, and kills
// it if it is not gone after stopGrace. It keeps its drives, so Restart
// brings the same server back. It reports whether the server exited before
// the kill.
func (s *Service) Terminate(ctx context.Context, n *torx.Node) (bool, error) {
	s.mu.Lock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		s.mu.Unlock()
		return false, fmt.Errorf("rustfs: %s is not running", n.Name())
	}
	proc, paused := m.proc, m.paused
	// Taken off the member first, so the watcher does not count the exit.
	m.proc, m.paused = nil, false
	s.mu.Unlock()
	if paused {
		_ = proc.Signal(ctx, syscall.SIGCONT)
	}
	_, err := torx.Shutdown(ctx, proc, syscall.SIGTERM, stopGrace)
	clean := err == nil
	if errors.Is(err, torx.ErrShutdownTimeout) {
		err = nil
	}
	if err != nil {
		s.mu.Lock()
		m.unstopped = fmt.Errorf("rustfs: stopping %s: %w", n.Name(), err)
		s.mu.Unlock()
	}
	return clean, err
}

// Restart launches the server again on the drives it had, with the
// environment set for it now.
func (s *Service) Restart(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	switch {
	case m == nil:
		return fmt.Errorf("rustfs: %s has never been started", n.Name())
	case m.unstopped != nil:
		return fmt.Errorf("rustfs: %s cannot restart over a process that may still be running: %w", n.Name(), m.unstopped)
	case m.proc != nil:
		return fmt.Errorf("rustfs: %s is running", n.Name())
	}
	return s.launchLocked(ctx, n, m)
}

// WipeDrive empties server n's drive d, as a disk replaced by a new one
// leaves it. The server may be running: it finds the drive empty the next
// time it looks, as it would a disk swapped under it.
func (s *Service) WipeDrive(ctx context.Context, n *torx.Node, d int) error {
	dir := s.Drive(n, d)
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `cd "$1" && find . -mindepth 1 -maxdepth 1 -exec rm -rf {} +`, "sh", dir))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("rustfs: wiping %s: %s", dir, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// Usage is how many KiB server n's drive d holds, in all and in each of
// its top-level directories -- the buckets, and RustFS's own .rustfs.sys --
// and in each directory of .rustfs.sys: what fills a drive.
func (s *Service) Usage(ctx context.Context, n *torx.Node, d int) (map[string]int64, error) {
	dir := s.Drive(n, d)
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `cd "$1" && du -sk .rustfs.sys/* .rustfs.sys/tmp/* * . 2>/dev/null; true`, "sh", dir))
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for line := range strings.SplitSeq(string(res.Stdout), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		if kb, err := strconv.ParseInt(f[0], 10, 64); err == nil {
			out[f[1]] = kb
		}
	}
	return out, nil
}

// Pause stops the server in its tracks with SIGSTOP, until Resume.
func (s *Service) Pause(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGSTOP, true)
}

// Resume continues a server paused by Pause.
func (s *Service) Resume(ctx context.Context, n *torx.Node) error {
	return s.signalPause(ctx, n, syscall.SIGCONT, false)
}

func (s *Service) signalPause(ctx context.Context, n *torx.Node, sig syscall.Signal, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	if m == nil || m.proc == nil {
		return fmt.Errorf("rustfs: %s is not running", n.Name())
	}
	if m.paused == paused {
		return nil
	}
	if err := m.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("rustfs: %v to %s: %w", sig, n.Name(), err)
	}
	m.paused = paused
	return nil
}

// Paused reports whether the server is paused.
func (s *Service) Paused(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.paused
}

// Running reports whether the server has a process, paused or not.
func (s *Service) Running(n *torx.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[n.Name()]
	return m != nil && m.proc != nil
}

// launchLocked starts the server's process: the one place a process is
// created, shared by StartNode and Restart.
func (s *Service) launchLocked(ctx context.Context, n *torx.Node, m *member) error {
	if m.proc != nil {
		return fmt.Errorf("rustfs: %s is already running", n.Name())
	}
	root := n.ServiceScratch(s.Name()).Root
	cmd := torx.Command(binary, "server")
	cmd.Dir = root
	cmd.Env = append([]string{
		"RUSTFS_VOLUMES=" + s.volumesLocked(),
		"RUSTFS_ADDRESS=" + net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[n.Name()])),
		"RUSTFS_ACCESS_KEY=" + AccessKey,
		"RUSTFS_SECRET_KEY=" + SecretKey,
		"RUSTFS_CONSOLE_ENABLE=false",
		// The drives of a server share its node's filesystem unless they
		// are limited, and RustFS refuses drives that share a device.
		"RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true",
		// Log to standard output, which torx captures and collects, as
		// well as to files in the scratch directory.
		"RUSTFS_OBS_LOG_STDOUT_ENABLED=true",
		"RUSTFS_OBS_LOG_DIRECTORY=" + filepath.Join(root, "logs"),
		"RUSTFS_OBS_LOGGER_LEVEL=info",
		"RUST_BACKTRACE=1",
	}, s.env...)
	cmd.Env = append(cmd.Env, s.extra[m.index]...)
	proc, err := s.StartCaptured(ctx, n, cmd)
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
	e := Exit{Node: n.Name(), Server: m.index, Code: code, Time: time.Now()}
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
	if i := strings.LastIndex(log, "panicked"); i >= 0 {
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

// Log returns up to the last max bytes of what the server's current or last
// process has logged.
func (s *Service) Log(ctx context.Context, n *torx.Node, max int) ([]byte, error) {
	path := filepath.Join(n.ServiceScratch(s.Name()).Root, "stdout.log")
	res, err := n.Exec(ctx, torx.Command("tail", "-c", strconv.Itoa(max), path))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("rustfs: reading %s's log: %s", n.Name(), strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}
