//go:build unix

// Package litestream runs, on torx nodes, the processes of a SQLite
// database Litestream replicates -- the application that writes it
// (lsapp), Litestream replicating it, and Litestream following its replica
// on another node -- and the one-shot commands around them: a sync that
// waits for the replica, a restore, and a check of a database's contents.
//
// Litestream and lsapp are resolved from each node's PATH; the harness puts
// the builds under test first on it.
package litestream

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
	"github.com/dotnwat/torx/diskfault"
)

// The binaries, resolved from each node's PATH.
const (
	Litestream = "litestream"
	App        = "lsapp"
)

// stopPolicy stops a process the way an operator would, and dumps a Go
// program's goroutines if it does not exit: Litestream's shutdown syncs
// the replica, which a faulty store can hold up.
var stopPolicy = torx.StopPolicy{Signal: syscall.SIGTERM, Grace: 45 * time.Second, Dump: syscall.SIGQUIT}

// Service runs the processes on its nodes. It starts none by itself: a job
// launches each by name, on the node it picks.
type Service struct {
	*torx.ServiceBase
	*torx.Procs

	mu      sync.Mutex
	ports   map[string]int          // process -> the port it serves on, if any
	limited map[*torx.Node][]string // the directories Limit mounted, by node
}

// New builds a service named name over nodes nodes.
func New(name string, nodes int) *Service {
	s := &Service{ports: map[string]int{}, limited: map[*torx.Node][]string{}}
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), s)
	// Each process logs to <name>.log on its node; one launched again keeps
	// the log of the one before it as <name>.<k>.log.
	s.SetCapturePolicy(torx.CaptureRotate)
	s.Procs = torx.NewProcs(s.ServiceBase)
	return s
}

// Dir is the service's directory on n, which holds everything it writes.
func (s *Service) Dir(n *torx.Node) string { return n.ServiceScratch(s.Name()).Root }

// StartNode makes the node's directory; processes start by Launch.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	return n.Mkdir(ctx, s.Dir(n))
}

// WaitNode has nothing to wait for.
func (s *Service) WaitNode(context.Context, *torx.Node) error { return nil }

// StopNode stops every process on the node.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	return s.Procs.StopNode(ctx, n, stopPolicy)
}

// CleanNode removes the node's directory, once the limits on any under it
// are lifted.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	dirs := s.limited[n]
	delete(s.limited, n)
	s.mu.Unlock()
	var errs []error
	for _, d := range dirs {
		if err := diskfault.Unlimit(ctx, n, d); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(append(errs, n.Rm(ctx, s.Dir(n)))...)
}

// Limit makes dir on n a directory of at most size bytes, which
// diskfault.Fill can fill: a disk that fills up. Limit dir before anything
// writes it. CleanNode lifts the limit.
func (s *Service) Limit(ctx context.Context, n *torx.Node, dir string, size int64) error {
	if err := diskfault.Limit(ctx, n, dir, size); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limited[n] = append(s.limited[n], dir)
	return nil
}

// Wipe removes everything dir holds, but not dir: it may be a limited
// directory's mount point.
func (s *Service) Wipe(ctx context.Context, n *torx.Node, dir string) error {
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `rm -rf "$1"/* "$1"/.[!.]* "$1"/..?*`, "sh", dir))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("wipe %s: %s", dir, oneLine(res.Stderr))
	}
	return nil
}

// Terminate stops process name with SIGTERM, which has Litestream sync its
// replica one last time, and kills it if it has not exited in time.
func (s *Service) Terminate(ctx context.Context, name string) (bool, error) {
	return s.Procs.Terminate(ctx, name, stopPolicy)
}

// AppConfig says how lsapp keeps the database.
type AppConfig struct {
	PageSize         int    `json:"page_size"`
	AutoVacuum       string `json:"auto_vacuum"`
	CachePages       int    `json:"cache_pages"`
	Autocheckpoint   int    `json:"autocheckpoint"`
	Synchronous      string `json:"synchronous"`
	JournalSizeLimit int64  `json:"journal_size_limit"`
}

// start launches process name on n with cmd, or, if the slot exists and
// its process is not running, launches it again with cmd.
func (s *Service) start(ctx context.Context, name string, n *torx.Node, cmd torx.Cmd) error {
	if s.Node(name) != nil {
		if s.Node(name) != n {
			return fmt.Errorf("%s runs on %s, not %s", name, s.Node(name).Name(), n.Name())
		}
		return s.Relaunch(ctx, name, cmd)
	}
	return s.Launch(ctx, name, n, cmd)
}

// LaunchApp starts lsapp as process name on n, keeping the database at db.
func (s *Service) LaunchApp(ctx context.Context, name string, n *torx.Node, db string, cfg AppConfig) error {
	s.mu.Lock()
	port, ok := s.ports[name]
	if !ok {
		var err error
		if port, err = n.AllocatePort(); err != nil {
			s.mu.Unlock()
			return err
		}
		s.ports[name] = port
	}
	s.mu.Unlock()
	args := []string{"serve", "-db", db,
		"-listen", net.JoinHostPort(n.Addr(), strconv.Itoa(port)),
		"-page-size", strconv.Itoa(cfg.PageSize),
		"-auto-vacuum", cfg.AutoVacuum,
		"-cache-pages", strconv.Itoa(cfg.CachePages),
		"-autocheckpoint", strconv.Itoa(cfg.Autocheckpoint),
		"-synchronous", cfg.Synchronous,
		"-journal-size-limit", strconv.FormatInt(cfg.JournalSizeLimit, 10),
	}
	if err := n.Mkdir(ctx, path.Dir(db)); err != nil {
		return err
	}
	return s.start(ctx, name, n, torx.Command(App, args...))
}

// AppURL is the URL lsapp process name serves at.
func (s *Service) AppURL(name string) string {
	n := s.Node(name)
	if n == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return "http://" + net.JoinHostPort(n.Addr(), strconv.Itoa(s.ports[name]))
}

// LaunchReplicate starts `litestream replicate` as process name on n, with
// the config at config.
func (s *Service) LaunchReplicate(ctx context.Context, name string, n *torx.Node, config string) error {
	return s.start(ctx, name, n, torx.Command(Litestream, "replicate", "-config", config))
}

// LaunchFollow starts `litestream restore -f` as process name on n: a
// restore of the database the config at config names into out, which then
// applies whatever the replica gains, every interval.
func (s *Service) LaunchFollow(ctx context.Context, name string, n *torx.Node, config, db, out string, interval time.Duration) error {
	return s.start(ctx, name, n, torx.Command(Litestream, "restore", "-config", config,
		"-f", "-follow-interval", interval.String(), "-o", out, db))
}

// WaitApp waits for lsapp process name to serve.
func (s *Service) WaitApp(ctx context.Context, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	url := s.AppURL(name) + "/health"
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !s.Running(name) {
			return false, s.notRunning(ctx, name)
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
	}, 50*time.Millisecond)
}

// exitGrace is how long notRunning waits for the record of an exit. A
// process stops counting as running the moment it exits, and its exit is
// recorded once the end of its log has been read: a moment later, a
// command's round trip to its node.
const exitGrace = 2 * time.Second

// notRunning says why process name is not running: how its last launch
// exited, if it did on its own. An exit of a launch before the last says
// nothing of this one, which was crashed or stopped if it left no record
// of its own.
func (s *Service) notRunning(ctx context.Context, name string) error {
	var exit *torx.ProcExit
	if launch := s.Launches(name); launch > 0 {
		ctx, cancel := context.WithTimeout(ctx, exitGrace)
		defer cancel()
		_ = torx.WaitUntil(ctx, func(context.Context) (bool, error) {
			for _, e := range slices.Backward(s.Exits()) {
				if e.Name == name && e.Launch == launch {
					exit = &e
					return true, nil
				}
			}
			return false, nil
		}, 10*time.Millisecond)
	}
	if exit != nil {
		return fmt.Errorf("%s exited with status %d:\n%s", name, exit.Code, exit.Tail)
	}
	return fmt.Errorf("%s: %w", name, torx.ErrNotRunning)
}

// SyncResult is what `litestream sync -wait -json` prints.
type SyncResult struct {
	TXID        uint64 `json:"txid"`
	ReplicaTXID uint64 `json:"replica_txid"`
}

// Sync runs `litestream sync -wait` against process proc's control socket,
// which returns once the replica holds every transaction the database had
// when it was called.
func (s *Service) Sync(ctx context.Context, n *torx.Node, socket, db string, timeout time.Duration) (SyncResult, error) {
	secs := max(1, int(timeout.Round(time.Second)/time.Second))
	res, err := n.Exec(ctx, torx.Command(Litestream, "sync", "-wait", "-json", "-timeout", strconv.Itoa(secs), "-socket", socket, db))
	if err != nil {
		return SyncResult{}, err
	}
	if res.ExitCode != 0 {
		return SyncResult{}, fmt.Errorf("litestream sync: %s", oneLine(res.Stderr, res.Stdout))
	}
	var out SyncResult
	if err := json.Unmarshal(res.Stdout, &out); err != nil {
		return SyncResult{}, fmt.Errorf("litestream sync: %w: %s", err, res.Stdout)
	}
	return out, nil
}

// Target says what a restore restores: the latest state, or the state as
// of a TXID or a time.
type Target struct {
	TXID      uint64    `json:"txid,omitempty"`
	Timestamp time.Time `json:"timestamp,omitzero"`
}

func (t Target) String() string {
	switch {
	case t.TXID != 0:
		return fmt.Sprintf("txid %d", t.TXID)
	case !t.Timestamp.IsZero():
		return "time " + t.Timestamp.Format(time.RFC3339Nano)
	}
	return "latest"
}

// RestoreResult is what `litestream restore -json` prints.
type RestoreResult struct {
	TXID string `json:"txid"`
}

// Restore runs `litestream restore` on n of the database the config at
// config names, into out, which must not exist.
func (s *Service) Restore(ctx context.Context, n *torx.Node, config, db, out string, t Target) (RestoreResult, error) {
	args := []string{"restore", "-config", config, "-o", out, "-json"}
	switch {
	case t.TXID != 0:
		args = append(args, "-txid", fmt.Sprintf("%016x", t.TXID))
	case !t.Timestamp.IsZero():
		args = append(args, "-timestamp", t.Timestamp.UTC().Format(time.RFC3339Nano))
	}
	res, err := n.Exec(ctx, torx.Command(Litestream, append(args, db)...))
	if err != nil {
		return RestoreResult{}, err
	}
	if res.ExitCode != 0 {
		return RestoreResult{}, fmt.Errorf("litestream restore: %s", oneLine(res.Stderr, res.Stdout))
	}
	var r RestoreResult
	if err := json.Unmarshal(res.Stdout, &r); err != nil {
		return RestoreResult{}, fmt.Errorf("litestream restore: %w: %s", err, res.Stdout)
	}
	return r, nil
}

// Verify runs `lsapp verify` on n against the database at db, read-only if
// another process writes it, and returns what it printed.
func (s *Service) Verify(ctx context.Context, n *torx.Node, db string, readonly bool) ([]byte, error) {
	args := []string{"verify", "-db", db}
	if readonly {
		args = append(args, "-readonly")
	}
	res, err := n.Exec(ctx, torx.Command(App, args...))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("lsapp verify: %s", oneLine(res.Stderr, res.Stdout))
	}
	return res.Stdout, nil
}

// oneLine is the last lines of the first of bs that is not empty, on one
// line: what a command that failed said last, which is why.
func oneLine(bs ...[]byte) string {
	for _, b := range bs {
		if s := strings.TrimSpace(string(b)); s != "" {
			lines := strings.Split(s, "\n")
			lines = lines[max(0, len(lines)-3):]
			s = strings.Join(lines, " | ")
			if len(s) > 1000 {
				s = "..." + s[len(s)-1000:]
			}
			return s
		}
	}
	return "no output"
}
