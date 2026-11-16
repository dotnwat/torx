//go:build unix

// Package nats runs a NATS cluster with JetStream on torx nodes -- one
// nats-server on each, joined by routes into one cluster -- and injects the
// faults a process can take: crashes, pauses, graceful and lame-duck stops,
// a disk of limited size.
//
// The server is the nats-server binary at $NATS_SERVER, or on PATH.
package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
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

const (
	// readyTimeout bounds how long a server may take to serve its
	// monitoring endpoint and enable JetStream: it recovers its streams
	// first.
	readyTimeout = 120 * time.Second
	// stopGrace is how long a stop lets a server exit after SIGTERM before
	// it is killed.
	stopGrace = 10 * time.Second
	// ClusterName is the name of the servers' cluster.
	ClusterName = "torx"
)

// Credentials of the two accounts every server knows: the system account,
// which the meta-level API needs, and the account the clients use.
const (
	SysUser = "sys"
	SysPass = "sys"
	AppUser = "app"
	AppPass = "app"
	// AppAccount is the account the clients use.
	AppAccount = "APP"
)

// Binary is the nats-server under test: $NATS_SERVER, or the one on PATH.
func Binary() string {
	if b := os.Getenv("NATS_SERVER"); b != "" {
		return b
	}
	return "nats-server"
}

// Config says what the cluster runs.
type Config struct {
	// SyncAlways has JetStream fsync every write; otherwise it syncs every
	// SyncInterval (the server's default, two minutes, if zero).
	SyncAlways   bool
	SyncInterval time.Duration
	// Tags are each server's placement tags, by index; a server with none
	// gets "az:<index mod 3>".
	Tags [][]string
	// JetStream are settings merged into every server's jetstream block,
	// and Server into the top level: keys and values as the server's
	// configuration file takes them.
	JetStream, Server map[string]any
	// Prepare, if set, runs before a server's first launch, once its
	// directory exists and before the server writes it: to mount a
	// size-limited filesystem over its store, say.
	Prepare func(ctx context.Context, name string, n *torx.Node) error
}

// Exit is a server that exited without the service stopping it.
type Exit struct {
	Proc string `json:"proc"`
	Node string `json:"node"`
	Code int    `json:"code"`
	Err  string `json:"error,omitempty"`
	// Signal is the signal that killed it, if one did.
	Signal string    `json:"signal,omitempty"`
	Time   time.Time `json:"time"`
	// Said is the end of what the server logged: a panic, for one.
	Said string `json:"said,omitempty"`
}

// proc is one server: its name, node, ports, and process while it runs.
type proc struct {
	name        string
	index       int
	node        *torx.Node
	clientPort  int
	clusterPort int
	httpPort    int
	proc        torx.Process
	paused      bool
	launches    int
	// unstopped is the error of a stop that could not establish the server
	// was gone; it may still hold its ports and directories.
	unstopped error
}

// Cluster runs a NATS cluster: a server on every node.
type Cluster struct {
	*torx.ServiceBase
	cfg Config

	mu    sync.Mutex
	procs map[string]*proc
	exits []Exit
	// limited are the directories Limit mounted, by node.
	limited map[string][]string
}

// New builds a cluster named name of nodes servers.
func New(name string, nodes int, cfg Config) *Cluster {
	c := &Cluster{cfg: cfg, procs: map[string]*proc{}}
	c.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), c)
	// A server restarted keeps the log of the one before it as
	// <name>.<k>.log.
	c.SetCapturePolicy(torx.CaptureRotate)
	return c
}

// StartNode makes the node's scratch directory; Start launches the servers.
func (c *Cluster) StartNode(ctx context.Context, n *torx.Node) error {
	return n.Mkdir(ctx, n.ServiceScratch(c.Name()).Root)
}

// Start prepares every node, then launches a server on each, each routed
// to all the others.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.ServiceBase.Start(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	for i, n := range c.Nodes() {
		if err := c.addLocked(fmt.Sprintf("s%d", i), i, n); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.mu.Unlock()
	for _, name := range c.Procs() {
		if err := c.Restart(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) addLocked(name string, index int, n *torx.Node) error {
	var ports []int
	for range 3 {
		p, err := n.AllocatePort()
		if err != nil {
			for _, q := range ports {
				n.ReleasePort(q)
			}
			return err
		}
		ports = append(ports, p)
	}
	c.procs[name] = &proc{name: name, index: index, node: n, clientPort: ports[0], clusterPort: ports[1], httpPort: ports[2]}
	return nil
}

// Wait waits for every server to serve with JetStream enabled.
func (c *Cluster) Wait(ctx context.Context) error {
	for _, name := range c.Procs() {
		if err := c.WaitReady(ctx, name, 0); err != nil {
			return err
		}
	}
	return nil
}

// WaitNode is unused: Wait waits for the servers.
func (c *Cluster) WaitNode(context.Context, *torx.Node) error { return nil }

// StopNode stops the server on the node.
func (c *Cluster) StopNode(ctx context.Context, n *torx.Node) error {
	c.mu.Lock()
	var mine []*proc
	for _, p := range c.procs {
		if p.node.Name() == n.Name() {
			mine = append(mine, p)
		}
	}
	c.mu.Unlock()
	var errs []error
	for _, p := range mine {
		if c.Paused(p.name) {
			_ = c.Resume(ctx, p.name)
		}
		if _, err := c.Terminate(ctx, p.name); err != nil && !errors.Is(err, errNotRunning) {
			errs = append(errs, err)
		}
		c.mu.Lock()
		if p.unstopped != nil {
			errs = append(errs, p.unstopped)
		}
		n.ReleasePort(p.clientPort)
		n.ReleasePort(p.clusterPort)
		n.ReleasePort(p.httpPort)
		delete(c.procs, p.name)
		c.mu.Unlock()
	}
	return errors.Join(errs...)
}

// CleanNode removes the node's scratch directory, once the directories
// Limit mounted there are unmounted.
func (c *Cluster) CleanNode(ctx context.Context, n *torx.Node) error {
	c.mu.Lock()
	dirs := c.limited[n.Name()]
	delete(c.limited, n.Name())
	c.mu.Unlock()
	for _, d := range dirs {
		if err := diskfault.Unlimit(ctx, n, d); err != nil {
			return err
		}
	}
	return n.Rm(ctx, n.ServiceScratch(c.Name()).Root)
}

// Limit mounts a filesystem of size bytes over dir, one of server name's
// directories, before the server first writes it (see Config.Prepare), so
// that diskfault.Fill can fill it.
func (c *Cluster) Limit(ctx context.Context, name, dir string, size int64) error {
	n := c.Node(name)
	if n == nil {
		return fmt.Errorf("nats: no server %s", name)
	}
	if err := n.Mkdir(ctx, dir); err != nil {
		return err
	}
	if err := diskfault.Limit(ctx, n, dir, size); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limited == nil {
		c.limited = map[string][]string{}
	}
	c.limited[n.Name()] = append(c.limited[n.Name()], dir)
	return nil
}

var errNotRunning = errors.New("nats: not running")

// Dir is server name's directory: its configuration, and its store in js/.
func (c *Cluster) Dir(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return path.Join(p.node.ServiceScratch(c.Name()).Root, name)
}

// StoreDir is server name's JetStream store directory.
func (c *Cluster) StoreDir(name string) string { return path.Join(c.Dir(name), "js") }

// URL is the client URL of server name, with the application's
// credentials.
func (c *Cluster) URL(name string) string {
	return "nats://" + AppUser + ":" + AppPass + "@" + c.Addr(name)
}

// SysURL is the client URL of server name with the system account's
// credentials.
func (c *Cluster) SysURL(name string) string {
	return "nats://" + SysUser + ":" + SysPass + "@" + c.Addr(name)
}

// URLs are every server's client URL, comma-separated, as a client is
// given them.
func (c *Cluster) URLs() string {
	var out []string
	for _, name := range c.Procs() {
		out = append(out, c.URL(name))
	}
	return strings.Join(out, ",")
}

// Addr is server name's client address.
func (c *Cluster) Addr(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.clientPort))
}

// MonitorURL is the base URL of server name's monitoring endpoint.
func (c *Cluster) MonitorURL(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return "http://" + net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.httpPort))
}

// Node is the node server name runs on.
func (c *Cluster) Node(name string) *torx.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.procs[name]; p != nil {
		return p.node
	}
	return nil
}

// ByNode is the server on node n, or "".
func (c *Cluster) ByNode(n *torx.Node) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, p := range c.procs {
		if p.node.Name() == n.Name() {
			return name
		}
	}
	return ""
}

// Procs returns the servers' names, sorted.
func (c *Cluster) Procs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.procs))
}

// Tags are server name's placement tags.
func (c *Cluster) Tags(name string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return nil
	}
	return c.tagsLocked(p)
}

func (c *Cluster) tagsLocked(p *proc) []string {
	if p.index < len(c.cfg.Tags) && len(c.cfg.Tags[p.index]) > 0 {
		return c.cfg.Tags[p.index]
	}
	return []string{fmt.Sprintf("az:%d", p.index%3)}
}

// configLocked is server p's configuration file, in JSON, which the server
// reads as it does its own format.
func (c *Cluster) configLocked(p *proc) ([]byte, error) {
	n := p.node
	dir := path.Join(n.ServiceScratch(c.Name()).Root, p.name)
	js := map[string]any{
		"store_dir": path.Join(dir, "js"),
		// Many servers share the host: don't let each think it has the
		// host's memory and disk to itself.
		"max_memory_store": 256 << 20,
		"max_file_store":   8 << 30,
	}
	switch {
	case c.cfg.SyncAlways:
		js["sync_interval"] = "always"
	case c.cfg.SyncInterval > 0:
		js["sync_interval"] = c.cfg.SyncInterval.String()
	}
	maps.Copy(js, c.cfg.JetStream)
	var routes []string
	for _, q := range c.procs {
		if q != p {
			routes = append(routes, "nats-route://"+net.JoinHostPort(q.node.Addr(), strconv.Itoa(q.clusterPort)))
		}
	}
	slices.Sort(routes)
	conf := map[string]any{
		"server_name": p.name,
		"listen":      net.JoinHostPort(n.Addr(), strconv.Itoa(p.clientPort)),
		"http":        net.JoinHostPort(n.Addr(), strconv.Itoa(p.httpPort)),
		"server_tags": c.tagsLocked(p),
		"jetstream":   js,
		"cluster": map[string]any{
			"name":   ClusterName,
			"listen": net.JoinHostPort(n.Addr(), strconv.Itoa(p.clusterPort)),
			"routes": routes,
		},
		"accounts": map[string]any{
			"SYS":      map[string]any{"users": []map[string]string{{"user": SysUser, "password": SysPass}}},
			AppAccount: map[string]any{"jetstream": "enabled", "users": []map[string]string{{"user": AppUser, "password": AppPass}}},
		},
		"system_account":     "SYS",
		"logtime":            true,
		"lame_duck_duration": "30s",
		// A lame-duck server stops accepting clients at once, and its
		// clients move to other servers.
		"lame_duck_grace_period": "1s",
	}
	maps.Copy(conf, c.cfg.Server)
	return json.MarshalIndent(conf, "", "  ")
}

// Restart starts the stopped server name.
func (c *Cluster) Restart(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	switch {
	case p == nil:
		return fmt.Errorf("nats: no server %s", name)
	case p.unstopped != nil:
		return fmt.Errorf("nats: %s cannot restart over a process that may still run: %w", name, p.unstopped)
	case p.proc != nil:
		return fmt.Errorf("nats: %s is running", name)
	}
	n := p.node
	dir := path.Join(n.ServiceScratch(c.Name()).Root, p.name)
	if err := n.Mkdir(ctx, dir); err != nil {
		return err
	}
	if p.launches == 0 {
		// A failed run keeps what the server left on disk -- its
		// configuration, and in js/ its streams' blocks and its Raft logs
		// -- to examine.
		c.AddArtifact(n, torx.Artifact{Name: p.name + "-data.tar.gz", Path: dir, Dir: true})
	}
	if p.launches == 0 && c.cfg.Prepare != nil {
		c.mu.Unlock()
		err := c.cfg.Prepare(ctx, name, n)
		c.mu.Lock()
		if err != nil {
			return fmt.Errorf("nats: preparing %s: %w", name, err)
		}
	}
	conf, err := c.configLocked(p)
	if err != nil {
		return err
	}
	confPath := path.Join(dir, "nats.conf")
	if err := n.WriteFile(ctx, confPath, conf); err != nil {
		return err
	}
	p.launches++
	cmd := torx.Command(Binary(), "-c", confPath)
	cmd.Dir = dir
	tp, err := c.StartCapturedAs(ctx, n, p.name, cmd)
	if err != nil {
		return err
	}
	p.proc, p.paused = tp, false
	go c.watch(p, tp)
	return nil
}

func (c *Cluster) watch(p *proc, tp torx.Process) {
	code, err := tp.Wait(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.proc != tp {
		return // stopped by the service
	}
	p.proc, p.paused = nil, false
	e := Exit{Proc: p.name, Node: p.node.Name(), Code: code, Time: time.Now()}
	if err != nil {
		e.Err = err.Error()
	}
	if sig, ok := torx.ExitSignal(tp); ok {
		e.Signal = sig.String()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if b, err := c.tail(ctx, p.node, p.name, 256<<10); err == nil {
		e.Said = Fatal(string(b))
	}
	c.exits = append(c.exits, e)
	_ = tp.Close()
}

// Log returns the last max bytes server name has logged.
func (c *Cluster) Log(ctx context.Context, name string, max int) ([]byte, error) {
	n := c.Node(name)
	if n == nil {
		return nil, fmt.Errorf("nats: no server %s", name)
	}
	return c.tail(ctx, n, name, max)
}

// LogPaths are the paths of every log server name has written, the
// current launch's and the rotated ones of launches before it.
func (c *Cluster) LogPaths(ctx context.Context, name string) ([]string, error) {
	n := c.Node(name)
	if n == nil {
		return nil, fmt.Errorf("nats: no server %s", name)
	}
	cur := c.CapturePath(n, name)
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `ls -1 -- "$1" "${1%.log}".*.log 2>/dev/null; true`, "sh", cur))
	if err != nil {
		return nil, err
	}
	var out []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(res.Stdout)), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out, nil
}

func (c *Cluster) tail(ctx context.Context, n *torx.Node, name string, max int) ([]byte, error) {
	res, err := n.Exec(ctx, torx.Command("tail", "-c", strconv.Itoa(max), c.CapturePath(n, name)))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("nats: reading %s's log: %s", name, strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

// Fatal is what a server that exited said last that says why: the panic
// or fatal error it printed, if any, with the start of its trace, or the
// last lines of its log.
func Fatal(log string) string {
	lines := strings.Split(strings.TrimRight(log, "\n"), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "fatal error: ") || strings.Contains(line, "[FTL]") {
			return strings.Join(lines[i:min(len(lines), i+40)], "\n")
		}
	}
	return lastLines(log, 12)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// Exits returns the servers that exited on their own, in order.
func (c *Cluster) Exits() []Exit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.exits)
}

// Running reports whether server name is running, paused or not.
func (c *Cluster) Running(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	return p != nil && p.proc != nil
}

// Paused reports whether server name is paused.
func (c *Cluster) Paused(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	return p != nil && p.paused
}

// WaitReady waits for server name to say it is up with JetStream enabled.
func (c *Cluster) WaitReady(ctx context.Context, name string, timeout time.Duration) error {
	return c.waitHealth(ctx, name, timeout, "/healthz?js-enabled-only=true")
}

// WaitHealthy waits for server name to say every stream and consumer it
// hosts is current.
func (c *Cluster) WaitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	return c.waitHealth(ctx, name, timeout, "/healthz")
}

func (c *Cluster) waitHealth(ctx context.Context, name string, timeout time.Duration, query string) error {
	if timeout == 0 {
		timeout = readyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := c.MonitorURL(name) + query
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !c.Running(name) {
			for _, e := range slices.Backward(c.Exits()) {
				if e.Proc == name {
					return false, fmt.Errorf("nats: %s exited with status %d before it was ready:\n%s", name, e.Code, e.Said)
				}
			}
			return false, fmt.Errorf("nats: %s: %w", name, errNotRunning)
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return torx.WaitForHTTP(pctx, url) == nil, nil
	}, 200*time.Millisecond)
}

// Crash kills server name outright, paused or not.
func (c *Cluster) Crash(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil || p.proc == nil {
		return fmt.Errorf("nats: %s: %w", name, errNotRunning)
	}
	err := p.proc.Close() // SIGKILL ends a paused process too
	p.proc, p.paused = nil, false
	if err != nil {
		p.unstopped = fmt.Errorf("nats: crashing %s: %w", name, err)
		return p.unstopped
	}
	return nil
}

// Terminate stops server name with SIGTERM, and kills it if it has not
// exited within the grace. It reports whether the server exited on its
// own.
func (c *Cluster) Terminate(ctx context.Context, name string) (bool, error) {
	return c.stop(ctx, name, syscall.SIGTERM, stopGrace)
}

// LameDuck has server name enter lame-duck mode (SIGUSR2): it stops
// accepting clients, moves its JetStream leaders away, and exits once its
// clients have left. It is killed if it has not exited within grace.
func (c *Cluster) LameDuck(ctx context.Context, name string, grace time.Duration) (bool, error) {
	return c.stop(ctx, name, syscall.SIGUSR2, grace)
}

func (c *Cluster) stop(ctx context.Context, name string, sig syscall.Signal, grace time.Duration) (bool, error) {
	c.mu.Lock()
	p := c.procs[name]
	if p == nil || p.proc == nil {
		c.mu.Unlock()
		return false, fmt.Errorf("nats: %s: %w", name, errNotRunning)
	}
	tp, paused := p.proc, p.paused
	p.proc, p.paused = nil, false
	c.mu.Unlock()
	if paused {
		_ = tp.Signal(ctx, syscall.SIGCONT)
	}
	_, err := torx.Shutdown(ctx, tp, sig, grace)
	clean := err == nil
	if errors.Is(err, torx.ErrShutdownTimeout) {
		err = nil
	}
	if err != nil {
		c.mu.Lock()
		p.unstopped = fmt.Errorf("nats: stopping %s: %w", name, err)
		c.mu.Unlock()
	}
	return clean, err
}

// Signal sends sig to running server name: SIGHUP to reload its
// configuration, say.
func (c *Cluster) Signal(ctx context.Context, name string, sig syscall.Signal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil || p.proc == nil {
		return fmt.Errorf("nats: %s: %w", name, errNotRunning)
	}
	return p.proc.Signal(ctx, sig)
}

// Pause stops server name with SIGSTOP; Resume continues it.
func (c *Cluster) Pause(ctx context.Context, name string) error {
	return c.signalPause(ctx, name, syscall.SIGSTOP, true)
}

// Resume continues a paused server.
func (c *Cluster) Resume(ctx context.Context, name string) error {
	return c.signalPause(ctx, name, syscall.SIGCONT, false)
}

func (c *Cluster) signalPause(ctx context.Context, name string, sig syscall.Signal, paused bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil || p.proc == nil {
		return fmt.Errorf("nats: %s: %w", name, errNotRunning)
	}
	if p.paused == paused {
		return nil
	}
	if err := p.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("nats: %v to %s: %w", sig, name, err)
	}
	p.paused = paused
	return nil
}

// Wipe removes stopped server name's store: a server whose disk was
// replaced, which comes back empty.
func (c *Cluster) Wipe(ctx context.Context, name string) error {
	if c.Running(name) {
		return fmt.Errorf("nats: %s is running", name)
	}
	n := c.Node(name)
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `rm -rf -- "$1"/* "$1"/.[!.]*`, "sh", c.StoreDir(name)))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("nats: wiping %s: %s", name, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}
