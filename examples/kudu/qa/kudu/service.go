//go:build unix

// Package kudu runs an Apache Kudu cluster on torx nodes -- masters and
// tablet servers, each a process of its own -- and the client node the
// suite reaches it through, and injects the faults a process can take:
// crashes, pauses, a lost disk, flags changed at runtime.
//
// The servers are the release binaries of Kudu's kudu-binary artifact,
// found under $KUDU_HOME (bin/kudu-master, bin/kudu-tserver, bin/kudu, and
// the libraries in lib/ they load). Kudu has no Go client, so the suite
// talks to the cluster through kudu-node (../../node), which serves Kudu's
// Java client over HTTP.
package kudu

import (
	"context"
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

// Roles a server process takes.
const (
	Master  = "master"
	TServer = "tserver"
)

const (
	// readyTimeout bounds how long a server may take to serve its web UI:
	// a tablet server replays its tablets' logs before it does.
	readyTimeout = 120 * time.Second
	// stopGrace is how long a stop lets a server exit after SIGTERM before
	// it is killed.
	stopGrace = 10 * time.Second
)

// Home is where the Kudu binaries are: $KUDU_HOME, or the release the
// harness installs.
func Home() string {
	if h := os.Getenv("KUDU_HOME"); h != "" {
		return h
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return path.Join(cache, "torx-kudu", "bin", "1.18.1")
}

// Config says what the cluster runs.
type Config struct {
	// Masters is how many masters run, one each on the first nodes.
	Masters int
	// DataDirs is how many data directories each tablet server has.
	DataDirs int
	// Flags, MasterFlags, and TServerFlags are flags every server, every
	// master, and every tablet server starts with, without the leading
	// dashes; they override the service's own.
	Flags, MasterFlags, TServerFlags map[string]string
	// Prepare, if set, runs before a server's first launch, once its
	// directory exists and before Kudu writes it: to mount a size-limited
	// filesystem over its WAL or data directories, say.
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
	// Said is the end of what the server logged: a FATAL's check, for one.
	Said string `json:"said,omitempty"`
}

// proc is one server: its name, role, node, ports, and process while it
// runs.
type proc struct {
	name     string
	role     string
	node     *torx.Node
	rpcPort  int
	webPort  int
	proc     torx.Process
	paused   bool
	launches int
	// extra are flags this server starts with beyond the config's, as the
	// job last set them.
	extra map[string]string
	// unstopped is the error of a stop that could not establish the server
	// was gone; it may still hold its ports and directories.
	unstopped error
}

// Cluster runs a Kudu cluster: a tablet server on every node, and a master
// on each of the first Config.Masters.
type Cluster struct {
	*torx.ServiceBase
	cfg Config

	mu    sync.Mutex
	procs map[string]*proc
	exits []Exit
	// limited are the directories Limit mounted, by node.
	limited map[string][]string
}

// New builds a cluster named name on nodes nodes.
func New(name string, nodes int, cfg Config) *Cluster {
	if cfg.Masters < 1 {
		cfg.Masters = 1
	}
	if cfg.DataDirs < 1 {
		cfg.DataDirs = 1
	}
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

// Start prepares every node, then launches the masters, each told of all
// of them, and then the tablet servers.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.ServiceBase.Start(ctx); err != nil {
		return err
	}
	nodes := c.Nodes()
	if c.cfg.Masters > len(nodes) {
		return fmt.Errorf("kudu: %d masters need as many nodes, have %d", c.cfg.Masters, len(nodes))
	}
	c.mu.Lock()
	for i, n := range nodes {
		if i < c.cfg.Masters {
			if err := c.addLocked(fmt.Sprintf("master-%d", i), Master, n); err != nil {
				c.mu.Unlock()
				return err
			}
		}
		if err := c.addLocked(fmt.Sprintf("tserver-%d", i), TServer, n); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.mu.Unlock()
	for _, name := range append(c.Procs(Master), c.Procs(TServer)...) {
		if err := c.Restart(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) addLocked(name, role string, n *torx.Node) error {
	rpc, err := n.AllocatePort()
	if err != nil {
		return err
	}
	web, err := n.AllocatePort()
	if err != nil {
		n.ReleasePort(rpc)
		return err
	}
	c.procs[name] = &proc{name: name, role: role, node: n, rpcPort: rpc, webPort: web}
	return nil
}

// Wait waits for every server to serve.
func (c *Cluster) Wait(ctx context.Context) error {
	for _, name := range c.Procs("") {
		if err := c.WaitReady(ctx, name, 0); err != nil {
			return err
		}
	}
	return nil
}

// WaitNode is unused: Wait waits for the servers.
func (c *Cluster) WaitNode(context.Context, *torx.Node) error { return nil }

// StopNode stops every server on the node.
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
		if _, err := c.Terminate(ctx, p.name); err != nil && !errors.Is(err, errNotRunning) {
			errs = append(errs, err)
		}
		c.mu.Lock()
		if p.unstopped != nil {
			errs = append(errs, p.unstopped)
		}
		n.ReleasePort(p.rpcPort)
		n.ReleasePort(p.webPort)
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
		return fmt.Errorf("kudu: no server %s", name)
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

var errNotRunning = errors.New("kudu: not running")

// Dir is server name's directory: its WAL in wal/, its data in data<i>/.
func (c *Cluster) Dir(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return path.Join(p.node.ServiceScratch(c.Name()).Root, name)
}

// WALDir is server name's WAL directory.
func (c *Cluster) WALDir(name string) string { return path.Join(c.Dir(name), "wal") }

// DataDirs are server name's data directories: one for a master.
func (c *Cluster) DataDirs(name string) []string {
	n := 1
	if strings.HasPrefix(name, TServer) {
		n = c.cfg.DataDirs
	}
	var out []string
	for i := range n {
		out = append(out, path.Join(c.Dir(name), "data"+strconv.Itoa(i)))
	}
	return out
}

// MasterAddrs is the masters' RPC addresses, comma-separated, as a
// client is given them.
func (c *Cluster) MasterAddrs() string {
	var out []string
	for _, name := range c.Procs(Master) {
		out = append(out, c.Addr(name))
	}
	return strings.Join(out, ",")
}

// Addr is server name's RPC address.
func (c *Cluster) Addr(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.rpcPort))
}

// WebAddr is server name's web UI address.
func (c *Cluster) WebAddr(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	if p == nil {
		return ""
	}
	return net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.webPort))
}

// ByAddr is the server whose RPC address is addr, or "".
func (c *Cluster) ByAddr(addr string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, p := range c.procs {
		if net.JoinHostPort(p.node.Addr(), strconv.Itoa(p.rpcPort)) == addr {
			return name
		}
	}
	return ""
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

// Procs returns the names of the servers of a role, or of both if role is
// "", sorted.
func (c *Cluster) Procs(role string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for name, p := range c.procs {
		if role == "" || p.role == role {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// flags are the flags server p starts with.
func (c *Cluster) flagsLocked(p *proc) []string {
	n := p.node
	dir := path.Join(n.ServiceScratch(c.Name()).Root, p.name)
	f := map[string]string{
		"fs_wal_dir":          path.Join(dir, "wal"),
		"rpc_bind_addresses":  net.JoinHostPort(n.Addr(), strconv.Itoa(p.rpcPort)),
		"webserver_interface": n.Addr(),
		"webserver_port":      strconv.Itoa(p.webPort),
		// The nodes have no NTP to synchronize with; they share the
		// host's clock.
		"time_source":               "system_unsync",
		"unlock_unsafe_flags":       "true",
		"unlock_experimental_flags": "true",
		"logtostderr":               "true",
		"enable_minidumps":          "false",
		"block_cache_capacity_mb":   "32",
		// Many servers share the host: don't let each think it has the
		// host's memory to itself.
		"memory_limit_hard_bytes": strconv.Itoa(2 << 30),
	}
	var masters []string
	for _, q := range c.procs {
		if q.role == Master {
			masters = append(masters, net.JoinHostPort(q.node.Addr(), strconv.Itoa(q.rpcPort)))
		}
	}
	slices.Sort(masters)
	maps.Copy(f, c.cfg.Flags)
	switch p.role {
	case Master:
		f["fs_data_dirs"] = path.Join(dir, "data0")
		if len(masters) > 1 {
			f["master_addresses"] = strings.Join(masters, ",")
		}
		maps.Copy(f, c.cfg.MasterFlags)
	case TServer:
		var data []string
		for i := range c.cfg.DataDirs {
			data = append(data, path.Join(dir, "data"+strconv.Itoa(i)))
		}
		f["fs_data_dirs"] = strings.Join(data, ",")
		f["tserver_master_addrs"] = strings.Join(masters, ",")
		maps.Copy(f, c.cfg.TServerFlags)
	}
	maps.Copy(f, p.extra)
	var out []string
	for _, k := range slices.Sorted(maps.Keys(f)) {
		out = append(out, "--"+k+"="+f[k])
	}
	return out
}

// SetExtraFlags sets the flags server name starts with, beyond the
// config's, from its next launch on.
func (c *Cluster) SetExtraFlags(name string, flags map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.procs[name]; p != nil {
		p.extra = maps.Clone(flags)
	}
}

// Restart starts the stopped server name.
func (c *Cluster) Restart(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[name]
	switch {
	case p == nil:
		return fmt.Errorf("kudu: no server %s", name)
	case p.unstopped != nil:
		return fmt.Errorf("kudu: %s cannot restart over a process that may still run: %w", name, p.unstopped)
	case p.proc != nil:
		return fmt.Errorf("kudu: %s is running", name)
	}
	n := p.node
	dir := path.Join(n.ServiceScratch(c.Name()).Root, p.name)
	// Kudu makes its directories, but not their parent.
	if err := n.Mkdir(ctx, dir); err != nil {
		return err
	}
	if p.launches == 0 {
		// A failed run keeps what the server left on disk -- its WAL, data,
		// and metadata -- for `kudu local_replica` to read.
		c.AddArtifact(n, torx.Artifact{Name: p.name + "-data.tar.gz", Path: dir, Dir: true})
	}
	if p.launches == 0 && c.cfg.Prepare != nil {
		c.mu.Unlock()
		err := c.cfg.Prepare(ctx, name, n)
		c.mu.Lock()
		if err != nil {
			return fmt.Errorf("kudu: preparing %s: %w", name, err)
		}
	}
	p.launches++
	cmd := torx.Command(path.Join(Home(), "bin", "kudu-"+p.role), c.flagsLocked(p)...)
	cmd.Dir = dir
	cmd.Env = Env()
	tp, err := c.StartCapturedAs(ctx, n, p.name, cmd)
	if err != nil {
		return err
	}
	p.proc, p.paused = tp, false
	go c.watch(p, tp)
	return nil
}

// Env is the environment a Kudu binary runs in: the libraries it ships
// with come first.
func Env() []string {
	return []string{"LD_LIBRARY_PATH=" + path.Join(Home(), "lib")}
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
	if b, err := c.tail(ctx, p.node, p.name, 64<<10); err == nil {
		e.Said = fatal(string(b))
	}
	c.exits = append(c.exits, e)
	_ = tp.Close()
}

// Log returns the last max bytes server name has logged.
func (c *Cluster) Log(ctx context.Context, name string, max int) ([]byte, error) {
	n := c.Node(name)
	if n == nil {
		return nil, fmt.Errorf("kudu: no server %s", name)
	}
	return c.tail(ctx, n, name, max)
}

func (c *Cluster) tail(ctx context.Context, n *torx.Node, name string, max int) ([]byte, error) {
	res, err := n.Exec(ctx, torx.Command("tail", "-c", strconv.Itoa(max), c.CapturePath(n, name)))
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("kudu: reading %s's log: %s", name, strings.TrimSpace(string(res.Stderr)))
	}
	return res.Stdout, nil
}

// fatal is what a server that exited said last that says why: its first
// FATAL line (a failed CHECK, say), if it logged one, and the last lines of
// its log.
func fatal(log string) string {
	tail := lastLines(log, 12)
	for line := range strings.SplitSeq(log, "\n") {
		if strings.HasPrefix(line, "F2") || strings.HasPrefix(line, "*** Check failure") {
			return line + "\n...\n" + tail
		}
	}
	return tail
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

// WaitReady waits for server name's web UI to say it is healthy.
func (c *Cluster) WaitReady(ctx context.Context, name string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = readyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := "http://" + c.WebAddr(name) + "/healthz"
	return torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		if !c.Running(name) {
			for _, e := range slices.Backward(c.Exits()) {
				if e.Proc == name {
					return false, fmt.Errorf("kudu: %s exited with status %d before it was ready:\n%s", name, e.Code, e.Said)
				}
			}
			return false, fmt.Errorf("kudu: %s: %w", name, errNotRunning)
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
		return fmt.Errorf("kudu: %s: %w", name, errNotRunning)
	}
	err := p.proc.Close() // SIGKILL ends a paused process too
	p.proc, p.paused = nil, false
	if err != nil {
		p.unstopped = fmt.Errorf("kudu: crashing %s: %w", name, err)
		return p.unstopped
	}
	return nil
}

// Terminate stops server name with SIGTERM, and kills it if it has not
// exited within the grace. It reports whether the server exited on its
// own.
func (c *Cluster) Terminate(ctx context.Context, name string) (bool, error) {
	c.mu.Lock()
	p := c.procs[name]
	if p == nil || p.proc == nil {
		c.mu.Unlock()
		return false, fmt.Errorf("kudu: %s: %w", name, errNotRunning)
	}
	tp, paused := p.proc, p.paused
	p.proc, p.paused = nil, false
	c.mu.Unlock()
	if paused {
		_ = tp.Signal(ctx, syscall.SIGCONT)
	}
	_, err := torx.Shutdown(ctx, tp, syscall.SIGTERM, stopGrace)
	clean := err == nil
	if errors.Is(err, torx.ErrShutdownTimeout) {
		err = nil
	}
	if err != nil {
		c.mu.Lock()
		p.unstopped = fmt.Errorf("kudu: stopping %s: %w", name, err)
		c.mu.Unlock()
	}
	return clean, err
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
		return fmt.Errorf("kudu: %s: %w", name, errNotRunning)
	}
	if p.paused == paused {
		return nil
	}
	if err := p.proc.Signal(ctx, sig); err != nil {
		return fmt.Errorf("kudu: %v to %s: %w", sig, name, err)
	}
	p.paused = paused
	return nil
}

// Wipe removes stopped server name's directories: a server whose disks
// were replaced, which comes back empty, as a server Kudu has never seen.
func (c *Cluster) Wipe(ctx context.Context, name string) error {
	if c.Running(name) {
		return fmt.Errorf("kudu: %s is running", name)
	}
	n := c.Node(name)
	return n.Rm(ctx, c.Dir(name))
}

// Tool runs the kudu command-line tool on node n, and returns what it
// printed, or an error with what it said if it failed.
func Tool(ctx context.Context, n *torx.Node, args ...string) (string, error) {
	cmd := torx.Command(path.Join(Home(), "bin", "kudu"), args...)
	cmd.Env = Env()
	res, err := n.Exec(ctx, cmd)
	if err != nil {
		return "", err
	}
	out := string(res.Stdout)
	if res.ExitCode != 0 {
		return out, fmt.Errorf("kudu %s: exit %d: %s", strings.Join(args, " "), res.ExitCode, toolStderr(res.Stderr))
	}
	return out, nil
}

// toolStderr is what a tool said, less the warning every run prints
// because the host's libselinux loads the bundled libpcre2.
func toolStderr(b []byte) string {
	var keep []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		if !strings.Contains(line, "no version information available") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, "\n")
}

// SetFlag changes flag on running server name, as `kudu <role> set_flag`
// does, forcing flags not marked as runtime-settable. It runs the tool on
// node from.
func (c *Cluster) SetFlag(ctx context.Context, from *torx.Node, name, flag, value string) error {
	role := TServer
	if strings.HasPrefix(name, Master) {
		role = Master
	}
	_, err := Tool(ctx, from, role, "set_flag", "-force", c.Addr(name), flag, value)
	return err
}
