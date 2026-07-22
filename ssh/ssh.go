// Package ssh is torx's SSH backend: it drives a node over an SSH connection,
// running commands through exec sessions and moving files over SFTP. It
// registers itself under the "ssh" backend kind, so a suite blank-imports this
// package to make ssh-kind nodes constructible:
//
//	import _ "github.com/dotnwat/torx/ssh"
//
// Authentication is public-key only and the host key is always verified against
// a known_hosts file (see Config). The heavy dependencies this brings --
// golang.org/x/crypto/ssh and github.com/pkg/sftp -- stay out of the torx core
// by living behind the registered BackendBuilder here.
package ssh

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/sftp"
	cryptossh "golang.org/x/crypto/ssh"

	"github.com/dotnwat/torx"
)

func init() {
	torx.RegisterBackend("ssh", build)
}

// build constructs an SSH backend from a node descriptor. It validates the
// configuration eagerly -- a bad key or missing host fails fast -- but defers
// dialing the connection until the first operation, since the driver serializes
// far more nodes than any single job uses and never dials remote nodes itself.
func build(d torx.BackendDescriptor) (torx.Backend, error) {
	if d.Host == "" {
		return nil, fmt.Errorf("ssh: backend descriptor has no host")
	}
	var cfg Config
	if len(d.Config) > 0 {
		if err := json.Unmarshal(d.Config, &cfg); err != nil {
			return nil, fmt.Errorf("ssh: backend config: %w", err)
		}
	}
	clientCfg, err := clientConfig(cfg)
	if err != nil {
		return nil, err
	}
	port := cfg.Port
	if port == 0 {
		port = defaultPort
	}
	return &backend{
		addr:      net.JoinHostPort(d.Host, strconv.Itoa(port)),
		clientCfg: clientCfg,
	}, nil
}

// backend drives one node over a single, lazily-dialed SSH connection. The
// connection and its SFTP client are created on first use and reused across
// operations; the crypto/ssh client is safe for the concurrent sessions a
// service's fan-out across its nodes implies.
type backend struct {
	addr      string
	clientCfg *cryptossh.ClientConfig

	mu     sync.Mutex
	client *cryptossh.Client
	sftp   *sftp.Client
}

var _ torx.Backend = (*backend)(nil)

// conn returns the node's SSH client, dialing on first use. The dial and
// handshake happen without holding b.mu -- both do network I/O and must not
// block unrelated operations, including close/teardown, on a wedged node.
func (b *backend) conn(ctx context.Context) (*cryptossh.Client, error) {
	b.mu.Lock()
	if b.client != nil {
		c := b.client
		b.mu.Unlock()
		return c, nil
	}
	b.mu.Unlock()

	client, err := b.dial(ctx)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		// Another caller connected while we dialed; keep theirs and drop ours.
		_ = client.Close()
		return b.client, nil
	}
	b.client = client
	return client, nil
}

// dial opens a new SSH connection to the node. The TCP dial and the handshake
// both honor ctx, and the handshake additionally honors the client config's
// connect timeout, which crypto/ssh's NewClientConn does not apply on its own.
func (b *backend) dial(ctx context.Context) (*cryptossh.Client, error) {
	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", b.addr)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: dial "+b.addr, err)
	}
	// NewClientConn takes no context or timeout, so a half-open or silent peer
	// would block the handshake forever. Bound it two ways: a ctx watcher that
	// closes the conn on cancellation, and the config's connect timeout as an I/O
	// deadline. Clear the deadline once connected so it does not affect sessions.
	if b.clientCfg.Timeout > 0 {
		_ = netConn.SetDeadline(time.Now().Add(b.clientCfg.Timeout))
	}
	stopWatch := context.AfterFunc(ctx, func() { _ = netConn.Close() })
	sshConn, chans, reqs, err := cryptossh.NewClientConn(netConn, b.addr, b.clientCfg)
	stopWatch()
	if err != nil {
		_ = netConn.Close()
		if ctx.Err() != nil {
			return nil, torx.Wrap(torx.ErrBackend, "ssh: handshake "+b.addr, ctx.Err())
		}
		return nil, torx.Wrap(torx.ErrBackend, "ssh: handshake "+b.addr, err)
	}
	_ = netConn.SetDeadline(time.Time{})
	return cryptossh.NewClient(sshConn, chans, reqs), nil
}

// close tears down the SSH connection and its SFTP client. A one-shot worker
// relies on process exit for cleanup; close exists for longer-lived callers and
// for tests.
func (b *backend) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sftp != nil {
		_ = b.sftp.Close()
		b.sftp = nil
	}
	if b.client != nil {
		_ = b.client.Close()
		b.client = nil
	}
}

// Exec runs cmd to completion over a fresh session and returns its captured
// output. A non-zero remote exit is reported in ExecResult.ExitCode, matching
// the Backend contract; only a transport failure returns an error.
func (b *backend) Exec(ctx context.Context, cmd torx.Cmd) (torx.ExecResult, error) {
	client, err := b.conn(ctx)
	if err != nil {
		return torx.ExecResult{}, err
	}
	sess, err := client.NewSession()
	if err != nil {
		return torx.ExecResult{}, torx.Wrap(torx.ErrBackend, "ssh: session", err)
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr
	if cmd.Stdin != nil {
		sess.Stdin = bytes.NewReader(cmd.Stdin)
	}

	runErr := runToCompletion(ctx, sess, remoteCommand(cmd, false))
	res := torx.ExecResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if ctx.Err() != nil {
		return res, torx.Wrap(torx.ErrBackend, "ssh: exec "+cmd.Path, ctx.Err())
	}
	if runErr != nil {
		var exit *cryptossh.ExitError
		if errors.As(runErr, &exit) {
			res.ExitCode = exit.ExitStatus()
			return res, nil
		}
		return res, torx.Wrap(torx.ErrBackend, "ssh: exec "+cmd.Path, runErr)
	}
	return res, nil
}

// pgidMarker prefixes the process-group id the stream wrapper prints, so the
// backend learns which remote group Close must kill.
const pgidMarker = "TORX_PGID:"

// killGroupTimeout bounds the remote kill Close issues, so tearing down a stream
// cannot block indefinitely on a wedged node. It is kept below the driver's
// worker grace period (torx.workerGracePeriod, 10s) so a cancelled worker's kill
// attempt can finish -- and a failure be observed -- before the worker itself is
// force-killed.
const killGroupTimeout = 8 * time.Second

// Stream starts cmd on the node and returns its combined output; Close kills the
// command and reaps it. The command runs as the leader of a new session (via
// setsid) so Close can SIGKILL the whole process group -- tearing down a service
// and its children even if they ignore SIGTERM or SIGHUP, and leaving the node's
// sshd untouched. setsid -w keeps the SSH session open for the command's
// lifetime rather than detaching it, and the leader prints its pid (the group
// id) before exec'ing the command in place.
func (b *backend) Stream(ctx context.Context, cmd torx.Cmd) (io.ReadCloser, error) {
	client, err := b.conn(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := client.NewSession()
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream session", err)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		_ = sess.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream", err)
	}
	sess.Stdout = pw
	sess.Stderr = pw
	if cmd.Stdin != nil {
		// Feed stdin to the wrapped command, matching Exec and the LocalBackend.
		// The setsid wrapper exec's the command in place, so it inherits this stdin.
		sess.Stdin = bytes.NewReader(cmd.Stdin)
	}
	if err := sess.Start(wrapForStream(cmd)); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		_ = sess.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	// Close the write end once the command exits so the reader sees EOF. This
	// must start before reading the marker: if the remote dies before emitting
	// it, this goroutine's pw.Close is what unblocks readPGID with EOF instead of
	// leaving it to block forever on a marker that will never arrive.
	go func() {
		_ = sess.Wait()
		_ = pw.Close()
	}()
	// Read the group id off the first line before handing back the stream, so
	// Close knows what to kill. Bound that read by ctx -- a deadline or a plain
	// cancellation -- with a watcher that trips an immediate read deadline once ctx
	// is done. A deadline-only bound would ignore cancellation: the operator's stop
	// context carries no deadline of its own, so cancelling it must still unblock
	// this read. Clear the deadline and stop the watcher afterward so neither
	// affects the caller's reads over the stream's lifetime.
	stopWatch := context.AfterFunc(ctx, func() { _ = pr.SetReadDeadline(time.Unix(1, 0)) })
	br := bufio.NewReader(pr)
	pgid, err := readPGID(br)
	stopWatch()
	_ = pr.SetReadDeadline(time.Time{})
	if err != nil {
		_ = sess.Close()
		_ = pw.Close()
		_ = pr.Close()
		if ctx.Err() != nil {
			return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, ctx.Err())
		}
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	return &sshStream{backend: b, sess: sess, pr: pr, r: br, pgid: pgid}, nil
}

// sshStream is the handle Stream returns: reading it yields the command's
// combined output, and Close kills the remote process group and reaps the
// session.
type sshStream struct {
	backend *backend
	sess    *cryptossh.Session
	pr      *os.File
	r       *bufio.Reader
	pgid    int
}

func (s *sshStream) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *sshStream) Close() error {
	killErr := s.backend.killGroup(s.pgid)
	_ = s.sess.Close()
	prErr := s.pr.Close()
	// Surface a failed remote kill above the local pipe close: a service believed
	// stopped but still running would contaminate a later job on the same node, so
	// this error is what lets teardown flag the node as not safe to reuse.
	if killErr != nil {
		return killErr
	}
	return prErr
}

// killGroup SIGKILLs a remote process group over a fresh session and reports
// whether the kill could be carried out. The group already being gone is success
// for teardown, but a transport failure or a kill that could not run is returned
// so the caller can treat the node as not confirmed clean rather than silently
// reused.
func (b *backend) killGroup(pgid int) error {
	if pgid < 2 {
		// Never signal group 1 (every process the caller may signal) or 0 (the
		// caller's own group). readPGID already rejects these, but Close must not
		// turn a stray id into kill -KILL -1 either.
		return nil
	}
	// Close carries no context, so bound the kill here: a wedged node must not
	// hang teardown forever.
	ctx, cancel := context.WithTimeout(context.Background(), killGroupTimeout)
	defer cancel()
	res, err := b.Exec(ctx, torx.Command("kill", "-KILL", "-"+strconv.Itoa(pgid)))
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: kill group", err)
	}
	if res.ExitCode != 0 && !strings.Contains(string(res.Stderr), "No such process") {
		return torx.Wrap(torx.ErrBackend, "ssh: kill group",
			fmt.Errorf("kill exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr))))
	}
	return nil
}

// wrapForStream builds the remote command line for Stream. setsid -w runs the
// command as a new session/group leader (so Close can kill the group) while
// waiting for it (so the SSH session lives as long as the command). The leader
// prints its pid -- the group id -- then exec's the command in place (via
// remoteCommand's exec form), so the command inherits that pid and stays the
// group leader. echo appends a trailing newline, so the marker is a complete
// line readPGID can read before exec runs the command: the handshake never
// waits on the command's own output. Keep the echo (or anything else that
// terminates the marker with a newline), or readPGID will block.
func wrapForStream(cmd torx.Cmd) string {
	payload := "echo " + pgidMarker + "$$; " + remoteCommand(cmd, true)
	return "setsid -w sh -c " + shQuote(payload)
}

// readPGID reads the process-group id the stream wrapper prints on its first
// line, up to the newline echo appends (see wrapForStream). That echo runs
// before the command is exec'd, so the marker is available immediately and does
// not depend on the command producing any output of its own. The complete line
// is required, and the id must be a plausible pgid (>= 2): a partial read or an
// out-of-range value is rejected rather than fed to a later kill.
func readPGID(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		// Without the trailing newline the marker is incomplete: the remote died
		// mid-write or before echoing it. A truncated pgid must never be trusted --
		// "TORX_PGID:1" cut from ":1234" would target process group 1.
		return 0, fmt.Errorf("ssh: stream: incomplete %q marker %q: %w", pgidMarker, strings.TrimSpace(line), err)
	}
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), pgidMarker)
	if !ok {
		return 0, fmt.Errorf("ssh: stream: expected %q marker, got %q", pgidMarker, strings.TrimSpace(line))
	}
	pgid, err := strconv.Atoi(rest)
	if err != nil {
		return 0, fmt.Errorf("ssh: stream: bad process-group id %q: %w", rest, err)
	}
	if pgid < 2 {
		// A session leader's pgid is its pid, always >= 2 (1 is init). Reject
		// anything lower so a negated pgid can never become kill -KILL -1 (every
		// process the caller may signal) or -0 (the caller's own group).
		return 0, fmt.Errorf("ssh: stream: implausible process-group id %d", pgid)
	}
	return pgid, nil
}

// Signal sends sig to a process on the node by running kill over a session. The
// pid is a pid on the node, e.g. one a service recorded in a pidfile. Because
// that pid crosses the wire from a node-side file, it is validated before use:
// kill treats a non-positive operand as a process group or a broadcast, so a
// stale or hostile pidfile holding 0 or -1 must never become "kill -KILL -1".
func (b *backend) Signal(ctx context.Context, pid int, sig syscall.Signal) error {
	if pid < 2 {
		// 1 is init, 0 is the caller's process group, and negatives target a group
		// or every process the user may signal. Only a specific process is allowed,
		// matching the pgid guard in killGroup and readPGID.
		return torx.Wrap(torx.ErrBackend, "ssh: signal", fmt.Errorf("refusing to signal unsafe pid %d", pid))
	}
	res, err := b.Exec(ctx, torx.Command("kill", "-"+strconv.Itoa(int(sig)), strconv.Itoa(pid)))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return torx.Wrap(torx.ErrBackend, "ssh: signal",
			fmt.Errorf("kill exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr))))
	}
	return nil
}

// runToCompletion starts line on sess and waits for it, cancelling via a remote
// signal and a channel close if ctx is done first. crypto/ssh sessions are not
// context-aware, so cancellation is layered on with a goroutine.
func runToCompletion(ctx context.Context, sess *cryptossh.Session, line string) error {
	if err := sess.Start(line); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- sess.Wait() }()
	select {
	case <-ctx.Done():
		_ = sess.Signal(cryptossh.SIGKILL)
		_ = sess.Close()
		<-done
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// remoteCommand renders a Cmd as a single shell command line for an exec
// request: the node's login shell interprets it, so environment assignments and
// the working directory are applied with env and cd, and every token is quoted.
// When execProc is set the program replaces the shell (exec) so it inherits the
// shell's pid; Stream relies on that, because that pid is the process-group id
// readPGID reports. The exec must sit after the cd prefix -- exec'ing the cd
// builtin would fail and never run the program -- and before env, so exec
// replaces the shell with env, which runs the program in place.
func remoteCommand(cmd torx.Cmd, execProc bool) string {
	var b strings.Builder
	if cmd.Dir != "" {
		b.WriteString("cd " + shQuote(cmd.Dir) + " && ")
	}
	if execProc {
		b.WriteString("exec ")
	}
	if len(cmd.Env) > 0 {
		b.WriteString("env")
		for _, e := range cmd.Env {
			b.WriteString(" " + shQuote(e))
		}
		b.WriteByte(' ')
	}
	b.WriteString(shQuote(cmd.Path))
	for _, a := range cmd.Args {
		b.WriteString(" " + shQuote(a))
	}
	return b.String()
}

// shQuote single-quotes s for safe use in a POSIX shell command, escaping any
// embedded single quotes.
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
