//go:build unix

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
//
// A node needs a POSIX sh. To kill a service's whole process group on
// teardown, Stream runs each command as the leader of its own group: sshd
// usually provides that already (OpenSSH starts every command in a new
// session, and bash and zsh exec a lone -c command in place), and otherwise
// the wrapper creates one with setsid(1) -- util-linux or busybox -- or perl,
// whichever the node has. A node with neither, under an sshd that leaves
// commands in its own group (Dropbear), cannot stream; the error says so.
package ssh

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
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
	"golang.org/x/sys/unix"

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

// conn returns the node's SSH client, dialing on first use. cached reports
// whether the returned client came from the cache (and may therefore have gone
// stale before client.Wait() ran evict). The dial and handshake happen without
// holding b.mu -- both do network I/O and must not block unrelated operations,
// including close/teardown, on a wedged node.
func (b *backend) conn(ctx context.Context) (*cryptossh.Client, bool, error) {
	b.mu.Lock()
	if b.client != nil {
		c := b.client
		b.mu.Unlock()
		return c, true, nil
	}
	b.mu.Unlock()

	client, err := b.dial(ctx)
	if err != nil {
		return nil, false, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		// Another caller connected while we dialed; keep theirs and drop ours.
		_ = client.Close()
		return b.client, true, nil
	}
	b.client = client
	go func() {
		_ = client.Wait()
		b.evict(client)
	}()
	return client, false, nil
}

// isChannelRejection reports whether err is a peer-sent channel open rejection
// (such as MaxSessions) over an otherwise live SSH connection, as opposed to a
// dead transport that should be evicted and redialed.
func isChannelRejection(err error) bool {
	_, ok := errors.AsType[*cryptossh.OpenChannelError](err)
	return ok
}

// evict drops client (and any SFTP client layered over it) from the cache once
// the underlying SSH connection ends, so the next operation dials a fresh
// connection instead of failing forever against a dead one.
func (b *backend) evict(client *cryptossh.Client) {
	b.mu.Lock()
	var sc *sftp.Client
	if b.client == client {
		b.client = nil
		sc = b.sftp
		b.sftp = nil
	}
	b.mu.Unlock()
	if sc != nil {
		_ = sc.Close()
	}
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
	if !stopWatch() {
		// ctx ended during the handshake, so the watcher closed (or is closing)
		// netConn; never return a client backed by a dying connection.
		if sshConn != nil {
			_ = sshConn.Close()
		}
		_ = netConn.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: handshake "+b.addr, ctx.Err())
	}
	if err != nil {
		_ = netConn.Close()
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
	client, cached, err := b.conn(ctx)
	if err != nil {
		return torx.ExecResult{}, err
	}
	sess, err := client.NewSession()
	if err != nil && cached && !isChannelRejection(err) && ctx.Err() == nil {
		// The cached client died before client.Wait() reached evict; evict
		// synchronously and retry once on a fresh dial.
		b.evict(client)
		client, _, err = b.conn(ctx)
		if err != nil {
			return torx.ExecResult{}, err
		}
		sess, err = client.NewSession()
	}
	if err != nil {
		return torx.ExecResult{}, torx.Wrap(torx.ErrBackend, "ssh: session", err)
	}
	return execSession(ctx, sess, cmd)
}

// execOn runs cmd over a fresh session on a specific SSH client without
// falling back to a reconnected client. Used by sshStream for Signal and
// killGroup so a stale pgid from a dropped connection is never sent over a new
// connection (where a restarted node/container may have reused that pid).
func execOn(ctx context.Context, client *cryptossh.Client, cmd torx.Cmd) (torx.ExecResult, error) {
	sess, err := client.NewSession()
	if err != nil {
		return torx.ExecResult{}, torx.Wrap(torx.ErrBackend, "ssh: session", err)
	}
	return execSession(ctx, sess, cmd)
}

func execSession(ctx context.Context, sess *cryptossh.Session, cmd torx.Cmd) (torx.ExecResult, error) {
	defer func() { _ = sess.Close() }()

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
		if exit, ok := errors.AsType[*cryptossh.ExitError](runErr); ok {
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
// force-killed. The wait for the exit report that follows a kill shares the
// same deadline, so it cannot stretch teardown past it.
const killGroupTimeout = 8 * time.Second

// exitReportTimeout bounds how long teardown waits, after a kill it delivered,
// for the node to report the command's death. sshd reports it as soon as it
// reaps the command, which a SIGKILL of the whole group makes a matter of
// milliseconds; the bound is for a node slow to reap, or an sshd that never
// reports, and teardown must not hang on that.
const exitReportTimeout = 2 * time.Second

// Stream starts cmd on the node and returns the handle to it: its combined
// output, and the means to signal it, wait for it, and close it, which kills it
// and reaps it. The command runs as the leader of its own process group, so
// Close can SIGKILL that group -- tearing down a service and its children even
// if they ignore SIGTERM or SIGHUP -- without touching the node's sshd. The
// remote wrapper (see wrapForStream) arranges the group, prints its id as the
// first line, and then exec's the command in place, so the group id is also the
// command's own pid, which is what Signal targets.
//
// The session is driven as a raw channel rather than through a
// cryptossh.Session, because the command's exit has to be observed on its own.
// sshd reports it (an exit-status or exit-signal request) as soon as it reaps
// the command, but closes the channel only once the command's output pipes
// have drained, and a child that inherited them holds them open past the exit.
// Session.Wait returns only on the close, so through it a command that exited
// promptly on a signal would look like one still running until its children
// were killed -- and Shutdown would report a timeout for a clean exit.
func (b *backend) Stream(ctx context.Context, cmd torx.Cmd) (torx.Process, error) {
	client, cached, err := b.conn(ctx)
	if err != nil {
		return nil, err
	}
	ch, reqs, err := client.OpenChannel("session", nil)
	if err != nil && cached && !isChannelRejection(err) && ctx.Err() == nil {
		b.evict(client)
		client, _, err = b.conn(ctx)
		if err != nil {
			return nil, err
		}
		ch, reqs, err = client.OpenChannel("session", nil)
	}
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream session", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = ch.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream", err)
	}
	// Allocate the combined output pipe before launching the remote command so
	// an EMFILE failure cannot leave an already-started remote process leaked.
	pr, pw, err := os.Pipe()
	if err != nil {
		_ = ch.Close()
		_ = outW.Close()
		_ = outR.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream", err)
	}
	s := &sshStream{backend: b, client: client, ch: ch, pr: pr, exited: make(chan struct{}), done: make(chan struct{})}
	// Watch the session's requests from the start: the exit report is one of
	// them, and crypto/ssh delivers them on a bounded queue that stalls the
	// whole connection if nobody drains it.
	go s.watchRequests(reqs)
	if err := s.exec(cmd); err != nil {
		_ = ch.Close()
		_ = outW.Close()
		_ = outR.Close()
		_ = pw.Close()
		_ = pr.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	// Copy only stdout into outW while reading the PGID marker so early stderr
	// from the remote command (e.g. a failed cd or startup logs) cannot race
	// ahead of the TORX_PGID line and corrupt the handshake. Drain stderr into
	// a separate buffer from the start too: stdout and stderr share the SSH
	// channel's flow-control window, so leaving stderr unread during readPGID
	// would deadlock if early stderr filled the window before the marker.
	go func() {
		_, _ = io.Copy(outW, s.ch)
		_ = outW.Close()
	}()
	errPipe := startStderrDrain(s.ch.Stderr())
	// Read the group id off the first line before handing back the stream, so
	// Close knows what to kill. Bound that read by ctx -- a deadline or a plain
	// cancellation -- with a watcher that trips an immediate read deadline once ctx
	// is done. A deadline-only bound would ignore cancellation: the operator's stop
	// context carries no deadline of its own, so cancelling it must still unblock
	// this read. Clear the deadline and stop the watcher afterward so neither
	// affects the caller's reads over the stream's lifetime.
	stopWatch := context.AfterFunc(ctx, func() { _ = outR.SetReadDeadline(time.Unix(1, 0)) })
	br := bufio.NewReader(outR)
	pgid, err := readPGID(br)
	stopWatch()
	_ = outR.SetReadDeadline(time.Time{})
	if err != nil {
		if ctx.Err() == nil {
			// Give any trailing stderr diagnostic from the failed wrapper a
			// brief window to arrive before closing the channel.
			select {
			case <-errPipe.done:
			case <-time.After(50 * time.Millisecond):
			}
		}
		_ = ch.Close()
		_ = outR.Close()
		_ = pw.Close()
		_ = pr.Close()
		if ctx.Err() != nil {
			return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, ctx.Err())
		}
		if diag := errPipe.snippet(512); diag != "" {
			err = fmt.Errorf("%w (stderr: %s)", err, diag)
		}
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	s.pgid = pgid
	go s.copyOutput(pw, br, outR, errPipe)
	// Match the LocalBackend, whose command dies with the context that started it:
	// once Stream returns, nothing else watches ctx, so without this a cancelled
	// run would leave the command running on the node. The watcher and Close share
	// one teardown, run at most once.
	s.stopWatch = context.AfterFunc(ctx, func() { _ = s.teardown() })
	return s, nil
}

// sshStream is the handle Stream returns: reading it yields the command's
// combined output, Signal and Wait address the remote command by the pid the
// wrapper reported, and Close kills the remote process group and reaps the
// session. Cancelling the context that started the stream tears it down the same
// way, through the shared teardown.
type sshStream struct {
	backend   *backend
	client    *cryptossh.Client // the SSH connection this stream started on
	ch        cryptossh.Channel
	pr        *os.File
	pgid      int
	stopWatch func() bool
	once      sync.Once
	killErr   error
	exited    chan struct{} // closed once the node has reported the command's exit
	code      int           // the reported exit status, valid once exited is closed: the code, or -1 for a signal death
	sig       os.Signal     // the signal of a signal death the node named, valid once exited is closed; nil otherwise
	done      chan struct{} // closed once the session has ended, with the exit reported or without it
}

// exec starts cmd on the session, through the stream wrapper, feeding it
// Cmd.Stdin as Exec and the LocalBackend do: the wrapper exec's the command in
// place, so it inherits the session's stdin.
func (s *sshStream) exec(cmd torx.Cmd) error {
	ok, err := s.ch.SendRequest("exec", true, cryptossh.Marshal(struct{ Command string }{wrapForStream(cmd)}))
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the node refused the exec request")
	}
	go func() {
		if cmd.Stdin != nil {
			_, _ = io.Copy(s.ch, bytes.NewReader(cmd.Stdin))
		}
		_ = s.ch.CloseWrite()
	}()
	return nil
}

// watchRequests handles the session's requests until the channel closes,
// recording the command's exit when the node reports it -- an exit-status
// request with the code, or an exit-signal request for a signal death, which
// the Process contract renders as -1, keeping the signal's name for
// ExitSignal -- and refusing whatever else wants a reply, as OpenSSH's client
// does. The exit is reported at most once, so the first report is the
// command's.
func (s *sshStream) watchRequests(reqs <-chan *cryptossh.Request) {
	reported := false
	report := func(code int, sig os.Signal) {
		if reported {
			return
		}
		reported = true
		s.code, s.sig = code, sig
		close(s.exited)
	}
	for req := range reqs {
		switch req.Type {
		case "exit-status":
			if len(req.Payload) >= 4 {
				report(int(binary.BigEndian.Uint32(req.Payload)), nil)
			}
		case "exit-signal":
			report(-1, exitSignal(req.Payload))
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
	close(s.done)
}

// exitSignal is the signal an exit-signal request names (RFC 4254, section
// 6.10: the name without SIG, whether a core was dumped, an error message,
// and a language tag), or nil for a name this host has no number for.
func exitSignal(payload []byte) os.Signal {
	var msg struct {
		Signal     string
		CoreDumped bool
		Error      string
		Lang       string
	}
	if cryptossh.Unmarshal(payload, &msg) != nil {
		return nil
	}
	if num := unix.SignalNum("SIG" + msg.Signal); num != 0 {
		return num
	}
	return nil
}

// maxEarlyStderr caps how much pre-marker stderr stderrPipe buffers in memory
// while readPGID waits on stdout. Bytes beyond this cap are still drained from
// the SSH channel (so the shared flow-control window never stalls) and
// discarded.
const maxEarlyStderr = 4 << 20

// stderrPipe drains the channel's stderr from the moment exec starts so early
// stderr cannot fill the shared SSH channel flow-control window while readPGID
// waits on stdout. Bytes arriving before attach are buffered in memory up to
// maxEarlyStderr, then flushed in order once attach hands over the combined
// output pipe.
type stderrPipe struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	pw        *os.File
	streaming bool
	done      chan struct{}
}

func startStderrDrain(r io.Reader) *stderrPipe {
	sp := &stderrPipe{done: make(chan struct{})}
	go func() {
		defer close(sp.done)
		var chunk [32 << 10]byte
		for {
			n, err := r.Read(chunk[:])
			if n > 0 {
				sp.mu.Lock()
				if sp.streaming {
					pw := sp.pw
					sp.mu.Unlock()
					if _, werr := pw.Write(chunk[:n]); werr != nil {
						return
					}
					_, _ = io.Copy(pw, r)
					return
				}
				if rem := maxEarlyStderr - sp.buf.Len(); rem > 0 {
					sp.buf.Write(chunk[:min(n, rem)])
				}
				sp.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return sp
}

// snippet returns up to maxBytes of trimmed pre-marker stderr captured so far,
// for surfacing shell startup errors when readPGID fails.
func (sp *stderrPipe) snippet(maxBytes int) string {
	sp.mu.Lock()
	raw := bytes.TrimSpace(sp.buf.Bytes())
	if len(raw) > maxBytes {
		raw = raw[:maxBytes]
	}
	out := string(raw)
	sp.mu.Unlock()
	return out
}

func (sp *stderrPipe) attach(pw *os.File) {
	sp.mu.Lock()
	sp.pw = pw
	sp.streaming = true
	// Flush buffered bytes under sp.mu so the drainer cannot write newer
	// stderr bytes to pw ahead of them.
	if sp.buf.Len() > 0 {
		_, _ = io.Copy(pw, &sp.buf)
		sp.buf.Reset()
	}
	sp.mu.Unlock()
	<-sp.done
}

// copyOutput merges the remainder of the command's stdout (already buffered in
// stdout plus outR) and its stderr (starting with any bytes buffered during
// readPGID) into pw, interleaved as they arrive, and closes pw once both have
// ended -- when the node has closed the channel, or the connection has dropped.
func (s *sshStream) copyOutput(pw *os.File, stdout io.Reader, outR *os.File, errPipe *stderrPipe) {
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(pw, stdout)
		_ = outR.Close()
	})
	wg.Go(func() {
		errPipe.attach(pw)
	})
	wg.Wait()
	_ = pw.Close()
}

func (s *sshStream) Read(p []byte) (int, error) { return s.pr.Read(p) }

// Signal sends sig to the remote command, by the pid the wrapper reported: the
// command was exec'd in place, so that pid is the command's own, not a shell's.
// Once the node has reported the command's exit it is refused, as the
// LocalBackend refuses it, rather than sent to whatever process may hold the
// pid by now. A session that ended without reporting the exit -- the connection
// dropped -- says nothing about the command, which may well still be running,
// so the signal is sent as usual and fails only if the node cannot be reached.
func (s *sshStream) Signal(ctx context.Context, sig os.Signal) error {
	select {
	case <-s.exited:
		return torx.Wrap(torx.ErrBackend, "ssh: signal", os.ErrProcessDone)
	default:
	}
	return signalOn(ctx, s.client, s.pgid, sig)
}

// Wait blocks until the node reports the command's exit or ctx is done. sshd
// reaps the command itself, so Wait only observes the exit; the status is what
// the node reported: the exit code, or -1 for a command terminated by a signal.
// The node reports a signal death as such only when the command was the
// session's own process, which it is under OpenSSH with a login shell that
// exec's its command (bash, zsh); a login shell that forks it (dash) reports
// its own exit instead, 128 plus the signal number, as a shell does. The report
// is observed on its own, not through the session's end: the command's output
// pipes may stay open past its exit, held by a child, and the exit counts all
// the same.
func (s *sshStream) Wait(ctx context.Context) (int, error) {
	select {
	case <-s.exited:
	case <-s.done:
	case <-ctx.Done():
		select {
		case <-s.exited:
		case <-s.done:
			// Exited as ctx ran out: the outcome is the better answer.
		default:
			return -1, torx.Wrap(torx.ErrBackend, "ssh: wait", ctx.Err())
		}
	}
	select {
	case <-s.exited:
		return s.code, nil
	default:
	}
	// The session ended without an exit report: the connection dropped, or the
	// node closed the channel without sending one. A report never follows a
	// close, so this is final.
	return -1, torx.Wrap(torx.ErrBackend, "ssh: wait", errors.New("the session ended without reporting the command's exit"))
}

// ExitSignal implements torx.ExitSignaler: the signal the node named in its
// report of the command's death, once it has reported one. The node names it
// under the same condition as it reports a signal death at all, which Wait
// describes.
func (s *sshStream) ExitSignal() (os.Signal, bool) {
	select {
	case <-s.exited:
		return s.sig, s.sig != nil
	default:
		return nil, false
	}
}

// Close tears the stream down and reports whether the remote kill succeeded. A
// failed kill means a service believed stopped may still be running, which must
// surface so the node is not silently reused.
func (s *sshStream) Close() error {
	if s.stopWatch != nil {
		s.stopWatch()
	}
	return s.teardown()
}

// teardown kills the remote process group and reaps the session exactly once,
// whether it is Close or the context watcher that reaches it first. It records
// the kill outcome so Close can return it. The group is killed even once the
// session has ended: an exit the session reported is the command's alone, and
// its children may outlive it, while a session that failed reported nothing
// at all. Only once the group has emptied could its id name another group on
// the node, and then only after the node's pid space has wrapped around in
// between; a caller that closes soon after its wait, as Shutdown does, keeps
// that window negligible.
//
// A kill that ran is followed by a bounded wait for the node's report of the
// command's death, so that it reaches the handle -- and a Wait after the Close
// can say how the command died -- before the session is closed from this side.
// Closing it first would race the report: a close that reaches sshd before it
// has reaped the command discards the report, and Wait could then only say
// the exit went unobserved. The wait is for the report, not for the session
// to end: a pipe held open by a process outside the group -- a daemon the
// command double-forked -- keeps the session open past the report, and
// nothing reads the output once the handle is closed.
func (s *sshStream) teardown() error {
	s.once.Do(func() {
		// Close carries no context, so bound the teardown here: a wedged node
		// must not hang it forever.
		ctx, cancel := context.WithTimeout(context.Background(), killGroupTimeout)
		defer cancel()
		s.killErr = killGroupOn(ctx, s.client, s.pgid)
		if s.killErr == nil {
			wctx, wcancel := context.WithTimeout(ctx, exitReportTimeout)
			select {
			case <-s.exited:
			case <-s.done:
			case <-wctx.Done():
			}
			wcancel()
		}
		_ = s.ch.Close()
		_ = s.pr.Close()
	})
	return s.killErr
}

// killGroupOn SIGKILLs a remote process group over a fresh session on client
// (the connection the stream started on) and reports whether the kill could be
// carried out. It deliberately does not reconnect if client has dropped: after
// a node or container restart, pgid may have been reused by an unrelated
// process, so an unconfirmed kill surfaces as ErrBackend instead.
func killGroupOn(ctx context.Context, client *cryptossh.Client, pgid int) error {
	if pgid < 2 {
		// Never signal group 1 (every process the caller may signal) or 0 (the
		// caller's own group). readPGID already rejects these, but Close must not
		// turn a stray id into kill -KILL -1 either.
		return nil
	}
	if _, err := execOn(ctx, client, torx.Command("kill", "-KILL", "-"+strconv.Itoa(pgid))); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: kill group", err)
	}
	return nil
}

// streamPrologue is the POSIX sh that runs on the node ahead of a streamed
// command, once $c holds the command line to exec (see wrapForStream). It makes
// the command the leader of its own process group, prints that group's id as the
// marker line, and exec's the command -- so the group Close kills never contains
// anything but the command and its descendants.
//
// It first reads the process group it is in: from /proc/$$/stat on Linux (present
// in every container, and busybox ps has no -p), stripping through the last ") "
// before splitting because the comm field may itself contain spaces or
// parentheses, and from ps elsewhere (macOS, BSD). Then, in order:
//
//   - If this shell already leads its group, it just runs the command. That is
//     the common case under OpenSSH, whose do_exec_no_pty calls setsid() before
//     exec'ing the login shell, when the login shell exec's a lone -c command in
//     place (bash, zsh).
//   - Otherwise this shell is not a group leader, so setsid(2) will succeed for
//     it directly: setsid(1) then exec's without forking, needs no -w, and the
//     command keeps the pid the login shell above is waiting on. util-linux and
//     busybox both provide it. This handles a login shell that forks (dash,
//     Debian's /bin/sh), any depth of ForceCommand or audit wrappers, and an
//     sshd that does not isolate commands at all (Dropbear).
//   - Failing that, perl's setpgrp(0,0) does the same with a new process group;
//     macOS has perl but no setsid(1).
//   - With none of these, it refuses and says what would fix it. It also refuses
//     when the group cannot be read at all, since without knowing whether it is
//     a leader it cannot tell whether setsid would fork and detach the session.
//
// echo appends a trailing newline, so the marker is a complete line readPGID
// can read before exec runs the command: the handshake never waits on the
// command's own output. Keep the echo (or anything else that terminates the
// marker with a newline), or readPGID will block.
const streamPrologue = `if [ -r /proc/$$/stat ]; then s=$(cat /proc/$$/stat); s=${s##*\) }; set -- $s; pgid=$3; ` +
	`else pgid=$(ps -o pgid= -p $$ 2>/dev/null | tr -d " "); fi; ` +
	`if [ -z "$pgid" ]; then echo "torx: cannot determine the process group of pid $$: the node has neither /proc nor ps"; exit 1; fi; ` +
	`if [ "$pgid" = "$$" ]; then echo ` + pgidMarker + `$$; eval "$c"; exit $?; fi; ` +
	`if command -v setsid >/dev/null 2>&1; then exec setsid sh -c "echo ` + pgidMarker + `\$\$; $c"; fi; ` +
	`if command -v perl >/dev/null 2>&1; then exec perl -e 'setpgrp(0,0) or die "setpgrp: $!"; exec @ARGV or die "exec: $!"' -- sh -c "echo ` + pgidMarker + `\$\$; $c"; fi; ` +
	`echo "torx: pid $$ shares process group $pgid with the sshd or a wrapper above it and cannot start one of its own:` +
	` install util-linux setsid (or perl) on the node, or use an sshd that starts each command in a new session, as OpenSSH does"; exit 1`

// wrapForStream builds the remote command line for Stream: a sh that stores the
// command line (remoteCommand's exec form, so the command replaces the shell
// that runs it) in $c and then runs streamPrologue. $c is expanded once, inside
// double quotes, into the -c string of the shell setsid or perl starts, and the
// result of a variable expansion is not rescanned, so the single-quoted words
// remoteCommand produced reach that shell intact.
func wrapForStream(cmd torx.Cmd) string {
	return "sh -c " + shQuote("c="+shQuote(remoteCommand(cmd, true))+"; "+streamPrologue)
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
// sig must be a syscall.Signal so signalArg can format its portable POSIX name.
func (b *backend) Signal(ctx context.Context, pid int, sig os.Signal) error {
	arg, err := validateSignal(pid, sig)
	if err != nil {
		return err
	}
	res, err := b.Exec(ctx, torx.Command("kill", "-"+arg, strconv.Itoa(pid)))
	if err != nil {
		return err
	}
	if err := res.ExitError("kill"); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: signal", err)
	}
	return nil
}

func signalOn(ctx context.Context, client *cryptossh.Client, pid int, sig os.Signal) error {
	arg, err := validateSignal(pid, sig)
	if err != nil {
		return err
	}
	res, err := execOn(ctx, client, torx.Command("kill", "-"+arg, strconv.Itoa(pid)))
	if err != nil {
		return err
	}
	if err := res.ExitError("kill"); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: signal", err)
	}
	return nil
}

func validateSignal(pid int, sig os.Signal) (string, error) {
	num, ok := sig.(syscall.Signal)
	if !ok {
		return "", torx.Wrap(torx.ErrBackend, "ssh: signal", fmt.Errorf("unsupported signal %v (%T)", sig, sig))
	}
	if pid < 2 {
		// 1 is init, 0 is the caller's process group, and negatives target a group
		// or every process the user may signal. Only a specific process is allowed,
		// matching the pgid guard in killGroupOn and readPGID.
		return "", torx.Wrap(torx.ErrBackend, "ssh: signal", fmt.Errorf("refusing to signal unsafe pid %d", pid))
	}
	return signalArg(num), nil
}

// signalArg formats sig for POSIX kill(1): signal 0 (liveness probe) as "0",
// named signals by their portable POSIX name without the "SIG" prefix so a
// signal sent across OSes (e.g. macOS to Linux) does not map to a different
// number on the node, and any unnamed signal numerically.
func signalArg(sig syscall.Signal) string {
	if sig == 0 {
		return "0"
	}
	if name := unix.SignalName(sig); name != "" {
		return strings.TrimPrefix(name, "SIG")
	}
	return strconv.Itoa(int(sig))
}

// runToCompletion starts line on sess and waits for it, cancelling via a remote
// signal and a channel close if ctx is done first. crypto/ssh sessions are not
// context-aware, so cancellation is layered on with a goroutine.
func runToCompletion(ctx context.Context, sess *cryptossh.Session, line string) error {
	if err := sess.Start(line); err != nil {
		return err
	}
	var waitErr error
	done := make(chan struct{})
	go func() { waitErr = sess.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		// The session is killed: ctx says why, not its wait.
		_ = sess.Signal(cryptossh.SIGKILL)
		_ = sess.Close()
		<-done
		return ctx.Err()
	case <-done:
		return waitErr
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
