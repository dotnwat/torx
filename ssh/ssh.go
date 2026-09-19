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
	client, err := b.conn(ctx)
	if err != nil {
		return nil, err
	}
	ch, reqs, err := client.OpenChannel("session", nil)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream session", err)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		_ = ch.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream", err)
	}
	s := &sshStream{backend: b, ch: ch, pr: pr, exited: make(chan struct{}), done: make(chan struct{})}
	// Watch the session's requests from the start: the exit report is one of
	// them, and crypto/ssh delivers them on a bounded queue that stalls the
	// whole connection if nobody drains it.
	go s.watchRequests(reqs)
	if err := s.exec(cmd); err != nil {
		_ = ch.Close()
		_ = pw.Close()
		_ = pr.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	// Copy the command's output into the pipe and close the write end once
	// both streams have ended, so the reader sees EOF. This must start before
	// reading the marker: if the remote dies before emitting it, that close is
	// what unblocks readPGID with EOF instead of leaving it to block forever on
	// a marker that will never arrive.
	go s.copyOutput(pw)
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
		_ = ch.Close()
		_ = pr.Close()
		if ctx.Err() != nil {
			return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, ctx.Err())
		}
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	s.r = br
	s.pgid = pgid
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
	ch        cryptossh.Channel
	pr        *os.File
	r         *bufio.Reader
	pgid      int
	stopWatch func() bool
	once      sync.Once
	killErr   error
	exited    chan struct{} // closed once the node has reported the command's exit
	code      int           // the reported exit status, valid once exited is closed: the code, or -1 for a signal death
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
// the Process contract renders as -1 -- and refusing whatever else wants a
// reply, as OpenSSH's client does. The exit is reported at most once, so the
// first report is the command's.
func (s *sshStream) watchRequests(reqs <-chan *cryptossh.Request) {
	reported := false
	report := func(code int) {
		if reported {
			return
		}
		reported = true
		s.code = code
		close(s.exited)
	}
	for req := range reqs {
		switch req.Type {
		case "exit-status":
			if len(req.Payload) >= 4 {
				report(int(binary.BigEndian.Uint32(req.Payload)))
			}
		case "exit-signal":
			report(-1)
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
	close(s.done)
}

// copyOutput copies the command's stdout and stderr into pw, interleaved as
// they arrive, and closes pw once both have ended -- when the node has closed
// the channel, or the connection has dropped.
func (s *sshStream) copyOutput(pw *os.File) {
	var wg sync.WaitGroup
	for _, r := range []io.Reader{s.ch, s.ch.Stderr()} {
		wg.Go(func() { _, _ = io.Copy(pw, r) })
	}
	wg.Wait()
	_ = pw.Close()
}

func (s *sshStream) Read(p []byte) (int, error) { return s.r.Read(p) }

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
	return s.backend.Signal(ctx, s.pgid, sig)
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
		s.killErr = s.backend.killGroup(ctx, s.pgid)
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

// killGroup SIGKILLs a remote process group over a fresh session and reports
// whether the kill could be carried out. A transport failure -- an unreachable
// node, a session that would not open, ctx running out -- is returned so the
// caller can treat the node as not confirmed clean rather than silently reused.
// Once the kill actually runs its exit status is not inspected: the group is
// led by this stream's own command (see wrapForStream), so a non-zero exit
// means the group is already gone, which is success for teardown. kill(1)
// cannot distinguish that from other failures by exit code anyway, and its
// diagnostic text is locale- and implementation-dependent, so relying on
// either would be less reliable than the transport error the run-or-not
// signal already provides.
func (b *backend) killGroup(ctx context.Context, pgid int) error {
	if pgid < 2 {
		// Never signal group 1 (every process the caller may signal) or 0 (the
		// caller's own group). readPGID already rejects these, but Close must not
		// turn a stray id into kill -KILL -1 either.
		return nil
	}
	if _, err := b.Exec(ctx, torx.Command("kill", "-KILL", "-"+strconv.Itoa(pgid))); err != nil {
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
	`if [ "$pgid" = "$$" ]; then echo ` + pgidMarker + `$$; eval "$c"; fi; ` +
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
// kill(1) takes the signal by number, so sig must be a syscall.Signal.
func (b *backend) Signal(ctx context.Context, pid int, sig os.Signal) error {
	num, ok := sig.(syscall.Signal)
	if !ok {
		return torx.Wrap(torx.ErrBackend, "ssh: signal", fmt.Errorf("unsupported signal %v (%T)", sig, sig))
	}
	if pid < 2 {
		// 1 is init, 0 is the caller's process group, and negatives target a group
		// or every process the user may signal. Only a specific process is allowed,
		// matching the pgid guard in killGroup and readPGID.
		return torx.Wrap(torx.ErrBackend, "ssh: signal", fmt.Errorf("refusing to signal unsafe pid %d", pid))
	}
	res, err := b.Exec(ctx, torx.Command("kill", "-"+strconv.Itoa(int(num)), strconv.Itoa(pid)))
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
