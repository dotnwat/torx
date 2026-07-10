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

// conn returns the node's SSH client, dialing on first use. The TCP dial honors
// ctx; the handshake honors the client config's timeout.
func (b *backend) conn(ctx context.Context) (*cryptossh.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		return b.client, nil
	}
	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", b.addr)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: dial "+b.addr, err)
	}
	sshConn, chans, reqs, err := cryptossh.NewClientConn(netConn, b.addr, b.clientCfg)
	if err != nil {
		_ = netConn.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: handshake "+b.addr, err)
	}
	b.client = cryptossh.NewClient(sshConn, chans, reqs)
	return b.client, nil
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
	if err := sess.Start(wrapForStream(cmd)); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		_ = sess.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	// Read the group id off the first line before handing back the stream, so
	// Close knows what to kill.
	br := bufio.NewReader(pr)
	pgid, err := readPGID(br)
	if err != nil {
		_ = sess.Close()
		_ = pw.Close()
		_ = pr.Close()
		return nil, torx.Wrap(torx.ErrBackend, "ssh: stream "+cmd.Path, err)
	}
	// Once the command exits, close the write end so the reader sees EOF.
	go func() {
		_ = sess.Wait()
		_ = pw.Close()
	}()
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
	s.backend.killGroup(s.pgid)
	_ = s.sess.Close()
	return s.pr.Close()
}

// killGroup SIGKILLs a remote process group over a fresh session. It is
// best-effort: the group may already be gone.
func (b *backend) killGroup(pgid int) {
	_, _ = b.Exec(context.Background(), torx.Command("kill", "-KILL", "-"+strconv.Itoa(pgid)))
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
// not depend on the command producing any output of its own.
func readPGID(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil && line == "" {
		return 0, err
	}
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), pgidMarker)
	if !ok {
		return 0, fmt.Errorf("ssh: stream: expected %q marker, got %q", pgidMarker, strings.TrimSpace(line))
	}
	pgid, err := strconv.Atoi(rest)
	if err != nil {
		return 0, fmt.Errorf("ssh: stream: bad process-group id %q: %w", rest, err)
	}
	return pgid, nil
}

// Signal sends sig to a process on the node by running kill over a session. The
// pid is a pid on the node, e.g. one a service recorded in a pidfile.
func (b *backend) Signal(ctx context.Context, pid int, sig syscall.Signal) error {
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
