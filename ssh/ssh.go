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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

	runErr := runToCompletion(ctx, sess, remoteCommand(cmd))
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

// Stream is not yet supported by the SSH backend: running a long-lived process
// on a remote node needs process-group teardown that is future work. Callers
// that only run commands to completion use Exec.
func (b *backend) Stream(ctx context.Context, cmd torx.Cmd) (io.ReadCloser, error) {
	return nil, torx.Wrap(torx.ErrBackend, "ssh: stream", errors.New("streaming is not yet supported"))
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
func remoteCommand(cmd torx.Cmd) string {
	var b strings.Builder
	if cmd.Dir != "" {
		b.WriteString("cd " + shQuote(cmd.Dir) + " && ")
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
