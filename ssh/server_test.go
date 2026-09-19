package ssh

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/sftp"
	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/dotnwat/torx"
)

// testServer is an in-process SSH server for exercising the backend
// hermetically: it speaks real SSH (crypto/ssh server side) and real SFTP
// (pkg/sftp server) on a loopback listener, running exec requests as local
// subprocesses against the real filesystem. It deliberately does not reproduce
// OpenSSH-specific behavior, but it drives the whole client path under go test
// with no external sshd.
type testServer struct {
	config Config // client settings that reach this server
	sc     *cryptossh.ServerConfig
	ln     net.Listener

	// silentExec makes exec requests reply success and then close the channel
	// with no output -- not even the pgid marker -- simulating a remote that dies
	// before the stream handshake completes.
	silentExec bool

	// stallExec makes exec requests reply success but then neither produce output
	// nor close the channel, simulating a remote that accepts the command but
	// never emits the pgid marker, so only a client-side deadline unblocks it.
	stallExec bool

	// stallSFTP makes the sftp subsystem request reply success but never start the
	// server, so the client's version negotiation hangs -- a wedged SFTP subsystem
	// that only a cancelled context can unblock.
	stallSFTP bool

	// forkDepth is how many forking shell layers serveExec puts between the
	// session leader it creates and the command, standing in for login shells
	// and wrappers that place the command differently. 0 exec's the command in
	// place, as bash or zsh do for a single -c command, so the command is the
	// session leader. 1 makes the leader fork the command, as dash (Debian's
	// /bin/sh) does for any -c command; 2 adds a forking wrapper (a ForceCommand
	// or audit script) between them. Whatever the depth, the session is the
	// command's own, and Stream must run it.
	forkDepth int

	// sharedGroup makes serveExec run the command in this test process's own
	// session and process group instead of a new session: the shape an sshd that
	// does not isolate commands (Dropbear) produces, where the group is shared
	// with the server itself. Stream must either move the command into a group of
	// its own or refuse; killing the shared group would kill this test binary.
	sharedGroup bool

	// path, when set, replaces PATH in the command's environment, to stand in
	// for nodes missing the tools Stream's wrapper can fall back on.
	path string

	// exitDelay makes serveExec hold a streamed command's exit report back for
	// that long after the command has ended, standing in for an sshd slower to
	// reap and report than the client is to close: the shape in which a close
	// sent straight after a kill reaches the server first and the report is
	// discarded with the channel. Exec sessions, the kill among them, are not
	// delayed, so the kill returns while the report is still pending.
	exitDelay time.Duration
}

// newTestServer starts a server on the loopback and returns a handle whose
// descriptor builds a backend that authenticates to and trusts it.
func newTestServer(t *testing.T) *testServer {
	t.Helper()
	s := buildTestServer(t)
	go s.serve()
	return s
}

// buildTestServer sets up the server and its listener but does not start
// accepting connections, so a caller can tweak fields (e.g. silentExec) before
// calling serve.
func buildTestServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()

	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	idFile := filepath.Join(dir, "id_ed25519")
	writePrivateKey(t, idFile, clientPriv)
	clientAuthKey, err := cryptossh.NewPublicKey(clientPub)
	if err != nil {
		t.Fatalf("client public key: %v", err)
	}

	hostPub, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("host key: %v", err)
	}
	hostSigner, err := cryptossh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatalf("host signer: %v", err)
	}
	hostAuthKey, err := cryptossh.NewPublicKey(hostPub)
	if err != nil {
		t.Fatalf("host public key: %v", err)
	}

	sc := &cryptossh.ServerConfig{
		PublicKeyCallback: func(_ cryptossh.ConnMetadata, key cryptossh.PublicKey) (*cryptossh.Permissions, error) {
			if bytes.Equal(key.Marshal(), clientAuthKey.Marshal()) {
				return &cryptossh.Permissions{}, nil
			}
			return nil, errors.New("unknown public key")
		},
	}
	sc.AddHostKey(hostSigner)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	khFile := filepath.Join(dir, "known_hosts")
	writeKnownHosts(t, khFile, ln.Addr().String(), hostAuthKey)

	s := &testServer{
		sc: sc,
		ln: ln,
		config: Config{
			Port:         port,
			User:         "torx",
			IdentityFile: idFile,
			KnownHosts:   khFile,
		},
	}
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// descriptor builds the ssh BackendDescriptor a client uses to reach the server.
func (s *testServer) descriptor(t *testing.T) torx.BackendDescriptor {
	t.Helper()
	return descriptorFor(t, "127.0.0.1", s.config)
}

// descriptorFor builds an ssh descriptor for host from cfg.
func descriptorFor(t *testing.T, host string, cfg Config) torx.BackendDescriptor {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return torx.BackendDescriptor{Kind: "ssh", Host: host, Config: raw}
}

// dialBackend starts a server and returns a backend connected to it.
func dialBackend(t *testing.T) *backend {
	t.Helper()
	s := newTestServer(t)
	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)
	return b
}

// dialBackendWith is dialBackend with a hook to adjust the server (e.g. its
// forkDepth) before it starts accepting connections.
func dialBackendWith(t *testing.T, configure func(*testServer)) *backend {
	t.Helper()
	s := buildTestServer(t)
	configure(s)
	go s.serve()
	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)
	return b
}

func (s *testServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *testServer) handleConn(conn net.Conn) {
	sshConn, chans, reqs, err := cryptossh.NewServerConn(conn, s.sc)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer func() { _ = sshConn.Close() }()
	go cryptossh.DiscardRequests(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			_ = nc.Reject(cryptossh.UnknownChannelType, "only session channels")
			continue
		}
		ch, chReqs, err := nc.Accept()
		if err != nil {
			continue
		}
		go s.serveSession(ch, chReqs)
	}
}

func (s *testServer) serveSession(ch cryptossh.Channel, reqs <-chan *cryptossh.Request) {
	for req := range reqs {
		switch req.Type {
		case "exec":
			var payload struct{ Command string }
			_ = cryptossh.Unmarshal(req.Payload, &payload)
			_ = req.Reply(true, nil)
			if s.silentExec {
				// Accept the exec but produce no output and close, mimicking a
				// remote that dies before printing the pgid marker.
				sendExit(ch, 1)
				_ = ch.Close()
				continue
			}
			if s.stallExec {
				// Accept the exec but leave the channel open and silent, mimicking a
				// remote that never emits the pgid marker.
				continue
			}
			go s.serveExec(ch, payload.Command)
		case "subsystem":
			var payload struct{ Name string }
			_ = cryptossh.Unmarshal(req.Payload, &payload)
			if payload.Name == "sftp" {
				_ = req.Reply(true, nil)
				if s.stallSFTP {
					// Accept the subsystem but never serve it, so the client's version
					// negotiation blocks until its context is cancelled.
					continue
				}
				go serveSFTP(ch)
			} else {
				_ = req.Reply(false, nil)
			}
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
}

// serveExec runs line as a local subprocess -- in a new session, as OpenSSH's
// do_exec_no_pty does, unless sharedGroup -- wiring the channel to its
// stdin/stdout/stderr and reporting how it ended. Connecting stdin lets the
// client's Cmd.Stdin reach the command. It runs to completion; a client that
// cancels simply closes its session, which unblocks the client side.
//
// The exit is reported as sshd reports it: as soon as the command is reaped,
// while the channel closes only once its output pipes have drained. The two
// part when a child the command left behind holds the pipes open, and a client
// must take the report for the exit it is. That needs pipes of the server's
// own for the output: os/exec's Wait would otherwise wait for the pipes too.
//
// Only Stream's wrapper (recognizable by its marker) has its process shape set
// by forkDepth; the shape is irrelevant to Exec, whose lines begin with cd or env
// and could not take a leading exec anyway.
func (s *testServer) serveExec(ch cryptossh.Channel, line string) {
	streamed := strings.Contains(line, pgidMarker)
	if streamed {
		line = shapeStream(line, s.forkDepth)
	}
	cmd := exec.Command("sh", "-c", line)
	if !s.sharedGroup {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	}
	if s.path != "" {
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "PATH=") {
				cmd.Env = append(cmd.Env, kv)
			}
		}
		cmd.Env = append(cmd.Env, "PATH="+s.path)
	}
	cmd.Stdin = ch
	outR, outW, err := os.Pipe()
	if err != nil {
		sendExit(ch, 255)
		_ = ch.Close()
		return
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = outR.Close()
		_ = outW.Close()
		sendExit(ch, 255)
		_ = ch.Close()
		return
	}
	cmd.Stdout = outW
	cmd.Stderr = errW
	var drained sync.WaitGroup
	drained.Go(func() { _, _ = io.Copy(ch, outR); _ = outR.Close() })
	drained.Go(func() { _, _ = io.Copy(ch.Stderr(), errR); _ = errR.Close() })
	err = cmd.Start()
	// The command holds its own copies of the write ends; drop the server's so
	// the pipes drain once every holder is gone.
	_ = outW.Close()
	_ = errW.Close()
	if err == nil {
		err = cmd.Wait()
	}
	if streamed && s.exitDelay > 0 {
		time.Sleep(s.exitDelay)
	}
	reportExit(ch, err)
	drained.Wait()
	_ = ch.Close()
}

// reportExit tells the client how the command ended, as OpenSSH's
// session_exit_message does: an exit-signal request naming the signal that
// killed it, or else an exit-status request with its exit code. A signal
// crypto/ssh has no name for is reported by its exit code, 255, as exec
// renders a signal death.
func reportExit(ch cryptossh.Channel, err error) {
	if ee, ok := errors.AsType[*exec.ExitError](err); ok {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			if name, ok := signalNames[ws.Signal()]; ok {
				sendExitSignal(ch, name)
				return
			}
		}
	}
	sendExit(ch, exitCode(err))
}

// signalNames maps the signals crypto/ssh names to the names an exit-signal
// request carries (RFC 4254, section 6.10: the signal name without SIG).
var signalNames = map[syscall.Signal]cryptossh.Signal{
	syscall.SIGABRT: cryptossh.SIGABRT,
	syscall.SIGALRM: cryptossh.SIGALRM,
	syscall.SIGFPE:  cryptossh.SIGFPE,
	syscall.SIGHUP:  cryptossh.SIGHUP,
	syscall.SIGILL:  cryptossh.SIGILL,
	syscall.SIGINT:  cryptossh.SIGINT,
	syscall.SIGKILL: cryptossh.SIGKILL,
	syscall.SIGPIPE: cryptossh.SIGPIPE,
	syscall.SIGQUIT: cryptossh.SIGQUIT,
	syscall.SIGSEGV: cryptossh.SIGSEGV,
	syscall.SIGTERM: cryptossh.SIGTERM,
	syscall.SIGUSR1: cryptossh.SIGUSR1,
	syscall.SIGUSR2: cryptossh.SIGUSR2,
}

// shapeStream places forkDepth forking shell layers between the session leader
// and the stream wrapper line. The shape is made explicit rather than left to
// the host's sh, because shells differ on whether a lone -c command is exec'd in
// place (bash, zsh, and macOS's dash do; Debian's dash forks): depth 0 exec's
// line so the wrapper is the leader; otherwise each layer runs line and then
// another command, which forces a fork at that layer.
func shapeStream(line string, forkDepth int) string {
	if forkDepth == 0 {
		return "exec " + line
	}
	line += "; :"
	for i := 1; i < forkDepth; i++ {
		line = "sh -c " + shQuote(line) + "; :"
	}
	return line
}

// toolsPATH returns a directory holding only the named tools (as symlinks to
// wherever PATH finds them now), for use as a command's whole PATH. It skips the
// test if a tool is not installed on this host.
func toolsPATH(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range names {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Skipf("%s not installed: %v", name, err)
		}
		if err := os.Symlink(path, filepath.Join(dir, name)); err != nil {
			t.Fatalf("symlink %s: %v", name, err)
		}
	}
	return dir
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() >= 0 {
		return ee.ExitCode()
	}
	return 255
}

func sendExit(ch cryptossh.Channel, code int) {
	_, _ = ch.SendRequest("exit-status", false, cryptossh.Marshal(struct{ Code uint32 }{uint32(code)}))
}

func sendExitSignal(ch cryptossh.Channel, sig cryptossh.Signal) {
	_, _ = ch.SendRequest("exit-signal", false, cryptossh.Marshal(struct {
		Signal     string
		CoreDumped bool
		Error      string
		Lang       string
	}{Signal: string(sig)}))
}

func serveSFTP(ch cryptossh.Channel) {
	server, err := sftp.NewServer(ch)
	if err != nil {
		_ = ch.Close()
		return
	}
	_ = server.Serve()
	_ = server.Close()
	_ = ch.Close()
}

func writePrivateKey(t *testing.T, path string, key ed25519.PrivateKey) {
	t.Helper()
	block, err := cryptossh.MarshalPrivateKey(key, "")
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
}

func writeKnownHosts(t *testing.T, path, addr string, key cryptossh.PublicKey) {
	t.Helper()
	line := knownhosts.Line([]string{addr}, key)
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatalf("write known_hosts: %v", err)
	}
}
