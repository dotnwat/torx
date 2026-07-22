package ssh

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

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
}

// newTestServer starts a server on the loopback and returns a handle whose
// descriptor builds a backend that authenticates to and trusts it.
func newTestServer(t *testing.T) *testServer {
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
	defer sshConn.Close()
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
			go serveExec(ch, payload.Command)
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

// serveExec runs line as a local subprocess in its own process group, wiring the
// channel to its stdin/stdout/stderr the way sshd does and reporting its exit
// status. Connecting stdin lets the client's Cmd.Stdin reach the command. It
// runs to completion; a client that cancels simply closes its session, which
// unblocks the client side.
func serveExec(ch cryptossh.Channel, line string) {
	cmd := exec.Command("sh", "-c", line)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = ch
	cmd.Stdout = ch
	cmd.Stderr = ch.Stderr()
	sendExit(ch, exitCode(cmd.Run()))
	_ = ch.Close()
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
