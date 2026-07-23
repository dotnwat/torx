package ssh

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	cryptossh "golang.org/x/crypto/ssh"

	"github.com/dotnwat/torx"
)

func TestExecCapturesOutputAndExit(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()

	res, err := b.Exec(ctx, torx.Command("echo", "hi"))
	if err != nil {
		t.Fatalf("exec echo: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != "hi" {
		t.Errorf("stdout = %q, want hi", res.Stdout)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit = %d, want 0", res.ExitCode)
	}

	// A non-zero exit is reported in ExitCode, not as an error.
	res, err = b.Exec(ctx, torx.Command("sh", "-c", "echo oops >&2; exit 3"))
	if err != nil {
		t.Fatalf("exec exit-3: %v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("exit = %d, want 3", res.ExitCode)
	}
	if !strings.Contains(string(res.Stderr), "oops") {
		t.Errorf("stderr = %q, want it to contain oops", res.Stderr)
	}
}

func TestExecEnvAndDir(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()

	res, err := b.Exec(ctx, torx.Cmd{Path: "sh", Args: []string{"-c", "echo $FOO"}, Env: []string{"FOO=bar"}})
	if err != nil {
		t.Fatalf("exec env: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != "bar" {
		t.Errorf("stdout = %q, want bar (env not applied)", res.Stdout)
	}

	dir := t.TempDir()
	res, err = b.Exec(ctx, torx.Cmd{Path: "pwd", Dir: dir})
	if err != nil {
		t.Fatalf("exec pwd: %v", err)
	}
	if strings.TrimSpace(string(res.Stdout)) != dir {
		t.Errorf("pwd = %q, want %q (dir not applied)", strings.TrimSpace(string(res.Stdout)), dir)
	}
}

func TestFileOperations(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()
	dir := t.TempDir()

	p := filepath.Join(dir, "f.txt")
	if err := b.WriteFile(ctx, p, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := b.ReadFile(ctx, p)
	if err != nil || string(got) != "hello" {
		t.Fatalf("read = %q, err %v; want hello", got, err)
	}

	if ok, err := b.Exists(ctx, p); err != nil || !ok {
		t.Errorf("Exists(existing) = %v, %v; want true, nil", ok, err)
	}
	if ok, err := b.Exists(ctx, filepath.Join(dir, "nope")); err != nil || ok {
		t.Errorf("Exists(missing) = %v, %v; want false, nil", ok, err)
	}

	local := filepath.Join(dir, "local.txt")
	if err := os.WriteFile(local, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(dir, "remote.txt")
	if err := b.Put(ctx, local, remote); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, _ := b.ReadFile(ctx, remote); string(got) != "payload" {
		t.Errorf("put content = %q, want payload", got)
	}
	back := filepath.Join(dir, "back.txt")
	if err := b.Get(ctx, remote, back); err != nil {
		t.Fatalf("get: %v", err)
	}
	if data, _ := os.ReadFile(back); string(data) != "payload" {
		t.Errorf("get content = %q, want payload", data)
	}

	nested := filepath.Join(dir, "a", "b", "c")
	if err := b.Mkdir(ctx, nested); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if ok, _ := b.Exists(ctx, nested); !ok {
		t.Errorf("mkdir did not create %s", nested)
	}
	if err := b.Rm(ctx, filepath.Join(dir, "a")); err != nil {
		t.Fatalf("rm: %v", err)
	}
	if ok, _ := b.Exists(ctx, filepath.Join(dir, "a")); ok {
		t.Errorf("rm did not remove the tree")
	}
}

// TestRmAbsentPathSucceeds pins the rm -rf contract: removing a path that does
// not exist is not an error. A service's pre-clean removes a node's stale data
// before its first start, when that data has never existed; an Rm that failed on
// absence would fail every service's first launch. pkg/sftp's RemoveAll surfaces
// its opening Stat's ErrNotExist, so the backend must absorb it, matching
// os.RemoveAll and thus LocalBackend.Rm.
func TestRmAbsentPathSucceeds(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()
	absent := filepath.Join(t.TempDir(), "never", "created")
	if err := b.Rm(ctx, absent); err != nil {
		t.Errorf("Rm(absent) = %v; want nil (rm -rf tolerates a missing path)", err)
	}
}

func TestSignal(t *testing.T) {
	b := dialBackend(t)
	// kill -0 against a live pid (this test process) succeeds; the point is that
	// Signal builds and runs the remote kill without error.
	if err := b.Signal(context.Background(), os.Getpid(), syscall.Signal(0)); err != nil {
		t.Errorf("Signal(kill -0 self) = %v, want nil", err)
	}
}

func TestSignalRejectsUnsafePID(t *testing.T) {
	b := dialBackend(t)
	// A pid read from a node-side pidfile is untrusted; a non-positive value must
	// be rejected before it can become a group- or system-wide kill on the node.
	for _, pid := range []int{0, 1, -1, -1000} {
		if err := b.Signal(context.Background(), pid, syscall.SIGKILL); err == nil {
			t.Errorf("Signal(pid=%d) = nil, want a refusal", pid)
		}
	}
}

func TestExecContextCancel(t *testing.T) {
	b := dialBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Exec(ctx, torx.Command("sleep", "5"))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("exec error = %v, want it to wrap context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exec did not return promptly after cancellation")
	}
}

// TestSFTPNegotiationHonorsCancel is the regression for SFTP ignoring the
// context after the SSH connection is up: a subsystem that accepts the request
// but never negotiates would wedge collection or cleanup forever. Cancelling the
// context must unblock the operation.
func TestSFTPNegotiationHonorsCancel(t *testing.T) {
	s := buildTestServer(t)
	s.stallSFTP = true
	go s.serve()

	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.ReadFile(ctx, "/anything")
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // let SFTP negotiation block
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("SFTP op should fail once the cancelled context unblocks negotiation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SFTP negotiation ignored context cancellation")
	}
}

func TestHostKeyVerificationRejects(t *testing.T) {
	s := newTestServer(t)
	// A known_hosts that trusts the WRONG key for the server's address.
	wrongPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongKey, err := cryptossh.NewPublicKey(wrongPub)
	if err != nil {
		t.Fatal(err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(s.config.Port))
	kh := filepath.Join(t.TempDir(), "known_hosts")
	writeKnownHosts(t, kh, addr, wrongKey)

	cfg := s.config
	cfg.KnownHosts = kh
	be, err := build(descriptorFor(t, "127.0.0.1", cfg))
	if err != nil {
		t.Fatalf("build: %v", err) // the config is well-formed; only the pinned key is wrong
	}
	b := be.(*backend)
	t.Cleanup(b.close)
	if _, err := b.Exec(context.Background(), torx.Command("echo", "hi")); err == nil {
		t.Fatal("expected host-key verification to reject the connection")
	}
}

func TestBuildRejectsBadConfig(t *testing.T) {
	if _, err := build(torx.BackendDescriptor{Kind: "ssh"}); err == nil {
		t.Error("expected an error when the descriptor has no host")
	}
	if _, err := build(descriptorFor(t, "h", Config{})); err == nil {
		t.Error("expected an error when the config is missing user/identity/known_hosts")
	}
}

// stallingBackend builds a backend pointed at a listener that accepts TCP but
// never speaks SSH, so the handshake blocks unless a deadline stops it.
// timeoutMS sets ConnectTimeoutMS (0 leaves it unset).
func stallingBackend(t *testing.T, timeoutMS int) *backend {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				<-stall // hold the connection open and silent until the test ends
				_ = c.Close()
			}(c)
		}
	}()

	dir := t.TempDir()
	idFile := filepath.Join(dir, "id")
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	writePrivateKey(t, idFile, priv)
	hostPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := cryptossh.NewPublicKey(hostPub)
	if err != nil {
		t.Fatal(err)
	}
	kh := filepath.Join(dir, "known_hosts")
	writeKnownHosts(t, kh, ln.Addr().String(), hostKey)

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	be, err := build(descriptorFor(t, host, Config{
		Port: port, User: "torx", IdentityFile: idFile, KnownHosts: kh, ConnectTimeoutMS: timeoutMS,
	}))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)
	return b
}

func assertHandshakeFails(t *testing.T, b *backend, ctx context.Context) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := b.Exec(ctx, torx.Command("true"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Exec should fail when the handshake cannot complete before its deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Exec hung: the SSH handshake ignored its deadline")
	}
}

func TestConnHandshakeHonorsContext(t *testing.T) {
	b := stallingBackend(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	assertHandshakeFails(t, b, ctx)
}

func TestConnHandshakeHonorsConnectTimeout(t *testing.T) {
	// ConnectTimeoutMS bounds the handshake even with no context deadline.
	assertHandshakeFails(t, stallingBackend(t, 300), context.Background())
}

func TestReadPGID(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  int
		ok    bool
	}{
		{"valid", "TORX_PGID:1234\n", 1234, true},
		{"valid with trailing output", "TORX_PGID:1234\nhello\n", 1234, true},
		{"no newline", "TORX_PGID:1234", 0, false},
		{"truncated pgid, no newline", "TORX_PGID:1", 0, false},
		{"pgid one", "TORX_PGID:1\n", 0, false},
		{"pgid zero", "TORX_PGID:0\n", 0, false},
		{"negative pgid", "TORX_PGID:-5\n", 0, false},
		{"non-numeric", "TORX_PGID:abc\n", 0, false},
		{"wrong marker", "GARBAGE\n", 0, false},
		{"empty", "", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pgid, err := readPGID(bufio.NewReader(strings.NewReader(tc.input)))
			if tc.ok {
				if err != nil {
					t.Fatalf("readPGID(%q) = error %v, want %d", tc.input, err, tc.want)
				}
				if pgid != tc.want {
					t.Errorf("readPGID(%q) = %d, want %d", tc.input, pgid, tc.want)
				}
				return
			}
			if err == nil {
				t.Errorf("readPGID(%q) = %d, nil; want an error", tc.input, pgid)
			}
		})
	}
}

func TestInitRegistersSSHKind(t *testing.T) {
	// The package init registered "ssh"; re-registering the same kind panics,
	// which proves the registration happened.
	defer func() {
		if recover() == nil {
			t.Error("expected re-registering \"ssh\" to panic, proving init registered it")
		}
	}()
	torx.RegisterBackend("ssh", func(torx.BackendDescriptor) (torx.Backend, error) { return nil, nil })
}
