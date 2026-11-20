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

// TestRmDoesNotFollowDirectorySymlink verifies that Rm unlinks directory
// symlinks without recursing into their targets, matching os.RemoveAll and
// rm -rf. pkg/sftp's RemoveAll uses Stat and would wipe the target directory.
func TestRmDoesNotFollowDirectorySymlink(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()
	root := t.TempDir()

	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	precious := filepath.Join(target, "keep.txt")
	if err := os.WriteFile(precious, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Direct removal of a symlink to a directory.
	directLink := filepath.Join(root, "direct-link")
	if err := os.Symlink(target, directLink); err != nil {
		t.Fatal(err)
	}
	if err := b.Rm(ctx, directLink); err != nil {
		t.Fatalf("Rm(directLink): %v", err)
	}
	if _, err := os.Lstat(directLink); !os.IsNotExist(err) {
		t.Errorf("directLink still exists after Rm: %v", err)
	}
	if got, err := os.ReadFile(precious); err != nil || string(got) != "keep" {
		t.Fatalf("target file damaged by Rm(directLink): %q, %v", got, err)
	}

	// Direct removal of a symlink to a directory with a trailing slash.
	trailingLink := filepath.Join(root, "trailing-link")
	if err := os.Symlink(target, trailingLink); err != nil {
		t.Fatal(err)
	}
	if err := b.Rm(ctx, trailingLink+"/"); err != nil {
		t.Fatalf("Rm(trailingLink/): %v", err)
	}
	if _, err := os.Lstat(trailingLink); !os.IsNotExist(err) {
		t.Errorf("trailingLink still exists after Rm: %v", err)
	}
	if got, err := os.ReadFile(precious); err != nil || string(got) != "keep" {
		t.Fatalf("target file damaged by Rm(trailingLink/): %q, %v", got, err)
	}

	// Recursive removal of a tree containing a symlink to a directory.
	tree := filepath.Join(root, "tree")
	if err := os.MkdirAll(tree, 0o755); err != nil {
		t.Fatal(err)
	}
	nestedLink := filepath.Join(tree, "nested-link")
	if err := os.Symlink(target, nestedLink); err != nil {
		t.Fatal(err)
	}
	if err := b.Rm(ctx, tree); err != nil {
		t.Fatalf("Rm(tree): %v", err)
	}
	if _, err := os.Lstat(tree); !os.IsNotExist(err) {
		t.Errorf("tree still exists after Rm: %v", err)
	}
	if got, err := os.ReadFile(precious); err != nil || string(got) != "keep" {
		t.Fatalf("target file damaged by Rm(tree): %q, %v", got, err)
	}
}

// TestRmRemovesDanglingSymlink verifies that Rm removes a broken symlink
// instead of treating Stat's ENOENT as an absent path.
func TestRmRemovesDanglingSymlink(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()
	root := t.TempDir()

	dangling := filepath.Join(root, "dangling")
	if err := os.Symlink(filepath.Join(root, "nowhere"), dangling); err != nil {
		t.Fatal(err)
	}
	if err := b.Rm(ctx, dangling); err != nil {
		t.Fatalf("Rm(dangling): %v", err)
	}
	if _, err := os.Lstat(dangling); !os.IsNotExist(err) {
		t.Errorf("dangling symlink still exists after Rm: %v", err)
	}
}

// TestRmWithSymlinkParentTraversal checks that Rm on a path containing a
// symlink followed by ".." deletes entries in the directory the symlink
// actually points to, not in the lexically-cleaned parent path.
func TestRmWithSymlinkParentTraversal(t *testing.T) {
	b := dialBackend(t)
	ctx := context.Background()
	root := t.TempDir()

	aTarget := filepath.Join(root, "a", "target")
	bSub := filepath.Join(root, "b", "sub")
	bTarget := filepath.Join(root, "b", "target")
	for _, d := range []string{aTarget, bSub, bTarget} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	keep := filepath.Join(aTarget, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(bTarget, "remove.txt")
	if err := os.WriteFile(victim, []byte("remove"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "a", "link")
	if err := os.Symlink(bSub, link); err != nil {
		t.Fatal(err)
	}

	// Build /root/a/link/../target without lexical cleaning.
	if err := b.Rm(ctx, link+"/../target"); err != nil {
		t.Fatalf("Rm(link/../target): %v", err)
	}
	if _, err := os.Lstat(bTarget); !os.IsNotExist(err) {
		t.Errorf("b/target still exists after Rm: %v", err)
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "keep" {
		t.Fatalf("unrelated a/target/keep.txt damaged by Rm: %q, %v", got, err)
	}
}

func TestSignalArg(t *testing.T) {
	cases := []struct {
		sig  syscall.Signal
		want string
	}{
		{0, "0"},
		{syscall.SIGTERM, "TERM"},
		{syscall.SIGKILL, "KILL"},
		{syscall.SIGSTOP, "STOP"},
		{syscall.SIGCONT, "CONT"},
		{syscall.SIGUSR1, "USR1"},
		{syscall.SIGQUIT, "QUIT"},
	}
	for _, tc := range cases {
		if got := signalArg(tc.sig); got != tc.want {
			t.Errorf("signalArg(%v) = %q, want %q", tc.sig, got, tc.want)
		}
	}
}

// TestReconnectsAfterConnectionDrop verifies that when the underlying SSH
// connection drops (e.g. during a network fault or sshd restart), the dead
// SSH and SFTP clients are evicted so subsequent operations redial cleanly.
func TestReconnectsAfterConnectionDrop(t *testing.T) {
	s := newTestServer(t)
	be, err := build(s.descriptor(t))
	if err != nil {
		t.Fatalf("build backend: %v", err)
	}
	b := be.(*backend)
	t.Cleanup(b.close)
	ctx := context.Background()

	// Establish both the SSH connection and the SFTP subsystem.
	if _, err := b.Exec(ctx, torx.Command("true")); err != nil {
		t.Fatalf("initial Exec: %v", err)
	}
	dir := t.TempDir()
	if _, err := b.Exists(ctx, dir); err != nil {
		t.Fatalf("initial Exists: %v", err)
	}

	// Drop the server side of the SSH connection while leaving the listener up.
	// Immediate Exec and SFTP calls must evict the dead client synchronously
	// and redial without waiting for client.Wait()'s background goroutine.
	s.dropConns()
	if _, err := b.Exec(ctx, torx.Command("true")); err != nil {
		t.Fatalf("Exec immediately after drop: %v", err)
	}
	if ok, err := b.Exists(ctx, dir); err != nil || !ok {
		t.Fatalf("Exists after Exec reconnect = (%v, %v), want (true, nil)", ok, err)
	}

	// Drop again and verify an immediate SFTP call (before any Exec) also
	// evicts the stale cached client and reconnects transparently.
	s.dropConns()
	if ok, err := b.Exists(ctx, dir); err != nil || !ok {
		t.Fatalf("Exists immediately after drop = (%v, %v), want (true, nil)", ok, err)
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
