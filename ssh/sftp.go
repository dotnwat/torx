//go:build unix

package ssh

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/pkg/sftp"
	cryptossh "golang.org/x/crypto/ssh"

	"github.com/dotnwat/torx"
)

// sftpClient returns the node's SFTP client, creating it over the SSH connection
// on first use and reusing it thereafter. Creation honors ctx: negotiating the
// subsystem is network I/O a wedged node could stall forever.
func (b *backend) sftpClient(ctx context.Context) (*sftp.Client, *cryptossh.Client, bool, error) {
	for attempt := range 2 {
		client, cached, err := b.conn(ctx)
		if err != nil {
			return nil, nil, false, err
		}
		b.mu.Lock()
		if b.sftp != nil && b.client == client {
			sc := b.sftp
			b.mu.Unlock()
			return sc, client, true, nil
		}
		b.mu.Unlock()

		// Create the client without holding b.mu: NewClient opens a channel and
		// negotiates the SFTP subsystem (network I/O), which must not block other
		// operations -- including close -- on a wedged node. Bound the negotiation by
		// ctx: on cancellation return the context error, and close any client that
		// still finishes so it is not leaked.
		type result struct {
			sc  *sftp.Client
			err error
		}
		ch := make(chan result, 1)
		go func() {
			sc, err := sftp.NewClient(client)
			ch <- result{sc, err}
		}()
		select {
		case <-ctx.Done():
			go func() {
				if r := <-ch; r.sc != nil {
					_ = r.sc.Close()
				}
			}()
			return nil, nil, false, torx.Wrap(torx.ErrBackend, "ssh: sftp client", ctx.Err())
		case r := <-ch:
			if r.err != nil {
				if attempt == 0 && cached && !isChannelRejection(r.err) && ctx.Err() == nil {
					b.evict(client)
					continue
				}
				return nil, nil, false, torx.Wrap(torx.ErrBackend, "ssh: sftp client", r.err)
			}
			b.mu.Lock()
			if b.client != client {
				// The SSH connection this SFTP client was opened on was evicted or
				// replaced while we negotiated; never cache an SFTP client from a
				// dead connection.
				b.mu.Unlock()
				_ = r.sc.Close()
				if attempt == 0 && ctx.Err() == nil {
					continue
				}
				return nil, nil, false, torx.Wrap(torx.ErrBackend, "ssh: sftp client", errors.New("ssh connection closed"))
			}
			if b.sftp != nil {
				// Another caller created one while we negotiated; keep theirs, drop ours.
				sc := b.sftp
				b.mu.Unlock()
				_ = r.sc.Close()
				return sc, client, true, nil
			}
			b.sftp = r.sc
			b.mu.Unlock()
			return r.sc, client, false, nil
		}
	}
	return nil, nil, false, torx.Wrap(torx.ErrBackend, "ssh: sftp client", errors.New("ssh connection closed"))
}

// sftpDo runs fn against the node's SFTP client and makes it cancellable.
// crypto/ssh and pkg/sftp are not context-aware, so a watcher closes the client
// on cancellation, which unblocks any in-flight request or transfer; the error is
// then mapped back to the context error. Closing clears the cached client so a
// later operation -- notably teardown, which runs on a context detached from the
// cancellation -- re-establishes a fresh one over the still-open SSH connection.
func (b *backend) sftpDo(ctx context.Context, op string, fn func(*sftp.Client) error) error {
	for attempt := range 2 {
		sc, client, cached, err := b.sftpClient(ctx)
		if err != nil {
			return err
		}
		stop := context.AfterFunc(ctx, func() { b.closeSFTP(sc) })
		err = fn(sc)
		stop()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return torx.Wrap(torx.ErrBackend, "ssh: "+op, ctx.Err())
		}
		if attempt == 0 && cached && (errors.Is(err, sftp.ErrSSHFxConnectionLost) || errors.Is(err, io.EOF)) {
			b.evict(client)
			continue
		}
		return torx.Wrap(torx.ErrBackend, "ssh: "+op, err)
	}
	return nil
}

// closeSFTP closes sc and, if it is still the cached client, clears the cache so
// the next operation negotiates a fresh one.
func (b *backend) closeSFTP(sc *sftp.Client) {
	b.mu.Lock()
	if b.sftp == sc {
		b.sftp = nil
	}
	b.mu.Unlock()
	_ = sc.Close()
}

// Put copies a local file to a path on the node.
func (b *backend) Put(ctx context.Context, localPath, nodePath string) error {
	return b.sftpDo(ctx, "put", func(sc *sftp.Client) error {
		in, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := sc.Create(nodePath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

// Get copies a file from the node to a local path.
func (b *backend) Get(ctx context.Context, nodePath, localPath string) error {
	return b.sftpDo(ctx, "get", func(sc *sftp.Client) error {
		in, err := sc.Open(nodePath)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := os.Create(localPath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

// ReadFile returns the contents of a file on the node.
func (b *backend) ReadFile(ctx context.Context, path string) ([]byte, error) {
	var data []byte
	err := b.sftpDo(ctx, "read file", func(sc *sftp.Client) error {
		f, err := sc.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		d, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		data = d
		return nil
	})
	return data, err
}

// WriteFile writes data to a file on the node, creating or truncating it.
func (b *backend) WriteFile(ctx context.Context, path string, data []byte) error {
	return b.sftpDo(ctx, "write file", func(sc *sftp.Client) error {
		f, err := sc.Create(path)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	})
}

// Exists reports whether a path exists on the node.
func (b *backend) Exists(ctx context.Context, path string) (bool, error) {
	exists := false
	err := b.sftpDo(ctx, "exists", func(sc *sftp.Client) error {
		if _, err := sc.Stat(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // absent, not an error; exists stays false
			}
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// Mkdir creates a directory on the node, including parents (mkdir -p).
func (b *backend) Mkdir(ctx context.Context, path string) error {
	return b.sftpDo(ctx, "mkdir", func(sc *sftp.Client) error {
		return sc.MkdirAll(path)
	})
}

// Rm removes a path on the node, recursively and without error if absent
// (rm -rf). It inspects entries with Lstat rather than Stat so that a symlink
// to a directory is unlinked without deleting the target directory's contents,
// and a dangling symlink is removed rather than treated as absent.
func (b *backend) Rm(ctx context.Context, p string) error {
	// Strip trailing slashes (preserving bare "/") so Lstat on a directory
	// symlink with a trailing slash inspects the symlink itself instead of
	// following it into the target directory, matching os.RemoveAll / LocalBackend.Rm.
	for len(p) > 1 && strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	return b.sftpDo(ctx, "rm", func(sc *sftp.Client) error {
		return sftpRemoveAll(sc, p)
	})
}

func sftpRemoveAll(sc *sftp.Client, p string) error {
	fi, err := sc.Lstat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if !fi.IsDir() {
		if err := sc.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	entries, err := sc.ReadDir(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if name == "." || name == ".." {
			continue
		}
		// Append without path.Join/path.Clean so a caller path containing
		// symlinks and ".." (e.g. /a/link/../target) keeps resolving children
		// in the same directory ReadDir just listed.
		child := p + "/" + name
		if strings.HasSuffix(p, "/") {
			child = p + name
		}
		if err := sftpRemoveAll(sc, child); err != nil {
			return err
		}
	}
	if err := sc.RemoveDirectory(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
