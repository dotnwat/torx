package ssh

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"

	"github.com/pkg/sftp"

	"github.com/dotnwat/torx"
)

// sftpClient returns the node's SFTP client, creating it over the SSH connection
// on first use and reusing it thereafter.
func (b *backend) sftpClient(ctx context.Context) (*sftp.Client, error) {
	client, err := b.conn(ctx)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.sftp != nil {
		sc := b.sftp
		b.mu.Unlock()
		return sc, nil
	}
	b.mu.Unlock()

	// Create the client without holding b.mu: NewClient opens a channel and
	// negotiates the SFTP subsystem (network I/O), which must not block other
	// operations -- including close -- on a wedged node.
	sc, err := sftp.NewClient(client)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: sftp client", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sftp != nil {
		// Another caller created one while we negotiated; keep theirs, drop ours.
		_ = sc.Close()
		return b.sftp, nil
	}
	b.sftp = sc
	return sc, nil
}

// Put copies a local file to a path on the node.
func (b *backend) Put(ctx context.Context, localPath, nodePath string) error {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return err
	}
	in, err := os.Open(localPath)
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: put", err)
	}
	defer in.Close()
	out, err := sc.Create(nodePath)
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: put", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return torx.Wrap(torx.ErrBackend, "ssh: put", err)
	}
	if err := out.Close(); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: put", err)
	}
	return nil
}

// Get copies a file from the node to a local path.
func (b *backend) Get(ctx context.Context, nodePath, localPath string) error {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return err
	}
	in, err := sc.Open(nodePath)
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: get", err)
	}
	defer in.Close()
	out, err := os.Create(localPath)
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: get", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return torx.Wrap(torx.ErrBackend, "ssh: get", err)
	}
	if err := out.Close(); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: get", err)
	}
	return nil
}

// ReadFile returns the contents of a file on the node.
func (b *backend) ReadFile(ctx context.Context, path string) ([]byte, error) {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return nil, err
	}
	f, err := sc.Open(path)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: read file", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: read file", err)
	}
	return data, nil
}

// WriteFile writes data to a file on the node, creating or truncating it.
func (b *backend) WriteFile(ctx context.Context, path string, data []byte) error {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return err
	}
	f, err := sc.Create(path)
	if err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: write file", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return torx.Wrap(torx.ErrBackend, "ssh: write file", err)
	}
	if err := f.Close(); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: write file", err)
	}
	return nil
}

// Exists reports whether a path exists on the node.
func (b *backend) Exists(ctx context.Context, path string) (bool, error) {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return false, err
	}
	if _, err := sc.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, torx.Wrap(torx.ErrBackend, "ssh: exists", err)
	}
	return true, nil
}

// Mkdir creates a directory on the node, including parents (mkdir -p).
func (b *backend) Mkdir(ctx context.Context, path string) error {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return err
	}
	if err := sc.MkdirAll(path); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: mkdir", err)
	}
	return nil
}

// Rm removes a path on the node, recursively and without error if absent
// (rm -rf).
func (b *backend) Rm(ctx context.Context, path string) error {
	sc, err := b.sftpClient(ctx)
	if err != nil {
		return err
	}
	if err := sc.RemoveAll(path); err != nil {
		return torx.Wrap(torx.ErrBackend, "ssh: rm", err)
	}
	return nil
}
