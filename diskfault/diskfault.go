//go:build unix

// Package diskfault injects storage faults on torx nodes: a directory with a
// hard size limit, filling it up, and corrupting a file's contents.
//
// Limit mounts a size-limited tmpfs over a directory -- a service's data
// directory, before the service first writes it -- and Fill takes every free
// byte of it, so the service's next write fails with ENOSPC, as it would on a
// disk that filled up; Free gives the space back. It acts through n.Exec,
// running mount, umount, and dd on the node, so it works wherever a node's
// commands may mount a filesystem: the nodes of a -netns run, which live in a
// mount namespace their user namespace owns, or a host reached as root. Check
// says whether a node can take these faults. Unlimit finds the limits it may
// remove in the node's /proc/self/mountinfo, so the node must run Linux.
//
// A tmpfs keeps its contents in memory, so a limited directory survives the
// crash of the process writing it, as a disk would, but not the node's own
// reboot, which a torx node does not have.
//
// Corrupt overwrites part of a file with random bytes, as a disk that
// silently returns garbage for some sectors -- a latent sector error, a
// misdirected write -- would leave it. It needs no privilege, only dd and
// stat on the node, and a file its commands may write.
package diskfault

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/dotnwat/torx"
)

const (
	// filler is the file Fill creates in a directory to take its free space.
	filler = ".torx-diskfault-fill"
	// source names the tmpfs Limit mounts, which is how Unlimit tells a
	// limit from any other filesystem mounted at a directory.
	source = "torx-diskfault"
)

// Check reports whether n can take disk faults, by mounting and unmounting a
// tiny limited directory under dir.
func Check(ctx context.Context, n *torx.Node, dir string) error {
	probe := filepath.Join(dir, ".torx-diskfault-check")
	if err := Limit(ctx, n, probe, 4096); err != nil {
		return err
	}
	if err := Unlimit(ctx, n, probe); err != nil {
		return err
	}
	return n.Rm(ctx, probe)
}

// Limit makes dir a directory of at most size bytes, by mounting a tmpfs of
// that size over it; dir is created if it does not exist. Whatever dir held
// is hidden until Unlimit, so limit a directory before anything is written
// to it.
func Limit(ctx context.Context, n *torx.Node, dir string, size int64) error {
	if err := n.Mkdir(ctx, dir); err != nil {
		return fmt.Errorf("diskfault: %s: %w", n.Name(), err)
	}
	return run(ctx, n, "mount", "-t", "tmpfs", "-o", "size="+strconv.FormatInt(size, 10), source, dir)
}

// Unlimit unmounts the tmpfs Limit mounted over dir, discarding what it held.
// A directory that is not limited, or does not exist, is left as it is, and
// so is any other filesystem mounted there: Unlimit unmounts only a limit on
// top, so a second Unlimit of a limit laid over a volume leaves the volume.
func Unlimit(ctx context.Context, n *torx.Node, dir string) error {
	if ok, err := n.Exists(ctx, dir); err != nil {
		return fmt.Errorf("diskfault: %s: %w", n.Name(), err)
	} else if !ok {
		return nil
	}
	if ok, err := limited(ctx, n, dir); err != nil || !ok {
		return err
	}
	return run(ctx, n, "umount", dir)
}

// limited reports whether the topmost filesystem mounted at dir is a limit.
// The mount table names mount points by their physical paths, so dir is
// resolved to one on the node first.
func limited(ctx context.Context, n *torx.Node, dir string) (bool, error) {
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `cd -P -- "$1" && pwd -P && cat /proc/self/mountinfo`, "sh", dir))
	if err != nil {
		return false, fmt.Errorf("diskfault: %s: reading its mounts: %w", n.Name(), err)
	}
	if res.ExitCode != 0 {
		return false, fmt.Errorf("diskfault: %s: reading its mounts at %s: exit %d: %s", n.Name(), dir, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	path, table, _ := strings.Cut(string(res.Stdout), "\n")
	return limitOnTop(table, path), nil
}

// limitOnTop reports whether, in table, a mount table in the form of
// /proc/self/mountinfo, the topmost filesystem mounted at path is a limit. A
// mount laid over another has the one beneath as its parent, so the topmost
// is one that no other mount at path has as its parent; should two qualify,
// the later one, mounted last, is on top.
func limitOnTop(table, path string) bool {
	type mount struct{ id, parent, fstype, source string }
	var at []mount
	for line := range strings.Lines(table) {
		// "36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw"
		pre, post, ok := strings.Cut(line, " - ")
		f, g := strings.Fields(pre), strings.Fields(post)
		if !ok || len(f) < 5 || len(g) < 2 || unescape(f[4]) != path {
			continue
		}
		at = append(at, mount{id: f[0], parent: f[1], fstype: g[0], source: unescape(g[1])})
	}
	for _, top := range slices.Backward(at) {
		covered := slices.ContainsFunc(at, func(m mount) bool { return m.parent == top.id })
		if !covered {
			return top.fstype == "tmpfs" && top.source == source
		}
	}
	return false
}

// unescape undoes the octal escapes of a mountinfo field, such as \040 for a
// space.
func unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if c, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(c))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// Fill takes every free byte of the filesystem holding dir, with a file of
// its own there, so the next write to it fails for lack of space. Free
// gives the space back.
func Fill(ctx context.Context, n *torx.Node, dir string) error {
	// dd writes until the filesystem is full and then fails saying so, which
	// is what is wanted; only a failure for another reason is an error. It
	// says so in the C locale, since the node's own would translate it.
	cmd := torx.Command("dd", "if=/dev/zero", "of="+filepath.Join(dir, filler), "bs=64k")
	cmd.Env = []string{"LC_ALL=C"}
	res, err := n.Exec(ctx, cmd)
	if err != nil {
		return fmt.Errorf("diskfault: %s: dd: %w", n.Name(), err)
	}
	if res.ExitCode != 0 && !strings.Contains(string(res.Stderr), "No space left") {
		return fmt.Errorf("diskfault: %s: filling %s: exit %d: %s", n.Name(), dir, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// Free removes what Fill wrote in dir.
func Free(ctx context.Context, n *torx.Node, dir string) error {
	if err := n.Rm(ctx, filepath.Join(dir, filler)); err != nil {
		return fmt.Errorf("diskfault: %s: %w", n.Name(), err)
	}
	return nil
}

// Corrupt overwrites size bytes of the file at path on n, from offset on,
// with random bytes, leaving the file's size as it was: a range that runs
// past the end is cut at it. The write goes through the page cache, which the
// kernel writes back before a read of the same range with O_DIRECT, so a
// process reading the file that way sees it too, and it may land while the
// file is open: a disk corrupts data under the process using it as readily
// as at rest.
func Corrupt(ctx context.Context, n *torx.Node, path string, offset, size int64) error {
	if offset < 0 || size < 0 {
		return fmt.Errorf("diskfault: %s: corrupting %s: offset %d and size %d must not be negative", n.Name(), path, offset, size)
	}
	// wc and dd as POSIX has them, so a BSD node takes the fault as a GNU
	// one does: no stat -c, and no byte-counted seek or count.
	res, err := n.Exec(ctx, torx.Command("sh", "-c", `wc -c < "$1"`, "sh", path))
	if err != nil {
		return fmt.Errorf("diskfault: %s: wc: %w", n.Name(), err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("diskfault: %s: reading the size of %s: exit %d: %s", n.Name(), path, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	length, err := strconv.ParseInt(strings.TrimSpace(string(res.Stdout)), 10, 64)
	if err != nil {
		return fmt.Errorf("diskfault: %s: the size of %s: %w", n.Name(), path, err)
	}
	size = min(size, max(length-offset, 0))
	if size == 0 {
		return nil
	}
	bs := blockSize(offset, size)
	return run(ctx, n, "dd", "if=/dev/urandom", "of="+path, "conv=notrunc",
		"bs="+strconv.FormatInt(bs, 10),
		"seek="+strconv.FormatInt(offset/bs, 10), "count="+strconv.FormatInt(size/bs, 10))
}

// blockSize is the largest power of two up to 64KiB that divides both offset
// and size, so that dd, which seeks and counts in blocks, can write exactly
// that range with blocks as large as the range allows.
func blockSize(offset, size int64) int64 {
	bs := int64(64 << 10)
	for offset%bs != 0 || size%bs != 0 {
		bs /= 2
	}
	return bs
}

// run runs name args on n and fails on a non-zero exit with what it said.
func run(ctx context.Context, n *torx.Node, name string, args ...string) error {
	res, err := n.Exec(ctx, torx.Command(name, args...))
	if err != nil {
		return fmt.Errorf("diskfault: %s: %s: %w", n.Name(), name, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("diskfault: %s: %s %s: exit %d: %s", n.Name(), name, strings.Join(args, " "), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}
