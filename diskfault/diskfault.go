//go:build unix

// Package diskfault injects storage faults on torx nodes: a directory with a
// hard size limit, and filling it up.
//
// Limit mounts a size-limited tmpfs over a directory -- a service's data
// directory, before the service first writes it -- and Fill takes every free
// byte of it, so the service's next write fails with ENOSPC, as it would on a
// disk that filled up; Free gives the space back. It acts through n.Exec,
// running mount, dd, and rm on the node, so it works wherever a node's
// commands may mount a filesystem: the nodes of a -netns run, which live in a
// mount namespace their user namespace owns, or a host reached as root. Check
// says whether a node can take these faults.
//
// A tmpfs keeps its contents in memory, so a limited directory survives the
// crash of the process writing it, as a disk would, but not the node's own
// reboot, which a torx node does not have.
package diskfault

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dotnwat/torx"
)

// filler is the file Fill creates in a directory to take its free space.
const filler = ".torx-diskfault-fill"

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
	return run(ctx, n, "mount", "-t", "tmpfs", "-o", "size="+strconv.FormatInt(size, 10), "tmpfs", dir)
}

// Unlimit unmounts the tmpfs Limit mounted over dir, discarding what it held.
// A directory that is not limited, or does not exist, is left as it is.
func Unlimit(ctx context.Context, n *torx.Node, dir string) error {
	if ok, err := n.Exists(ctx, dir); err != nil {
		return fmt.Errorf("diskfault: %s: %w", n.Name(), err)
	} else if !ok {
		return nil
	}
	res, err := n.Exec(ctx, torx.Command("umount", dir))
	if err != nil {
		return fmt.Errorf("diskfault: %s: umount: %w", n.Name(), err)
	}
	if res.ExitCode != 0 && !notMounted(res.Stderr) {
		return fmt.Errorf("diskfault: %s: umount %s: exit %d: %s", n.Name(), dir, res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	return nil
}

// Fill takes every free byte of the filesystem holding dir, with a file of
// its own there, so the next write to it fails for lack of space. Free
// gives the space back.
func Fill(ctx context.Context, n *torx.Node, dir string) error {
	// dd writes until the filesystem is full and then fails saying so, which
	// is what is wanted; only a failure for another reason is an error.
	res, err := n.Exec(ctx, torx.Command("dd", "if=/dev/zero", "of="+filepath.Join(dir, filler), "bs=64k"))
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

// notMounted reports whether umount's complaint is that there was nothing
// mounted: "not mounted" on Linux, "not currently mounted" on macOS.
func notMounted(stderr []byte) bool {
	s := string(stderr)
	return strings.Contains(s, "not mounted") || strings.Contains(s, "not currently mounted")
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
