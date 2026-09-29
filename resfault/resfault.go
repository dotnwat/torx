//go:build linux

// Package resfault takes resources away from torx nodes: it freezes a node
// outright, starves it of CPU, presses on its memory, and slows its disk --
// the gray failures of a machine that is up but barely working, which a
// system should route around rather than wait on.
//
// It acts on a node's cgroup, which a local node has under -cgroups (see
// torx.LocalBackend's Cgroup), by writing the cgroup's control files: every
// process the node runs -- a server and whatever it forks, and any command a
// job runs there -- is in that cgroup, so a fault takes all of them at once
// and a process cannot slip out from under it. Check says whether a node can
// take these faults. Reset lifts every one of them.
//
// Freezing differs from stopping a process with SIGSTOP: the frozen
// processes are not told, a parent waiting on a child sees nothing, and a
// command a job runs on a frozen node waits to start until the thaw, or its
// context ends.
package resfault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
)

// ErrNoCgroup is why a node cannot take these faults: its commands do not
// run in a cgroup of its own.
var ErrNoCgroup = errors.New("resfault: the node has no cgroup of its own (run with -cgroups)")

// cgroupOf returns the cgroup n's commands run in.
func cgroupOf(n *torx.Node) (string, error) {
	cg, ok := n.Backend.(interface{ CgroupPath() string })
	if !ok || cg.CgroupPath() == "" {
		return "", fmt.Errorf("%s: %w", n.Name(), ErrNoCgroup)
	}
	return cg.CgroupPath(), nil
}

// Check reports whether n can take resource faults: it has a cgroup, and
// the controllers the faults need -- cpu, io, and memory -- are enabled for
// it. Freeze needs none.
func Check(n *torx.Node) error {
	cg, err := cgroupOf(n)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(cg, "cgroup.controllers"))
	if err != nil {
		return fmt.Errorf("resfault: %s: %w", n.Name(), err)
	}
	have := strings.Fields(string(b))
	for _, want := range []string{"cpu", "io", "memory"} {
		found := false
		for _, h := range have {
			found = found || h == want
		}
		if !found {
			return fmt.Errorf("resfault: %s: the %s controller is not enabled for its cgroup (it has %q)", n.Name(), want, have)
		}
	}
	return nil
}

// Freeze stops every process on n where it stands, and returns once the
// kernel reports the node frozen. Thaw lets them run on.
func Freeze(ctx context.Context, n *torx.Node) error {
	return freeze(ctx, n, true)
}

// Thaw lets a frozen node's processes run on.
func Thaw(ctx context.Context, n *torx.Node) error {
	return freeze(ctx, n, false)
}

// freezer is a backend that freezes a node's processes itself, as
// torx.LocalBackend does: it holds back commands started meanwhile, which
// would otherwise hang the process starting them.
type freezer interface {
	Freeze(context.Context) error
	Thaw() error
}

func freeze(ctx context.Context, n *torx.Node, frozen bool) error {
	if _, err := cgroupOf(n); err != nil {
		return err
	}
	f, ok := n.Backend.(freezer)
	if !ok {
		return fmt.Errorf("resfault: %s: its backend cannot freeze it", n.Name())
	}
	var err error
	if frozen {
		err = f.Freeze(ctx)
	} else {
		err = f.Thaw()
	}
	if err != nil {
		return fmt.Errorf("resfault: %s: %w", n.Name(), err)
	}
	return nil
}

// cpuPeriod is the period cpu.max meters a node's CPU time over.
const cpuPeriod = 100 * time.Millisecond

// LimitCPU caps the CPU time n's processes get together at share of one CPU
// -- 0.05 is five percent -- however many CPUs they could otherwise use.
// A share of 0 or less lifts the cap.
func LimitCPU(n *torx.Node, share float64) error {
	cg, err := cgroupOf(n)
	if err != nil {
		return err
	}
	return write(n, cg, "cpu.max", cpuMax(share))
}

// cpuMax renders a share of one CPU as cpu.max's "quota period", in
// microseconds; the kernel refuses a quota under a millisecond.
func cpuMax(share float64) string {
	period := cpuPeriod.Microseconds()
	if share <= 0 {
		return "max " + strconv.FormatInt(period, 10)
	}
	quota := max(int64(share*float64(period)), 1000)
	return strconv.FormatInt(quota, 10) + " " + strconv.FormatInt(period, 10)
}

// LimitMemory throttles n's processes once their memory together passes
// high bytes: the kernel reclaims from them, swapping them out if it must,
// and slows their allocations, rather than killing them. A high of 0 or less
// lifts the limit.
func LimitMemory(n *torx.Node, high int64) error {
	cg, err := cgroupOf(n)
	if err != nil {
		return err
	}
	v := "max"
	if high > 0 {
		v = strconv.FormatInt(high, 10)
	}
	return write(n, cg, "memory.high", v)
}

// MemoryUsage is the memory n's processes use together, page cache and
// tmpfs pages they wrote included.
func MemoryUsage(n *torx.Node) (int64, error) {
	cg, err := cgroupOf(n)
	if err != nil {
		return 0, err
	}
	b, err := os.ReadFile(filepath.Join(cg, "memory.current"))
	if err != nil {
		return 0, fmt.Errorf("resfault: %s: %w", n.Name(), err)
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

// IOLimit bounds the disk bandwidth of a node's processes, in bytes and in
// operations per second; a zero field is unbounded.
type IOLimit struct {
	ReadBPS, WriteBPS   uint64
	ReadIOPS, WriteIOPS uint64
}

func (l IOLimit) String() string {
	f := func(v uint64) string {
		if v == 0 {
			return "max"
		}
		return strconv.FormatUint(v, 10)
	}
	return fmt.Sprintf("rbps=%s wbps=%s riops=%s wiops=%s", f(l.ReadBPS), f(l.WriteBPS), f(l.ReadIOPS), f(l.WriteIOPS))
}

// ThrottleIO bounds the bandwidth n's processes get from the block device
// holding dir. It takes a directory on a block device, not on a tmpfs, and
// it bounds what reaches the device: writes a process makes through the page
// cache reach it later, on writeback, so a limit shows most plainly on
// direct and synchronous I/O, and a filesystem that compresses (btrfs
// mounted with compress) sends the device less than the process wrote --
// next to nothing, for zeros. A zero IOLimit lifts the bounds.
func ThrottleIO(n *torx.Node, dir string, l IOLimit) error {
	cg, err := cgroupOf(n)
	if err != nil {
		return err
	}
	dev, err := blockDevice(dir)
	if err != nil {
		return fmt.Errorf("resfault: %s: %w", n.Name(), err)
	}
	return write(n, cg, "io.max", dev+" "+l.String())
}

// Reset thaws n and lifts every limit on it, including io.max's on any
// device.
func Reset(ctx context.Context, n *torx.Node) error {
	cg, err := cgroupOf(n)
	if err != nil {
		return err
	}
	errs := []error{
		freeze(ctx, n, false),
		LimitCPU(n, 0),
		LimitMemory(n, 0),
	}
	b, err := os.ReadFile(filepath.Join(cg, "io.max"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("resfault: %s: %w", n.Name(), err))
	}
	for line := range strings.Lines(string(b)) {
		if f := strings.Fields(line); len(f) > 0 {
			errs = append(errs, write(n, cg, "io.max", f[0]+" "+IOLimit{}.String()))
		}
	}
	return errors.Join(errs...)
}

func write(n *torx.Node, cg, file, value string) error {
	if err := os.WriteFile(filepath.Join(cg, file), []byte(value), 0); err != nil {
		return fmt.Errorf("resfault: %s: writing %q to %s: %w", n.Name(), value, file, err)
	}
	return nil
}

// blockDevice returns the "major:minor" of the block device holding dir.
// The filesystem mounted there names it, unless it names a device of its own
// with no disk behind it -- btrfs, overlayfs -- in which case the mount's
// source is looked at for the disk it is.
func blockDevice(dir string) (string, error) {
	path, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	table, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	m, ok := mountOf(string(table), path)
	if !ok {
		return "", fmt.Errorf("no mount holds %s", dir)
	}
	if !strings.HasPrefix(m.dev, "0:") {
		return m.dev, nil
	}
	var st syscall.Stat_t
	if err := syscall.Stat(m.source, &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFBLK {
		return "", fmt.Errorf("%s is on %s (%s), which is not a block device", dir, m.source, m.fstype)
	}
	return fmt.Sprintf("%d:%d", unixMajor(st.Rdev), unixMinor(st.Rdev)), nil
}

type mount struct{ point, dev, fstype, source string }

// mountOf finds, in a mount table in the form of /proc/self/mountinfo, the
// mount that holds path: the one mounted at path's longest prefix, the last
// such when several are stacked there.
func mountOf(table, path string) (mount, bool) {
	var best mount
	found := false
	for line := range strings.Lines(table) {
		// "36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw"
		pre, post, ok := strings.Cut(line, " - ")
		f, g := strings.Fields(pre), strings.Fields(post)
		if !ok || len(f) < 5 || len(g) < 2 {
			continue
		}
		point := unescape(f[4])
		if !within(path, point) {
			continue
		}
		if !found || len(point) >= len(best.point) {
			best, found = mount{point: point, dev: f[2], fstype: g[0], source: unescape(g[1])}, true
		}
	}
	return best, found
}

func within(path, dir string) bool {
	return dir == "/" || path == dir || strings.HasPrefix(path, dir+"/")
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

// unixMajor and unixMinor split a device number as glibc's major and minor
// do.
func unixMajor(dev uint64) uint64 { return (dev>>8)&0xfff | (dev>>32)&0xfffff000 }
func unixMinor(dev uint64) uint64 { return dev&0xff | (dev>>12)&0xffffff00 }
