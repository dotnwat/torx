//go:build linux

package resfault

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

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
