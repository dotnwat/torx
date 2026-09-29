//go:build linux

package resfault

import "testing"

func TestMountOf(t *testing.T) {
	table := `22 1 0:21 / /proc rw - proc proc rw
25 1 252:0 / / rw,relatime - ext4 /dev/mapper/root rw
40 25 0:35 /home /home rw,relatime - btrfs /dev/mapper/luks-c377 rw
41 40 0:50 / /home/u/lab rw - tmpfs torx-diskfault rw,size=1024k
42 41 0:51 / /home/u/lab rw - tmpfs stacked rw
43 25 0:52 / /mnt/with\040space rw - tmpfs t rw
`
	for _, tc := range []struct {
		path, point, dev, source string
	}{
		{"/etc/hosts", "/", "252:0", "/dev/mapper/root"},
		{"/home", "/home", "0:35", "/dev/mapper/luks-c377"},
		{"/home/u/data", "/home", "0:35", "/dev/mapper/luks-c377"},
		{"/home/u/lab/x", "/home/u/lab", "0:51", "stacked"},
		{"/home/u/labyrinth", "/home", "0:35", "/dev/mapper/luks-c377"},
		{"/mnt/with space/f", "/mnt/with space", "0:52", "t"},
	} {
		m, ok := mountOf(table, tc.path)
		if !ok || m.point != tc.point || m.dev != tc.dev || m.source != tc.source {
			t.Errorf("mountOf(%q) = %+v, %v; want %s on %s from %s", tc.path, m, ok, tc.point, tc.dev, tc.source)
		}
	}
}

func TestCPUMax(t *testing.T) {
	for _, tc := range []struct {
		share float64
		want  string
	}{
		{0, "max 100000"},
		{-1, "max 100000"},
		{0.05, "5000 100000"},
		{1.5, "150000 100000"},
		{0.001, "1000 100000"}, // the kernel's floor is a millisecond
	} {
		if got := cpuMax(tc.share); got != tc.want {
			t.Errorf("cpuMax(%v) = %q, want %q", tc.share, got, tc.want)
		}
	}
}

func TestIOLimitString(t *testing.T) {
	if got, want := (IOLimit{WriteBPS: 1 << 20, ReadIOPS: 50}).String(), "rbps=max wbps=1048576 riops=50 wiops=max"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDeviceNumbers(t *testing.T) {
	// makedev(252, 0), makedev(259, 3), and makedev(0x1000, 0xfff12), as
	// glibc encodes them.
	for _, tc := range []struct{ dev, major, minor uint64 }{
		{0xfc00, 252, 0},
		{0x10303, 259, 3},
		{0x1000fff00012, 0x1000, 0xfff12},
	} {
		if unixMajor(tc.dev) != tc.major || unixMinor(tc.dev) != tc.minor {
			t.Errorf("dev %#x: %d:%d, want %d:%d", tc.dev, unixMajor(tc.dev), unixMinor(tc.dev), tc.major, tc.minor)
		}
	}
}
