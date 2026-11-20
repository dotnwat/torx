//go:build linux

package resfault

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestWholeDisk(t *testing.T) {
	sys := t.TempDir()
	// NVMe controller char device (238:0) -> whole disk nvme0n1 (259:0) -> partition nvme0n1p2 (259:2).
	ctrlDir := filepath.Join(sys, "devices", "pci0", "nvme", "nvme0")
	diskDir := filepath.Join(ctrlDir, "nvme0n1")
	partDir := filepath.Join(diskDir, "nvme0n1p2")
	dmDir := filepath.Join(sys, "devices", "virtual", "block", "dm-0")
	blockDir := filepath.Join(sys, "dev", "block")
	for _, d := range []string{partDir, dmDir, blockDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range map[string]string{
		filepath.Join(ctrlDir, "dev"):       "238:0\n",
		filepath.Join(diskDir, "dev"):       "259:0\n",
		filepath.Join(partDir, "dev"):       "259:2\n",
		filepath.Join(partDir, "partition"): "2\n",
		filepath.Join(dmDir, "dev"):         "252:0\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{
		"259:0": diskDir,
		"259:2": partDir,
		"252:0": dmDir,
	} {
		if err := os.Symlink(target, filepath.Join(blockDir, name)); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		dev, want string
	}{
		{"259:2", "259:0"}, // partition maps to parent whole disk
		{"259:0", "259:0"}, // whole NVMe disk must NOT walk up to controller char dev 238:0
		{"252:0", "252:0"}, // virtual dm device stays itself
		{"99:9", "99:9"},   // missing sysfs entry falls back to input
	}
	for _, tc := range cases {
		if got := wholeDisk(sys, tc.dev); got != tc.want {
			t.Errorf("wholeDisk(%q) = %q, want %q", tc.dev, got, tc.want)
		}
	}
}

func TestBlockDeviceRelativeDir(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	wantDev, wantErr := blockDevice(wd)
	gotDev, gotErr := blockDevice(".")
	if (gotErr != nil) != (wantErr != nil) || gotDev != wantDev {
		t.Errorf("blockDevice(\".\") = (%q, %v), want (%q, %v) matching %q", gotDev, gotErr, wantDev, wantErr, wd)
	}
}
