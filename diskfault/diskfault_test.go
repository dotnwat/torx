//go:build unix

package diskfault

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dotnwat/torx"
)

func TestLimitOnTopLooksOnlyAtTheTopmostMount(t *testing.T) {
	// A root, a volume at /data, then a limit laid over the volume.
	volume := "22 1 0:21 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n" +
		"40 22 0:40 / /data rw,relatime shared:2 - xfs /dev/sdb1 rw\n"
	limited := volume + "41 40 0:41 / /data rw,relatime - tmpfs torx-diskfault rw,size=1024k\n"
	for _, tc := range []struct {
		name, table, path string
		want              bool
	}{
		{"limit over a volume", limited, "/data", true},
		{"volume, its limit gone", volume, "/data", false},
		{"not a mount point", limited, "/data/sub", false},
		{"root", limited, "/", false},
		{"another tmpfs", "30 22 0:30 / /data rw - tmpfs tmpfs rw\n", "/data", false},
		{"escaped path", `30 22 0:30 / /my\040data rw - tmpfs torx-diskfault rw` + "\n", "/my data", true},
		{"limit covered by a later mount", "30 22 0:30 / /data rw - tmpfs torx-diskfault rw\n" +
			"31 30 0:31 / /data rw - xfs /dev/sdb1 rw\n", "/data", false},
		// Neither mount is the other's parent: the later one is on top.
		{"two unrelated mounts, limit later", "30 22 0:30 / /data rw - xfs /dev/sdb1 rw\n" +
			"31 23 0:31 / /data rw - tmpfs torx-diskfault rw\n", "/data", true},
		{"two unrelated mounts, limit earlier", "30 22 0:30 / /data rw - tmpfs torx-diskfault rw\n" +
			"31 23 0:31 / /data rw - xfs /dev/sdb1 rw\n", "/data", false},
	} {
		if got := limitOnTop(tc.table, tc.path); got != tc.want {
			t.Errorf("%s: limitOnTop(%q) = %t, want %t", tc.name, tc.path, got, tc.want)
		}
	}
}

func TestUnescapeUndoesOctalEscapes(t *testing.T) {
	for in, want := range map[string]string{
		`/plain`:         "/plain",
		`/a\040b`:        "/a b",
		`/tab\011nl\012`: "/tab\tnl\n",
		`/back\134slash`: `/back\slash`,
		`/not\09escape`:  `/not\09escape`,
		`/short\04`:      `/short\04`,
	} {
		if got := unescape(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCorruptOverwritesOnlyTheRange(t *testing.T) {
	dir := t.TempDir()
	n := torx.NewNode(torx.NodeConfig{Name: "n", Backend: torx.LocalBackend{}, Scratch: torx.MakeScratch(dir, "n")})
	path := filepath.Join(dir, "file")
	orig := bytes.Repeat([]byte{0xa5}, 1<<20)
	for _, tc := range []struct {
		name         string
		offset, size int64
		from, to     int64 // the bytes that may change
	}{
		{"a sector in the middle", 4096, 4096, 4096, 8192},
		{"an unaligned range", 1000, 3, 1000, 1003},
		{"a range past the end", 1<<20 - 100, 4096, 1<<20 - 100, 1 << 20},
		{"a range beyond the end", 2 << 20, 4096, 0, 0},
		{"nothing", 5000, 0, 0, 0},
	} {
		if err := os.WriteFile(path, orig, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := Corrupt(context.Background(), n, path, tc.offset, tc.size); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(orig) {
			t.Fatalf("%s: the file is %d bytes, want %d", tc.name, len(got), len(orig))
		}
		if !bytes.Equal(got[:tc.from], orig[:tc.from]) || !bytes.Equal(got[tc.to:], orig[tc.to:]) {
			t.Errorf("%s: bytes outside [%d, %d) changed", tc.name, tc.from, tc.to)
		}
		if tc.to > tc.from+16 && bytes.Equal(got[tc.from:tc.to], orig[tc.from:tc.to]) {
			t.Errorf("%s: the range [%d, %d) is unchanged", tc.name, tc.from, tc.to)
		}
	}
	if err := Corrupt(context.Background(), n, filepath.Join(dir, "absent"), 0, 1); err == nil {
		t.Error("corrupting a file that does not exist succeeded")
	}
}

func TestBlockSizeDividesTheRange(t *testing.T) {
	for _, tc := range []struct{ offset, size, want int64 }{
		{0, 1 << 20, 64 << 10},
		{4096, 4096, 4096},
		{1 << 20, 512 << 10, 64 << 10},
		{1000, 3, 1},
		{1000, 1 << 20, 8},
		{0, 100, 4},
	} {
		if got := blockSize(tc.offset, tc.size); got != tc.want {
			t.Errorf("blockSize(%d, %d) = %d, want %d", tc.offset, tc.size, got, tc.want)
		}
	}
}
