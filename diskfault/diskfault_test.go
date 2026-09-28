//go:build unix

package diskfault

import "testing"

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
