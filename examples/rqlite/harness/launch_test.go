//go:build unix

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	version, major, err := parseVersion("rqlited v10.3.5 darwin arm64 go1.27.1 sqlite3.53.4 (commit unknown, compiler gc)\n")
	if err != nil || version != "v10.3.5" || major != 10 {
		t.Errorf("parseVersion = %q, %d, %v; want v10.3.5, 10, nil", version, major, err)
	}
	for _, bad := range []string{"", "rqlite v10.3.5", "rqlited 10.3.5", "rqlited vX.1"} {
		if _, _, err := parseVersion(bad); err == nil {
			t.Errorf("parseVersion(%q) succeeded", bad)
		}
	}
}

func TestMakeRunDirIsUniqueAndRepointsLatest(t *testing.T) {
	root := filepath.Join(t.TempDir(), "results")
	d1, err := makeRunDir(root)
	if err != nil {
		t.Fatalf("first run dir: %v", err)
	}
	d2, err := makeRunDir(root)
	if err != nil {
		t.Fatalf("second run dir: %v", err)
	}
	if d1 == d2 {
		t.Fatalf("two runs got the same directory %s", d1)
	}
	for _, d := range []string{d1, d2} {
		if info, err := os.Stat(d); err != nil || !info.IsDir() {
			t.Errorf("%s is not a directory: %v", d, err)
		}
	}
	latest, err := os.Readlink(filepath.Join(root, "latest"))
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest != filepath.Base(d2) {
		t.Errorf("latest -> %s, want the second run %s", latest, filepath.Base(d2))
	}
}

func TestArchiveParamsCopiesVerbatimUnderFixedName(t *testing.T) {
	src := filepath.Join(t.TempDir(), "sweep.json")
	if err := os.WriteFile(src, []byte(`{"rqlite.cluster": {"matrix": {"nodes": [5]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir()
	dst, err := archiveParams(src, runDir)
	if err != nil {
		t.Fatalf("archiveParams: %v", err)
	}
	if filepath.Base(dst) != paramsFile || filepath.Dir(dst) != runDir {
		t.Errorf("archived at %s, want %s", dst, filepath.Join(runDir, paramsFile))
	}
	got, _ := os.ReadFile(dst)
	want, _ := os.ReadFile(src)
	if string(got) != string(want) {
		t.Errorf("archived copy differs from the source")
	}
}

func TestInvocationRecordShape(t *testing.T) {
	inv := invocation{
		Argv:    []string{"harness", "rqlite.smoke"},
		Backend: "local",
		Rqlited: rqlitedInfo{Path: "/opt/homebrew/bin/rqlited", Version: "v10.3.5"},
	}
	b, err := json.Marshal(inv)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, key := range []string{`"argv"`, `"backend":"local"`, `"git"`, `"rqlited"`, `"suite_argv"`, `"job_patterns"`} {
		if !strings.Contains(s, key) {
			t.Errorf("record lacks %s: %s", key, s)
		}
	}
	if strings.Contains(s, `"params"`) {
		t.Errorf("record names params when none was given: %s", s)
	}
}

func TestRepoRootIsTheTorxModule(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Errorf("%s has no go.mod: %v", root, err)
	}
	if _, err := os.Stat(filepath.Join(root, "examples", "rqlite", "qa")); err != nil {
		t.Errorf("%s does not hold the suite: %v", root, err)
	}
}
