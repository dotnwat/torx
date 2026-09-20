//go:build unix

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStoreReplay covers what the tutorial's durability job relies on: a
// reopened log serves every acknowledged write, a torn tail left by a kill is
// dropped, and writes after the reopen land after the last good entry rather
// than after the torn one.
func TestStoreReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")
	if err := os.WriteFile(path, []byte(`{"key":"a","value":"1"}`+"\n"+`{"key":"b","value":"2"}`+"\n"+`{"key":"c","val`), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if st.Len() != 2 {
		t.Errorf("replayed %d keys, want 2", st.Len())
	}
	if v, ok := st.Get("b"); !ok || v != "2" {
		t.Errorf("b = %q, %v; want 2", v, ok)
	}
	if err := st.Put("c", "3"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = openStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st.Close() }()
	if st.Len() != 3 {
		t.Errorf("replayed %d keys after reopen, want 3", st.Len())
	}
	if v, ok := st.Get("c"); !ok || v != "3" {
		t.Errorf("c = %q, %v; want 3", v, ok)
	}
}

func TestStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")
	if err := os.WriteFile(path, []byte("not json\n"+`{"key":"a","value":"1"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openStore(path); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("open = %v, want a corruption error", err)
	}
}

func TestHandler(t *testing.T) {
	st, err := openStore(filepath.Join(t.TempDir(), "kv.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	srv := httptest.NewServer(st.handler())
	defer srv.Close()
	hc := srv.Client()

	if _, err := get(hc, srv.URL, "missing"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("get of a missing key = %v, want 404", err)
	}
	if err := put(hc, srv.URL, "greeting", "hello"); err != nil {
		t.Fatal(err)
	}
	if v, err := get(hc, srv.URL, "greeting"); err != nil || v != "hello" {
		t.Errorf("get = %q, %v; want hello", v, err)
	}
	resp, err := hc.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"keys":1`) {
		t.Errorf("stats: %s %s", resp.Status, body)
	}
}

func TestPercentile(t *testing.T) {
	if p := percentile(nil, 0.5); p != 0 {
		t.Errorf("percentile of nothing = %v, want 0", p)
	}
	lat := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 100 * time.Millisecond}
	if p := percentile(lat, 0.5); p != 3 {
		t.Errorf("p50 = %v, want 3", p)
	}
	if p := percentile(lat, 0.99); p != 100 {
		t.Errorf("p99 = %v, want 100", p)
	}
}
