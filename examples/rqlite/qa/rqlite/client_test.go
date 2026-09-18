//go:build unix

package rqlite

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Response bodies below are verbatim from rqlite v10.3.5, so the decoding is
// tested against what the server actually sends rather than what the client
// author assumed.

func TestStatementMarshalsInRqliteRequestForm(t *testing.T) {
	cases := []struct {
		stmt Statement
		want string
	}{
		{Stmt("SELECT 1"), `"SELECT 1"`},
		{Stmt("INSERT INTO t(name) VALUES(?)", "alice"), `["INSERT INTO t(name) VALUES(?)","alice"]`},
		{Stmt("INSERT INTO t(k, v) VALUES(?, ?)", 7, "x"), `["INSERT INTO t(k, v) VALUES(?, ?)",7,"x"]`},
	}
	for _, c := range cases {
		got, err := json.Marshal(c.stmt)
		if err != nil {
			t.Fatalf("marshal %v: %v", c.stmt, err)
		}
		if string(got) != c.want {
			t.Errorf("marshal %v = %s, want %s", c.stmt, got, c.want)
		}
	}
}

func TestValidLevel(t *testing.T) {
	for _, l := range Levels() {
		if !ValidLevel(l) {
			t.Errorf("ValidLevel(%q) = false", l)
		}
	}
	for _, l := range []string{"", "Weak", "bogus", "auto"} {
		if ValidLevel(l) {
			t.Errorf("ValidLevel(%q) = true", l)
		}
	}
}

// serve returns a client against a server that records the last request and
// answers every request with body at status.
func serve(t *testing.T, status int, body string) (*Client, *http.Request, *[]byte) {
	t.Helper()
	var last *http.Request
	var lastBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last = r
		lastBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(strings.TrimPrefix(srv.URL, "http://")), last, &lastBody
}

func TestExecuteSendsTransactionAndDecodesResults(t *testing.T) {
	var got *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"results":[{},{"last_insert_id":1,"rows_affected":1}]}`)
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))

	res, err := c.Execute(context.Background(),
		Stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"),
		Stmt("INSERT INTO t(name) VALUES(?)", "alice"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Method != http.MethodPost || got.URL.Path != "/db/execute" || got.URL.RawQuery != "transaction" {
		t.Errorf("request was %s %s?%s, want POST /db/execute?transaction", got.Method, got.URL.Path, got.URL.RawQuery)
	}
	if ct := got.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	want := `["CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",["INSERT INTO t(name) VALUES(?)","alice"]]`
	if string(body) != want {
		t.Errorf("body = %s, want %s", body, want)
	}
	if len(res) != 2 || res[1].LastInsertID != 1 || res[1].RowsAffected != 1 {
		t.Errorf("results = %+v, want two results with the insert recorded", res)
	}
}

func TestExecuteReportsStatementError(t *testing.T) {
	c, _, _ := serve(t, http.StatusOK, `{"results":[{"error":"no such table: nosuch"}]}`)
	_, err := c.Execute(context.Background(), Stmt("INSERT INTO nosuch(name) VALUES(1)"))
	if err == nil || !strings.Contains(err.Error(), "no such table: nosuch") {
		t.Fatalf("Execute error = %v, want the statement's error", err)
	}
}

func TestQueryDecodesRowsAndInt(t *testing.T) {
	c, _, _ := serve(t, http.StatusOK,
		`{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"alice"],[2,"bob"]]}]}`)
	res, err := c.Query(context.Background(), LevelStrong, Stmt("SELECT * FROM t"))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Columns) != 2 || len(res.Values) != 2 || res.Values[1][1] != "bob" {
		t.Errorf("result = %+v, want two columns and two rows", res)
	}
	n, err := res.Int()
	if err != nil || n != 1 {
		t.Errorf("Int() = %d, %v; want 1 (the first cell)", n, err)
	}

	c, _, _ = serve(t, http.StatusOK, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[42]]}]}`)
	n, err = c.QueryInt(context.Background(), LevelWeak, Stmt("SELECT COUNT(*) FROM t"))
	if err != nil || n != 42 {
		t.Errorf("QueryInt = %d, %v; want 42", n, err)
	}
}

func TestQueryRejectsUnknownLevelBeforeSending(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = io.WriteString(w, `{"results":[{"columns":["1"],"types":["integer"],"values":[[1]]}]}`)
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))
	if _, err := c.Query(context.Background(), "bogus", Stmt("SELECT 1")); err == nil {
		t.Fatal("Query with an unknown level succeeded; rqlite would have served it at the default level")
	}
	if requests != 0 {
		t.Errorf("client sent %d requests for an invalid level, want 0", requests)
	}
}

func TestQuerySendsLevel(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_, _ = io.WriteString(w, `{"results":[{"columns":["1"],"types":["integer"],"values":[[1]]}]}`)
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))
	if _, err := c.Query(context.Background(), LevelLinearizable, Stmt("SELECT 1")); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.URL.Path != "/db/query" || got.URL.Query().Get("level") != LevelLinearizable {
		t.Errorf("request was %s?%s, want /db/query?level=linearizable", got.URL.Path, got.URL.RawQuery)
	}
}

func TestNon200IsAnErrorCarryingTheBody(t *testing.T) {
	c, _, _ := serve(t, http.StatusServiceUnavailable, "leader not found\n")
	_, err := c.Query(context.Background(), LevelWeak, Stmt("SELECT 1"))
	if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "leader not found") {
		t.Fatalf("error = %v, want the status and rqlite's message", err)
	}
}

func TestNodesDecodesMembershipAndLeaderOf(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_, _ = io.WriteString(w, `{"nodes":[`+
			`{"id":"n0","api_addr":"http://127.0.0.1:5101","addr":"127.0.0.1:5102","version":"v10.3.5","voter":true,"reachable":false,"leader":false,"time":0.000007833,"time_s":"7.833µs"},`+
			`{"id":"n2","api_addr":"http://127.0.0.1:5105","addr":"127.0.0.1:5106","version":"v10.3.5","voter":true,"reachable":true,"leader":true,"time":0.000120792,"time_s":"120.833µs"}]}`)
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))

	view, err := c.Nodes(context.Background(), 2*time.Second)
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if got.URL.Path != "/nodes" || got.URL.Query().Get("ver") != "2" || got.URL.Query().Get("timeout") != "2s" {
		t.Errorf("request was %s?%s, want /nodes?ver=2&timeout=2s", got.URL.Path, got.URL.RawQuery)
	}
	if len(view) != 2 || view[0].ID != "n0" || view[0].Reachable || !view[1].Voter || view[1].Addr != "127.0.0.1:5106" {
		t.Errorf("view = %+v", view)
	}
	leader, ok := LeaderOf(view)
	if !ok || leader.ID != "n2" {
		t.Errorf("LeaderOf = %+v, %v; want n2", leader, ok)
	}
	if _, ok := LeaderOf(nil); ok {
		t.Error("LeaderOf(nil) reported a leader")
	}
}

func TestResultIntWithoutRows(t *testing.T) {
	if _, err := (Result{}).Int(); err == nil {
		t.Error("Int() on an empty result succeeded")
	}
	if _, err := (Result{Values: [][]any{{"text"}}}).Int(); err == nil {
		t.Error("Int() on a text cell succeeded")
	}
}
