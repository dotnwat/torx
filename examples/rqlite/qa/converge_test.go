//go:build unix

package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// countServer answers every query with the next value of a sequence: an
// error while the table does not exist yet, then rising counts, then the
// final count for good -- a node applying a backlog.
func countServer(t *testing.T, sequence []string) (*rqlite.Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(sequence) {
			i = len(sequence) - 1
		}
		_, _ = io.WriteString(w, sequence[i])
	}))
	t.Cleanup(srv.Close)
	return rqlite.NewClient(strings.TrimPrefix(srv.URL, "http://")), &calls
}

func count(n int) string {
	return fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[%d]]}]}`, n)
}

func TestAwaitRowsPollsThroughErrorsAndShortCounts(t *testing.T) {
	c, calls := countServer(t, []string{
		`{"results":[{"error":"no such table: events"}]}`,
		count(1), count(400), count(1100),
	})
	if err := awaitRows(t.Context(), c, rqlite.LevelNone, rqlite.Stmt("SELECT COUNT(*) FROM events"), 1100); err != nil {
		t.Fatalf("awaitRows: %v", err)
	}
	if got := calls.Load(); got < 4 {
		t.Errorf("converged after %d reads, want at least 4", got)
	}
}

func TestAwaitRowsReturnsAtOnceWhenAlreadyThere(t *testing.T) {
	c, calls := countServer(t, []string{count(20)})
	if err := awaitRows(t.Context(), c, rqlite.LevelLinearizable, rqlite.Stmt("SELECT COUNT(*) FROM t"), 20); err != nil {
		t.Fatalf("awaitRows: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("made %d reads, want 1", got)
	}
}
