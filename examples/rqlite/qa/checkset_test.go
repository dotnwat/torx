//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// ms is a history time.
func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func add(v int64, start, end int, out Outcome) Op {
	return Op{Process: "c", F: "add", Value: v, Start: ms(start), End: ms(end), Outcome: out}
}

func read(level string, start, end int, lower int64, vs ...int64) Op {
	return Op{Process: "c", F: "read", Mode: level, Node: "n", Lower: lower, Values: vs, Start: ms(start), End: ms(end), Outcome: Ok}
}

func kinds(as []Anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind+"/"+a.Severity)
	}
	slices.Sort(out)
	return out
}

func TestCheckSet(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ops   []Op
		final []int64
		want  []string
	}{
		{
			name:  "a clean history",
			ops:   []Op{add(1, 0, 10, Ok), add(2, 5, 15, Info), read(rqlite.LevelLinearizable, 20, 30, 0, 1, 2)},
			final: []int64{1, 2},
		},
		{
			name:  "an indeterminate add may be missing",
			ops:   []Op{add(1, 0, 10, Ok), add(2, 5, 15, Info), read(rqlite.LevelStrong, 20, 30, 0, 1)},
			final: []int64{1},
		},
		{
			name:  "an acknowledged add is lost",
			ops:   []Op{add(1, 0, 10, Ok), add(2, 5, 15, Ok)},
			final: []int64{1},
			want:  []string{"lost/error"},
		},
		{
			name:  "a linearizable read misses an acknowledged add",
			ops:   []Op{add(1, 0, 10, Ok), read(rqlite.LevelLinearizable, 20, 30, 0)},
			final: []int64{1},
			want:  []string{"stale-read/error"},
		},
		{
			name: "a strong read misses a value an earlier none read saw",
			// The add is indeterminate, but a read of any level that sees a
			// value proves it committed.
			ops:   []Op{add(1, 0, 100, Info), read(rqlite.LevelNone, 10, 20, 0, 1), read(rqlite.LevelStrong, 30, 40, 0)},
			final: []int64{1},
			want:  []string{"stale-read/error"},
		},
		{
			name:  "a read concurrent with an add need not see it",
			ops:   []Op{add(1, 0, 50, Ok), read(rqlite.LevelLinearizable, 20, 60, 0)},
			final: []int64{1},
		},
		{
			name:  "a read's lower bound limits what it must see",
			ops:   []Op{add(1, 0, 10, Ok), add(2, 0, 10, Ok), read(rqlite.LevelLinearizable, 20, 30, 1, 2)},
			final: []int64{1, 2},
		},
		{
			name:  "a stale weak read is a warning",
			ops:   []Op{add(1, 0, 10, Ok), read(rqlite.LevelWeak, 20, 30, 0)},
			final: []int64{1},
			want:  []string{"weak-stale-read/warn"},
		},
		{
			name:  "a freshness read may trail by its freshness",
			ops:   []Op{add(1, 0, 10, Ok), read(modeFresh, 500, 510, 0)},
			final: []int64{1},
		},
		{
			name:  "a freshness read may not trail by more",
			ops:   []Op{add(1, 0, 10, Ok), read(modeFresh, 1500, 1510, 0)},
			final: []int64{1},
			want:  []string{"stale-fresh-read/error"},
		},
		{
			name:  "a none read promises nothing about currency",
			ops:   []Op{add(1, 0, 10, Ok), read(rqlite.LevelNone, 20, 30, 0)},
			final: []int64{1},
		},
		{
			name:  "a value applied twice",
			ops:   []Op{add(1, 0, 10, Ok), read(rqlite.LevelNone, 20, 30, 0, 1, 1)},
			final: []int64{1, 1},
			want:  []string{"duplicate/error"}, // reported once, where first seen
		},
		{
			name:  "a value nobody wrote",
			ops:   []Op{add(1, 0, 10, Ok)},
			final: []int64{1, 7},
			want:  []string{"phantom/error"},
		},
		{
			name:  "a value whose add definitely failed",
			ops:   []Op{add(1, 0, 10, Ok), add(2, 0, 10, Fail)},
			final: []int64{1, 2},
			want:  []string{"resurrected/error"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := kinds(checkSet(tc.ops, tc.final))
			if !slices.Equal(got, tc.want) {
				t.Errorf("anomalies = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDiffIsAMultisetDifference(t *testing.T) {
	missing, extra := diff([]int64{1, 2, 2, 3}, []int64{2, 3, 3, 4})
	if !slices.Equal(missing, []int64{1, 2}) || !slices.Equal(extra, []int64{3, 4}) {
		t.Errorf("diff = %v, %v; want [1 2], [3 4]", missing, extra)
	}
}

func TestClassify(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	for _, tc := range []struct {
		err  error
		want Outcome
	}{
		{nil, Ok},
		{fmt.Errorf("rqlite: %w", refused), Fail},
		{&rqlite.StatusError{Path: "/db/execute", Code: 503, Text: "leader not found"}, Fail},
		{fmt.Errorf("rqlite: %w", reset), Info},
		{&rqlite.StatusError{Path: "/db/execute", Code: 408, Text: "queue wait timeout"}, Info},
		{errors.New("rqlite: /db/execute: leadership lost while committing log"), Info},
		{context.DeadlineExceeded, Info},
	} {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("classify(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}
