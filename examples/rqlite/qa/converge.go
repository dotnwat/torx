//go:build unix

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// convergeTimeout bounds how far a node's own copy may trail the log the
// cluster has committed: a none-level read on a follower, or a restarted node
// applying what it received while it was down.
const convergeTimeout = 10 * time.Second

// awaitRows polls a single-value query at level on c until it returns want,
// bounded by convergeTimeout. It is the assertion for a read whose guarantee
// is convergence rather than currency: rqlite acknowledges a write once the
// log entry is committed, and applies it to each node's SQLite copy after
// that, so a copy read directly may trail so far that the table does not
// exist there yet -- which surfaces as a query error, not a short count.
// Both are "not yet" until the deadline; the last error is kept for the
// failure message.
func awaitRows(ctx context.Context, c *rqlite.Client, level string, stmt rqlite.Statement, want int64) error {
	ctx, cancel := context.WithTimeout(ctx, convergeTimeout)
	defer cancel()
	var got int64
	var lastErr error
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		n, err := c.QueryInt(ctx, level, stmt)
		if err != nil {
			lastErr = err
			return false, nil
		}
		got = n
		return n == want, nil
	}, 0)
	if err == nil {
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("%w; last read: %w", err, lastErr)
	}
	return fmt.Errorf("%w; last count %d, want %d", err, got, want)
}
