// Readiness primitives: wait for a condition to hold before proceeding.
//
// WaitUntil is the polling core; WaitForPort, WaitForHTTP, and WaitForLog build
// on it for common service-startup checks. All are context-aware -- the caller
// bounds the wait with a context deadline -- and return an error wrapping
// ErrReadinessTimeout when the context is done before the condition holds. The
// checks take plain inputs (an address, a URL, a read function), so they are
// usable without a Backend and testable in isolation. The rule they support:
// start a service, then block only until it is verifiably up, never on a bare
// sleep.
package torx

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	defaultPollBackoff = 100 * time.Millisecond
	dialTimeout        = time.Second
)

// WaitUntil polls until poll reports ready, poll returns an error, or ctx is
// done. It calls poll immediately, then again every backoff (a non-positive
// backoff uses a small default). A poll error is returned as-is; a ctx that is
// done first yields an error wrapping ErrReadinessTimeout and the ctx cause, so
// errors.Is distinguishes a deadline from a cancellation. Pass a ctx with a
// deadline to bound the wait.
func WaitUntil(ctx context.Context, poll func(context.Context) (bool, error), backoff time.Duration) error {
	if backoff <= 0 {
		backoff = defaultPollBackoff
	}
	ticker := time.NewTicker(backoff)
	defer ticker.Stop()
	for {
		ready, err := poll(ctx)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return Wrap(ErrReadinessTimeout, "readiness: wait", ctx.Err())
		case <-ticker.C:
		}
	}
}

// WaitForPort waits until a TCP connection to addr succeeds. A refused or failed
// connection is treated as not-ready, so an address that never listens surfaces
// as ErrReadinessTimeout once ctx is done.
func WaitForPort(ctx context.Context, addr string) error {
	return WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		var d net.Dialer
		conn, err := d.DialContext(dialCtx, "tcp", addr)
		if err != nil {
			return false, nil
		}
		_ = conn.Close()
		return true, nil
	}, defaultPollBackoff)
}

// WaitForHTTP waits until a GET to url returns a response with a status below
// 500 -- the server is up and not reporting a server error, so a 503 during
// startup keeps it waiting. A request that cannot be built or sent is treated as
// not-ready, so an unreachable or malformed url surfaces as ErrReadinessTimeout
// once ctx is done.
func WaitForHTTP(ctx context.Context, url string) error {
	client := &http.Client{}
	return WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false, nil
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, nil
		}
		_ = resp.Body.Close()
		return resp.StatusCode < 500, nil
	}, defaultPollBackoff)
}

// WaitForLog waits until substr appears in the content returned by read. read
// yields the current log content -- the full content, so a match is never split
// across reads -- and a read error is treated as not-ready, so a log file that
// does not exist yet simply keeps the wait polling. read is any function that
// returns bytes, which a backend wires to a node's log file; WaitForLog itself
// depends on no Backend.
func WaitForLog(ctx context.Context, read func(context.Context) ([]byte, error), substr string) error {
	return WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		content, err := read(ctx)
		if err != nil {
			return false, nil
		}
		return strings.Contains(string(content), substr), nil
	}, defaultPollBackoff)
}
