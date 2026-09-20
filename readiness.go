//go:build unix

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
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Each probe is bounded on its own, apart from the wait's deadline: a probe
// that stalls -- a dial the network drops, a connection accepted into the
// kernel backlog and never served, a proxy holding the request for its own
// upstream -- costs one attempt, and the next probe starts fresh. Without
// this, one stuck probe consumes the whole wait, and ErrReadinessTimeout
// stops meaning "the condition did not hold within the deadline".
const (
	defaultPollBackoff = 100 * time.Millisecond
	// dialTimeout bounds one TCP connect in WaitForPort.
	dialTimeout = time.Second
	// httpProbeTimeout bounds one GET in WaitForHTTP, through the response
	// headers. It leaves room for a readiness handler that does real work --
	// a quorum check, a ping to a store -- under load.
	httpProbeTimeout = 5 * time.Second
	// maxProbeBody bounds how much of a readiness response WaitForHTTP reads
	// before closing it. A readiness body is a status line, not a payload;
	// reading it lets the transport reuse the connection at once.
	maxProbeBody = 64 << 10
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
// once ctx is done. Each probe is bounded by a few seconds, so one that stalls
// costs one attempt rather than the whole wait; an endpoint that legitimately
// takes longer to answer is not a readiness probe, and a caller that must poll
// one builds on WaitUntil with its own client.
//
// The wait leaves nothing connected to the server: its probes run on a
// transport of their own, whose connections are closed when the wait
// returns, and never on the process-wide http.DefaultTransport a suite's own
// clients usually share. A probe connection left pooled there is not only a
// connection the server sees open; the caller's first request can race the
// transport returning it, dial a second connection while it waits, be handed
// the probe's, and leave the second one open and never used -- and a server
// shutting down gracefully then waits on a connection nothing is in flight
// on. (net/http's Shutdown gives such a connection five seconds.) The
// transport of their own is the default transport's configuration -- proxy,
// dial timeouts, a TLS config a suite set on it -- with a pool of its own. A
// suite that replaced http.DefaultTransport with a RoundTripper of another
// type has chosen how every request in the process travels, probes included:
// they run on it as it is, and their connections are closed when the wait
// returns only if it has a CloseIdleConnections method.
func WaitForHTTP(ctx context.Context, url string) error {
	return waitForHTTP(ctx, url, httpProbeTimeout)
}

// waitForHTTP is WaitForHTTP with the per-probe bound as a parameter, so tests
// can make a stall cheap.
func waitForHTTP(ctx context.Context, url string, probe time.Duration) error {
	client := &http.Client{Transport: probeTransport()}
	defer client.CloseIdleConnections()
	return WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		ctx, cancel := context.WithTimeout(ctx, probe)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false, nil
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, nil
		}
		// Read the body (bounded) before closing it: a body read to its end
		// hands the connection back to the transport before Close returns,
		// where one closed unread is drained and handed back afterwards, in
		// the background. It keeps each probe's connection reusable by the
		// next probe, and the transport's pool settled when the wait ends.
		// Not on a 101, though: the transport hands that response's
		// connection over as the body and stops watching the context, so a
		// read of it blocks for as long as the peer stays silent, past any
		// deadline. Close alone closes the connection.
		if resp.StatusCode != http.StatusSwitchingProtocols {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxProbeBody))
		}
		_ = resp.Body.Close()
		return resp.StatusCode < 500, nil
	}, defaultPollBackoff)
}

// probeTransport returns the transport for one wait's probes: the default
// transport's settings -- proxy from the environment, dial timeouts, TLS
// configuration -- with a connection pool of its own. A default transport of
// some other type cannot be cloned, and dropping its settings for a bare
// http.Transport would fail probes its owner's own requests pass, so probes
// run on it as it is.
func probeTransport() http.RoundTripper {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return http.DefaultTransport
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
