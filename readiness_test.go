package torx

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitUntilSucceeds(t *testing.T) {
	calls := 0
	err := WaitUntil(context.Background(), func(context.Context) (bool, error) {
		calls++
		return calls >= 3, nil
	}, time.Millisecond)
	if err != nil {
		t.Fatalf("WaitUntil err = %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("polled %d times, want 3", calls)
	}
}

func TestWaitUntilTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := WaitUntil(ctx, func(context.Context) (bool, error) { return false, nil }, time.Millisecond)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Errorf("err = %v, want ErrReadinessTimeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap DeadlineExceeded", err)
	}
}

func TestWaitUntilPollError(t *testing.T) {
	boom := errors.New("boom")
	err := WaitUntil(context.Background(), func(context.Context) (bool, error) {
		return false, boom
	}, time.Millisecond)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
	if errors.Is(err, ErrReadinessTimeout) {
		t.Errorf("a poll error must not be reported as a readiness timeout")
	}
}

func TestWaitUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := WaitUntil(ctx, func(context.Context) (bool, error) { return false, nil }, time.Second)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Errorf("err = %v, want ErrReadinessTimeout", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap Canceled", err)
	}
}

func TestWaitForPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForPort(ctx, addr); err != nil {
		t.Fatalf("WaitForPort on a live listener: %v", err)
	}

	ln.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if err := WaitForPort(ctx2, addr); !errors.Is(err, ErrReadinessTimeout) {
		t.Errorf("WaitForPort on a closed port: err = %v, want ErrReadinessTimeout", err)
	}
}

func TestWaitForHTTP(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForHTTP(ctx, up.URL); err != nil {
		t.Fatalf("WaitForHTTP on a healthy server: %v", err)
	}

	// A 5xx means "not ready yet", so the wait times out rather than passing.
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer busy.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if err := WaitForHTTP(ctx2, busy.URL); !errors.Is(err, ErrReadinessTimeout) {
		t.Errorf("WaitForHTTP on a 503 server: err = %v, want ErrReadinessTimeout", err)
	}
}

// connStates is an http.Server ConnState hook that tracks open connections,
// so a test can see what a client left behind on the server.
type connStates struct {
	mu    sync.Mutex
	conns map[net.Conn]http.ConnState
}

func newConnStates() *connStates { return &connStates{conns: map[net.Conn]http.ConnState{}} }

func (c *connStates) hook(conn net.Conn, st http.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st == http.StateClosed || st == http.StateHijacked {
		delete(c.conns, conn)
		return
	}
	c.conns[conn] = st
}

// count returns how many connections are open, and how many of those have
// never sent a request.
func (c *connStates) count() (open, unused int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, st := range c.conns {
		open++
		if st == http.StateNew {
			unused++
		}
	}
	return open, unused
}

// trackedServer serves a small body and reports its connection states.
func trackedServer(t *testing.T) (*httptest.Server, *connStates) {
	t.Helper()
	states := newConnStates()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	}))
	srv.Config.ConnState = states.hook
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, states
}

// A readiness wait leaves nothing connected to the server once it returns:
// its probes do not run on the process-wide transport, and their own
// transport's connections are closed with the wait.
func TestWaitForHTTPLeavesNoConnection(t *testing.T) {
	srv, states := trackedServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForHTTP(ctx, srv.URL); err != nil {
		t.Fatal(err)
	}
	// The server notices the close on its next read, a moment later.
	deadline := time.Now().Add(2 * time.Second)
	for {
		open, _ := states.count()
		if open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connection(s) still open on the server after the wait returned", open)
		}
		time.Sleep(time.Millisecond)
	}
}

// The caller's first request after a readiness wait must not leave the
// server a connection that never sent a request. With the probe on the
// process-wide transport, Go 1.27's transport returned the probe's
// connection to the pool after Body.Close had returned, and a request in
// that window dialed a second connection, was handed the probe's, and left
// the second unused: net/http's Shutdown then waits five seconds on it.
// The race is not certain, so this repeats the sequence.
func TestWaitForHTTPThenRequestLeavesNoUnusedConnection(t *testing.T) {
	srv, states := trackedServer(t)
	dt, _ := http.DefaultTransport.(*http.Transport)
	for i := range 20 {
		if dt != nil {
			// An idle connection of the caller's own would mask the race.
			dt.CloseIdleConnections()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := WaitForHTTP(ctx, srv.URL); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		// The caller's request, on the process-wide transport as a suite's
		// client usually is.
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		// Past the window in which a late-pooled probe connection would
		// have been handed to the request.
		time.Sleep(20 * time.Millisecond)
		if open, unused := states.count(); unused != 0 {
			t.Fatalf("round %d: the server has %d open connection(s), %d never used", i, open, unused)
		}
	}
}

// A server that answers a probe with 101 Switching Protocols hands the wait
// the connection itself as the response body, with nothing left watching
// the deadline. The wait must close it rather than read it: a read blocks
// for as long as the server stays silent.
func TestWaitForHTTPSilentUpgradeReturns(t *testing.T) {
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: torx\r\n\r\n")
		// And then nothing: the connection stays open and silent.
	}))
	t.Cleanup(func() {
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		srv.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitForHTTP(ctx, srv.URL, 50*time.Millisecond) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForHTTP on a 101 response: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the wait is still blocked on a silent upgraded connection")
	}
}

// roundTripperFunc is an http.RoundTripper of a type other than
// *http.Transport, as a suite's instrumentation wrapper would be.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A suite that replaced http.DefaultTransport with a RoundTripper of another
// type -- here one wrapping a transport that trusts its TLS test server's
// certificate -- keeps that configuration for probes: the wait runs them on
// it as it is, not on a transport with none of its settings.
func TestWaitForHTTPKeepsWrappedDefaultTransport(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	inner := srv.Client().Transport
	var through atomic.Int32
	defer func(rt http.RoundTripper) { http.DefaultTransport = rt }(http.DefaultTransport)
	http.DefaultTransport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		through.Add(1)
		return inner.RoundTrip(r)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForHTTP(ctx, srv.URL); err != nil {
		t.Fatalf("WaitForHTTP through a wrapped default transport: %v", err)
	}
	if through.Load() == 0 {
		t.Error("no probe travelled through the wrapped default transport")
	}
}

// stallingServer serves a handler that holds each of the first stalls
// requests open until the client gives up, then answers 200. It returns the
// server and a count of requests seen.
func stallingServer(t *testing.T, stalls int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= stalls {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// A probe that stalls costs one attempt, not the whole wait: a server that
// holds its first request open and answers the next one is ready.
func TestWaitForHTTPStalledProbeCostsOneAttempt(t *testing.T) {
	srv, hits := stallingServer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := waitForHTTP(ctx, srv.URL, 20*time.Millisecond); err != nil {
		t.Fatalf("WaitForHTTP after one stalled probe: %v", err)
	}
	if n := hits.Load(); n < 2 {
		t.Errorf("server saw %d probes, want the wait to move past the stalled one", n)
	}
}

// A server that never answers still times out at the wait's deadline, and the
// wait keeps probing until then rather than hanging on the first request.
func TestWaitForHTTPAlwaysStalledTimesOut(t *testing.T) {
	srv, hits := stallingServer(t, 1<<30)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := waitForHTTP(ctx, srv.URL, 20*time.Millisecond)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("err = %v, want ErrReadinessTimeout", err)
	}
	if n := hits.Load(); n < 2 {
		t.Errorf("server saw %d probes, want repeated attempts within the deadline", n)
	}
}

func TestWaitForLog(t *testing.T) {
	calls := 0
	read := func(context.Context) ([]byte, error) {
		calls++
		switch calls {
		case 1:
			return nil, errors.New("log not created yet") // tolerated: keep polling
		case 2:
			return []byte("starting up\n"), nil // substring not present yet
		default:
			return []byte("starting up\nbinding to port 9092\n"), nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := WaitForLog(ctx, read, "binding to port"); err != nil {
		t.Fatalf("WaitForLog: %v", err)
	}
	if calls < 3 {
		t.Errorf("read called %d times, want it to poll past the error and the non-match", calls)
	}
}
