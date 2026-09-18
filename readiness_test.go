package torx

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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
