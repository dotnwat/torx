// Emitting trace events from anywhere in a job's execution.
//
// A job's event sink is carried on its context so framework and service code can
// narrate what they do -- binding nodes, starting a service, waiting for
// readiness, collecting artifacts -- without threading the sink through every
// signature. The worker puts the sink on the context before running the job;
// lifecycle code, service hooks, and tests then call Logf or Emit. Each event
// records the source location it was emitted from, captured at runtime. With no
// sink on the context (for example while the driver sizes a job) these are
// no-ops.
package torx

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type sinkContextKey struct{}

// WithSink returns a context carrying sink, so code reachable from it can emit
// events into the job's trace via Emit and Logf.
func WithSink(ctx context.Context, sink EventSink) context.Context {
	return context.WithValue(ctx, sinkContextKey{}, sink)
}

func sinkFrom(ctx context.Context) EventSink {
	s, _ := ctx.Value(sinkContextKey{}).(EventSink)
	return s
}

// Emit sends e to the event sink carried by ctx, filling in the call site and
// time when unset. It does nothing if ctx carries no sink.
func Emit(ctx context.Context, e Event) {
	if e.Site == nil {
		e.Site = callerSite(2)
	}
	emit(ctx, e)
}

// Logf emits a formatted log event to ctx's sink, recording the caller's source
// location. Service hooks and lifecycle code use it to narrate the job's trace.
func Logf(ctx context.Context, level, format string, args ...any) {
	emit(ctx, Event{
		Kind:    EventLog,
		Level:   level,
		Message: fmt.Sprintf(format, args...),
		Site:    callerSite(2),
	})
}

func emit(ctx context.Context, e Event) {
	sink := sinkFrom(ctx)
	if sink == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	sink.Emit(e)
}

// callerSite captures the source location skip frames above callerSite itself
// (skip 2 reaches the caller of the public Emit/Logf/Log that built the event).
// It accounts for inlining, so the frame is the logical caller.
func callerSite(skip int) *Site {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return nil
	}
	site := &Site{File: filepath.Base(file), Line: line}
	if fn := runtime.FuncForPC(pc); fn != nil {
		site.Func = shortFunc(fn.Name())
	}
	return site
}

// shortFunc drops the package path from a fully qualified function name, leaving
// e.g. "torx.(*ServiceBase).Start".
func shortFunc(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}
