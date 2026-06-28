// Emitting trace events from anywhere in a job's execution.
//
// A job's event sink is carried on its context so framework and service code can
// narrate what they do -- binding nodes, starting a service, waiting for
// readiness, collecting artifacts -- without threading the sink through every
// signature. The worker puts the sink on the context before running the job;
// lifecycle code, service hooks, and tests then call Logf or Emit. With no sink
// on the context (for example while the driver sizes a job) these are no-ops.
package torx

import (
	"context"
	"fmt"
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

// Emit sends e to the event sink carried by ctx, stamping the current time when
// e has none. It does nothing if ctx carries no sink.
func Emit(ctx context.Context, e Event) {
	sink := sinkFrom(ctx)
	if sink == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	sink.Emit(e)
}

// Logf emits a formatted log event to ctx's sink. Service hooks use it to record
// what they are doing in the job's trace.
func Logf(ctx context.Context, level, format string, args ...any) {
	Emit(ctx, Event{Kind: EventLog, Level: level, Message: fmt.Sprintf(format, args...)})
}
