package torx

import (
	"context"
	"strings"
	"testing"
)

func TestLogfCapturesCallSite(t *testing.T) {
	var sink InMemoryEventSink
	ctx := WithSink(context.Background(), &sink)

	Logf(ctx, "info", "hello") // the site recorded should be this line

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	site := events[0].Site
	if site == nil {
		t.Fatal("no call site captured")
	}
	if site.File != "log_test.go" {
		t.Errorf("file = %q, want log_test.go", site.File)
	}
	if site.Line == 0 {
		t.Errorf("line not captured")
	}
	if !strings.Contains(site.Func, "TestLogfCapturesCallSite") {
		t.Errorf("func = %q, want it to name the caller", site.Func)
	}
}

func TestEmitPreservesExplicitSite(t *testing.T) {
	var sink InMemoryEventSink
	ctx := WithSink(context.Background(), &sink)

	Emit(ctx, Event{Kind: EventLog, Message: "x", Site: &Site{File: "given.go", Line: 7}})

	events := sink.Events()
	if len(events) != 1 || events[0].Site == nil || events[0].Site.File != "given.go" {
		t.Errorf("Emit overwrote an explicit site: %+v", events)
	}
}

func TestComponentTagsEvents(t *testing.T) {
	var sink InMemoryEventSink
	ctx := WithComponent(WithSink(context.Background(), &sink), "redis")

	Logf(ctx, "info", "starting")

	if got := sink.Events()[0].Component; got != "redis" {
		t.Errorf("component = %q, want redis", got)
	}
}

func TestLogfNoSinkIsNoop(t *testing.T) {
	// No sink on the context: must not panic.
	Logf(context.Background(), "info", "ignored")
	Emit(context.Background(), Event{Kind: EventLog, Message: "ignored"})
}
