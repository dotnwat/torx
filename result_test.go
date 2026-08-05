package torx

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestInMemoryEventSink(t *testing.T) {
	var sink InMemoryEventSink
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	sink.Emit(Event{Kind: EventRunning, Time: now, Source: "job-1"})
	sink.Emit(Event{Kind: EventLog, Time: now, Source: "job-1", Level: "info", Message: "hello"})
	sink.Emit(Event{Kind: EventFinished, Time: now, Source: "job-1"})

	got := sink.Events()
	if len(got) != 3 {
		t.Fatalf("len(Events) = %d, want 3", len(got))
	}
	if got[0].Kind != EventRunning || got[1].Message != "hello" || got[2].Kind != EventFinished {
		t.Errorf("events captured incorrectly: %+v", got)
	}

	// Events returns a copy: mutating it must not affect the sink.
	got[0].Source = "mutated"
	if sink.Events()[0].Source != "job-1" {
		t.Errorf("Events() did not return a copy")
	}
}

func TestErrorInfoFrom(t *testing.T) {
	if ErrorInfoFrom(nil) != nil {
		t.Errorf("ErrorInfoFrom(nil) != nil")
	}

	boom := errors.New("boom")
	info := ErrorInfoFrom(Wrap(ErrBackend, "backend: exec", boom))
	if info.Kind != ErrBackend.Error() {
		t.Errorf("Kind = %q, want %q", info.Kind, ErrBackend.Error())
	}
	if !strings.Contains(info.Message, "boom") {
		t.Errorf("Message = %q, want it to contain the cause", info.Message)
	}

	// A plain error has a message but no torx category.
	plain := ErrorInfoFrom(boom)
	if plain.Kind != "" {
		t.Errorf("plain Kind = %q, want empty", plain.Kind)
	}
	if plain.Message != "boom" {
		t.Errorf("plain Message = %q, want boom", plain.Message)
	}
}

func TestSuiteResultJSONRoundTrip(t *testing.T) {
	start := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	suite := SuiteResult{Jobs: []JobResult{
		{
			ID:      "pkg.Bench",
			Params:  Params{"clients": 50},
			Status:  StatusPass,
			Start:   start,
			Stop:    start.Add(1200 * time.Millisecond),
			Summary: "12345 ops/s",
			Data:    json.RawMessage(`{"throughput":12345,"unit":"ops/s"}`),
		},
		{
			ID:     "pkg.Fail",
			Status: StatusFail,
			Start:  start,
			Stop:   start.Add(800 * time.Millisecond),
			Error:  &ErrorInfo{Message: "torx: backend operation failed: boom", Kind: ErrBackend.Error()},
		},
	}}

	data, err := suite.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}

	var got SuiteResult
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Jobs) != 2 {
		t.Fatalf("jobs = %d, want 2", len(got.Jobs))
	}
	if got.Jobs[0].Status != StatusPass || got.Jobs[1].Status != StatusFail {
		t.Errorf("statuses round-tripped wrong: %+v", got.Jobs)
	}
	if got.Jobs[0].Summary != "12345 ops/s" {
		t.Errorf("summary lost: %q", got.Jobs[0].Summary)
	}
	if got.Jobs[0].Params.Int("clients", 0) != 50 {
		t.Errorf("params lost: %+v", got.Jobs[0].Params)
	}

	// Data is carried verbatim and stays valid JSON the core never interprets.
	var payload map[string]any
	if err := json.Unmarshal(got.Jobs[0].Data, &payload); err != nil {
		t.Fatalf("Data not valid JSON after round-trip: %v", err)
	}
	if payload["throughput"] != float64(12345) {
		t.Errorf("Data round-tripped wrong: %v", payload)
	}

	if got.Jobs[1].Error == nil || got.Jobs[1].Error.Kind != ErrBackend.Error() {
		t.Errorf("error info lost: %+v", got.Jobs[1].Error)
	}
}

func TestSuiteResultCountsOkRender(t *testing.T) {
	suite := SuiteResult{Jobs: []JobResult{
		{ID: "a", Status: StatusPass},
		{ID: "b", Status: StatusFail, Error: &ErrorInfo{Message: "nope"}},
		{ID: "c", Status: StatusFlaky},
		{ID: "d", Status: StatusIgnore},
	}}

	c := suite.Counts()
	if c[StatusPass] != 1 || c[StatusFail] != 1 || c[StatusFlaky] != 1 || c[StatusIgnore] != 1 {
		t.Errorf("counts = %v", c)
	}
	if suite.Ok() {
		t.Errorf("Ok() = true, want false (has a failure)")
	}

	r := suite.Render()
	for _, want := range []string{"PASS", "FAIL", "nope", "4 jobs", "1 passed", "1 failed"} {
		if !strings.Contains(r, want) {
			t.Errorf("Render() missing %q\n%s", want, r)
		}
	}
}

func TestSuiteResultOkEmptyAndAllPass(t *testing.T) {
	if !(SuiteResult{}).Ok() {
		t.Errorf("empty suite Ok() = false, want true")
	}
	pass := SuiteResult{Jobs: []JobResult{{Status: StatusPass}, {Status: StatusIgnore}}}
	if !pass.Ok() {
		t.Errorf("pass+ignore Ok() = false, want true")
	}
}

func TestJobResultRender(t *testing.T) {
	start := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	r := JobResult{
		ID:      "pkg.Bench",
		Status:  StatusPass,
		Start:   start,
		Stop:    start.Add(time.Second),
		Summary: "12345 ops/s p99=5ms",
	}
	if r.Duration() != time.Second {
		t.Errorf("Duration = %v, want 1s", r.Duration())
	}
	out := r.Render()
	for _, want := range []string{"PASS", "pkg.Bench", "1s", "12345 ops/s"} {
		if !strings.Contains(out, want) {
			t.Errorf("Render() missing %q\n%s", want, out)
		}
	}
}
