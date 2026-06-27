package torx

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAssignmentRoundTrip(t *testing.T) {
	a := Assignment{
		JobID:  "pkg.Job",
		Params: Params{"n": 3, "flag": true},
		Nodes: []NodeDescriptor{
			{Name: "node-0", Role: "broker", Scratch: "/tmp/torx/node-0", Backend: BackendDescriptor{Kind: "local"}},
		},
		Session: SessionConfig{ResultsDir: "/results/job", TimeoutMS: 30000},
	}

	var buf bytes.Buffer
	if err := EncodeAssignment(&buf, a); err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := DecodeAssignment(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", got.SchemaVersion, SchemaVersion)
	}
	if got.JobID != "pkg.Job" {
		t.Errorf("JobID = %q", got.JobID)
	}
	if got.Params.Int("n", 0) != 3 {
		t.Errorf("Params n = %d, want 3", got.Params.Int("n", 0))
	}
	if len(got.Nodes) != 1 || got.Nodes[0].Name != "node-0" || got.Nodes[0].Backend.Kind != "local" {
		t.Errorf("Nodes = %+v", got.Nodes)
	}
	if got.Session.ResultsDir != "/results/job" || got.Session.TimeoutMS != 30000 {
		t.Errorf("Session = %+v", got.Session)
	}
}

func TestDecodeAssignmentVersionMismatch(t *testing.T) {
	r := strings.NewReader(`{"schema_version":999,"job_id":"x","nodes":[],"session":{"results_dir":"/r"}}`)
	if _, err := DecodeAssignment(r); err == nil {
		t.Errorf("expected an error for an unsupported schema version")
	}
}

func TestMessageStreamRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewMessageWriter(&buf)
	start := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)

	for _, m := range []Message{
		EventMessage(Event{Kind: EventRunning, Time: start, Source: "pkg.Job"}),
		EventMessage(Event{Kind: EventLog, Time: start, Level: "info", Message: "hello"}),
		ResultMessage(JobResult{ID: "pkg.Job", Status: StatusPass, Start: start, Stop: start.Add(time.Second)}),
	} {
		if err := w.Write(m); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// One JSON object per line.
	if n := strings.Count(buf.String(), "\n"); n != 3 {
		t.Errorf("got %d newlines, want 3 (NDJSON)", n)
	}

	r := NewMessageReader(&buf)
	m1, err := r.Read()
	if err != nil || m1.Event == nil || m1.Event.Kind != EventRunning {
		t.Errorf("m1 = %+v, err %v", m1, err)
	}
	m2, err := r.Read()
	if err != nil || m2.Event == nil || m2.Event.Message != "hello" {
		t.Errorf("m2 = %+v, err %v", m2, err)
	}
	m3, err := r.Read()
	if err != nil || m3.Result == nil || m3.Result.Status != StatusPass {
		t.Errorf("m3 = %+v, err %v", m3, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("end-of-stream err = %v, want io.EOF", err)
	}
}

func TestMessageReaderToleratesUnknownFields(t *testing.T) {
	// A newer peer adds fields this version does not know; decoding ignores them.
	stream := `{"event":{"kind":"log","message":"hi","future_field":123},"another_future":true}` + "\n"
	r := NewMessageReader(strings.NewReader(stream))

	m, err := r.Read()
	if err != nil {
		t.Fatalf("decode with unknown fields: %v", err)
	}
	if m.Event == nil || m.Event.Message != "hi" {
		t.Errorf("m = %+v, want event with message hi", m)
	}
}
