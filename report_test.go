package torx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConsoleReporter(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}

	r.Report(JobResult{ID: "a", Status: StatusPass, Start: t0, Stop: t0.Add(time.Second)})
	r.Report(JobResult{ID: "b", Status: StatusFail, Start: t0, Stop: t0, Error: &ErrorInfo{Message: "boom"}})
	r.Finish(SuiteResult{Jobs: []JobResult{{Status: StatusPass}, {Status: StatusFail}}})

	out := buf.String()
	for _, want := range []string{"PASS", "a", "FAIL", "b", "boom", "2 jobs: 1 passed, 1 failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("console output missing %q:\n%s", want, out)
		}
	}
}

func TestJSONReporterRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	r := NewJSONReporter(&buf)
	in := []JobResult{{ID: "a", Status: StatusPass}, {ID: "b", Status: StatusFail}}
	for _, res := range in {
		r.Report(res)
	}
	r.Finish(SuiteResult{Jobs: in})

	// One JSON object per line.
	if n := strings.Count(buf.String(), "\n"); n != 2 {
		t.Errorf("got %d lines, want 2: %q", n, buf.String())
	}

	got, err := ReadResults(&buf)
	if err != nil {
		t.Fatalf("ReadResults: %v", err)
	}
	if len(got.Jobs) != 2 || got.Jobs[0].ID != "a" || got.Jobs[1].Status != StatusFail {
		t.Errorf("round-trip = %+v, want [a PASS, b FAIL]", got.Jobs)
	}
}

func TestReadResultsToleratesTruncatedTail(t *testing.T) {
	// A run killed mid-write leaves complete lines followed by a partial one.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(JobResult{ID: "a", Status: StatusPass}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(JobResult{ID: "b", Status: StatusPass}); err != nil {
		t.Fatal(err)
	}
	buf.WriteString(`{"id":"c","stat`) // truncated, as if SIGKILLed here

	got, err := ReadResults(&buf)
	if err != nil {
		t.Fatalf("ReadResults on a truncated tail: %v", err)
	}
	if len(got.Jobs) != 2 {
		t.Errorf("recovered %d results, want 2 complete ones: %+v", len(got.Jobs), got.Jobs)
	}
}

func TestReadResultsRejectsCorruptRecord(t *testing.T) {
	// Corruption in the middle of the stream (not a truncated tail) is an error.
	r := strings.NewReader(`{"id":"a","status":"PASS"}` + "\n" + "garbage\n")
	if _, err := ReadResults(r); err == nil {
		t.Errorf("expected an error for a corrupt record")
	}
}

func TestRunReportsEachResult(t *testing.T) {
	rep := &recordingReporter{}
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{}, sizedRequests(3, 1, nil),
		RunOptions{MaxParallel: 1, Reporters: []Reporter{rep}})

	if len(rep.reported) != 3 {
		t.Errorf("reported %d results, want 3", len(rep.reported))
	}
	if rep.finished == nil {
		t.Fatal("Finish was not called")
	}
	if len(rep.finished.Jobs) != len(res.Jobs) {
		t.Errorf("Finish saw %d jobs, run returned %d", len(rep.finished.Jobs), len(res.Jobs))
	}
}

func TestRunReportsUnschedulable(t *testing.T) {
	// A job that fails before scheduling (needs 5 nodes, pool has 1) is still
	// reported, not just returned in the aggregate.
	rep := &recordingReporter{}
	Run(context.Background(), testPool(t, 1), InProcessLauncher{}, sizedRequests(1, 5, nil),
		RunOptions{Reporters: []Reporter{rep}})

	if len(rep.reported) != 1 || rep.reported[0].Status != StatusFail {
		t.Errorf("reported = %+v, want one FAIL", rep.reported)
	}
}

func TestRunFinishSeesPersistErr(t *testing.T) {
	// A run-directory failure must reach the reporters' Finish summary -- the
	// console's closing line is where an operator learns why the exit status is
	// non-zero -- not only the suite returned to the caller.
	rep := &recordingReporter{}
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{}, sizedRequests(1, 1, nil),
		RunOptions{RunDir: filepath.Join(t.TempDir(), "absent"), Reporters: []Reporter{rep}})
	if res.PersistErr == "" {
		t.Fatalf("PersistErr empty though the run directory does not exist")
	}
	if rep.finished == nil || rep.finished.PersistErr == "" {
		t.Fatalf("Finish saw no PersistErr though the run directory was unusable: %+v", rep.finished)
	}
	var buf bytes.Buffer
	if err := (ConsoleReporter{W: &buf}).Finish(*rep.finished); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "results not persisted") {
		t.Errorf("console summary %q does not mention the persistence failure", buf.String())
	}
}

// recordingReporter captures what the driver reports for assertions.
type recordingReporter struct {
	reported []JobResult
	finished *SuiteResult
}

func (r *recordingReporter) Report(res JobResult) error {
	r.reported = append(r.reported, res)
	return nil
}

func (r *recordingReporter) Finish(s SuiteResult) error {
	r.finished = &s
	return nil
}

// failWriter fails every write, standing in for a full disk or a broken pipe.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestRunSurfacesReporterWriteFailure(t *testing.T) {
	// The job itself passes; only the reporter's write fails. The run must report
	// the persistence failure rather than exiting as a clean success.
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{}, sizedRequests(1, 1, nil),
		RunOptions{Reporters: []Reporter{ConsoleReporter{W: failWriter{}}}})
	if res.PersistErr == "" {
		t.Errorf("PersistErr empty though a reporter write failed")
	}
	if res.Ok() {
		t.Errorf("run reported Ok despite a reporter that could not persist results")
	}
}
