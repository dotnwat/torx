package torx

import (
	"context"
	"os"
	"testing"
)

// TestMain lets the test binary stand in for a worker: when re-executed with
// TORX_TEST_WORKER set, it runs worker mode and exits, so SelfExecLauncher can
// spawn a real worker subprocess from the registered test jobs.
func TestMain(m *testing.M) {
	if os.Getenv("TORX_TEST_WORKER") == "1" {
		os.Exit(workerMain())
	}
	os.Exit(m.Run())
}

func TestSelfExecLauncher(t *testing.T) {
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	var sink InMemoryEventSink

	res, err := launcher.Launch(context.Background(), Assignment{JobID: "wtest.pass"}, &sink)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status != StatusPass {
		t.Errorf("status = %v, want PASS", res.Status)
	}
	if res.Summary != "ok" {
		t.Errorf("summary = %q, want ok", res.Summary)
	}

	ran := false
	for _, e := range sink.Events() {
		if e.Kind == EventLog && e.Message == "ran" {
			ran = true
		}
	}
	if !ran {
		t.Errorf("'ran' event was not forwarded from the worker; got %+v", sink.Events())
	}
}

func TestSelfExecLauncherJobFailure(t *testing.T) {
	launcher := SelfExecLauncher{Env: []string{"TORX_TEST_WORKER=1"}}
	res, err := launcher.Launch(context.Background(), Assignment{JobID: "wtest.fail"}, discardSink{})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if res.Status != StatusFail {
		t.Errorf("status = %v, want FAIL", res.Status)
	}
}
