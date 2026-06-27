// Persisting a run to a results tree on disk.
//
// A run gets a timestamped directory under the results root, with a "latest"
// symlink pointing at it and an aggregate run.json. Each job gets a subdirectory
// holding its execution trace -- events.ndjson (machine-readable, one event per
// line) and test_log (human-readable) -- its result.json, and, under
// <service>/<node>/, the artifacts collected from its services. The driver builds
// the run directory; the worker fills in each job's subdirectory.
package torx

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// teeSink fans each event out to several sinks.
type teeSink struct{ sinks []EventSink }

func (t teeSink) Emit(e Event) {
	for _, s := range t.sinks {
		s.Emit(e)
	}
}

// traceSink persists a job's event timeline to its results directory, as both a
// machine-readable events.ndjson (one event per line) and a human-readable
// test_log. It is safe for concurrent use, since a job may emit from several
// goroutines.
type traceSink struct {
	mu     sync.Mutex
	events *os.File
	human  *os.File
	enc    *json.Encoder
}

// newTraceSink opens the trace files in dir.
func newTraceSink(dir string) (*traceSink, error) {
	events, err := os.Create(filepath.Join(dir, "events.ndjson"))
	if err != nil {
		return nil, err
	}
	human, err := os.Create(filepath.Join(dir, "test_log"))
	if err != nil {
		_ = events.Close()
		return nil, err
	}
	return &traceSink{events: events, human: human, enc: json.NewEncoder(events)}, nil
}

// Emit appends the event to both trace files.
func (s *traceSink) Emit(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(e)
	fmt.Fprintln(s.human, renderEvent(e))
}

// Close closes the trace files.
func (s *traceSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.events.Close()
	if cerr := s.human.Close(); err == nil {
		err = cerr
	}
	return err
}

// renderEvent formats an event as one human-readable test_log line.
func renderEvent(e Event) string {
	ts := e.Time.Format("15:04:05.000")
	switch e.Kind {
	case EventLog:
		level := e.Level
		if level == "" {
			level = "info"
		}
		return fmt.Sprintf("%s %-8s %s", ts, strings.ToUpper(level), e.Message)
	case EventRunning:
		return fmt.Sprintf("%s RUNNING  %s", ts, e.Source)
	case EventFinished:
		return fmt.Sprintf("%s FINISHED %s", ts, e.Message)
	default:
		return fmt.Sprintf("%s %-8s %s", ts, string(e.Kind), e.Message)
	}
}

// sanitizeID makes a variant id safe as a single path component.
func sanitizeID(id string) string {
	return strings.ReplaceAll(id, "/", "_")
}

// jobResultsDir creates and returns the per-job directory under runDir, or "" if
// runDir is empty or the directory cannot be created.
func jobResultsDir(runDir, id string) string {
	if runDir == "" {
		return ""
	}
	dir := filepath.Join(runDir, sanitizeID(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ""
	}
	return dir
}

// writeResultJSON writes res as result.json in dir, best-effort; dir == "" skips.
func writeResultJSON(dir string, res JobResult) {
	if dir == "" {
		return
	}
	if b, err := json.MarshalIndent(res, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "result.json"), b, 0o644)
	}
}

// makeRunDir creates a run directory named stamp under root and repoints a
// "latest" symlink at it, returning the run directory.
func makeRunDir(root, stamp string) (string, error) {
	dir := filepath.Join(root, stamp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	latest := filepath.Join(root, "latest")
	_ = os.Remove(latest)
	_ = os.Symlink(stamp, latest) // relative target, best-effort
	return dir, nil
}

// writeRunJSON writes the aggregate run.json in dir, best-effort.
func writeRunJSON(dir string, suite SuiteResult) {
	if b, err := suite.JSON(); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "run.json"), b, 0o644)
	}
}
