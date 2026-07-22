// Results and the event stream.
//
// A job produces a JobResult -- its status, timing, an optional opaque data
// payload, and a structured error -- and a suite aggregates them into a
// SuiteResult. Both render to a concise human report and marshal to JSON; v1 is
// human-first, with a richer machine/LLM-oriented schema left for later. While a
// job runs it emits Events (lifecycle transitions and log lines) to an
// EventSink, which the driver merges into a single timeline; InMemoryEventSink
// captures them for tests.
package torx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Status is the outcome of a job.
type Status string

const (
	StatusPass   Status = "PASS"
	StatusFail   Status = "FAIL"
	StatusIgnore Status = "IGNORE"
	StatusFlaky  Status = "FLAKY"
)

// EventKind classifies an Event.
type EventKind string

const (
	EventRunning  EventKind = "running"
	EventLog      EventKind = "log"
	EventFinished EventKind = "finished"
)

// Event is one record in a job's timeline. Kind selects which fields are
// meaningful: Log carries Message and Level, and the lifecycle kinds (Running,
// Finished) carry only Kind, Time, and Source. Component, when set, names the
// part of the system that emitted the event (e.g. a service name), so the message
// need not repeat it. Site, when set, is the source location it was emitted from.
type Event struct {
	Kind      EventKind `json:"kind"`
	Time      time.Time `json:"time"`
	Source    string    `json:"source,omitempty"`
	Component string    `json:"component,omitempty"`
	Message   string    `json:"message,omitempty"`
	Level     string    `json:"level,omitempty"`
	Site      *Site     `json:"site,omitempty"`
}

// Site is the source location an event was emitted from, captured at runtime by
// Emit, Logf, and JobContext.Log. File is the base file name; Func is the
// shortened function name. Being captured at execution, it never goes stale.
type Site struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Func string `json:"func,omitempty"`
}

// EventSink consumes events emitted by a running job. Emit is best-effort: an
// implementation that writes to a file or forwards to the driver handles its
// own transport failures rather than burdening every emit site. Implementations
// must be safe for concurrent use, since a job may emit from several goroutines.
type EventSink interface {
	Emit(Event)
}

// InMemoryEventSink records emitted events for inspection in tests.
type InMemoryEventSink struct {
	mu     sync.Mutex
	events []Event
}

// Emit records e.
func (s *InMemoryEventSink) Emit(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

// Events returns a copy of the recorded events in emission order.
func (s *InMemoryEventSink) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// ErrorInfo is a structured view of a job failure. Kind is the torx category
// sentinel when the error carries one (see Error), so a report can classify a
// failure as well as print it. Stack is populated when a stack is available
// (e.g. a recovered panic) and is otherwise empty.
type ErrorInfo struct {
	Message string `json:"message"`
	Kind    string `json:"kind,omitempty"`
	Stack   string `json:"stack,omitempty"`
}

// ErrorInfoFrom builds an ErrorInfo from err, or returns nil if err is nil.
func ErrorInfoFrom(err error) *ErrorInfo {
	if err == nil {
		return nil
	}
	info := &ErrorInfo{Message: err.Error()}
	var te *Error
	if errors.As(err, &te) {
		info.Kind = te.Kind.Error()
	}
	return info
}

// JobResult is the outcome of one job.
//
// torx deliberately does not model or aggregate benchmark metrics. Real
// benchmarks differ too much -- latency distributions, histograms,
// per-operation breakdowns, time series -- and aggregation is benchmark
// specific (percentiles cannot be averaged across nodes, for one). A benchmark
// therefore records whatever it measured as its own JSON in Data, which torx
// carries verbatim into the result artifact without interpreting it; making
// sense of Data is the benchmark's or a downstream tool's job. Summary is an
// author-written one-line headline for the human report. Large or custom
// outputs (raw histograms, CSVs) belong in collected artifacts, not Data.
type JobResult struct {
	ID      string          `json:"id"`
	Status  Status          `json:"status"`
	Start   time.Time       `json:"start"`
	Stop    time.Time       `json:"stop"`
	Summary string          `json:"summary,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   *ErrorInfo      `json:"error,omitempty"`
}

// Duration is the wall-clock time the job took.
func (r JobResult) Duration() time.Duration {
	return r.Stop.Sub(r.Start)
}

// Render returns a concise human-readable summary of the job.
func (r JobResult) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-6s %s  (%s)", r.Status, r.ID, r.Duration().Round(time.Millisecond))
	if r.Summary != "" {
		fmt.Fprintf(&b, "\n        %s", r.Summary)
	}
	if r.Error != nil {
		fmt.Fprintf(&b, "\n        %s", r.Error.Message)
	}
	return b.String()
}

// SuiteResult aggregates the results of a run.
type SuiteResult struct {
	Jobs []JobResult `json:"jobs"`
	// Cancelled is set when the run's context was cancelled (a deadline or an
	// operator stop) before the driver finished scheduling every request. The
	// recorded jobs are then only a prefix of what was asked for, so the run did
	// not complete regardless of how those jobs fared.
	Cancelled bool `json:"cancelled,omitempty"`
}

// Counts returns the number of jobs in each status.
func (s SuiteResult) Counts() map[Status]int {
	counts := make(map[Status]int)
	for _, j := range s.Jobs {
		counts[j.Status]++
	}
	return counts
}

// Ok reports whether the run completed with no failures. A cancelled run is
// never Ok even if every job it managed to run passed, since the rest never ran.
// Ignored and flaky jobs do not count as failures.
func (s SuiteResult) Ok() bool {
	if s.Cancelled {
		return false
	}
	for _, j := range s.Jobs {
		if j.Status == StatusFail {
			return false
		}
	}
	return true
}

// Render returns a human-readable report: one line per job, then a summary.
func (s SuiteResult) Render() string {
	var b strings.Builder
	for _, j := range s.Jobs {
		b.WriteString(j.Render())
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(s.summaryLine())
	return b.String()
}

// summaryLine is the one-line tally of job outcomes shared by Render and
// ConsoleReporter.
func (s SuiteResult) summaryLine() string {
	c := s.Counts()
	line := fmt.Sprintf("%d jobs: %d passed, %d failed, %d flaky, %d ignored",
		len(s.Jobs), c[StatusPass], c[StatusFail], c[StatusFlaky], c[StatusIgnore])
	if s.Cancelled {
		line += " (run cancelled before completion)"
	}
	return line
}

// JSON marshals the suite result as indented JSON, the machine-readable
// artifact written alongside the human report.
func (s SuiteResult) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
