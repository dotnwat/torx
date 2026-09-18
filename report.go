// Reporters: where a run's results go as they land.
//
// A Reporter is the results plane, distinct from the EventSink timeline of
// §3.13: the sink carries a job's live events, while a reporter consumes the
// final JobResult of each job. The driver calls Report once per job -- including
// jobs that failed before they could be scheduled -- and Finish once with the
// aggregate. ConsoleReporter renders the human report incrementally;
// JSONReporter writes a newline-delimited JSON stream that stays parseable even
// if the run is killed mid-write, and ReadResults reconstructs a SuiteResult
// from it.

package torx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// Reporter consumes job results as a run progresses. Report is called once per
// job as its result lands; Finish is called once after the last result, with the
// aggregate. Each returns an error if it could not deliver the result (e.g. a
// failed write to a results file), which the driver records as a run-level
// persistence failure rather than discarding. The driver calls a reporter from a
// single goroutine, so an implementation need not be safe for concurrent use.
type Reporter interface {
	Report(JobResult) error
	Finish(SuiteResult) error
}

// ConsoleReporter writes one human-readable line per job as it completes,
// followed by a summary, to W. It produces the same report as
// SuiteResult.Render, streamed rather than printed at the end.
type ConsoleReporter struct {
	W io.Writer
}

var _ Reporter = ConsoleReporter{}

// Report prints the job's rendered result.
func (r ConsoleReporter) Report(res JobResult) error {
	_, err := fmt.Fprintln(r.W, res.Render())
	return err
}

// Finish prints the run summary.
func (r ConsoleReporter) Finish(s SuiteResult) error {
	_, err := fmt.Fprintf(r.W, "\n%s\n", s.summaryLine())
	return err
}

// JSONReporter writes each job result as a line of JSON to W as it lands. The
// newline-delimited framing means a run killed mid-flight still leaves a file of
// complete, parseable results; when W is a file, each line is flushed to disk
// before Report returns. The lines are the artifact -- the aggregate is not
// re-summarized -- and ReadResults reconstructs a SuiteResult from them.
type JSONReporter struct {
	w   io.Writer
	enc *json.Encoder
}

var _ Reporter = (*JSONReporter)(nil)

// NewJSONReporter returns a reporter that writes newline-delimited JSON to w.
func NewJSONReporter(w io.Writer) *JSONReporter {
	return &JSONReporter{w: w, enc: json.NewEncoder(w)}
}

// Report writes res as one line of JSON, flushing to disk when backed by a file.
func (r *JSONReporter) Report(res JobResult) error {
	if err := r.enc.Encode(res); err != nil { // Encode appends a newline (NDJSON)
		return err
	}
	if f, ok := r.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

// Finish does nothing: the per-job lines are the complete artifact.
func (r *JSONReporter) Finish(SuiteResult) error { return nil }

// ReadResults reconstructs a SuiteResult from a newline-delimited JSON result
// stream as written by JSONReporter. A truncated final record -- the mark of a
// run killed mid-write -- is treated as end of input, so a partial file still
// yields every complete result.
func ReadResults(r io.Reader) (SuiteResult, error) {
	dec := json.NewDecoder(r)
	var jobs []JobResult
	for {
		var res JobResult
		err := dec.Decode(&res)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return SuiteResult{}, fmt.Errorf("read results: %w", err)
		}
		jobs = append(jobs, res)
	}
	return SuiteResult{Jobs: jobs}, nil
}
