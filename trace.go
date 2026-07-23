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

// renderEvent formats an event as one human-readable test_log line:
// "<time> [<component>] <file:line> <tag> <text>", where tag is the level (log
// events) or the kind (lifecycle events).
func renderEvent(e Event) string {
	ts := e.Time.Format("15:04:05.000")
	comp := ""
	if e.Component != "" {
		comp = "[" + e.Component + "]"
	}
	site := ""
	if e.Site != nil {
		site = fmt.Sprintf("%s:%d", e.Site.File, e.Site.Line)
	}
	tag, text := eventTagText(e)
	return fmt.Sprintf("%s %-9s %-20s %-8s %s", ts, comp, site, tag, text)
}

func eventTagText(e Event) (tag, text string) {
	switch e.Kind {
	case EventLog:
		tag = strings.ToUpper(e.Level)
		if tag == "" {
			tag = "INFO"
		}
		return tag, e.Message
	case EventRunning:
		return "RUNNING", e.Source
	case EventFinished:
		return "FINISHED", e.Message
	default:
		return string(e.Kind), e.Message
	}
}

// sanitizeID encodes a variant id as a single safe path component. It
// percent-encodes the path separator, the percent sign itself, and any control
// byte; the encoding is reversible and therefore injective, so two distinct
// variant ids never collapse to the same directory (a plain '/' -> '_'
// substitution would). Everything else a variant id may contain -- the [ ] = ,
// and quotes from JSON-encoded values -- is legal in a POSIX filename and is kept
// for readability.
func sanitizeID(id string) string {
	var b strings.Builder
	for i := range len(id) {
		c := id[i]
		if c == '/' || c == '%' || c < 0x20 {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
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

// makeRunDir creates a unique run directory under root, names it after stamp,
// repoints a "latest" symlink at it, and returns it. A one-second timestamp is
// not unique on its own: two runs started in the same second under the same root
// would otherwise share a directory and truncate each other's traces, results,
// and run.json. MkdirTemp appends a random suffix and creates the directory
// exclusively, so each run gets its own.
func makeRunDir(root, stamp string) (string, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(root, stamp+"-")
	if err != nil {
		return "", err
	}
	repointLatest(root, filepath.Base(dir))
	return dir, nil
}

// repointLatest moves root/latest to point at name via a temporary symlink and a
// rename, so a concurrent run never observes a missing or half-written link (the
// rename is atomic). The temporary name embeds the unique run name so parallel
// runs do not collide on it. Updating the convenience link is best-effort and
// never fails the run.
func repointLatest(root, name string) {
	tmp := filepath.Join(root, ".latest."+name)
	latest := filepath.Join(root, "latest")
	_ = os.Remove(tmp)
	if err := os.Symlink(name, tmp); err != nil {
		return
	}
	if err := os.Rename(tmp, latest); err != nil {
		_ = os.Remove(tmp)
	}
}

// writeRunJSON writes the aggregate run.json in dir, returning any error so the
// caller can surface a failure to persist the run summary.
func writeRunJSON(dir string, suite SuiteResult) error {
	b, err := suite.JSON()
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "run.json"), b, 0o644)
}
