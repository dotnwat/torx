package torx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// The fuzz targets cover every parser that takes input from outside the
// process: the -params file, the -pool manifest, the driver->worker assignment,
// the worker->driver message stream, and the results file. Each asserts that
// malformed input is rejected with an error rather than a panic, and that
// accepted input round-trips through the matching encoder. Run one for longer
// with, for example:
//
//	go test -run '^$' -fuzz FuzzParseParamsOverrides -fuzztime 60s .

func FuzzParseParamsOverrides(f *testing.F) {
	for _, seed := range []string{
		`{}`,
		`{"j": {"matrix": {"clients": [1, 8, 32], "trial": [1, 2]}}}`,
		`{"j": {"configs": [{"clients": 64, "pipeline": 8}]}}`,
		`{"j": {"matrix": {"a": [true, "x", 1.5]}, "configs": [{}]}}`,
		`{"j": {}}`,
		`{"j": {"matrix": {}}}`,
		`{"j": {"matrix": {"a": []}}}`,
		`{"j": {"configs": []}}`,
		`{"j": {"matrix": {"a": [null]}}}`,
		`{"j": {"configs": [{"a": null}]}}`,
		`{"j": {"unknown": 1}}`,
		`{"j": null}`,
		`[]`,
		`{"j": {"matrix": {"a": [[1, 2]]}}}`,
		`{"j": {"matrix": {"a": [{"nested": {"deep": [1]}}]}}}`,
		// 30 two-value dimensions parse cheaply but expand to 2^30 variants.
		`{"j": {"matrix": {` + thirtyBinaryDims + `}}}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		ov, err := ParseParamsOverrides(data)
		if err != nil {
			return
		}
		// The envelope is documented as strict; check the invariants it promises.
		for id, o := range ov {
			if len(o.Matrix) == 0 && len(o.Configs) == 0 {
				t.Fatalf("%q: accepted an override with neither matrix nor configs", id)
			}
			for dim, values := range o.Matrix {
				if len(values) == 0 {
					t.Fatalf("%q: accepted an empty dimension %q", id, dim)
				}
				for _, v := range values {
					if containsNull(v) {
						t.Fatalf("%q: accepted null in dimension %q", id, dim)
					}
				}
			}
			for i, c := range o.Configs {
				for k, v := range c {
					if containsNull(v) {
						t.Fatalf("%q: accepted null at configs[%d].%s", id, i, k)
					}
				}
			}
			// Expansion is every matrix point plus every config, nothing more or less.
			// The product of a few dozen two-value dimensions is astronomical for a
			// tiny input, and materializing it here would only exhaust the fuzzer's
			// memory, so the expansion itself is checked for small matrices only;
			// parsing and the round trip below still cover every accepted input.
			want := len(o.Configs)
			if len(o.Matrix) > 0 {
				want += product(o.Matrix, maxExpansion)
			}
			if want <= maxExpansion {
				if got := len(o.variants()); got != want {
					t.Fatalf("%q: variants() = %d, want %d", id, got, want)
				}
			}
		}
		// What was accepted re-encodes to something that parses back identically.
		enc, err := json.Marshal(ov)
		if err != nil {
			t.Fatalf("marshal accepted overrides: %v", err)
		}
		again, err := ParseParamsOverrides(enc)
		if err != nil {
			t.Fatalf("re-parse of %s: %v", enc, err)
		}
		if !reflect.DeepEqual(ov, again) {
			t.Fatalf("round trip changed the overrides:\n first: %#v\nsecond: %#v", ov, again)
		}
	})
}

// maxExpansion is the largest cross product a fuzz iteration materializes.
const maxExpansion = 1 << 12

// product is the number of points in a matrix's cross product, saturating at
// cap+1 so that a huge matrix neither overflows nor is expanded.
func product(m map[string][]any, cap int) int {
	n := 1
	for _, values := range m {
		if len(values) == 0 || n > cap/len(values) {
			return cap + 1
		}
		n *= len(values)
	}
	return n
}

// thirtyBinaryDims is a matrix body of 30 dimensions with two values each.
var thirtyBinaryDims = func() string {
	dims := make([]string, 30)
	for i := range dims {
		dims[i] = fmt.Sprintf(`"d%d": [0, 1]`, i)
	}
	return strings.Join(dims, ", ")
}()

// containsNull reports whether a decoded JSON value is, or contains, null.
func containsNull(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case []any:
		return slices.ContainsFunc(x, containsNull)
	case map[string]any:
		for _, e := range x {
			if containsNull(e) {
				return true
			}
		}
	}
	return false
}

func FuzzPoolFromManifest(f *testing.F) {
	for _, seed := range []string{
		`{"nodes": []}`,
		`{"nodes": [{"name": "n0", "scratch": "/tmp/torx"}]}`,
		`{"nodes": [{"name": "n0", "address": "10.0.0.5", "scratch": "/var/tmp/torx",
		   "ports": {"min": 30000, "max": 31000},
		   "resources": {"cpus": 4, "memory_mb": 8192, "labels": ["nvme"]},
		   "backend": {"kind": "local"}}]}`,
		`{"nodes": [{"name": "n0", "scratch": "/s", "backend": {"kind": "ssh", "host": "h", "config": {"user": "u"}}}]}`,
		`{"nodes": [{"name": "n0", "scratch": "/s"}, {"name": "n0", "scratch": "/s"}]}`,
		`{"nodes": [{"name": "../x", "scratch": "/s"}]}`,
		`{"nodes": [{"name": "a\\b", "scratch": "/s"}]}`,
		`{"nodes": [{"name": "n0", "scratch": "/s", "ports": {"min": 10, "max": 5}}]}`,
		`{"nodes": [{"name": "n0", "scratch": "/s", "ports": {"min": 0, "max": 5}}]}`,
		`{"nodes": [{"name": "n0", "scratch": "/s", "resources": {"cpus": -1}}]}`,
		`{"nodes": [{"name": "n0"}]}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return
		}
		pool, err := PoolFromManifest(m)
		if err != nil {
			return
		}
		if pool.Size() != len(m.Nodes) {
			t.Fatalf("pool has %d nodes, manifest %d", pool.Size(), len(m.Nodes))
		}
		seen := map[string]bool{}
		for _, mn := range m.Nodes {
			if seen[mn.Name] {
				t.Fatalf("accepted duplicate node name %q", mn.Name)
			}
			seen[mn.Name] = true
			// The name becomes a directory component, so the loader must have rejected
			// anything with a separator or a traversal meaning. A backslash is an
			// ordinary character in a Unix filename and is allowed.
			if strings.Contains(mn.Name, "/") || mn.Name == "." || mn.Name == ".." || mn.Name == "" {
				t.Fatalf("accepted node name %q, which is not a single path component", mn.Name)
			}
			if mn.Scratch == "" {
				t.Fatalf("accepted node %q without a scratch directory", mn.Name)
			}
			if r := mn.Ports; r != nil && (r.Min <= 0 || r.Max <= r.Min) {
				t.Fatalf("accepted node %q with port range [%d,%d)", mn.Name, r.Min, r.Max)
			}
		}
	})
}

func FuzzDecodeAssignment(f *testing.F) {
	var seed bytes.Buffer
	_ = EncodeAssignment(&seed, Assignment{
		SchemaVersion: SchemaVersion,
		JobID:         "suite.job",
		Params:        Params{"clients": 8, "mode": "read"},
		Nodes: []NodeDescriptor{{
			Name: "n0", Address: "10.0.0.5", Scratch: "/var/tmp/torx",
			Backend: BackendDescriptor{Kind: "ssh", Host: "10.0.0.5", Config: json.RawMessage(`{"user":"torx"}`)},
			Ports:   &PortRange{Min: 30000, Max: 31000},
		}},
		Session: SessionConfig{ResultsDir: "/results/run", TimeoutMS: 60000},
	})
	f.Add(seed.Bytes())
	f.Add([]byte(`{"schema_version": 1, "job_id": "j", "nodes": []}`))
	f.Add([]byte(`{"schema_version": 2, "job_id": "j", "nodes": []}`))
	f.Add([]byte(`{"schema_version": 1, "job_id": "j", "nodes": [{"backend": {"config": "not an object"}}]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) {
		a, err := DecodeAssignment(bytes.NewReader(data))
		if err != nil {
			return
		}
		if a.SchemaVersion != SchemaVersion {
			t.Fatalf("accepted schema version %d", a.SchemaVersion)
		}
		// Encoding what was decoded, decoding that, and encoding again must reach
		// a fixed point: the wire form is canonical after one round trip.
		var first bytes.Buffer
		if err := EncodeAssignment(&first, a); err != nil {
			t.Fatalf("encode: %v", err)
		}
		b, err := DecodeAssignment(bytes.NewReader(first.Bytes()))
		if err != nil {
			t.Fatalf("decode of own encoding %s: %v", first.Bytes(), err)
		}
		var second bytes.Buffer
		if err := EncodeAssignment(&second, b); err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(first.Bytes(), second.Bytes()) {
			t.Fatalf("encoding is not a fixed point:\n first: %s\nsecond: %s", first.Bytes(), second.Bytes())
		}
		if b.JobID != a.JobID || len(b.Nodes) != len(a.Nodes) || !reflect.DeepEqual(b.Session, a.Session) {
			t.Fatalf("round trip changed the assignment:\n first: %+v\nsecond: %+v", a, b)
		}
	})
}

func FuzzMessageReader(f *testing.F) {
	var seed bytes.Buffer
	mw := NewMessageWriter(&seed)
	_ = mw.Write(EventMessage(Event{Kind: EventRunning, Source: "worker"}))
	_ = mw.Write(EventMessage(Event{Kind: EventLog, Level: "info", Message: "hello", Component: "svc",
		Site: &Site{File: "svc.go", Line: 12, Func: "svc.Start"}}))
	_ = mw.Write(ResultMessage(JobResult{ID: "j", Status: StatusPass, Summary: "ok", Params: Params{"a": 1}}))
	f.Add(seed.Bytes())
	f.Add([]byte("{}\n{}\n"))
	f.Add([]byte("{}{}")) // json.Decoder streams concatenated values; newlines are not required
	f.Add([]byte(`{"event": null, "result": null}` + "\n"))
	f.Add([]byte(`{"result": {"status": "PASS", "data": {"any": [1, 2]}}}` + "\n{\"trunc"))
	f.Add([]byte("\n\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		mr := NewMessageReader(bytes.NewReader(data))
		var msgs []Message
		for {
			m, err := mr.Read()
			if err != nil {
				break
			}
			msgs = append(msgs, m)
		}
		// Everything read writes back out and reads in again unchanged.
		var out bytes.Buffer
		mw := NewMessageWriter(&out)
		for _, m := range msgs {
			if err := mw.Write(m); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		mr = NewMessageReader(bytes.NewReader(out.Bytes()))
		for i, want := range msgs {
			got, err := mr.Read()
			if err != nil {
				t.Fatalf("re-read message %d: %v", i, err)
			}
			if !sameMessage(got, want) {
				t.Fatalf("message %d changed in round trip:\n first: %s\nsecond: %s", i, mustJSON(want), mustJSON(got))
			}
		}
	})
}

func FuzzReadResults(f *testing.F) {
	var seed bytes.Buffer
	r := NewJSONReporter(&seed)
	_ = r.Report(JobResult{ID: "a", Status: StatusPass, Summary: "ok", Params: Params{"n": 1}})
	_ = r.Report(JobResult{ID: "b", Status: StatusFail, Error: &ErrorInfo{Message: "boom"}})
	f.Add(seed.Bytes())
	f.Add(seed.Bytes()[:seed.Len()-5]) // a record cut mid-write
	f.Add([]byte(`{"id": "x", "status": "PASS", "data": {"k": [1, "two", null]}}` + "\n"))
	f.Add([]byte(`{"id": "x"} garbage`))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, data []byte) {
		suite, err := ReadResults(bytes.NewReader(data))
		if err != nil {
			return
		}
		// The reporter's output is the artifact; what it writes for these results
		// must read back as the same results.
		var out bytes.Buffer
		rep := NewJSONReporter(&out)
		for _, j := range suite.Jobs {
			if err := rep.Report(j); err != nil {
				t.Fatalf("report: %v", err)
			}
		}
		again, err := ReadResults(bytes.NewReader(out.Bytes()))
		if err != nil {
			t.Fatalf("re-read: %v", err)
		}
		if len(again.Jobs) != len(suite.Jobs) {
			t.Fatalf("round trip: %d results became %d", len(suite.Jobs), len(again.Jobs))
		}
		for i := range suite.Jobs {
			if mustJSON(again.Jobs[i]) != mustJSON(suite.Jobs[i]) {
				t.Fatalf("result %d changed in round trip:\n first: %s\nsecond: %s", i, mustJSON(suite.Jobs[i]), mustJSON(again.Jobs[i]))
			}
		}
	})
}

// sameMessage compares two messages by their JSON encoding, which is what the
// wire carries; Go-level equality would trip over time.Time monotonic readings
// and json.RawMessage whitespace.
func sameMessage(a, b Message) bool { return mustJSON(a) == mustJSON(b) }

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unmarshalable: " + err.Error() + ">"
	}
	return string(b)
}
