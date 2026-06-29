// The driver<->worker wire protocol.
//
// The driver sends the worker one Assignment -- the job to run, its parameters,
// the nodes allocated to it, and session config -- as a single JSON document,
// and the worker streams Messages back as newline-delimited JSON: lifecycle and
// log events while it runs, then a final message carrying the JobResult. The
// Assignment carries a SchemaVersion the worker validates on startup -- the one
// handshake that establishes the protocol version for the rest of the exchange.
// Decoding ignores unknown fields, so a newer peer's extra data does not break
// an older one.
package torx

import (
	"encoding/json"
	"fmt"
	"io"
)

// SchemaVersion is the current wire protocol version.
const SchemaVersion = 1

// BackendDescriptor names the transport for an assigned node and carries its
// configuration. The core builds the "local" kind directly; any other kind is
// constructed by a registered BackendBuilder that interprets Config. Host is the
// common field most transports need; Config holds the kind-specific remainder
// (for ssh: user, identity file, ...), opaque to the core.
type BackendDescriptor struct {
	Kind   string          `json:"kind"`
	Host   string          `json:"host,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`
}

// NodeDescriptor is the serializable description of a node assigned to a job; the
// worker rebuilds a Node from it. It carries identity and how to reach the node,
// not resource capacity -- the driver has already matched the job's demand.
type NodeDescriptor struct {
	Name    string            `json:"name"`
	Role    string            `json:"role,omitempty"`
	Address string            `json:"address,omitempty"`
	Scratch string            `json:"scratch"`
	Backend BackendDescriptor `json:"backend"`
}

// SessionConfig is the run-wide configuration a worker needs.
type SessionConfig struct {
	ResultsDir string `json:"results_dir"`
	TimeoutMS  int    `json:"timeout_ms,omitempty"`
}

// Assignment is everything the driver hands a worker to run one job.
type Assignment struct {
	SchemaVersion int              `json:"schema_version"`
	JobID         string           `json:"job_id"`
	Params        Params           `json:"params,omitempty"`
	Nodes         []NodeDescriptor `json:"nodes"`
	Session       SessionConfig    `json:"session"`
}

// Message is one record on the worker->driver stream: a lifecycle or log Event,
// or the final Result.
type Message struct {
	Event  *Event     `json:"event,omitempty"`
	Result *JobResult `json:"result,omitempty"`
}

// EventMessage wraps an event for the stream.
func EventMessage(e Event) Message { return Message{Event: &e} }

// ResultMessage wraps the final result for the stream.
func ResultMessage(r JobResult) Message { return Message{Result: &r} }

// EncodeAssignment writes a to w as a single JSON document, stamping the current
// schema version.
func EncodeAssignment(w io.Writer, a Assignment) error {
	a.SchemaVersion = SchemaVersion
	if err := json.NewEncoder(w).Encode(a); err != nil {
		return fmt.Errorf("wire: encode assignment: %w", err)
	}
	return nil
}

// DecodeAssignment reads one JSON Assignment from r and checks its schema
// version.
func DecodeAssignment(r io.Reader) (Assignment, error) {
	var a Assignment
	if err := json.NewDecoder(r).Decode(&a); err != nil {
		return Assignment{}, fmt.Errorf("wire: decode assignment: %w", err)
	}
	if a.SchemaVersion != SchemaVersion {
		return Assignment{}, fmt.Errorf("wire: unsupported schema version %d (want %d)", a.SchemaVersion, SchemaVersion)
	}
	return a, nil
}

// MessageWriter writes Messages as newline-delimited JSON.
type MessageWriter struct {
	enc *json.Encoder
}

// NewMessageWriter returns a MessageWriter over w.
func NewMessageWriter(w io.Writer) *MessageWriter {
	return &MessageWriter{enc: json.NewEncoder(w)}
}

// Write encodes one message followed by a newline.
func (mw *MessageWriter) Write(m Message) error {
	if err := mw.enc.Encode(m); err != nil {
		return fmt.Errorf("wire: write message: %w", err)
	}
	return nil
}

// MessageReader reads newline-delimited JSON Messages.
type MessageReader struct {
	dec *json.Decoder
}

// NewMessageReader returns a MessageReader over r.
func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{dec: json.NewDecoder(r)}
}

// Read returns the next message, or io.EOF when the stream ends.
func (mr *MessageReader) Read() (Message, error) {
	var m Message
	if err := mr.dec.Decode(&m); err != nil {
		return Message{}, err
	}
	return m, nil
}
