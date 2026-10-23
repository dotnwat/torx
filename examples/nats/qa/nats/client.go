//go:build unix

package nats

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Client is one connection to the cluster, and the JetStream requests the
// suite makes over it. It speaks the JetStream API directly -- a request on
// a subject, a JSON answer -- rather than through nats.go's JetStream
// layer, so that nothing retries a request behind its back and every
// answer is classified the same way.
type Client struct {
	NC *nats.Conn
}

// Connect connects to the servers at urls (comma-separated), first to the
// first, as the client named name. A client that loses its server
// reconnects to another for as long as it lives.
func Connect(urls, name string) (*Client, error) {
	nc, err := nats.Connect(urls,
		nats.Name(name),
		nats.DontRandomize(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.ReconnectJitter(50*time.Millisecond, 50*time.Millisecond),
		nats.Timeout(2*time.Second),
		nats.PingInterval(2*time.Second),
		nats.MaxPingsOutstanding(2),
		nats.RetryOnFailedConnect(true),
		// Buffer nothing while disconnected: a request made then fails at
		// once rather than going out late, after its caller gave up.
		nats.ReconnectBufSize(-1),
	)
	if err != nil {
		return nil, err
	}
	return &Client{NC: nc}, nil
}

// Close closes the connection.
func (c *Client) Close() { c.NC.Close() }

// Server is the name of the server the client is connected to, or "".
func (c *Client) Server() string { return c.NC.ConnectedServerName() }

// Outcome is what came of a request.
type Outcome string

const (
	// OK means the request took effect, and the answer says how.
	OK Outcome = "ok"
	// Fail means the server answered that the request did not take
	// effect.
	Fail Outcome = "fail"
	// Unknown means no answer came, or one that does not say: the request
	// may have taken effect or not.
	Unknown Outcome = "unknown"
)

// APIError is the error a JetStream API answer carries.
type APIError struct {
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code"`
	Description string `json:"description"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s (%d/%d)", e.Description, e.Code, e.ErrCode)
}

// JetStream error codes the suite tells apart.
const (
	ErrCodeNoMessage         = 10037 // no message found
	ErrCodeWrongLastSeq      = 10071 // wrong last sequence
	ErrCodeWrongLastSeqConst = 10164 // wrong last sequence, on a replicated stream
	ErrCodeStreamNotFound    = 10059
	ErrCodeConsumerNotFound  = 10014
	ErrCodeNoLeader          = 10008 // JetStream cluster not available
)

// Result is what came of a request: its outcome, and the error if it was
// not OK.
type Result struct {
	Outcome Outcome
	// API is the JetStream error the server answered with, if any.
	API *APIError
	Err error
}

// Classify says what a request's error means for whether it took effect:
// an answer from JetStream refusing it is a failure, and so is a refusal
// before it went out; anything else -- a timeout, a dropped connection, no
// one to answer -- leaves it unknown.
func Classify(err error) Result {
	var api *APIError
	switch {
	case err == nil:
		return Result{Outcome: OK}
	case errors.As(err, &api):
		return Result{Outcome: Fail, API: api, Err: err}
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrBadSubject), errors.Is(err, nats.ErrMaxPayload):
		return Result{Outcome: Fail, Err: err}
	case errors.Is(err, nats.ErrConnectionReconnecting):
		// Nothing is buffered while disconnected (ReconnectBufSize -1),
		// so the request never left.
		return Result{Outcome: Fail, Err: err}
	}
	return Result{Outcome: Unknown, Err: err}
}

// MaxWait bounds how long a request waits for its answer, whatever its
// context allows: an answer lost on the way -- a route that dropped it, a
// server that restarted -- never comes, and a caller with minutes left
// would wait them all out.
const MaxWait = 10 * time.Second

// bounded is ctx, ending MaxWait from now if it would end later.
func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if d, ok := ctx.Deadline(); ok && time.Until(d) <= MaxWait {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, MaxWait)
}

// Request sends a JetStream API request with body (marshalled to JSON
// unless it is already bytes) and decodes the answer into out. An answer
// carrying an error is returned as an *APIError.
func (c *Client) Request(ctx context.Context, subject string, body, out any) error {
	ctx, cancel := bounded(ctx)
	defer cancel()
	var data []byte
	switch b := body.(type) {
	case nil:
	case []byte:
		data = b
	default:
		var err error
		if data, err = json.Marshal(body); err != nil {
			return err
		}
	}
	msg, err := c.NC.RequestWithContext(ctx, subject, data)
	if err != nil {
		return err
	}
	return decode(msg.Data, out)
}

func decode(data []byte, out any) error {
	var e struct {
		Error *APIError `json:"error"`
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("nats: undecodable answer %q: %w", data, err)
	}
	if e.Error != nil {
		return e.Error
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("nats: undecodable answer %q: %w", data, err)
	}
	return nil
}

// PubAck is JetStream's answer to a publish it stored.
type PubAck struct {
	Stream    string `json:"stream"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
	// Val is a counter's value after an increment.
	Val string `json:"val,omitempty"`
	// BatchID and BatchSize are an atomic batch's, on its commit's ack.
	BatchID   string `json:"batch,omitempty"`
	BatchSize int    `json:"count,omitempty"`
}

// Publish publishes data to subject with headers hdr and waits for
// JetStream's ack.
func (c *Client) Publish(ctx context.Context, subject string, data []byte, hdr nats.Header) (*PubAck, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	m := &nats.Msg{Subject: subject, Data: data, Header: hdr}
	resp, err := c.NC.RequestMsgWithContext(ctx, m)
	if err != nil {
		return nil, err
	}
	var ack PubAck
	if err := decode(resp.Data, &ack); err != nil {
		return nil, err
	}
	if ack.Stream == "" {
		return nil, fmt.Errorf("nats: an ack with no stream: %q", resp.Data)
	}
	return &ack, nil
}

// PublishNoAck publishes a message with a reply subject no one listens
// on: a message of an atomic batch before its commit, which JetStream
// answers only if it fails.
func (c *Client) PublishNoAck(subject string, data []byte, hdr nats.Header) error {
	return c.NC.PublishMsg(&nats.Msg{Subject: subject, Data: data, Header: hdr})
}

// StoredMsg is a message as a stream holds it.
type StoredMsg struct {
	Subject string      `json:"subject"`
	Seq     uint64      `json:"seq"`
	Header  nats.Header `json:"-"`
	Data    []byte      `json:"data,omitempty"`
	Time    time.Time   `json:"time"`
	Hdrs    []byte      `json:"hdrs,omitempty"`
}

// MsgGetRequest selects a message to get: by sequence, the last on a
// subject, or the next on a subject from a sequence.
type MsgGetRequest struct {
	Seq     uint64 `json:"seq,omitempty"`
	LastFor string `json:"last_by_subj,omitempty"`
	NextFor string `json:"next_by_subj,omitempty"`
}

// MsgGet gets a message from stream's leader. It returns nil, and no
// error, for no message.
func (c *Client) MsgGet(ctx context.Context, stream string, req MsgGetRequest) (*StoredMsg, error) {
	var resp struct {
		Message *StoredMsg `json:"message"`
	}
	err := c.Request(ctx, "$JS.API.STREAM.MSG.GET."+stream, req, &resp)
	var api *APIError
	if errors.As(err, &api) && api.ErrCode == ErrCodeNoMessage {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if resp.Message == nil {
		return nil, fmt.Errorf("nats: a message get answer with no message")
	}
	if len(resp.Message.Hdrs) > 0 {
		if resp.Message.Header, err = ParseHeader(resp.Message.Hdrs); err != nil {
			return nil, err
		}
	}
	return resp.Message, nil
}

// DirectGet gets a message from any of stream's replicas that answers
// first, as direct gets do. It returns nil, and no error, for no message.
func (c *Client) DirectGet(ctx context.Context, stream string, req MsgGetRequest) (*StoredMsg, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	resp, err := c.NC.RequestWithContext(ctx, "$JS.API.DIRECT.GET."+stream, data)
	if err != nil {
		return nil, err
	}
	if st := resp.Header.Get("Status"); st != "" {
		if st == "404" {
			return nil, nil
		}
		code, _ := strconv.Atoi(st)
		return nil, &APIError{Code: code, Description: resp.Header.Get("Description")}
	}
	seq, err := strconv.ParseUint(resp.Header.Get("Nats-Sequence"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("nats: a direct get answer with no sequence: %v", resp.Header)
	}
	m := &StoredMsg{Subject: resp.Header.Get("Nats-Subject"), Seq: seq, Header: resp.Header, Data: resp.Data}
	if ts := resp.Header.Get("Nats-Time-Stamp"); ts != "" {
		m.Time, _ = time.Parse(time.RFC3339Nano, ts)
	}
	return m, nil
}

// ParseHeader decodes a message's headers as a stream stores them: the
// NATS/1.0 line, then MIME headers.
func ParseHeader(b []byte) (nats.Header, error) {
	r := textproto.NewReader(bufio.NewReader(bytes.NewReader(b)))
	line, err := r.ReadLine()
	if err != nil || !strings.HasPrefix(line, "NATS/1.0") {
		return nil, fmt.Errorf("nats: undecodable headers %q", b)
	}
	h, err := r.ReadMIMEHeader()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("nats: undecodable headers %q: %w", b, err)
	}
	return nats.Header(h), nil
}
