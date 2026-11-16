//go:build unix

package slatedb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client calls one slatedb-node's API.
type Client struct {
	url  string
	http *http.Client
}

// NewClient returns a client of the process serving at url.
func NewClient(url string) *Client {
	return &Client{url: url, http: &http.Client{}}
}

// Error is an error a process answered: SlateDB's, with its kind --
// "closed-fenced", "unavailable", "transaction", "invalid", "data",
// "internal" -- or the node's own. A write that SlateDB accepted but could
// not make durable says so with a kind prefixed "durable:"; such a write
// may survive.
type Error struct {
	Status int
	Kind   string
	Msg    string
}

func (e *Error) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Kind, e.Msg) }

// Accepted reports whether err says the write was accepted before it
// failed: it may then take effect.
func Accepted(err error) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && strings.HasPrefix(e.Kind, "durable:")
}

// Kind is the kind of err if it is an Error, "" otherwise.
func Kind(err error) string {
	if e, ok := errors.AsType[*Error](err); ok {
		return strings.TrimPrefix(e.Kind, "durable:")
	}
	return ""
}

// Definite reports whether err says the operation did not happen: an
// answer from the node that it failed before SlateDB took the write.
// Any other failure -- no answer, a timeout -- leaves it in doubt.
func Definite(err error) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && !strings.HasPrefix(e.Kind, "durable:")
}

func (c *Client) call(ctx context.Context, path string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Kind  string `json:"kind"`
		}
		if json.Unmarshal(body, &e) != nil {
			e.Error = strings.TrimSpace(string(body))
		}
		return &Error{Status: resp.StatusCode, Kind: e.Kind, Msg: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

// Durability is what a read may see: "remote", only what object storage
// holds, or "memory", the writer's unflushed writes too.
type Durability string

const (
	Remote Durability = "remote"
	Memory Durability = "memory"
)

// Value is a value read, and the sequence number of the write that wrote
// it; Found is false for a key with no value.
type Value struct {
	Found bool
	Value string
	Seq   uint64
}

// Get reads key.
func (c *Client) Get(ctx context.Context, key string, d Durability) (Value, error) {
	var out struct {
		Value *string `json:"value"`
		Seq   uint64  `json:"seq"`
	}
	if err := c.call(ctx, "/get", map[string]any{"key": key, "durability": d}, &out); err != nil {
		return Value{}, err
	}
	if out.Value == nil {
		return Value{}, nil
	}
	return Value{Found: true, Value: *out.Value, Seq: out.Seq}, nil
}

// KV is a key and its value, as a scan returns them.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Seq   uint64 `json:"seq"`
}

// Scan reads the keys in [start, end); "" leaves a bound open.
func (c *Client) Scan(ctx context.Context, start, end string, d Durability) ([]KV, error) {
	in := map[string]any{"durability": d}
	if start != "" {
		in["start"] = start
	}
	if end != "" {
		in["end"] = end
	}
	var out struct {
		KVs []KV `json:"kvs"`
	}
	if err := c.call(ctx, "/scan", in, &out); err != nil {
		return nil, err
	}
	return out.KVs, nil
}

// Put writes value to key, or deletes key if value is nil, and returns
// the write's sequence number. With durable it returns once the write is
// in object storage.
func (c *Client) Put(ctx context.Context, key string, value *string, durable bool) (uint64, error) {
	var out struct {
		Seq uint64 `json:"seq"`
	}
	err := c.call(ctx, "/put", map[string]any{"key": key, "value": value, "durable": durable}, &out)
	return out.Seq, err
}

// Merge merges value into key, and returns the merge's sequence number.
// With durable it returns once the merge is in object storage.
func (c *Client) Merge(ctx context.Context, key, value string, durable bool) (uint64, error) {
	var out struct {
		Seq uint64 `json:"seq"`
	}
	err := c.call(ctx, "/merge", map[string]any{"key": key, "value": value, "durable": durable}, &out)
	return out.Seq, err
}

// Op is one write of a batch: a put, or a delete if Value is nil.
type Op struct {
	Key   string  `json:"key"`
	Value *string `json:"value"`
}

// Batch writes ops atomically and returns the batch's sequence number.
func (c *Client) Batch(ctx context.Context, ops []Op, durable bool) (uint64, error) {
	var out struct {
		Seq uint64 `json:"seq"`
	}
	err := c.call(ctx, "/batch", map[string]any{"ops": ops, "durable": durable}, &out)
	return out.Seq, err
}

// TxnOp is one operation of a transaction: "get", "put", "del", or
// "append", which reads the key and writes back what it read with Value
// appended after a space.
type TxnOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Txn runs ops in a transaction at isolation "si" or "ssi" and commits it.
// It returns what each get and append read (nil for a key with no value),
// and the commit's sequence number, 0 for a transaction that wrote
// nothing.
func (c *Client) Txn(ctx context.Context, isolation string, ops []TxnOp, durable bool) ([]*string, uint64, error) {
	var out struct {
		Results []*string `json:"results"`
		Seq     *uint64   `json:"seq"`
	}
	err := c.call(ctx, "/txn", map[string]any{"isolation": isolation, "ops": ops, "durable": durable}, &out)
	var seq uint64
	if out.Seq != nil {
		seq = *out.Seq
	}
	return out.Results, seq, err
}

// SnapshotOpen opens a snapshot of the writer's database, and returns its
// ID and the sequence number it reads at.
func (c *Client) SnapshotOpen(ctx context.Context) (id, seq uint64, err error) {
	var out struct {
		ID  uint64 `json:"id"`
		Seq uint64 `json:"seq"`
	}
	err = c.call(ctx, "/snapshot/open", map[string]any{}, &out)
	return out.ID, out.Seq, err
}

// SnapshotScan reads every key of snapshot id.
func (c *Client) SnapshotScan(ctx context.Context, id uint64) ([]KV, error) {
	var out struct {
		KVs []KV `json:"kvs"`
	}
	if err := c.call(ctx, "/snapshot/scan", map[string]any{"id": id}, &out); err != nil {
		return nil, err
	}
	return out.KVs, nil
}

// SnapshotClose closes snapshot id.
func (c *Client) SnapshotClose(ctx context.Context, id uint64) error {
	return c.call(ctx, "/snapshot/close", map[string]any{"id": id}, nil)
}

// Clone checkpoints the writer's database, every write it has taken, and
// clones the checkpoint to the path name.
func (c *Client) Clone(ctx context.Context, name string) error {
	return c.call(ctx, "/clone", map[string]any{"name": name}, nil)
}

// CloneScan opens a reader on the clone at path name, and reads every key.
func (c *Client) CloneScan(ctx context.Context, name string) ([]KV, error) {
	var out struct {
		KVs []KV `json:"kvs"`
	}
	if err := c.call(ctx, "/clone/scan", map[string]any{"name": name}, &out); err != nil {
		return nil, err
	}
	return out.KVs, nil
}

// Flush flushes the writer's WAL to object storage.
func (c *Client) Flush(ctx context.Context) error {
	return c.call(ctx, "/flush", map[string]any{}, nil)
}

// Status is what a process says of itself.
type Status struct {
	Role          string  `json:"role"`
	ClockOffsetMS int64   `json:"clock_offset_ms"`
	Ended         *string `json:"ended"`
	DurableSeq    uint64  `json:"durable_seq"`
	CloseReason   *string `json:"close_reason"`
	ManifestID    uint64  `json:"manifest_id"`
}

// Status asks the process for its status.
func (c *Client) Status(ctx context.Context) (Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/status", nil)
	if err != nil {
		return Status{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Status{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var st Status
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status: %s", resp.Status)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// CloseIdle closes the client's idle connections.
func (c *Client) CloseIdle() { c.http.CloseIdleConnections() }
