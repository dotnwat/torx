//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/nats/qa/nats"
	gonats "github.com/nats-io/nats.go"
)

// Streams the clients work, and the subjects each takes.
const (
	// regStream holds registers: each subject reg.<k> is one, whose value
	// is the last message on it. Clients write them unconditionally and
	// with compare-and-set on the subject's last sequence, and read them
	// from the stream's leader and from any replica.
	regStream = "reg"
	// logStream is a log every client appends to with compare-and-set on
	// the stream's last sequence: one append at a time wins.
	logStream = "log"
	// ctrStream holds counters (allow_msg_counter): each subject ctr.<k>
	// is one, which clients add to with Nats-Incr, and whose messages each
	// carry the counter's value after their increment.
	ctrStream = "ctr"
	// batchStream takes atomic batches (allow_atomic): a client's batch of
	// messages over several subjects, all of which the stream takes or
	// none.
	batchStream = "batch"
)

// idHeader carries the id of the write a message is, on every message
// written: a counter's message holds the counter's value, not what was
// sent.
const idHeader = "Torx-Id"

// op is one operation a client issued, and what came of it.
type op struct {
	Index  int    `json:"i"`
	Client int    `json:"c"`
	Kind   string `json:"kind"`
	Stream string `json:"stream"`
	// Subject is the subject written or read.
	Subject string `json:"subj,omitempty"`
	// Value is the id of the message written: unique to one logical
	// write, and the same in each of its attempts.
	Value string `json:"value,omitempty"`
	// Values and Subjects are a batch's messages' ids and subjects, in
	// order.
	Values   []string `json:"values,omitempty"`
	Subjects []string `json:"subjs,omitempty"`
	// MsgID is the message id the write carried for deduplication.
	MsgID string `json:"msgid,omitempty"`
	// Expect is the last sequence -- the subject's, or the stream's for an
	// append -- a conditional write required.
	Expect *uint64 `json:"expect,omitempty"`
	// Incr is what an increment added to its counter.
	Incr int64 `json:"incr,omitempty"`
	// Attempt numbers a write's attempts from 0: an attempt after the
	// first resends it after an answer was lost.
	Attempt int `json:"attempt,omitempty"`
	// Call and Return are when the request went out and its answer came
	// back, in nanoseconds since the run's start; Return is 0 for no
	// answer.
	Call   int64        `json:"call"`
	Return int64        `json:"ret,omitempty"`
	Out    nats.Outcome `json:"out"`
	Code   int          `json:"code,omitempty"`
	Err    string       `json:"err,omitempty"`
	// Seq is the sequence a write was acked at -- a batch's last message's
	// -- or of the message a read returned.
	Seq uint64 `json:"seq,omitempty"`
	// Dup says the write's ack called it a duplicate.
	Dup bool `json:"dup,omitempty"`
	// Val is a counter's value: after an acked increment, or as read.
	Val string `json:"val,omitempty"`
	// Count is how many messages an acked batch's ack says it took.
	Count int `json:"count,omitempty"`
	// Found says a read found a message; Read is its value.
	Found bool   `json:"found,omitempty"`
	Read  string `json:"read,omitempty"`
	// Server is the server the client was connected to when it called.
	Server string `json:"server,omitempty"`
}

func (o op) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%d c%d %s %s", o.Index, o.Client, o.Kind, o.Stream)
	if o.Subject != "" {
		fmt.Fprintf(&b, " %s", o.Subject)
	}
	if o.Value != "" {
		fmt.Fprintf(&b, " %s", o.Value)
	}
	if len(o.Values) > 0 {
		fmt.Fprintf(&b, " %v on %v", o.Values, o.Subjects)
	}
	if o.Incr != 0 {
		fmt.Fprintf(&b, " %+d", o.Incr)
	}
	if o.Expect != nil {
		fmt.Fprintf(&b, " expect=%d", *o.Expect)
	}
	if o.Attempt > 0 {
		fmt.Fprintf(&b, " attempt=%d", o.Attempt)
	}
	fmt.Fprintf(&b, " [%.3fs,", float64(o.Call)/1e9)
	if o.Return > 0 {
		fmt.Fprintf(&b, "%.3fs]", float64(o.Return)/1e9)
	} else {
		b.WriteString("?]")
	}
	fmt.Fprintf(&b, " %s", o.Out)
	if o.Seq > 0 {
		fmt.Fprintf(&b, " seq=%d", o.Seq)
	}
	if o.Dup {
		b.WriteString(" dup")
	}
	if o.Val != "" {
		fmt.Fprintf(&b, " val=%s", o.Val)
	}
	if isRead(o.Kind) {
		if o.Found {
			fmt.Fprintf(&b, " read=%s", o.Read)
		} else if o.Out == nats.OK {
			b.WriteString(" none")
		}
	}
	if o.Err != "" {
		fmt.Fprintf(&b, " (%s)", o.Err)
	}
	if o.Server != "" {
		fmt.Fprintf(&b, " via %s", o.Server)
	}
	return b.String()
}

func isRead(kind string) bool { return kind == "get" || kind == "dget" || kind == "cget" }

// opWeights are how often a client draws each kind of operation.
var opWeights = map[string]int{
	"put":     3, // write a register unconditionally
	"cas":     3, // write a register if its last sequence is the one the client last saw
	"get":     3, // read a register from the stream's leader
	"dget":    2, // read a register from any replica (direct get)
	"append":  1, // append to the log if its last sequence is the one the client last saw
	"incr":    2, // add to a counter
	"cget":    1, // read a counter from the stream's leader
	"batch":   1, // write an atomic batch, conditional on its first subject's last sequence or not
	"enqueue": 2, // write a message for the consumer to deliver
}

// opKinds are the kinds, in the order a client draws them.
var opKinds = []string{"put", "cas", "get", "dget", "append", "incr", "cget", "batch", "enqueue"}

// workload is the clients' load, and the history of what they did.
type workload struct {
	clients   []*nats.Client
	keys      int
	valueSize int
	timeout   time.Duration
	weights   map[string]int
	// retry is the chance a write whose answer was lost is sent again,
	// with the same message id.
	retry float64
	// rate is how many operations a second each client issues at most.
	rate  int
	start time.Time

	mu   sync.Mutex
	hist []op
	// seen is each client's last sequence seen of each subject, and of
	// the log.
	seen    []map[string]uint64
	logSeen []uint64
}

func newWorkload(clients []*nats.Client, keys, valueSize, rate int, timeout time.Duration, weights map[string]int) *workload {
	w := &workload{
		clients: clients, keys: keys, valueSize: valueSize, rate: rate, timeout: timeout, weights: weights,
		retry: 0.5, start: time.Now(), logSeen: make([]uint64, len(clients)),
	}
	for range clients {
		w.seen = append(w.seen, map[string]uint64{})
	}
	return w
}

func (w *workload) now() int64 { return int64(time.Since(w.start)) }

func (w *workload) record(o op) op {
	w.mu.Lock()
	defer w.mu.Unlock()
	o.Index = len(w.hist)
	w.hist = append(w.hist, o)
	return o
}

func (w *workload) history() []op {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]op(nil), w.hist...)
}

func (w *workload) seenOf(c int, subject string) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seen[c][subject]
}

// run is client c's loop until ctx ends.
func (w *workload) run(ctx context.Context, c int, rng *rand.Rand) {
	var kinds []string
	total := 0
	for _, k := range opKinds {
		if w.weights[k] > 0 {
			kinds = append(kinds, k)
			total += w.weights[k]
		}
	}
	var tick *time.Ticker
	if w.rate > 0 {
		tick = time.NewTicker(time.Second / time.Duration(w.rate))
		defer tick.Stop()
	}
	for n := 0; ctx.Err() == nil; n++ {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
		r := rng.IntN(total)
		var kind string
		for _, k := range kinds {
			if r < w.weights[k] {
				kind = k
				break
			}
			r -= w.weights[k]
		}
		key := rng.IntN(w.keys)
		value := fmt.Sprintf("c%d-%d", c, n)
		switch kind {
		case "put":
			w.write(ctx, c, rng, op{Kind: kind, Stream: regStream, Subject: fmt.Sprintf("reg.%d", key), Value: value}, rng.IntN(2) == 0)
		case "cas":
			subject := fmt.Sprintf("reg.%d", key)
			e := w.seenOf(c, subject)
			w.write(ctx, c, rng, op{Kind: kind, Stream: regStream, Subject: subject, Value: value, Expect: &e}, rng.IntN(2) == 0)
		case "append":
			w.mu.Lock()
			e := w.logSeen[c]
			w.mu.Unlock()
			w.write(ctx, c, rng, op{Kind: kind, Stream: logStream, Subject: "log.x", Value: value, Expect: &e}, rng.IntN(2) == 0)
		case "incr":
			w.write(ctx, c, rng, op{Kind: kind, Stream: ctrStream, Subject: fmt.Sprintf("ctr.%d", key), Value: value, Incr: int64(rng.IntN(16) - 5)}, rng.IntN(2) == 0)
		case "get", "dget":
			w.read(ctx, c, kind, regStream, fmt.Sprintf("reg.%d", key))
		case "cget":
			w.read(ctx, c, kind, ctrStream, fmt.Sprintf("ctr.%d", key))
		case "batch":
			w.batch(ctx, c, rng, value)
		case "enqueue":
			w.write(ctx, c, rng, op{Kind: kind, Stream: workStream, Subject: fmt.Sprintf("work.%d", key), Value: value}, rng.IntN(2) == 0)
		}
	}
}

// payload is a write's message: its value, then filler to a size drawn
// up to valueSize.
func (w *workload) payload(rng *rand.Rand, value string) []byte {
	b := []byte(value + ":")
	if w.valueSize > 0 {
		n := rng.IntN(w.valueSize + 1)
		for range n {
			b = append(b, byte('a'+rng.IntN(26)))
		}
	}
	return b
}

// valueOf is the id of the write a message is: its id header, or else the
// start of its payload.
func valueOf(hdr gonats.Header, data []byte) string {
	if id := hdr.Get(idHeader); id != "" {
		return id
	}
	v, _, _ := strings.Cut(string(data), ":")
	return v
}

// counterVal is the value a counter's message holds, or "".
func counterVal(data []byte) string {
	var v struct {
		Val string `json:"val"`
	}
	if json.Unmarshal(data, &v) != nil {
		return ""
	}
	return v.Val
}

// write publishes o, and if its answer is lost may send it again, with
// the same message id if it has one.
func (w *workload) write(ctx context.Context, c int, rng *rand.Rand, o op, msgID bool) {
	o.Client = c
	if msgID {
		o.MsgID = o.Value
	}
	var data []byte
	if o.Stream != ctrStream {
		data = w.payload(rng, o.Value)
	}
	cl := w.clients[c]
	for attempt := 0; ; attempt++ {
		o.Attempt = attempt
		hdr := gonats.Header{}
		hdr.Set(idHeader, o.Value)
		if o.MsgID != "" {
			hdr.Set("Nats-Msg-Id", o.MsgID)
		}
		if o.Stream == ctrStream {
			hdr.Set("Nats-Incr", fmt.Sprintf("%+d", o.Incr))
		}
		if o.Expect != nil {
			name := "Nats-Expected-Last-Subject-Sequence"
			if o.Kind == "append" {
				name = "Nats-Expected-Last-Sequence"
			}
			hdr.Set(name, strconv.FormatUint(*o.Expect, 10))
		}
		o.Server = cl.Server()
		o.Call = w.now()
		rctx, cancel := context.WithTimeout(ctx, w.timeout)
		ack, err := cl.Publish(rctx, o.Subject, data, hdr)
		cancel()
		res := nats.Classify(err)
		o.Out, o.Err, o.Code, o.Seq, o.Dup, o.Val, o.Return = res.Outcome, "", 0, 0, false, "", 0
		if res.Outcome != nats.Unknown {
			o.Return = w.now()
		}
		if err != nil {
			o.Err = err.Error()
		}
		if res.API != nil {
			o.Code = res.API.ErrCode
		}
		if ack != nil {
			o.Seq, o.Dup, o.Val = ack.Seq, ack.Duplicate, ack.Val
		}
		w.record(o)
		w.mu.Lock()
		switch {
		case res.Outcome == nats.OK && o.Kind == "append":
			w.logSeen[c] = ack.Seq
		case res.Outcome == nats.OK:
			w.seen[c][o.Subject] = ack.Seq
		case o.Kind == "append" && (o.Code == nats.ErrCodeWrongLastSeq || o.Code == nats.ErrCodeWrongLastSeqConst):
			// Learn the log's end for the next append.
			if seq, ok := lastSeqIn(o.Err); ok {
				w.logSeen[c] = seq
			} else if si, err := cl.StreamInfo(ctx, logStream); err == nil {
				w.logSeen[c] = si.State.LastSeq
			}
		}
		w.mu.Unlock()
		// Resend only what may have been lost, under its message id, and
		// only into a run still going.
		if res.Outcome != nats.Unknown || o.MsgID == "" || ctx.Err() != nil || rng.Float64() >= w.retry || attempt >= 2 {
			return
		}
	}
}

// batch writes an atomic batch of one to six messages over the batch
// stream's subjects, half the time conditional on its first subject's last
// sequence being the one the client last saw. The messages before the
// commit go out with no reply subject: JetStream stages them, and answers
// only the commit.
func (w *workload) batch(ctx context.Context, c int, rng *rand.Rand, value string) {
	cl := w.clients[c]
	n := 1 + rng.IntN(6)
	o := op{Client: c, Kind: "batch", Stream: batchStream}
	for i := range n {
		o.Values = append(o.Values, fmt.Sprintf("%s.%d", value, i))
		o.Subjects = append(o.Subjects, fmt.Sprintf("batch.%d", rng.IntN(w.keys)))
	}
	if rng.IntN(2) == 0 {
		e := w.seenOf(c, o.Subjects[0])
		o.Expect = &e
	}
	o.Server = cl.Server()
	o.Call = w.now()
	var ack *nats.PubAck
	var err error
	for i := range n {
		hdr := gonats.Header{}
		hdr.Set(idHeader, o.Values[i])
		hdr.Set("Nats-Batch-Id", value)
		hdr.Set("Nats-Batch-Sequence", strconv.Itoa(i+1))
		if i == 0 && o.Expect != nil {
			hdr.Set("Nats-Expected-Last-Subject-Sequence", strconv.FormatUint(*o.Expect, 10))
		}
		data := w.payload(rng, o.Values[i])
		if i < n-1 {
			if err = cl.PublishNoAck(o.Subjects[i], data, hdr); err != nil {
				break
			}
			continue
		}
		hdr.Set("Nats-Batch-Commit", "1")
		rctx, cancel := context.WithTimeout(ctx, w.timeout)
		ack, err = cl.Publish(rctx, o.Subjects[i], data, hdr)
		cancel()
	}
	res := nats.Classify(err)
	o.Out = res.Outcome
	if res.Outcome != nats.Unknown {
		o.Return = w.now()
	}
	if err != nil {
		o.Err = err.Error()
	}
	if res.API != nil {
		o.Code = res.API.ErrCode
	}
	if ack != nil {
		o.Seq, o.Count = ack.Seq, ack.BatchSize
	}
	w.record(o)
	if ack != nil {
		w.mu.Lock()
		for i, s := range o.Subjects {
			w.seen[c][s] = ack.Seq - uint64(n-1-i)
		}
		w.mu.Unlock()
	}
}

// lastSeqIn is the sequence a "wrong last sequence: N" error names.
func lastSeqIn(msg string) (uint64, bool) {
	_, after, ok := strings.Cut(msg, "wrong last sequence: ")
	if !ok {
		return 0, false
	}
	if i := strings.IndexFunc(after, func(r rune) bool { return r < '0' || r > '9' }); i >= 0 {
		after = after[:i]
	}
	n, err := strconv.ParseUint(after, 10, 64)
	return n, err == nil
}

// read reads the last message on subject of stream: from the stream's
// leader (get, cget), or from whichever replica answers first (dget).
func (w *workload) read(ctx context.Context, c int, kind, stream, subject string) {
	cl := w.clients[c]
	o := op{Client: c, Kind: kind, Stream: stream, Subject: subject, Server: cl.Server(), Call: w.now()}
	rctx, cancel := context.WithTimeout(ctx, w.timeout)
	var m *nats.StoredMsg
	var err error
	if kind == "dget" {
		m, err = cl.DirectGet(rctx, stream, nats.MsgGetRequest{LastFor: subject})
	} else {
		m, err = cl.MsgGet(rctx, stream, nats.MsgGetRequest{LastFor: subject})
	}
	cancel()
	res := nats.Classify(err)
	o.Out = res.Outcome
	if res.Outcome != nats.Unknown {
		o.Return = w.now()
	}
	if err != nil {
		o.Err = err.Error()
		if res.API != nil {
			o.Code = res.API.ErrCode
		}
	}
	if m != nil {
		o.Found, o.Seq, o.Read = true, m.Seq, valueOf(m.Header, m.Data)
		if stream == ctrStream {
			o.Val = counterVal(m.Data)
		}
		if m.Subject != "" && m.Subject != subject {
			o.Err = fmt.Sprintf("asked for %s, got a message on %s", subject, m.Subject)
		}
	}
	o = w.record(o)
	if o.Out == nats.OK {
		w.mu.Lock()
		w.seen[c][subject] = o.Seq
		w.mu.Unlock()
	}
}
