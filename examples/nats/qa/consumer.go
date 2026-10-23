//go:build unix

package main

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// workStream holds the messages the consumer delivers; clients enqueue
	// them on work.<k>.
	workStream = "work"
	// consumerName is the durable pull consumer of workStream the
	// consumers share.
	consumerName = "w"
	// ackWait is how long the consumer waits for an ack before it
	// redelivers.
	ackWait = 2 * time.Second
)

// delivery is one message the consumer delivered to a consumer client, and
// what came of acking it.
type delivery struct {
	Index  int `json:"i"`
	Client int `json:"c"`
	// Seq and CSeq are the stream's and the consumer's sequences of the
	// delivery; Delivered is how many times the consumer has delivered
	// the message, this one included.
	Seq       uint64 `json:"seq"`
	CSeq      uint64 `json:"cseq"`
	Delivered uint64 `json:"n"`
	Subject   string `json:"subj"`
	Value     string `json:"value"`
	// Fetch is when the fetch that got it was sent; Time, when it came.
	Fetch int64 `json:"fetch"`
	Time  int64 `json:"time"`
	// Ack is what the client did with it -- "ack" (and waited for the
	// server to confirm), "nak", or "none" -- and AckOut what came of an
	// ack, which was confirmed at AckReturn.
	Ack       string       `json:"ack"`
	AckOut    nats.Outcome `json:"ack_out,omitempty"`
	AckErr    string       `json:"ack_err,omitempty"`
	AckReturn int64        `json:"ack_ret,omitempty"`
	// Drain says it came while the consumers drained the stream, after the
	// faults.
	Drain bool `json:"drain,omitempty"`
}

func (d delivery) String() string {
	s := fmt.Sprintf("#%d c%d seq=%d cseq=%d n=%d %s %s fetch@%.3fs got@%.3fs %s", d.Index, d.Client, d.Seq, d.CSeq, d.Delivered, d.Subject, d.Value,
		float64(d.Fetch)/1e9, float64(d.Time)/1e9, d.Ack)
	if d.Ack == "ack" {
		s += " " + string(d.AckOut)
		if d.AckOut == nats.OK {
			s += fmt.Sprintf("@%.3fs", float64(d.AckReturn)/1e9)
		}
	}
	return s
}

// consumers is the consumer clients' load, and what was delivered.
type consumers struct {
	w       *workload
	clients []*nats.Client

	mu         sync.Mutex
	deliveries []delivery
	fetchErrs  map[string]int
}

func (cs *consumers) record(d delivery) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	d.Index = len(cs.deliveries)
	cs.deliveries = append(cs.deliveries, d)
}

func (cs *consumers) history() []delivery {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return slices.Clone(cs.deliveries)
}

// consumerConfig is the durable consumer's config.
func consumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    -1,
		MaxAckPending: 1000,
		FilterSubject: "work.>",
	}
}

// run is consumer client c's loop until ctx ends: fetch a few messages,
// and ack most, waiting for the server to confirm, nak some, and leave
// some for the consumer to redeliver once their ack wait runs out. In a
// drain, it acks every one.
func (cs *consumers) run(ctx context.Context, c int, rng *rand.Rand, drain bool) {
	cl := cs.clients[c]
	js, err := jetstream.New(cl.NC)
	if err != nil {
		return
	}
	var cons jetstream.Consumer
	for ctx.Err() == nil {
		if cons == nil {
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			cons, err = js.Consumer(rctx, workStream, consumerName)
			cancel()
			if err != nil {
				cs.fetchErr(err)
				cons = nil
				sleepCtx(ctx, 200*time.Millisecond)
				continue
			}
		}
		fetch := cs.w.now()
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			cs.fetchErr(err)
			sleepCtx(ctx, 100*time.Millisecond)
			continue
		}
		for m := range batch.Messages() {
			d := delivery{Client: c, Fetch: fetch, Time: cs.w.now(), Subject: m.Subject(), Value: valueOf(m.Headers(), m.Data()), Drain: drain}
			if md, err := m.Metadata(); err == nil {
				d.Seq, d.CSeq, d.Delivered = md.Sequence.Stream, md.Sequence.Consumer, md.NumDelivered
			}
			switch r := rng.IntN(20); {
			case drain || r >= 2:
				d.Ack = "ack"
				actx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := m.DoubleAck(actx)
				cancel()
				res := nats.Classify(err)
				d.AckOut = res.Outcome
				if err != nil {
					d.AckErr = err.Error()
				} else {
					d.AckReturn = cs.w.now()
				}
			case r == 1:
				d.Ack = "nak"
				_ = m.Nak()
			default:
				d.Ack = "none"
			}
			cs.record(d)
		}
		if err := batch.Error(); err != nil {
			cs.fetchErr(err)
		}
	}
}

func (cs *consumers) fetchErr(err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.fetchErrs == nil {
		cs.fetchErrs = map[string]int{}
	}
	cs.fetchErrs[err.Error()]++
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// drained reports whether the consumer has nothing left to deliver and
// nothing delivered that waits for an ack.
func drained(ctx context.Context, cl *nats.Client) (bool, string) {
	js, err := jetstream.New(cl.NC)
	if err != nil {
		return false, err.Error()
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(rctx, workStream, consumerName)
	if err != nil {
		return false, err.Error()
	}
	ci := cons.CachedInfo()
	state := fmt.Sprintf("pending %d, ack pending %d, redelivered %d, delivered %d/%d, ack floor %d/%d",
		ci.NumPending, ci.NumAckPending, ci.NumRedelivered, ci.Delivered.Consumer, ci.Delivered.Stream, ci.AckFloor.Consumer, ci.AckFloor.Stream)
	return ci.NumPending == 0 && ci.NumAckPending == 0, state
}

// checkDeliveries holds what the consumer delivered to the work stream's
// final contents f: every delivery is of the message the stream holds at
// its sequence; every message the stream holds was delivered, once the
// consumers drained it; the consumer delivered messages first in the
// stream's order; it never gave a consumer sequence twice, nor counted a
// message's deliveries down; and it never delivered a message again after
// it confirmed the message's ack.
func checkDeliveries(ds []delivery, f *final, drained bool) []anomaly {
	var bad []anomaly
	first := map[uint64]delivery{}  // the first delivery of each sequence
	acked := map[uint64]delivery{}  // the first confirmed ack of each sequence
	byCSeq := map[uint64]delivery{} // each consumer sequence's delivery
	last := map[uint64]delivery{}   // the latest delivery of each sequence
	for _, d := range ds {
		m, ok := f.bySeq[d.Seq]
		if !ok || m.Value != d.Value || m.Subject != d.Subject {
			bad = append(bad, anomaly{Kind: "consumer-phantom", Detail: fmt.Sprintf("delivered %s at %d on %s; the stream holds %+v there: %s", d.Value, d.Seq, d.Subject, m, d)})
		}
		if prev, ok := byCSeq[d.CSeq]; ok && prev.Seq != d.Seq {
			bad = append(bad, anomaly{Kind: "consumer-seq-reuse", Detail: fmt.Sprintf("consumer sequence %d delivered twice, for different messages: %s; %s", d.CSeq, prev, d)})
		}
		byCSeq[d.CSeq] = d
		if prev, ok := last[d.Seq]; ok && d.Delivered <= prev.Delivered && d.CSeq > prev.CSeq {
			bad = append(bad, anomaly{Kind: "delivery-count", Detail: fmt.Sprintf("message %d delivered again with count %d after count %d: %s; %s", d.Seq, d.Delivered, prev.Delivered, prev, d)})
		}
		if prev, ok := last[d.Seq]; !ok || d.CSeq > prev.CSeq {
			last[d.Seq] = d
		}
		if a, ok := acked[d.Seq]; ok && d.Fetch > a.AckReturn {
			bad = append(bad, anomaly{Kind: "redelivered-after-ack", Detail: fmt.Sprintf("message %d delivered by a fetch sent after its ack was confirmed: %s; then %s", d.Seq, a, d)})
		}
		if _, ok := first[d.Seq]; !ok {
			first[d.Seq] = d
		}
		if d.Ack == "ack" && d.AckOut == nats.OK {
			if _, ok := acked[d.Seq]; !ok {
				acked[d.Seq] = d
			}
		}
	}
	// First deliveries in consumer sequence order are in stream order.
	var firsts []delivery
	for _, d := range first {
		if d.Delivered == 1 {
			firsts = append(firsts, d)
		}
	}
	slices.SortFunc(firsts, func(a, b delivery) int { return cmp.Compare(a.CSeq, b.CSeq) })
	for i := 1; i < len(firsts); i++ {
		if firsts[i].Seq < firsts[i-1].Seq {
			bad = append(bad, anomaly{Kind: "consumer-order", Detail: fmt.Sprintf("first delivered %d after %d: %s; %s", firsts[i].Seq, firsts[i-1].Seq, firsts[i-1], firsts[i])})
			break
		}
	}
	if drained {
		var missed []uint64
		for _, seq := range slices.Sorted(maps.Keys(f.bySeq)) {
			if _, ok := first[seq]; !ok {
				missed = append(missed, seq)
			}
		}
		if len(missed) > 0 {
			bad = append(bad, anomaly{Kind: "consumer-missed", Detail: fmt.Sprintf("the consumer drained the stream and never delivered %d of its messages: %v", len(missed), missed[:min(len(missed), 20)])})
		}
	}
	return bad
}
