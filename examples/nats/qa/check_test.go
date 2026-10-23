//go:build unix

package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/dotnwat/torx/examples/nats/qa/nats"
)

// h builds a history from ops, numbering them.
func h(ops ...op) []op {
	for i := range ops {
		ops[i].Index = i
		if ops[i].Stream == "" {
			ops[i].Stream = regStream
		}
	}
	return ops
}

func put(c int, subj, value string, call, ret int64, seq uint64) op {
	o := op{Client: c, Kind: "put", Subject: subj, Value: value, Call: call, Return: ret, Out: nats.OK, Seq: seq}
	if ret == 0 {
		o.Out, o.Seq = nats.Unknown, 0
	}
	return o
}

func cas(c int, subj, value string, expect uint64, call, ret int64, seq uint64) op {
	o := put(c, subj, value, call, ret, seq)
	o.Kind, o.Expect = "cas", &expect
	return o
}

func get(c int, subj string, call, ret int64, seq uint64, value string) op {
	return op{Client: c, Kind: "get", Subject: subj, Call: call, Return: ret, Out: nats.OK, Seq: seq, Found: seq > 0, Read: value}
}

func kinds(as []anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	slices.Sort(out)
	return out
}

func checkAll(t *testing.T, hist []op, msgs []msg) []string {
	t.Helper()
	f := newFinal(regStream, msgs)
	bad := checkStream(hist, regStream, f)
	reads, unknown := checkReads(context.Background(), hist, f, map[string]bool{"get": true}, 10*time.Second)
	if len(unknown) > 0 {
		t.Fatalf("undecided: %v", unknown)
	}
	for _, a := range append(bad, reads...) {
		t.Log(a)
	}
	return kinds(append(bad, reads...))
}

func TestCheckClean(t *testing.T) {
	hist := h(
		put(0, "reg.0", "a", 0, 10, 1),
		get(1, "reg.0", 11, 12, 1, "a"),
		cas(1, "reg.0", "b", 1, 13, 20, 3),
		put(2, "reg.1", "c", 14, 16, 2),
		get(0, "reg.0", 15, 25, 1, "a"), // concurrent with b: may see a
		put(2, "reg.1", "d", 30, 0, 0),  // unknown, landed at 4
		get(0, "reg.1", 40, 41, 4, "d"),
	)
	msgs := []msg{m(1, "reg.0", "a"), m(2, "reg.1", "c"), m(3, "reg.0", "b"), m(4, "reg.1", "d")}
	if got := checkAll(t, hist, msgs); len(got) > 0 {
		t.Fatalf("a clean history: %v", got)
	}
}

func TestCheckFindsEach(t *testing.T) {
	for _, tc := range []struct {
		name string
		hist []op
		msgs []msg
		want []string
	}{{
		name: "lost write",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(0, "reg.0", "b", 11, 12, 2)),
		msgs: []msg{m(1, "reg.0", "a")},
		want: []string{"lost"},
	}, {
		name: "phantom",
		hist: h(put(0, "reg.0", "a", 0, 10, 1)),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "zz")},
		want: []string{"phantom"},
	}, {
		name: "ack mismatch",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(1, "reg.0", "b", 0, 10, 1)),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "b")},
		want: []string{"ack-mismatch"},
	}, {
		name: "cas after the wrong sequence",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(0, "reg.0", "b", 11, 12, 2), cas(1, "reg.0", "c", 1, 13, 14, 3)),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "b"), m(3, "reg.0", "c")},
		want: []string{"condition"},
	}, {
		name: "real-time order",
		hist: h(put(0, "reg.0", "a", 0, 10, 2), put(1, "reg.1", "b", 20, 30, 1)),
		msgs: []msg{m(1, "reg.1", "b"), m(2, "reg.0", "a")},
		want: []string{"realtime-order"},
	}, {
		name: "stale read",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(0, "reg.0", "b", 11, 12, 2), get(1, "reg.0", 20, 21, 1, "a")),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "b")},
		want: []string{"nonlinearizable-read"},
	}, {
		name: "new then old",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(0, "reg.0", "b", 11, 0, 0), get(1, "reg.0", 20, 21, 2, "b"), get(2, "reg.0", 22, 23, 1, "a")),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "b")},
		want: []string{"nonlinearizable-read"},
	}, {
		name: "refused cas told an old sequence",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), put(0, "reg.0", "b", 11, 12, 2),
			op{Client: 1, Kind: "cas", Subject: "reg.0", Value: "c", Expect: new(uint64(7)), Call: 20, Return: 21, Out: nats.Fail, Code: nats.ErrCodeWrongLastSeq, Err: "wrong last sequence: 1 (400/10071)"}),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "b")},
		want: []string{"nonlinearizable-read"},
	}, {
		name: "refused write visible",
		hist: h(op{Client: 1, Kind: "cas", Subject: "reg.0", Value: "c", Expect: new(uint64(7)), Call: 20, Return: 21, Out: nats.Fail, Code: nats.ErrCodeWrongLastSeq, Err: "wrong last sequence: 0 (400/10071)"}),
		msgs: []msg{m(1, "reg.0", "c")},
		want: []string{"condition", "failed-visible"},
	}, {
		name: "dirty read",
		hist: h(put(0, "reg.0", "a", 0, 10, 1), get(1, "reg.0", 20, 21, 2, "x")),
		msgs: []msg{m(1, "reg.0", "a")},
		want: []string{"dirty-read"},
	}, {
		name: "duplicate under a message id",
		hist: h(func() op { o := put(0, "reg.0", "a", 0, 0, 0); o.MsgID = "a"; return o }(),
			func() op { o := put(0, "reg.0", "a", 5, 10, 2); o.MsgID, o.Attempt = "a", 1; return o }()),
		msgs: []msg{m(1, "reg.0", "a"), m(2, "reg.0", "a")},
		want: []string{"duplicate"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkAll(t, tc.hist, tc.msgs); !slices.Equal(got, tc.want) {
				t.Fatalf("found %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLastSeqIn(t *testing.T) {
	for msg, want := range map[string]uint64{
		"wrong last sequence: 5308 (400/10071)": 5308,
		"nats: wrong last sequence: 0":          0,
	} {
		if got, ok := lastSeqIn(msg); !ok || got != want {
			t.Errorf("lastSeqIn(%q) = %d, %v; want %d", msg, got, ok, want)
		}
	}
	if _, ok := lastSeqIn("wrong last sequence (400/10164)"); ok {
		t.Errorf("a refusal that names no sequence named one")
	}
}

func m(seq uint64, subject, value string) msg { return msg{Seq: seq, Subject: subject, Value: value} }

func TestCheckBatches(t *testing.T) {
	b := func(values, subjects []string, out nats.Outcome, seq uint64, count int) op {
		return op{Kind: "batch", Stream: batchStream, Values: values, Subjects: subjects, Out: out, Seq: seq, Count: count, Call: 1, Return: 2}
	}
	for _, tc := range []struct {
		name string
		hist []op
		msgs []msg
		want []string
	}{{
		name: "whole",
		hist: h(b([]string{"a.0", "a.1"}, []string{"batch.0", "batch.1"}, nats.OK, 3, 2)),
		msgs: []msg{m(1, "batch.9", "x"), m(2, "batch.0", "a.0"), m(3, "batch.1", "a.1")},
	}, {
		name: "none of an unknown one",
		hist: h(b([]string{"a.0", "a.1"}, []string{"batch.0", "batch.1"}, nats.Unknown, 0, 0)),
	}, {
		name: "partial",
		hist: h(b([]string{"a.0", "a.1", "a.2"}, []string{"batch.0", "batch.1", "batch.0"}, nats.Unknown, 0, 0)),
		msgs: []msg{m(1, "batch.0", "a.0"), m(2, "batch.1", "a.1")},
		want: []string{"batch-partial"},
	}, {
		name: "interleaved",
		hist: h(b([]string{"a.0", "a.1"}, []string{"batch.0", "batch.1"}, nats.Unknown, 0, 0)),
		msgs: []msg{m(1, "batch.0", "a.0"), m(2, "batch.1", "zz"), m(3, "batch.1", "a.1")},
		want: []string{"batch-interleaved"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinal(batchStream, tc.msgs)
			if got := kinds(checkBatches(tc.hist, f)); !slices.Equal(got, tc.want) {
				t.Fatalf("found %v, want %v", got, tc.want)
			}
		})
	}
	// The per-message checks see a batch's messages as writes of their own.
	hist := h(b([]string{"a.0", "a.1"}, []string{"batch.0", "batch.1"}, nats.OK, 3, 2))
	f := newFinal(batchStream, []msg{m(1, "batch.0", "a.0"), m(2, "batch.1", "a.1")})
	if got := kinds(checkStream(hist, batchStream, f)); !slices.Equal(got, []string{"ack-mismatch", "lost"}) {
		t.Fatalf("a batch acked one past where it is: %v", got)
	}
}

func TestCheckCounters(t *testing.T) {
	c := func(seq uint64, value, incr, val string) msg {
		return msg{Seq: seq, Subject: "ctr.0", Value: value, Incr: incr, Val: val}
	}
	good := []msg{c(1, "a", "+5", "5"), c(2, "b", "-2", "3"), c(3, "c", "+10", "13")}
	hist := h(
		op{Kind: "incr", Stream: ctrStream, Subject: "ctr.0", Value: "b", Incr: -2, Out: nats.OK, Seq: 2, Val: "3"},
		op{Kind: "cget", Stream: ctrStream, Subject: "ctr.0", Out: nats.OK, Found: true, Seq: 3, Val: "13", Read: "c"},
	)
	if got := kinds(checkCounters(hist, newFinal(ctrStream, good))); len(got) > 0 {
		t.Fatalf("good counters: %v", got)
	}
	bad := []msg{c(1, "a", "+5", "5"), c(2, "b", "-2", "4"), c(3, "c", "+10", "14")}
	if got := kinds(checkCounters(hist, newFinal(ctrStream, bad))); !slices.Equal(got, []string{"counter-ack", "counter-arith", "counter-read"}) {
		t.Fatalf("bad counters: %v", got)
	}
}
