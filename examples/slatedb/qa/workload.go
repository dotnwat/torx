//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/slatedb/qa/slatedb"
)

// value makes a unique value named name: "name|" and then bytes derived
// from the name, to a length drawn from rng, so that a value read can be
// checked byte for byte.
func (j *chaosJob) value(name string, rng *rand.Rand) string {
	n := rng.IntN(j.valueSize)
	return name + "|" + pad(name, n)
}

func pad(name string, n int) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	x := h.Sum64()
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = alphabet[x%uint64(len(alphabet))]
	}
	return string(b)
}

// intact reports whether v is a value value made, whole.
func intact(v string) bool {
	name, rest, ok := strings.Cut(v, "|")
	return ok && rest == pad(name, len(rest))
}

// begin starts recording an operation.
func (j *chaosJob) begin(client int, proc string, kind opKind) *op {
	return &op{Client: client, Proc: proc, Launch: j.launchOf(proc), Kind: kind, Call: time.Now()}
}

// end records a finished operation.
func (j *chaosJob) end(o *op) {
	j.mu.Lock()
	defer j.mu.Unlock()
	o.ID = len(j.hist)
	j.hist = append(j.hist, *o)
	for k, r := range o.Read {
		if r.Found && !intact(r.Value) {
			j.notes = append(j.notes, anomaly{Kind: "corrupt", Key: k, Ops: []int{o.ID},
				Detail: fmt.Sprintf("op %d read a value of %s that is not one written: %q", o.ID, k, r.Value)})
		}
	}
}

// refused reports whether err says the request never reached the process:
// the operation did not happen.
func refused(err error) bool {
	oe, ok := errors.AsType[*net.OpError](err)
	return ok && oe.Op == "dial"
}

// snapshot opens a snapshot on writer w, scans it two to four times over a
// few seconds while the other clients write and the compactor and
// collector work, and closes it.
func (j *chaosJob) snapshot(ctx context.Context, c *slatedb.Client, w string, id int, rng *rand.Rand) {
	sid, seq, err := c.SnapshotOpen(ctx)
	if err != nil {
		return
	}
	launch := j.launchOf(w)
	name := fmt.Sprintf("%s/%d/%d", w, launch, sid)
	for i := range 2 + rng.IntN(3) {
		if i > 0 {
			select {
			case <-time.After(time.Duration(500+rng.IntN(2500)) * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
		o := j.begin(id, w, opScan)
		o.Snapshot, o.SnapSeq = name, seq
		if o.Launch != launch {
			return // the writer restarted: the snapshot is gone
		}
		kvs, err := c.SnapshotScan(ctx, sid)
		o.Return = time.Now()
		if err != nil {
			o.Outcome, o.Err = "fail", err.Error()
			j.end(o)
			return
		}
		o.Outcome = "ok"
		o.Read = map[string]read{}
		for _, k := range j.keys {
			o.Read[k] = read{}
		}
		for _, kv := range kvs {
			if _, ok := o.Read[kv.Key]; ok {
				o.Read[kv.Key] = read{Found: true, Value: kv.Value, Seq: kv.Seq}
			}
		}
		j.end(o)
	}
	_ = c.SnapshotClose(ctx, sid)
}

// clone clones the writer's database to a path of its own, and scans the
// clone two or three times over a few seconds; the final reads scan it
// again, once the source has compacted and collected for the rest of the
// run.
func (j *chaosJob) clone(ctx context.Context, c *slatedb.Client, w string, id, n int, rng *rand.Rand) {
	name := fmt.Sprintf("clone-%d-%d", id, n)
	if err := c.Clone(ctx, name); err != nil {
		return
	}
	j.mu.Lock()
	j.cloneNames = append(j.cloneNames, name)
	j.mu.Unlock()
	for i := range 2 + rng.IntN(2) {
		if i > 0 {
			select {
			case <-time.After(time.Duration(1000+rng.IntN(3000)) * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
		_ = j.scanClone(ctx, c, w, id, name) // the history has the outcome
	}
}

func (j *chaosJob) scanClone(ctx context.Context, c *slatedb.Client, w string, id int, name string) error {
	o := j.begin(id, w, opScan)
	o.Clone = name
	kvs, err := c.CloneScan(ctx, name)
	o.Return = time.Now()
	if err != nil {
		o.Outcome, o.Err = "fail", err.Error()
		j.end(o)
		return err
	}
	o.Outcome = "ok"
	o.Read = map[string]read{}
	for _, k := range j.keys {
		o.Read[k] = read{}
	}
	for _, kv := range kvs {
		if _, ok := o.Read[kv.Key]; ok {
			o.Read[kv.Key] = read{Found: true, Value: kv.Value, Seq: kv.Seq}
		}
	}
	j.end(o)
	return nil
}

// lastRefused reports whether client id's last operation was refused a
// connection.
func (j *chaosJob) lastRefused(id int) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i := len(j.hist) - 1; i >= 0 && i >= len(j.hist)-64; i-- {
		if j.hist[i].Client == id {
			return strings.Contains(j.hist[i].Err, "connection refused")
		}
	}
	return false
}

// finishWrite records a write's outcome.
func (j *chaosJob) finishWrite(o *op, seq uint64, err error) {
	o.Return = time.Now()
	o.Seq = seq
	switch {
	case err == nil:
		o.Outcome = "ok"
	case slatedb.Definite(err) || refused(err):
		o.Outcome, o.Err = "fail", err.Error()
	default:
		o.Outcome, o.Err = "info", err.Error()
	}
	j.end(o)
}

// client runs operations until run ends: mostly durable writes and reads
// of durable data from the writer, some writes that do not await
// durability, batches, scans, and reads from the reader. An operation in
// flight when run ends runs to its end under ctx, so that every write has
// landed, or failed, before the final reads.
func (j *chaosJob) client(ctx, run context.Context, id int, rng *rand.Rand) {
	clients := map[string]*slatedb.Client{}
	defer func() {
		for _, c := range clients {
			c.CloseIdle()
		}
	}()
	clientOf := func(p string) *slatedb.Client {
		if c, ok := clients[p]; ok {
			return c
		}
		c := slatedb.NewClient(j.db.URL(p))
		clients[p] = c
		return c
	}
	for n := 0; run.Err() == nil; n++ {
		w := j.writer()
		c := clientOf(w)
		octx, cancel := context.WithTimeout(ctx, opTimeout)
		name := fmt.Sprintf("c%d.%d", id, n)
		x := rng.IntN(100)
		if j.isolation != "" && x < 15 {
			j.txn(octx, c, w, id, n, rng)
			cancel()
			continue
		}
		if j.merges && x >= 15 && x < 25 {
			j.merge(octx, c, w, id, n, rng)
			cancel()
			continue
		}
		switch {
		case x < 35: // put
			k := pick(rng, j.keys...)
			v := j.value(name, rng)
			o := j.begin(id, w, opPut)
			o.Writes = []write{{Key: k, Value: &v}}
			o.Durable = rng.IntN(5) > 0
			seq, err := c.Put(octx, k, &v, o.Durable)
			j.finishWrite(o, seq, err)
		case x < 40: // delete
			k := pick(rng, j.keys...)
			o := j.begin(id, w, opPut)
			o.Writes = []write{{Key: k}}
			o.Durable = true
			seq, err := c.Put(octx, k, nil, true)
			j.finishWrite(o, seq, err)
		case x < 50: // batch
			o := j.begin(id, w, opBatch)
			var ops []slatedb.Op
			seen := map[string]bool{}
			for i := range 1 + rng.IntN(4) {
				k := pick(rng, j.keys...)
				if seen[k] {
					continue
				}
				seen[k] = true
				var v *string
				if rng.IntN(5) > 0 {
					s := j.value(fmt.Sprintf("%s.%d", name, i), rng)
					v = &s
				}
				ops = append(ops, slatedb.Op{Key: k, Value: v})
				o.Writes = append(o.Writes, write{Key: k, Value: v})
			}
			o.Durable = true
			seq, err := c.Batch(octx, ops, true)
			j.finishWrite(o, seq, err)
		case x < 85: // get
			k := pick(rng, j.keys...)
			p, reader := w, false
			if j.reader && rng.IntN(4) == 0 && j.db.Running(readerP) {
				p, reader = readerP, true
			}
			o := j.begin(id, p, opGet)
			o.Key, o.Reader = k, reader
			v, err := clientOf(p).Get(octx, k, slatedb.Remote)
			o.Return = time.Now()
			if err != nil {
				o.Outcome, o.Err = "fail", err.Error()
			} else {
				o.Outcome = "ok"
				o.Read = map[string]read{k: {Found: v.Found, Value: v.Value, Seq: v.Seq}}
			}
			j.end(o)
		case x < 87: // a snapshot, scanned now and again over a few seconds
			j.snapshot(octx, c, w, id, rng)
		case x < 88 && j.clones: // a clone, scanned now and again
			j.clone(octx, c, w, id, n, rng)
		default: // scan
			p, reader := w, false
			if j.reader && rng.IntN(4) == 0 && j.db.Running(readerP) {
				p, reader = readerP, true
			}
			o := j.begin(id, p, opScan)
			o.Reader = reader
			kvs, err := clientOf(p).Scan(octx, "", "k~", slatedb.Remote)
			o.Return = time.Now()
			if err != nil {
				o.Outcome, o.Err = "fail", err.Error()
			} else {
				o.Outcome = "ok"
				o.Read = map[string]read{}
				for _, k := range j.keys {
					o.Read[k] = read{}
				}
				for _, kv := range kvs {
					o.Read[kv.Key] = read{Found: true, Value: kv.Value, Seq: kv.Seq}
				}
			}
			j.end(o)
		}
		cancel()
		if run.Err() == nil && rng.IntN(10) == 0 {
			time.Sleep(time.Duration(rng.IntN(50)) * time.Millisecond)
		}
		if j.lastRefused(id) {
			// The writer is down: wait for it rather than spin.
			time.Sleep(100 * time.Millisecond)
		}
	}
}
