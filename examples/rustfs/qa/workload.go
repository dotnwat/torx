//go:build unix

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/rustfs/qa/s3"
)

// A value is an object's contents, named by an id no other write uses:
// "c3-17" is client 3's 17th write. Its body is a header naming the id and
// its size, then bytes generated from the id, so that any read can be
// checked byte for byte against the write that made it, without the reader
// remembering the body.

// body generates the body of value id at size bytes, the header included
// when there is room for it.
func body(id string, size int) []byte {
	header := fmt.Sprintf("torx %s %d\n", id, size)
	b := make([]byte, size)
	n := copy(b, header)
	h := fnv.New64a()
	h.Write([]byte(id))
	rng := rand.New(rand.NewPCG(h.Sum64(), uint64(size)))
	for i := n; i < size; i += 8 {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], rng.Uint64())
		copy(b[i:], w[:])
	}
	return b
}

// identify reads which value a body is, and checks it is that value's body
// to the byte. A body too short for its header is identified by size alone
// against the values the caller knows, so identify returns "" for it and
// the caller looks the ETag up instead.
func identify(b []byte) (id string, err error) {
	line, _, ok := bytes.Cut(b, []byte("\n"))
	if !ok || !bytes.HasPrefix(line, []byte("torx ")) {
		return "", nil
	}
	fields := strings.Fields(string(line))
	if len(fields) != 3 {
		return "", fmt.Errorf("a header %q", line)
	}
	id = fields[1]
	size, err := strconv.Atoi(fields[2])
	if err != nil {
		return "", fmt.Errorf("a header %q", line)
	}
	if size != len(b) {
		return id, fmt.Errorf("%d bytes of value %s, which has %d", len(b), id, size)
	}
	want := body(id, size)
	if i := mismatch(b, want); i >= 0 {
		return id, fmt.Errorf("value %s differs from byte %d of %d", id, i, size)
	}
	return id, nil
}

// mismatch is the first offset at which a and b differ, or -1.
func mismatch(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return min(len(a), len(b))
	}
	return -1
}

// etagOf is the ETag S3 gives an object written in one piece: the quoted
// MD5 of its body.
func etagOf(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// multipartETag is the ETag S3 gives an object assembled from parts: the
// MD5 of the parts' MD5s, and how many there were.
func multipartETag(parts [][]byte) string {
	var sums []byte
	for _, p := range parts {
		s := md5.Sum(p)
		sums = append(sums, s[:]...)
	}
	sum := md5.Sum(sums)
	return `"` + hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)) + `"`
}

// values maps every value written to its ETag and back, so a read that
// returns only an ETag -- a listing, a HEAD -- names the value it saw.
type values struct {
	mu     sync.Mutex
	byETag map[string]string
	etag   map[string]string
	size   map[string]int
}

func newValues() *values {
	return &values{byETag: map[string]string{}, etag: map[string]string{}, size: map[string]int{}}
}

func (v *values) add(id, etag string, size int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.byETag[etag] = id
	v.etag[id] = etag
	v.size[id] = size
}

// lookup names the value with ETag etag, "?" for one never written.
func (v *values) lookup(etag string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	if id, ok := v.byETag[etag]; ok {
		return id
	}
	return "?" + etag
}

func (v *values) etagOf(id string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.etag[id]
}

// mix is the workload's operation weights.
var mix = []struct {
	f      string
	weight int
}{
	{"put", 6},
	{"put-if-absent", 2},
	{"put-if-match", 5},
	{"multipart", 1},
	{"get", 8},
	{"head", 2},
	{"delete", 2},
	{"delete-if-match", 1},
	{"list", 2},
}

// workload is the clients' shared configuration.
type workload struct {
	bucket  string
	keys    []string
	servers []string // node names, in the order of each client's conns
	h       *History
	vals    *values
	// sizes draws a write's size.
	sizes func(rng *rand.Rand) int
	// timeout bounds each operation; a large body gets more.
	timeout time.Duration
	// noMultipart leaves multipart uploads out of the mix.
	noMultipart bool
	// anomalies collects reads whose bodies are not a value's.
	anomalies *anomalies
}

// client is one client process: it issues operations one at a time, each
// to a server picked at random, until its context ends.
type client struct {
	w     *workload
	label string // the process name in the history, if not client-<id>
	id    int
	rng   *rand.Rand
	conns []*s3.Client // one per server, in the order of w.servers
	seq   int
	// seen is the value this client last saw or wrote at each key, the
	// precondition its put-if-match and delete-if-match send, and history
	// every value it has seen there.
	seen    map[string]string
	history map[string][]string
}

func (c *client) name() string {
	if c.label != "" {
		return c.label
	}
	return "client-" + strconv.Itoa(c.id)
}

func (c *client) run(ctx context.Context) {
	total := 0
	for _, m := range mix {
		total += m.weight
	}
	for ctx.Err() == nil {
		pick := c.rng.IntN(total)
		f := ""
		for _, m := range mix {
			if pick < m.weight {
				f = m.f
				break
			}
			pick -= m.weight
		}
		if f == "multipart" && c.w.noMultipart {
			f = "put"
		}
		if op := c.do(ctx, f); op.Outcome == Fail && (op.Status == 0 || op.Code == "NotReady") {
			// The server is down, or starting: do not spin on it.
			sleepCtx(ctx, 50*time.Millisecond)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// newValue mints the id of this client's next write.
func (c *client) newValue() string {
	c.seq++
	return "c" + strconv.Itoa(c.id) + "-" + strconv.Itoa(c.seq)
}

func (c *client) do(ctx context.Context, f string) Op {
	return c.doKey(ctx, f, c.rng.IntN(len(c.conns)), c.w.keys[c.rng.IntN(len(c.w.keys))])
}

// doKey issues one operation f on key through server, records it, and
// returns it.
func (c *client) doKey(ctx context.Context, f string, server int, key string) Op {
	conn := c.conns[server]
	op := Op{Process: c.name(), F: f, Node: c.w.servers[server], Key: key}
	timeout := c.w.timeout
	switch f {
	case "put", "put-if-absent", "put-if-match", "multipart":
		op.Value = c.newValue()
		op.Size = c.w.sizes(c.rng)
		if f == "multipart" {
			op.Size = max(op.Size, 2*multipartPart+1)
		}
		timeout += time.Duration(op.Size>>20) * time.Second
	case "list", "list-versions":
		op.Key = ""
	}
	if f == "put-if-match" || f == "delete-if-match" {
		op.Expect = c.seen[key]
		if old := c.history[key]; len(old) > 0 && c.rng.IntN(4) == 0 {
			// Now and then a value seen before, likely overwritten since.
			op.Expect = old[c.rng.IntN(len(old))]
		}
		if op.Expect == "" {
			// Nothing seen to expect yet: expect a value no write made,
			// which must fail.
			op.Expect = "none"
		}
	}
	// An operation in flight when the run ends runs on to its answer, or
	// its timeout: cut short, it would be one more whose outcome is unknown.
	octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	op.Start = c.w.h.Now()
	var err error
	unsent := false
	switch f {
	case "put", "put-if-absent", "put-if-match":
		b := body(op.Value, op.Size)
		c.w.vals.add(op.Value, etagOf(b), op.Size)
		var cond s3.Cond
		switch f {
		case "put-if-absent":
			cond.IfNoneMatch = "*"
		case "put-if-match":
			cond.IfMatch = c.expectETag(op.Expect)
		}
		var wr s3.Written
		wr, err = conn.PutObject(octx, c.w.bucket, key, b, cond)
		op.Version = wr.VersionID
	case "multipart":
		var completing bool
		completing, err = c.multipart(octx, conn, &op)
		// An upload that failed before its completion was sent, which is
		// what makes the object, certainly had no effect.
		unsent = err != nil && !completing
	case "get":
		var o s3.Object
		o, err = conn.GetObject(octx, c.w.bucket, key, "")
		if err == nil {
			op.Value = c.readValue(&op, o.Body, o.ETag)
		} else if s3.IsCode(err, "NoSuchKey") {
			err = nil // a read of no object
		}
	case "head":
		var o s3.Object
		o, err = conn.HeadObject(octx, c.w.bucket, key)
		if err == nil {
			op.Value = c.w.vals.lookup(o.ETag)
		} else if s3.IsCode(err, "NoSuchKey") {
			err = nil
		}
	case "delete":
		var del s3.Deleted
		del, err = conn.DeleteObject(octx, c.w.bucket, key, "", s3.Cond{})
		op.Version = del.VersionID
	case "delete-if-match":
		var del s3.Deleted
		del, err = conn.DeleteObject(octx, c.w.bucket, key, "", s3.Cond{IfMatch: c.expectETag(op.Expect)})
		op.Version = del.VersionID
	case "list-versions":
		var vs []s3.Version
		vs, err = conn.ListVersions(octx, c.w.bucket, "")
		for _, v := range vs {
			e := VersionEntry{Key: v.Key, Version: v.VersionID, DeleteMarker: v.DeleteMarker, Modified: v.LastModified}
			if !v.DeleteMarker {
				e.Value = c.w.vals.lookup(v.ETag)
			}
			op.Versions = append(op.Versions, e)
		}
	case "list":
		var es []s3.Entry
		es, err = conn.ListObjects(octx, c.w.bucket, "")
		if err == nil {
			op.Listed = map[string]string{}
			for _, e := range es {
				op.Listed[e.Key] = c.w.vals.lookup(e.ETag)
			}
		}
	}
	op.End = c.w.h.Now()
	record(&op, err)
	if unsent {
		op.Outcome = Fail
	}
	c.w.h.Add(op)
	c.learn(op)
	return op
}

// expectETag is the ETag of value id, or one no object has for a value no
// write made.
func (c *client) expectETag(id string) string {
	if e := c.w.vals.etagOf(id); e != "" {
		return e
	}
	return `"00000000000000000000000000000000"`
}

// readValue names the value a GET returned, and checks its body is that
// value's, byte for byte, and its ETag the value's ETag.
func (c *client) readValue(op *Op, b []byte, etag string) string {
	id, err := identify(b)
	if id == "" && err == nil {
		id = c.w.vals.lookup(etag)
		err = fmt.Errorf("a %d-byte body with no header, and ETag %s", len(b), etag)
	}
	if err == nil && etag != c.w.vals.etagOf(id) {
		err = fmt.Errorf("value %s with ETag %s, which was written as %s", id, etag, c.w.vals.etagOf(id))
	}
	if err != nil {
		c.w.anomalies.add(anomaly{Kind: "corrupt-read", Key: op.Key, Node: op.Node, Process: op.Process,
			At: op.Start, Detail: err.Error()})
	}
	return id
}

// learn remembers what an operation showed this client of its key.
func (c *client) learn(op Op) {
	if op.Outcome != Ok {
		return
	}
	saw := func(k, v string) {
		c.seen[k] = v
		if v != "" && !slices.Contains(c.history[k], v) {
			c.history[k] = append(c.history[k], v)
		}
	}
	switch op.F {
	case "put", "put-if-absent", "put-if-match", "multipart", "get", "head":
		saw(op.Key, op.Value)
	case "delete", "delete-if-match":
		saw(op.Key, "")
	case "list":
		for k, v := range op.Listed {
			saw(k, v)
		}
	}
}

// multipartPart is the size of each part of a multipart upload but the
// last: S3's smallest.
const multipartPart = 5 << 20

// multipart writes op.Value in parts of multipartPart bytes, the last
// smaller. Its ETag is known before the upload starts, so the value can be
// recognized wherever it turns up, however the upload ends.
func (c *client) multipart(ctx context.Context, conn *s3.Client, op *Op) (completing bool, err error) {
	b := body(op.Value, op.Size)
	var parts [][]byte
	for off := 0; off < len(b); off += multipartPart {
		parts = append(parts, b[off:min(off+multipartPart, len(b))])
	}
	c.w.vals.add(op.Value, multipartETag(parts), op.Size)
	id, err := conn.CreateMultipartUpload(ctx, c.w.bucket, op.Key)
	if err != nil {
		return false, err
	}
	etags := make([]string, len(parts))
	for i, p := range parts {
		if etags[i], err = conn.UploadPart(ctx, c.w.bucket, op.Key, id, i+1, p); err != nil {
			c.abort(conn, op.Key, id)
			return false, err
		}
	}
	var wr s3.Written
	wr, err = conn.CompleteMultipartUpload(ctx, c.w.bucket, op.Key, id, etags, s3.Cond{})
	op.Version = wr.VersionID
	if err != nil && classify(err) == Fail {
		c.abort(conn, op.Key, id)
	}
	return true, err
}

// abort abandons an upload that cannot complete, so its parts do not
// linger; whether it works does not matter to the history.
func (c *client) abort(conn *s3.Client, key, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.AbortMultipartUpload(ctx, c.w.bucket, key, id)
}
