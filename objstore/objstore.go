// Package objstore is an object store a job serves to the system it tests:
// an in-memory server of the part of Amazon S3's HTTP API that object-store
// clients use, whose faults the job injects at will and whose every request
// it records.
//
// Systems built on object storage -- databases that keep their data in a
// bucket, tiered storage, log stores -- trust a handful of guarantees from
// it: a write is atomic, a read sees the latest write, a conditional write
// (If-None-Match, If-Match) takes effect only if its condition holds when
// it does. They must also survive what an object store does under load and
// on a bad network: an error before a request takes effect, an error or a
// lost connection after it took effect, a request that takes seconds, a
// client cut off from the store. A real store does those things rarely and
// never on cue; this one does them when a job asks, to the clients, the
// operations, and the keys a Rule names, while keeping the guarantees
// exactly: every request takes effect, or not, atomically under one lock.
//
// Every request is recorded (History) with the client that sent it, what it
// was, what the store did, and what it answered, so a job can read what
// the store saw against what the system claims, and an object of the store
// can be read directly (Get, List) to audit what the system left behind.
// WriteTar archives the objects, to start the system again from what a
// failed run left.
//
// Clients are told apart by the access key ID in a request's signature,
// which the store reads but does not check: give each process of the
// system an access key of its own, and a rule can single it out. The store
// serves path-style requests (http://host/bucket/key); point a client at
// it with path-style addressing, plain HTTP, and any secret.
package objstore

import (
	"archive/tar"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Op is the kind of a request.
type Op string

// The kinds of request the store serves.
const (
	OpPut      Op = "put"         // PUT of an object, conditional or not
	OpCopy     Op = "copy"        // PUT with x-amz-copy-source
	OpGet      Op = "get"         // GET of an object, or of a range of it
	OpHead     Op = "head"        // HEAD of an object
	OpDelete   Op = "delete"      // DELETE of an object
	OpDeletes  Op = "deletes"     // POST ?delete: a batch of deletes
	OpList     Op = "list"        // GET of a bucket: ListObjectsV2
	OpCreateMP Op = "mp-create"   // POST ?uploads: start a multipart upload
	OpPart     Op = "mp-part"     // PUT ?partNumber&uploadId: upload a part
	OpComplete Op = "mp-complete" // POST ?uploadId: complete a multipart upload
	OpAbort    Op = "mp-abort"    // DELETE ?uploadId: abort a multipart upload
	OpBucket   Op = "bucket"      // PUT, HEAD, or DELETE of a bucket
)

// Mutating reports whether a request of kind op can change what the store
// holds.
func (op Op) Mutating() bool {
	switch op {
	case OpPut, OpCopy, OpDelete, OpDeletes, OpComplete, OpBucket:
		return true
	}
	return false
}

// Object is what the store knows of an object.
type Object struct {
	Key          string
	Size         int64
	ETag         string // quoted, as S3 sends it
	LastModified time.Time
	// Meta is the object's user metadata, by name without the
	// x-amz-meta- prefix, lower case.
	Meta map[string]string
	// ContentType is the Content-Type it was written with, if any.
	ContentType string
}

type object struct {
	Object
	data []byte
}

type upload struct {
	bucket, key string
	header      http.Header // the create request's, for metadata
	parts       map[int][]byte
}

// Options configure a Server.
type Options struct {
	// Now is the store's clock, for Last-Modified; time.Now if nil.
	Now func() time.Time
	// Rand draws whether a rule's fault fires; a fixed seed of its own if
	// nil. Draws are serialized.
	Rand *rand.Rand
	// UniqueETags gives every write an ETag of its own. Otherwise, as in
	// S3, an object's ETag is the MD5 of its content (of its parts', for
	// a multipart upload), and two writes of the same bytes share one --
	// which an If-Match cannot tell apart.
	UniqueETags bool
	// Logf, if set, is told of every fault the store injects.
	Logf func(format string, args ...any)
}

// Server is the store, and the HTTP server in front of it.
type Server struct {
	opts Options

	mu      sync.Mutex
	buckets map[string]map[string]*object
	uploads map[string]*upload
	nextID  int64

	rulesMu sync.Mutex
	rules   []*rule
	ruleSeq int
	rngMu   sync.Mutex
	rng     *rand.Rand

	histMu  sync.Mutex
	history []Event
	seq     atomic.Int64

	srv *http.Server
	wg  sync.WaitGroup
}

// New returns a store with no buckets.
func New(opts Options) *Server {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	rng := opts.Rand
	if rng == nil {
		rng = rand.New(rand.NewPCG(1, 2))
	}
	return &Server{
		opts:    opts,
		buckets: map[string]map[string]*object{},
		uploads: map[string]*upload{},
		rng:     rng,
	}
}

// Serve listens on addr ("host:port", port 0 for any) and serves the store
// there until Close. It returns the endpoint to point clients at:
// "http://host:port".
func (s *Server) Serve(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 30 * time.Second}
	s.wg.Go(func() { _ = s.srv.Serve(ln) })
	return "http://" + ln.Addr().String(), nil
}

// Close stops serving, cutting off the requests in flight.
func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Close()
	s.wg.Wait()
	return err
}

// CreateBucket makes an empty bucket, if there is none of that name.
func (s *Server) CreateBucket(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] == nil {
		s.buckets[name] = map[string]*object{}
	}
}

// Get returns an object's content and what the store knows of it.
func (s *Server) Get(bucket, key string) ([]byte, Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.buckets[bucket][key]
	if o == nil {
		return nil, Object{}, false
	}
	return slices.Clone(o.data), o.copyInfo(), true
}

// List returns the objects of a bucket whose keys begin with prefix, in key
// order.
func (s *Server) List(bucket, prefix string) []Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Object
	for k, o := range s.buckets[bucket] {
		if strings.HasPrefix(k, prefix) {
			out = append(out, o.copyInfo())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Usage returns how many objects the store holds, and their bytes.
func (s *Server) Usage() (objects int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		for _, o := range b {
			objects++
			bytes += o.Size
		}
	}
	return objects, bytes
}

// WriteTar writes every object to w as a tar archive, an entry bucket/key
// for each, so that a run's objects outlive it: unpacked, a client that
// reads a local directory as a bucket (object_store's LocalFileSystem, for
// one) can start from them. Metadata is not written.
func (s *Server) WriteTar(w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tw := tar.NewWriter(w)
	for _, name := range slices.Sorted(maps.Keys(s.buckets)) {
		b := s.buckets[name]
		for _, k := range slices.Sorted(maps.Keys(b)) {
			o := b[k]
			hdr := &tar.Header{Name: name + "/" + k, Mode: 0o644, Size: o.Size, ModTime: o.LastModified, Typeflag: tar.TypeReg}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if _, err := tw.Write(o.data); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

func (o *object) copyInfo() Object {
	c := o.Object
	c.Meta = maps.Clone(o.Meta)
	if c.Meta == nil {
		c.Meta = map[string]string{}
	}
	return c
}

// request is one request, parsed.
type request struct {
	op       Op
	client   string
	bucket   string
	key      string
	uploadID string
	part     int
	match    string // what a rule's Prefix is matched against
	ev       *Event
}

// ServeHTTP serves one S3 request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := s.parse(r)
	ev := &Event{
		Seq:    s.seq.Add(1),
		Start:  s.opts.Now(),
		Client: req.client,
		Op:     req.op,
		Bucket: req.bucket,
		Key:    req.key,
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		ev.Cond = "If-None-Match: " + v
	}
	if v := r.Header.Get("If-Match"); v != "" {
		ev.Cond = strings.TrimSpace(ev.Cond + " If-Match: " + v)
	}
	if v := r.Header.Get("Range"); v != "" {
		ev.Range = v
	}
	req.ev = ev
	defer func() {
		ev.End = s.opts.Now()
		s.histMu.Lock()
		s.history = append(s.history, *ev)
		s.histMu.Unlock()
	}()

	// The request's body is read before any fault, so that a fault after
	// its effect has the whole request to apply, as a store would.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		ev.Status = -1
		return
	}
	switch req.op {
	case OpList:
		req.match = r.URL.Query().Get("prefix")
	case OpDeletes:
		if keys, _, err := parseDeletes(body); err == nil && len(keys) > 0 {
			req.match = keys[0]
		}
	default:
		req.match = req.key
	}
	f := s.draw(req)
	if f != nil {
		ev.Fault = f.Name + ":" + string(f.Action)
		if s.opts.Logf != nil {
			s.opts.Logf("objstore: fault %s on %s %s %s/%s", ev.Fault, req.client, req.op, req.bucket, req.key)
		}
		switch f.Action {
		case Fail:
			s.fail(w, ev, f.status(), "injected failure")
			return
		case Drop:
			ev.Status = -1
			hangUp(w)
			return
		case Delay:
			t := time.NewTimer(f.Delay)
			select {
			case <-t.C:
			case <-r.Context().Done():
				t.Stop()
				ev.Status = -1 // the client gave up; the request has no effect
				return
			}
		case Stall:
			// The request takes effect after the delay whether or not the
			// client is still there to hear of it.
			time.Sleep(f.Delay)
		case Hang:
			<-r.Context().Done()
			ev.Status = -1
			return
		}
	}

	rec := &recorder{header: http.Header{}}
	s.handle(rec, r, req, body)
	ev.Status = rec.status
	ev.ETag = rec.header.Get("ETag")

	if f != nil {
		switch f.Action {
		case FailAfter:
			s.fail(w, ev, f.status(), "injected failure after the request took effect")
			return
		case DropAfter:
			ev.Status = -1
			hangUp(w)
			return
		case DelayAfter:
			t := time.NewTimer(f.Delay)
			select {
			case <-t.C:
			case <-r.Context().Done():
				t.Stop()
				ev.Status = -1
				return
			}
		}
	}
	if r.Context().Err() != nil {
		ev.Status = -1 // the client gave up before the answer
		return
	}
	rec.flush(w)
}

// recorder buffers a response, so a fault after a request's effect can
// replace it.
type recorder struct {
	header http.Header
	status int
	body   []byte
}

func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}
func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body = append(r.body, p...)
	return len(p), nil
}

func (r *recorder) flush(w http.ResponseWriter) {
	maps.Copy(w.Header(), r.header)
	if r.status == 0 {
		r.status = http.StatusOK
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body)
}

// hangUp closes the request's connection without an answer.
func hangUp(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetLinger(0) // a reset, as a dead peer's
			}
			_ = conn.Close()
			return
		}
	}
	panic(http.ErrAbortHandler)
}

func (s *Server) fail(w http.ResponseWriter, ev *Event, status int, msg string) {
	ev.Status = status
	code := "InternalError"
	switch status {
	case http.StatusServiceUnavailable:
		code = "SlowDown"
	case http.StatusForbidden:
		code = "AccessDenied"
	}
	writeError(w, status, code, msg)
}

// parse works out what a request is and who sent it.
func (s *Server) parse(r *http.Request) request {
	var req request
	req.client = accessKey(r.Header.Get("Authorization"))
	p := strings.TrimPrefix(r.URL.Path, "/")
	req.bucket, req.key, _ = strings.Cut(p, "/")
	q := r.URL.Query()
	req.uploadID = q.Get("uploadId")
	req.part, _ = strconv.Atoi(q.Get("partNumber"))
	switch {
	case req.key == "" && r.Method == http.MethodGet:
		req.op = OpList
	case req.key == "" && r.Method == http.MethodPost && q.Has("delete"):
		req.op = OpDeletes
	case req.key == "":
		req.op = OpBucket
	case r.Method == http.MethodPost && q.Has("uploads"):
		req.op = OpCreateMP
	case r.Method == http.MethodPost && req.uploadID != "":
		req.op = OpComplete
	case r.Method == http.MethodPut && req.uploadID != "":
		req.op = OpPart
	case r.Method == http.MethodDelete && req.uploadID != "":
		req.op = OpAbort
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		req.op = OpCopy
	case r.Method == http.MethodPut:
		req.op = OpPut
	case r.Method == http.MethodGet:
		req.op = OpGet
	case r.Method == http.MethodHead:
		req.op = OpHead
	case r.Method == http.MethodDelete:
		req.op = OpDelete
	default:
		req.op = Op(strings.ToLower(r.Method))
	}
	return req
}

// accessKey is the access key ID of a SigV4 Authorization header:
// "AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request, ...".
func accessKey(auth string) string {
	_, cred, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return ""
	}
	id, _, _ := strings.Cut(cred, "/")
	return id
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request, req request, body []byte) {
	switch req.op {
	case OpBucket:
		s.bucketOp(w, r, req)
	case OpList:
		s.list(w, r, req)
	case OpDeletes:
		s.deletes(w, req, body)
	case OpPut:
		s.put(w, r, req, body)
	case OpCopy:
		s.copy(w, r, req)
	case OpGet, OpHead:
		s.get(w, r, req)
	case OpDelete:
		s.delete(w, req)
	case OpCreateMP:
		s.createMP(w, r, req)
	case OpPart:
		s.putPart(w, req, body)
	case OpComplete:
		s.complete(w, r, req, body)
	case OpAbort:
		s.abort(w, req)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported request")
	}
}

func (s *Server) bucketOp(w http.ResponseWriter, r *http.Request, req request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[req.bucket]
	switch r.Method {
	case http.MethodPut:
		if b == nil {
			s.buckets[req.bucket] = map[string]*object{}
			req.ev.Applied = true
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		if b == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if b == nil {
			writeError(w, http.StatusNotFound, "NoSuchBucket", "no such bucket")
			return
		}
		if len(b) > 0 {
			writeError(w, http.StatusConflict, "BucketNotEmpty", "bucket not empty")
			return
		}
		delete(s.buckets, req.bucket)
		req.ev.Applied = true
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported request")
	}
}

// bucket returns the named bucket, or answers NoSuchBucket. s.mu is held.
func (s *Server) bucket(w http.ResponseWriter, name string) map[string]*object {
	b := s.buckets[name]
	if b == nil {
		writeError(w, http.StatusNotFound, "NoSuchBucket", "no such bucket: "+name)
	}
	return b
}

// precondition checks a write's If-Match and If-None-Match against what the
// key holds, and answers the request if they fail. s.mu is held.
func precondition(w http.ResponseWriter, r *http.Request, cur *object) bool {
	if v := r.Header.Get("If-None-Match"); v != "" {
		if v != "*" {
			writeError(w, http.StatusNotImplemented, "NotImplemented", "If-None-Match on a write takes only *")
			return false
		}
		if cur != nil {
			writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "the object exists")
			return false
		}
	}
	if v := r.Header.Get("If-Match"); v != "" {
		if cur == nil {
			writeError(w, http.StatusNotFound, "NoSuchKey", "no such key")
			return false
		}
		if !etagMatch(v, cur.ETag) {
			writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "the ETag does not match")
			return false
		}
	}
	return true
}

func etagMatch(header, etag string) bool {
	for v := range strings.SplitSeq(header, ",") {
		v = strings.TrimSpace(v)
		if v == "*" || v == etag || `"`+strings.Trim(v, `"`)+`"` == etag {
			return true
		}
	}
	return false
}

func meta(h http.Header) map[string]string {
	m := map[string]string{}
	for k, v := range h {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-meta-"); ok && len(v) > 0 {
			m[name] = v[0]
		}
	}
	return m
}

func (s *Server) etag(data []byte) string {
	if s.opts.UniqueETags {
		s.nextID++
		return fmt.Sprintf(`"%016x"`, s.nextID)
	}
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, req request, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		return
	}
	if !precondition(w, r, b[req.key]) {
		return
	}
	o := &object{data: body}
	o.Key = req.key
	o.Size = int64(len(body))
	o.ETag = s.etag(body)
	o.LastModified = s.opts.Now()
	o.Meta = meta(r.Header)
	o.ContentType = r.Header.Get("Content-Type")
	b[req.key] = o
	req.ev.Applied = true
	req.ev.Size = o.Size
	w.Header().Set("ETag", o.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) copy(w http.ResponseWriter, r *http.Request, req request) {
	// The source is URL-encoded, as a path is: "bucket/key", with or
	// without a leading slash.
	src := r.Header.Get("X-Amz-Copy-Source")
	if u, err := url.PathUnescape(src); err == nil {
		src = u
	}
	srcBucket, srcKey, _ := strings.Cut(strings.TrimPrefix(src, "/"), "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		return
	}
	from := s.buckets[srcBucket][srcKey]
	if from == nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", "no such key: "+src)
		return
	}
	if !precondition(w, r, b[req.key]) {
		return
	}
	o := &object{data: from.data}
	o.Key = req.key
	o.Size = from.Size
	o.ETag = from.ETag
	if s.opts.UniqueETags {
		o.ETag = s.etag(from.data)
	}
	o.LastModified = s.opts.Now()
	o.Meta = from.copyInfo().Meta
	o.ContentType = from.ContentType
	if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		o.Meta = meta(r.Header)
		o.ContentType = r.Header.Get("Content-Type")
	}
	b[req.key] = o
	req.ev.Applied = true
	req.ev.Size = o.Size
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><LastModified>%s</LastModified><ETag>%s</ETag></CopyObjectResult>`,
		o.LastModified.UTC().Format(time.RFC3339Nano), xmlEscape(o.ETag))
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, req request) {
	s.mu.Lock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		s.mu.Unlock()
		return
	}
	o := b[req.key]
	if o == nil {
		s.mu.Unlock()
		if req.op == OpHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeError(w, http.StatusNotFound, "NoSuchKey", "no such key")
		return
	}
	// The object is immutable once written, so it is served outside the
	// lock; a write replaces it rather than changing it.
	s.mu.Unlock()

	if v := r.Header.Get("If-Match"); v != "" && !etagMatch(v, o.ETag) {
		writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "the ETag does not match")
		return
	}
	if v := r.Header.Get("If-None-Match"); v != "" && etagMatch(v, o.ETag) {
		w.Header().Set("ETag", o.ETag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if v := r.Header.Get("If-Unmodified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && o.LastModified.Truncate(time.Second).After(t) {
			writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "modified since")
			return
		}
	}
	if v := r.Header.Get("If-Modified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && !o.LastModified.Truncate(time.Second).After(t) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	h := w.Header()
	h.Set("ETag", o.ETag)
	h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	if o.ContentType != "" {
		h.Set("Content-Type", o.ContentType)
	}
	for k, v := range o.Meta {
		h.Set("x-amz-meta-"+k, v)
	}
	data := o.data
	status := http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" {
		start, end, ok := parseRange(rg, int64(len(data)))
		if !ok {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "the range is not satisfiable")
			return
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
		data = data[start:end]
		status = http.StatusPartialContent
	}
	h.Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	if req.op == OpGet {
		_, _ = w.Write(data)
		req.ev.Size = int64(len(data))
	}
}

// parseRange parses a single-range Range header against an object of size
// n, returning the half-open [start, end).
func parseRange(h string, n int64) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(h, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, _ := strings.Cut(spec, "-")
	if a == "" { // the last b bytes
		k, err := strconv.ParseInt(b, 10, 64)
		if err != nil || k <= 0 {
			return 0, 0, false
		}
		return max(0, n-k), n, n > 0
	}
	lo, err := strconv.ParseInt(a, 10, 64)
	if err != nil || lo >= n {
		return 0, 0, false
	}
	hi := n - 1
	if b != "" {
		if hi, err = strconv.ParseInt(b, 10, 64); err != nil || hi < lo {
			return 0, 0, false
		}
		hi = min(hi, n-1)
	}
	return lo, hi + 1, true
}

func (s *Server) delete(w http.ResponseWriter, req request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		return
	}
	if _, ok := b[req.key]; ok {
		delete(b, req.key)
		req.ev.Applied = true
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deletes(w http.ResponseWriter, req request, body []byte) {
	keys, quiet, err := parseDeletes(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	s.mu.Lock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		s.mu.Unlock()
		return
	}
	var deleted []string
	for _, k := range keys {
		if _, ok := b[k]; ok {
			delete(b, k)
			deleted = append(deleted, k)
		}
	}
	s.mu.Unlock()
	req.ev.Applied = len(deleted) > 0
	req.ev.Keys = deleted
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	if !quiet {
		for _, k := range keys {
			fmt.Fprintf(&sb, "<Deleted><Key>%s</Key></Deleted>", xmlEscape(k))
		}
	}
	sb.WriteString("</DeleteResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, sb.String())
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, req request) {
	q := r.URL.Query()
	if q.Get("list-type") != "2" && q.Has("uploads") {
		// ListMultipartUploads: none are reported.
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><Bucket>%s</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`, xmlEscape(req.bucket))
		return
	}
	prefix := q.Get("prefix")
	delim := q.Get("delimiter")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = max(after, tok)
	}
	maxKeys := 1000
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v >= 0 && v < maxKeys {
		maxKeys = v
	}

	s.mu.Lock()
	b := s.bucket(w, req.bucket)
	if b == nil {
		s.mu.Unlock()
		return
	}
	var keys []string
	for k := range b {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	type entry struct {
		obj    Object
		prefix string
	}
	var entries []entry
	seen := map[string]bool{}
	truncated := false
	next := ""
	for _, k := range keys {
		var e entry
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				e.prefix = k[:len(prefix)+i+len(delim)]
			}
		}
		if e.prefix != "" && seen[e.prefix] {
			continue
		}
		if len(entries) == maxKeys {
			truncated = true
			break
		}
		if e.prefix != "" {
			seen[e.prefix] = true
			// Resume past every key under the prefix.
			next = e.prefix + "\U0010FFFF"
		} else {
			e.obj = b[k].copyInfo()
			next = k
		}
		entries = append(entries, e)
	}
	s.mu.Unlock()

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&sb, "<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>",
		xmlEscape(req.bucket), xmlEscape(prefix), len(entries), maxKeys, truncated)
	if delim != "" {
		fmt.Fprintf(&sb, "<Delimiter>%s</Delimiter>", xmlEscape(delim))
	}
	if truncated {
		fmt.Fprintf(&sb, "<NextContinuationToken>%s</NextContinuationToken>", xmlEscape(next))
	}
	for _, e := range entries {
		if e.prefix != "" {
			fmt.Fprintf(&sb, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", xmlEscape(e.prefix))
			continue
		}
		fmt.Fprintf(&sb, "<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>",
			xmlEscape(e.obj.Key), e.obj.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"), xmlEscape(e.obj.ETag), e.obj.Size)
	}
	sb.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, sb.String())
}

func (s *Server) createMP(w http.ResponseWriter, r *http.Request, req request) {
	s.mu.Lock()
	if s.bucket(w, req.bucket) == nil {
		s.mu.Unlock()
		return
	}
	s.nextID++
	id := fmt.Sprintf("upload-%d", s.nextID)
	s.uploads[id] = &upload{bucket: req.bucket, key: req.key, header: r.Header.Clone(), parts: map[int][]byte{}}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`,
		xmlEscape(req.bucket), xmlEscape(req.key), id)
}

func (s *Server) putPart(w http.ResponseWriter, req request, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.uploads[req.uploadID]
	if u == nil || u.bucket != req.bucket || u.key != req.key {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "no such upload")
		return
	}
	if req.part < 1 || req.part > 10000 {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "bad part number")
		return
	}
	u.parts[req.part] = body
	sum := md5.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request, req request, body []byte) {
	parts, err := parseComplete(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.uploads[req.uploadID]
	if u == nil || u.bucket != req.bucket || u.key != req.key {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "no such upload")
		return
	}
	b := s.bucket(w, req.bucket)
	if b == nil {
		return
	}
	if !precondition(w, r, b[req.key]) {
		return
	}
	var data []byte
	sums := md5.New()
	for i, p := range parts {
		chunk, ok := u.parts[p.number]
		if !ok || (i > 0 && p.number <= parts[i-1].number) {
			writeError(w, http.StatusBadRequest, "InvalidPart", "a part is missing or out of order")
			return
		}
		sum := md5.Sum(chunk)
		if !etagMatch(p.etag, `"`+hex.EncodeToString(sum[:])+`"`) {
			writeError(w, http.StatusBadRequest, "InvalidPart", "a part's ETag does not match")
			return
		}
		sums.Write(sum[:])
		data = append(data, chunk...)
	}
	delete(s.uploads, req.uploadID)
	o := &object{data: data}
	o.Key = req.key
	o.Size = int64(len(data))
	o.ETag = fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sums.Sum(nil)), len(parts))
	if s.opts.UniqueETags {
		o.ETag = s.etag(data)
	}
	o.LastModified = s.opts.Now()
	o.Meta = meta(u.header)
	o.ContentType = u.header.Get("Content-Type")
	b[req.key] = o
	req.ev.Applied = true
	req.ev.Size = o.Size
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`,
		xmlEscape(req.bucket), xmlEscape(req.key), xmlEscape(o.ETag))
}

func (s *Server) abort(w http.ResponseWriter, req request) {
	s.mu.Lock()
	delete(s.uploads, req.uploadID)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, xmlEscape(msg))
}
