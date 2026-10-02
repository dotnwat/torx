package objstore

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// client signs nothing, but names itself as SigV4 would.
type client struct {
	t        *testing.T
	endpoint string
	key      string
	http     *http.Client
}

func newStore(t *testing.T, opts Options) (*Server, *client) {
	t.Helper()
	s := New(opts)
	endpoint, err := s.Serve("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.CreateBucket("b")
	return s, &client{t: t, endpoint: endpoint, key: "AKID1", http: &http.Client{Timeout: 5 * time.Second}}
}

func (c *client) do(method, path string, body []byte, hdr ...string) (*http.Response, []byte, error) {
	req, err := http.NewRequest(method, c.endpoint+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.key+"/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp, b, err
}

func (c *client) must(method, path string, body []byte, want int, hdr ...string) (*http.Response, []byte) {
	c.t.Helper()
	resp, b, err := c.do(method, path, body, hdr...)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	if resp.StatusCode != want {
		c.t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, want, b)
	}
	return resp, b
}

func TestPutGetHeadDelete(t *testing.T) {
	_, c := newStore(t, Options{})
	resp, _ := c.must("PUT", "/b/dir/k", []byte("hello world"), 200, "x-amz-meta-putid", "abc", "Content-Type", "text/plain")
	etag := resp.Header.Get("ETag")
	if etag != `"5eb63bbbe01eeed093cb22bb8f5acdc3"` {
		t.Fatalf("ETag %s, want the MD5 of the content", etag)
	}
	resp, b := c.must("GET", "/b/dir/k", nil, 200)
	if string(b) != "hello world" || resp.Header.Get("x-amz-meta-putid") != "abc" || resp.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("GET: %q, headers %v", b, resp.Header)
	}
	resp, b = c.must("GET", "/b/dir/k", nil, 206, "Range", "bytes=6-")
	if string(b) != "world" || resp.Header.Get("Content-Range") != "bytes 6-10/11" {
		t.Fatalf("range: %q, %s", b, resp.Header.Get("Content-Range"))
	}
	_, b = c.must("GET", "/b/dir/k", nil, 206, "Range", "bytes=-5")
	if string(b) != "world" {
		t.Fatalf("suffix range: %q", b)
	}
	_, b = c.must("GET", "/b/dir/k", nil, 206, "Range", "bytes=0-4")
	if string(b) != "hello" {
		t.Fatalf("bounded range: %q", b)
	}
	c.must("GET", "/b/dir/k", nil, 416, "Range", "bytes=11-")
	resp, b = c.must("HEAD", "/b/dir/k", nil, 200)
	if len(b) != 0 || resp.Header.Get("Content-Length") != "11" || resp.Header.Get("x-amz-meta-putid") != "abc" {
		t.Fatalf("HEAD: %q, %v", b, resp.Header)
	}
	c.must("DELETE", "/b/dir/k", nil, 204)
	c.must("DELETE", "/b/dir/k", nil, 204)
	c.must("GET", "/b/dir/k", nil, 404)
	c.must("HEAD", "/b/dir/k", nil, 404)
	c.must("GET", "/nobucket/k", nil, 404)
}

func TestConditionalPut(t *testing.T) {
	s, c := newStore(t, Options{})
	c.must("PUT", "/b/m/1", []byte("one"), 200, "If-None-Match", "*")
	c.must("PUT", "/b/m/1", []byte("two"), 412, "If-None-Match", "*")
	resp, _ := c.must("HEAD", "/b/m/1", nil, 200)
	etag := resp.Header.Get("ETag")
	c.must("PUT", "/b/m/1", []byte("three"), 412, "If-Match", `"nope"`)
	c.must("PUT", "/b/m/1", []byte("three"), 200, "If-Match", etag)
	c.must("PUT", "/b/m/1", []byte("four"), 412, "If-Match", etag)
	c.must("PUT", "/b/m/2", []byte("x"), 404, "If-Match", etag)
	data, _, _ := s.Get("b", "m/1")
	if string(data) != "three" {
		t.Fatalf("m/1 holds %q", data)
	}
}

// Two writes of the same bytes share an ETag, as in S3, unless UniqueETags.
func TestETagABA(t *testing.T) {
	for _, unique := range []bool{false, true} {
		t.Run(fmt.Sprint(unique), func(t *testing.T) {
			_, c := newStore(t, Options{UniqueETags: unique})
			r1, _ := c.must("PUT", "/b/k", []byte("a"), 200)
			c.must("PUT", "/b/k", []byte("b"), 200)
			c.must("PUT", "/b/k", []byte("a"), 200)
			want := 200
			if unique {
				want = 412
			}
			c.must("PUT", "/b/k", []byte("c"), want, "If-Match", r1.Header.Get("ETag"))
		})
	}
}

type listResult struct {
	IsTruncated           bool
	NextContinuationToken string
	Contents              []struct {
		Key  string
		Size int64
	}
	CommonPrefixes []struct{ Prefix string }
}

func (c *client) list(query string) listResult {
	c.t.Helper()
	_, b := c.must("GET", "/b?list-type=2&"+query, nil, 200)
	var r listResult
	if err := xml.Unmarshal(b, &r); err != nil {
		c.t.Fatal(err)
	}
	return r
}

func TestList(t *testing.T) {
	_, c := newStore(t, Options{})
	for _, k := range []string{"a/1", "a/2", "a/3", "b/x/1", "b/y/1", "c"} {
		c.must("PUT", "/b/"+k, []byte(k), 200)
	}
	keys := func(r listResult) string {
		var out []string
		for _, o := range r.Contents {
			out = append(out, o.Key)
		}
		for _, p := range r.CommonPrefixes {
			out = append(out, p.Prefix+"*")
		}
		return strings.Join(out, ",")
	}
	if got := keys(c.list("prefix=a/")); got != "a/1,a/2,a/3" {
		t.Fatalf("prefix: %s", got)
	}
	if got := keys(c.list("prefix=a/&start-after=a/1")); got != "a/2,a/3" {
		t.Fatalf("start-after: %s", got)
	}
	if got := keys(c.list("delimiter=/")); got != "c,a/*,b/*" {
		t.Fatalf("delimiter: %s", got)
	}
	if got := keys(c.list("prefix=b/&delimiter=/")); got != "b/x/*,b/y/*" {
		t.Fatalf("prefix and delimiter: %s", got)
	}
	// Paging, with and without a delimiter.
	var all []string
	tok := ""
	for {
		r := c.list("max-keys=2&continuation-token=" + tok)
		all = append(all, keys(r))
		if !r.IsTruncated {
			break
		}
		tok = r.NextContinuationToken
	}
	if got := strings.Join(all, "|"); got != "a/1,a/2|a/3,b/x/1|b/y/1,c" {
		t.Fatalf("pages: %s", got)
	}
	all = nil
	tok = ""
	for {
		r := c.list("max-keys=1&delimiter=/&continuation-token=" + tok)
		all = append(all, keys(r))
		if !r.IsTruncated {
			break
		}
		tok = r.NextContinuationToken
	}
	if got := strings.Join(all, "|"); got != "a/*|b/*|c" {
		t.Fatalf("pages with a delimiter: %s", got)
	}
}

func TestDeletesAndCopy(t *testing.T) {
	s, c := newStore(t, Options{})
	for _, k := range []string{"x", "y", "z"} {
		c.must("PUT", "/b/"+k, []byte(k), 200, "x-amz-meta-m", k)
	}
	c.must("PUT", "/b/y2", nil, 200, "X-Amz-Copy-Source", "/b/y")
	data, info, ok := s.Get("b", "y2")
	if !ok || string(data) != "y" || info.Meta["m"] != "y" {
		t.Fatalf("copy: %q %v", data, info)
	}
	// A source is URL-encoded.
	c.must("PUT", "/b/a%20b", []byte("spaced"), 200)
	c.must("PUT", "/b/c", nil, 200, "X-Amz-Copy-Source", "b/a%20b")
	if data, _, _ := s.Get("b", "c"); string(data) != "spaced" {
		t.Fatalf("copy of an encoded source: %q", data)
	}
	body := `<Delete><Object><Key>x</Key></Object><Object><Key>z</Key></Object><Object><Key>gone</Key></Object></Delete>`
	_, b := c.must("POST", "/b?delete", []byte(body), 200)
	if !strings.Contains(string(b), "<Key>x</Key>") || !strings.Contains(string(b), "<Key>gone</Key>") {
		t.Fatalf("delete result: %s", b)
	}
	var keys []string
	for _, o := range s.List("b", "") {
		keys = append(keys, o.Key)
	}
	if got := strings.Join(keys, ","); got != "a b,c,y,y2" {
		t.Fatalf("left: %s", got)
	}
}

func TestMultipart(t *testing.T) {
	s, c := newStore(t, Options{})
	_, b := c.must("POST", "/b/big?uploads", nil, 200, "x-amz-meta-id", "u1")
	var init struct{ UploadId string }
	if err := xml.Unmarshal(b, &init); err != nil {
		t.Fatal(err)
	}
	r1, _ := c.must("PUT", "/b/big?partNumber=1&uploadId="+init.UploadId, []byte("hello "), 200)
	r2, _ := c.must("PUT", "/b/big?partNumber=2&uploadId="+init.UploadId, []byte("world"), 200)
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		r1.Header.Get("ETag"), r2.Header.Get("ETag"))
	c.must("PUT", "/b/big", []byte("taken"), 200)
	c.must("POST", "/b/big?uploadId="+init.UploadId, []byte(complete), 412, "If-None-Match", "*")
	c.must("POST", "/b/big?uploadId="+init.UploadId, []byte(complete), 200)
	data, info, _ := s.Get("b", "big")
	if string(data) != "hello world" || info.Meta["id"] != "u1" || !strings.HasSuffix(info.ETag, `-2"`) {
		t.Fatalf("completed: %q %v", data, info)
	}
	c.must("POST", "/b/big?uploadId="+init.UploadId, []byte(complete), 404)
}

// TestStreamingUpload: a body in S3's aws-chunked encoding is stored as
// its payload, whether its chunks are signed or its checksum trails it,
// and one that is not whole is refused.
func TestStreamingUpload(t *testing.T) {
	s, c := newStore(t, Options{})
	trailer := []string{"Content-Encoding", "aws-chunked", "X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		"X-Amz-Trailer", "x-amz-checksum-crc32c"}
	c.must("PUT", "/b/trailer", []byte("5\r\nhello\r\n0\r\nx-amz-checksum-crc32c:mnG7TA==\r\n\r\n"), 200,
		append(trailer, "X-Amz-Decoded-Content-Length", "5")...)
	signed := []string{"Content-Encoding", "aws-chunked", "X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}
	c.must("PUT", "/b/signed", []byte("6;chunk-signature=ab\r\nhello \r\n5;chunk-signature=cd\r\nworld\r\n0;chunk-signature=ef\r\n\r\n"), 200,
		append(signed, "X-Amz-Decoded-Content-Length", "11")...)
	c.must("PUT", "/b/empty", []byte("0\r\n\r\n"), 200, trailer...)
	for k, want := range map[string]string{"trailer": "hello", "signed": "hello world", "empty": ""} {
		if data, info, ok := s.Get("b", k); !ok || string(data) != want || info.Size != int64(len(want)) {
			t.Fatalf("%s: stored %q (%d bytes, %v), want %q", k, data, info.Size, ok, want)
		}
	}
	_, got := c.must("GET", "/b/signed", nil, 200)
	if string(got) != "hello world" {
		t.Fatalf("read back %q", got)
	}

	// A part of a multipart upload is a streaming upload too.
	_, b := c.must("POST", "/b/big?uploads", nil, 200)
	var init struct{ UploadId string }
	if err := xml.Unmarshal(b, &init); err != nil {
		t.Fatal(err)
	}
	r1, _ := c.must("PUT", "/b/big?partNumber=1&uploadId="+init.UploadId, []byte("4\r\npart\r\n0\r\n\r\n"), 200, trailer...)
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, r1.Header.Get("ETag"))
	c.must("POST", "/b/big?uploadId="+init.UploadId, []byte(complete), 200)
	if data, _, _ := s.Get("b", "big"); string(data) != "part" {
		t.Fatalf("multipart: stored %q", data)
	}

	// A body cut short, one with no final chunk, and one that is not as
	// long as it said are refused, and nothing is stored.
	for name, body := range map[string]string{
		"cut short":      "5\r\nhel",
		"no final chunk": "5\r\nhello\r\n",
		"bad size":       "five\r\nhello\r\n0\r\n\r\n",
		"wrong length":   "4\r\nhell\r\n0\r\n\r\n",
	} {
		c.must("PUT", "/b/bad", []byte(body), 400, append(trailer, "X-Amz-Decoded-Content-Length", "5")...)
		if _, _, ok := s.Get("b", "bad"); ok {
			t.Fatalf("%s: stored", name)
		}
	}
	// A body that only looks chunked is stored as it is.
	c.must("PUT", "/b/plain", []byte("5\r\nhello\r\n0\r\n\r\n"), 200)
	if data, _, _ := s.Get("b", "plain"); string(data) != "5\r\nhello\r\n0\r\n\r\n" {
		t.Fatalf("plain: stored %q", data)
	}
}

func TestFaults(t *testing.T) {
	s, c := newStore(t, Options{})
	other := *c
	other.key = "AKID2"

	// Fail: before the request takes effect, and only for its client.
	id := s.AddRule(Rule{Name: "f", Clients: []string{"AKID1"}, Ops: []Op{OpPut}, Prob: 1, Action: Fail, Status: 503})
	c.must("PUT", "/b/k", []byte("1"), 503)
	if _, _, ok := s.Get("b", "k"); ok {
		t.Fatal("a failed put took effect")
	}
	other.must("PUT", "/b/k", []byte("2"), 200)
	s.RemoveRule(id)

	// FailAfter: the put takes effect, and the client hears 500.
	id = s.AddRule(Rule{Name: "fa", Prob: 1, Action: FailAfter, Prefix: "k"})
	c.must("PUT", "/b/k", []byte("3"), 500, "If-None-Match", "*") // precondition fails: no effect
	c.must("PUT", "/b/k2", []byte("3"), 500, "If-None-Match", "*")
	if data, _, _ := s.Get("b", "k2"); string(data) != "3" {
		t.Fatalf("fail-after: k2 holds %q", data)
	}
	s.RemoveRule(id)

	// DropAfter: the put takes effect and the connection is reset.
	id = s.AddRule(Rule{Name: "da", Prob: 1, Action: DropAfter, Ops: []Op{OpDelete}})
	if _, _, err := c.do("DELETE", "/b/k2", nil); err == nil {
		t.Fatal("drop-after: the client heard an answer")
	}
	if _, _, ok := s.Get("b", "k2"); ok {
		t.Fatal("drop-after: the delete did not take effect")
	}
	s.RemoveRule(id)

	// Drop: no effect, and no answer.
	id = s.AddRule(Rule{Name: "d", Prob: 1, Action: Drop, Ops: []Op{OpPut}})
	if _, _, err := c.do("PUT", "/b/k3", []byte("x")); err == nil {
		t.Fatal("drop: the client heard an answer")
	}
	if _, _, ok := s.Get("b", "k3"); ok {
		t.Fatal("drop: the put took effect")
	}
	s.RemoveRule(id)

	// Hang: the client times out, and the put never takes effect.
	slow := *c
	slow.http = &http.Client{Timeout: 200 * time.Millisecond}
	id = s.AddRule(Rule{Name: "h", Prob: 1, Action: Hang})
	if _, _, err := slow.do("PUT", "/b/k4", []byte("x")); !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(fmt.Sprint(err), "Timeout") {
		t.Fatalf("hang: %v", err)
	}
	s.RemoveRule(id)

	// Stall: the client times out, and the put lands afterwards.
	id = s.AddRule(Rule{Name: "s", Prob: 1, Action: Stall, Delay: 500 * time.Millisecond, Ops: []Op{OpPut}})
	if _, _, err := slow.do("PUT", "/b/k5", []byte("late")); err == nil {
		t.Fatal("stall: the client heard an answer in time")
	}
	if _, _, ok := s.Get("b", "k5"); ok {
		t.Fatal("stall: the put landed early")
	}
	s.RemoveRule(id)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, _, ok := s.Get("b", "k5"); ok {
			if string(data) != "late" {
				t.Fatalf("stall: k5 holds %q", data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stall: the put never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Delay: a client that gives up cancels the request.
	id = s.AddRule(Rule{Name: "dl", Prob: 1, Action: Delay, Delay: time.Second, Ops: []Op{OpPut}})
	if _, _, err := slow.do("PUT", "/b/k6", []byte("x")); err == nil {
		t.Fatal("delay: the client heard an answer in time")
	}
	s.RemoveRule(id)
	time.Sleep(1200 * time.Millisecond)
	if _, _, ok := s.Get("b", "k6"); ok {
		t.Fatal("delay: a cancelled put took effect")
	}

	// The history records each request, its fault, and its effect.
	want := []string{
		"f:fail:put:k:false:503",
		"fa:fail-after:put:k:false:500",
		"fa:fail-after:put:k2:true:500",
		"da:drop-after:delete:k2:true:-1",
		"d:drop:put:k3:false:-1",
		"h:hang:put:k4:false:-1",
		"s:stall:put:k5:true:-1",
		"dl:delay:put:k6:false:-1",
	}
	got := strings.Join(sortedBySeq(s, nil), "\n")
	if got != strings.Join(want, "\n") {
		t.Fatalf("faulted requests:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}

// sortedBySeq returns the faulted events in the order they arrived.
func sortedBySeq(s *Server, _ []string) []string {
	h := s.History()
	slices.SortFunc(h, func(a, b Event) int { return int(a.Seq - b.Seq) })
	var out []string
	for _, ev := range h {
		if ev.Fault != "" {
			out = append(out, fmt.Sprintf("%s:%s:%s:%v:%d", ev.Fault, ev.Op, ev.Key, ev.Applied, ev.Status))
		}
	}
	return out
}

func TestProbability(t *testing.T) {
	s, c := newStore(t, Options{})
	s.AddRule(Rule{Name: "half", Prob: 0.5, Action: Fail, Ops: []Op{OpGet}})
	c.must("PUT", "/b/k", []byte("x"), 200)
	fails := 0
	for range 400 {
		resp, _, err := c.do("GET", "/b/k", nil)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == 500 {
			fails++
		}
	}
	if fails < 120 || fails > 280 {
		t.Fatalf("%d of 400 gets failed at probability 0.5", fails)
	}
}

func TestWriteTar(t *testing.T) {
	s, c := newStore(t, Options{})
	c.must("PUT", "/b/d/e/f", []byte("deep"), 200)
	c.must("PUT", "/b/a", []byte("top"), 200)
	var buf bytes.Buffer
	if err := s.WriteTar(&buf); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	var got []string
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		got = append(got, hdr.Name+"="+string(b))
	}
	if want := "b/a=top,b/d/e/f=deep"; strings.Join(got, ",") != want {
		t.Fatalf("tar holds %v, want %s", got, want)
	}
}
