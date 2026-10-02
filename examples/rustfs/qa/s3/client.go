// Package s3 is a small client for the S3 API, enough to drive an object
// store under test: buckets, versioning, objects with their preconditions,
// listings, and multipart uploads. It signs requests with AWS Signature
// Version 4 and sends each exactly once: it never retries, follows no
// redirect, and keeps no state between requests beyond its connections, so
// a test's history records every request as it was sent and every answer as
// it came back.
package s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Client talks to one S3 endpoint.
type Client struct {
	endpoint  *url.URL
	accessKey string
	secretKey string
	region    string
	http      *http.Client
}

// New returns a client of the endpoint, "http://host:port", that signs with
// the given credentials for region "us-east-1". Each client has a transport
// of its own, so one client's connections are not another's.
func New(endpoint, accessKey, secretKey string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	tr.MaxIdleConnsPerHost = 16
	return &Client{
		endpoint:  u,
		accessKey: accessKey,
		secretKey: secretKey,
		region:    "us-east-1",
		http: &http.Client{
			Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Endpoint is the URL the client sends to.
func (c *Client) Endpoint() string { return c.endpoint.String() }

// Close closes the client's idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// Error is an answer the server gave with a status other than success: the
// HTTP status, and the S3 error code and message from its body, when it had
// one.
type Error struct {
	Status    int
	Code      string
	Message   string
	RequestID string
	// Header is the answer's headers.
	Header http.Header
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("s3: HTTP %d", e.Status)
	}
	return fmt.Sprintf("s3: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// IsCode reports whether err is an answer with the S3 error code code.
func IsCode(err error, code string) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && e.Code == code
}

// StatusOf is err's HTTP status, or 0 when err is no answer from the
// server.
func StatusOf(err error) int {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Status
	}
	return 0
}

// request is one S3 request before it is signed.
type request struct {
	method string
	bucket string
	key    string
	query  url.Values
	header http.Header
	body   []byte
}

// response is a successful answer.
type response struct {
	status int
	header http.Header
	body   []byte
}

func (c *Client) do(ctx context.Context, r request) (*response, error) {
	u := *c.endpoint
	path := "/"
	if r.bucket != "" {
		path += r.bucket
		if r.key != "" {
			path += "/" + r.key
		}
	}
	u.Path = path
	u.RawPath = encodePath(path)
	u.RawQuery = canonicalQuery(r.query)
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, u.String(), body)
	if err != nil {
		return nil, err
	}
	maps.Copy(req.Header, r.header)
	req.ContentLength = int64(len(r.body))
	c.sign(req, r.body, time.Now())
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode, RequestID: resp.Header.Get("x-amz-request-id"), Header: resp.Header}
		var x struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}
		if xml.Unmarshal(data, &x) == nil {
			e.Code, e.Message = x.Code, x.Message
		}
		if e.Code == "" {
			// A HEAD answer has no body; name the common cases as a
			// GET's body would.
			switch resp.StatusCode {
			case http.StatusNotFound:
				e.Code = "NoSuchKey"
			case http.StatusPreconditionFailed:
				e.Code = "PreconditionFailed"
			case http.StatusNotModified:
				e.Code = "NotModified"
			}
		}
		return nil, e
	}
	return &response{status: resp.StatusCode, header: resp.Header, body: data}, nil
}

// sign adds a Signature Version 4 authorization to req, which carries
// payload as its body.
func (c *Client) sign(req *http.Request, payload []byte, now time.Time) {
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	day := now.Format("20060102")
	sum := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	// Sign the host and every x-amz- header.
	names := []string{"host"}
	for k := range req.Header {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-") || lk == "content-md5" {
			names = append(names, lk)
		}
	}
	slices.Sort(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		v := req.URL.Host
		if k != "host" {
			v = strings.Join(req.Header.Values(k), ",")
		}
		canonHeaders.WriteString(k + ":" + strings.TrimSpace(v) + "\n")
	}
	signed := strings.Join(names, ";")
	canon := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonHeaders.String(),
		signed,
		payloadHash,
	}, "\n")
	scope := day + "/" + c.region + "/s3/aws4_request"
	h := sha256.Sum256([]byte(canon))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	key := hmacSHA256([]byte("AWS4"+c.secretKey), day)
	key = hmacSHA256(key, c.region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.accessKey+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// encodePath escapes a path as Signature Version 4 requires: every byte but
// the unreserved ones and '/'.
func encodePath(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		ch := p[i]
		if unreserved(ch) || ch == '/' {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func encodeQuery(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if unreserved(ch) {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func unreserved(ch byte) bool {
	return 'A' <= ch && ch <= 'Z' || 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' ||
		ch == '-' || ch == '_' || ch == '.' || ch == '~'
}

// canonicalQuery renders a query string sorted and escaped as Signature
// Version 4 requires; a key with no value renders as "key=".
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var parts []string
	for _, k := range keys {
		vs := slices.Clone(q[k])
		slices.Sort(vs)
		for _, v := range vs {
			parts = append(parts, encodeQuery(k)+"="+encodeQuery(v))
		}
	}
	return strings.Join(parts, "&")
}

// CreateBucket creates a bucket.
func (c *Client) CreateBucket(ctx context.Context, bucket string) error {
	_, err := c.do(ctx, request{method: http.MethodPut, bucket: bucket})
	return err
}

// SetVersioning enables versioning on a bucket, or suspends it.
func (c *Client) SetVersioning(ctx context.Context, bucket string, enabled bool) error {
	status := "Suspended"
	if enabled {
		status = "Enabled"
	}
	body := []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>` + status + `</Status></VersioningConfiguration>`)
	_, err := c.do(ctx, request{method: http.MethodPut, bucket: bucket, query: url.Values{"versioning": {""}}, body: body,
		header: http.Header{"Content-Md5": {contentMD5(body)}}})
	return err
}

func contentMD5(b []byte) string {
	sum := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// Cond is the preconditions of a write or a delete. IfMatch makes it apply
// only when the object's current ETag is IfMatch; IfNoneMatch "*" makes a
// write apply only when the key holds no object.
type Cond struct {
	IfMatch     string
	IfNoneMatch string
}

func (cd Cond) set(h http.Header) {
	if cd.IfMatch != "" {
		h.Set("If-Match", cd.IfMatch)
	}
	if cd.IfNoneMatch != "" {
		h.Set("If-None-Match", cd.IfNoneMatch)
	}
}

// Written is what a write returned: the object's ETag, and its version id
// in a bucket with versioning.
type Written struct {
	ETag      string
	VersionID string
}

// PutObject writes an object.
func (c *Client) PutObject(ctx context.Context, bucket, key string, body []byte, cond Cond) (Written, error) {
	h := http.Header{"Content-Md5": {contentMD5(body)}, "Content-Type": {"application/octet-stream"}}
	cond.set(h)
	resp, err := c.do(ctx, request{method: http.MethodPut, bucket: bucket, key: key, header: h, body: body})
	if err != nil {
		return Written{}, err
	}
	return Written{ETag: resp.header.Get("ETag"), VersionID: resp.header.Get("x-amz-version-id")}, nil
}

// CopyObject copies srcKey of the bucket to key, the copy conditional on
// cond. srcIfMatch, when not empty, makes it apply only when the source's
// ETag is srcIfMatch.
func (c *Client) CopyObject(ctx context.Context, bucket, key, srcKey, srcIfMatch string, cond Cond) (Written, error) {
	h := http.Header{"X-Amz-Copy-Source": {encodePath("/" + bucket + "/" + srcKey)}}
	if srcIfMatch != "" {
		h.Set("X-Amz-Copy-Source-If-Match", srcIfMatch)
	}
	cond.set(h)
	resp, err := c.do(ctx, request{method: http.MethodPut, bucket: bucket, key: key, header: h})
	if err != nil {
		return Written{}, err
	}
	var x struct {
		ETag string `xml:"ETag"`
	}
	// A copy can fail after its 200 status, in the body.
	if xerr := xml.Unmarshal(resp.body, &x); xerr != nil || x.ETag == "" {
		var e struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}
		if xml.Unmarshal(resp.body, &e) == nil && e.Code != "" {
			return Written{}, &Error{Status: resp.status, Code: e.Code, Message: e.Message}
		}
		return Written{}, fmt.Errorf("s3: copy answered %q", resp.body)
	}
	return Written{ETag: x.ETag, VersionID: resp.header.Get("x-amz-version-id")}, nil
}

// Object is an object read back.
type Object struct {
	Body         []byte
	ETag         string
	VersionID    string
	LastModified string
}

// GetObject reads an object, or the version versionID of it when that is
// not empty.
func (c *Client) GetObject(ctx context.Context, bucket, key, versionID string) (Object, error) {
	q := url.Values{}
	if versionID != "" {
		q.Set("versionId", versionID)
	}
	resp, err := c.do(ctx, request{method: http.MethodGet, bucket: bucket, key: key, query: q})
	if err != nil {
		return Object{}, err
	}
	return Object{
		Body: resp.body, ETag: resp.header.Get("ETag"), VersionID: resp.header.Get("x-amz-version-id"),
		LastModified: resp.header.Get("Last-Modified"),
	}, nil
}

// HeadObject reads an object's metadata; its Body is empty.
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (Object, error) {
	resp, err := c.do(ctx, request{method: http.MethodHead, bucket: bucket, key: key})
	if err != nil {
		return Object{}, err
	}
	return Object{ETag: resp.header.Get("ETag"), VersionID: resp.header.Get("x-amz-version-id"),
		LastModified: resp.header.Get("Last-Modified")}, nil
}

// Deleted is what a delete returned: in a bucket with versioning, the
// version it removed or the delete marker it made.
type Deleted struct {
	VersionID    string
	DeleteMarker bool
}

// DeleteObject deletes an object, or the version versionID of it.
func (c *Client) DeleteObject(ctx context.Context, bucket, key, versionID string, cond Cond) (Deleted, error) {
	q := url.Values{}
	if versionID != "" {
		q.Set("versionId", versionID)
	}
	h := http.Header{}
	cond.set(h)
	resp, err := c.do(ctx, request{method: http.MethodDelete, bucket: bucket, key: key, query: q, header: h})
	if err != nil {
		return Deleted{}, err
	}
	return Deleted{VersionID: resp.header.Get("x-amz-version-id"), DeleteMarker: resp.header.Get("x-amz-delete-marker") == "true"}, nil
}

// Entry is an object in a listing.
type Entry struct {
	Key          string `xml:"Key"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

// ListObjects lists every object of the bucket under prefix, a page at a
// time.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) ([]Entry, error) {
	var out []Entry
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {"1000"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, request{method: http.MethodGet, bucket: bucket, query: q})
		if err != nil {
			return out, err
		}
		var x struct {
			Contents              []Entry `xml:"Contents"`
			IsTruncated           bool    `xml:"IsTruncated"`
			NextContinuationToken string  `xml:"NextContinuationToken"`
		}
		if err := xml.Unmarshal(resp.body, &x); err != nil {
			return out, fmt.Errorf("s3: list: %w", err)
		}
		out = append(out, x.Contents...)
		if !x.IsTruncated {
			return out, nil
		}
		if x.NextContinuationToken == "" {
			return out, errors.New("s3: list: truncated without a continuation token")
		}
		token = x.NextContinuationToken
	}
}

// Version is an object version, or a delete marker, in a listing of
// versions.
type Version struct {
	Key          string
	VersionID    string
	ETag         string
	Size         int64
	IsLatest     bool
	DeleteMarker bool
	LastModified string
}

// ListVersions lists every version of every object of the bucket under
// prefix, newest first for each key.
func (c *Client) ListVersions(ctx context.Context, bucket, prefix string) ([]Version, error) {
	var out []Version
	keyMarker, versionMarker := "", ""
	for {
		q := url.Values{"versions": {""}, "prefix": {prefix}, "max-keys": {"1000"}}
		if keyMarker != "" {
			q.Set("key-marker", keyMarker)
			q.Set("version-id-marker", versionMarker)
		}
		resp, err := c.do(ctx, request{method: http.MethodGet, bucket: bucket, query: q})
		if err != nil {
			return out, err
		}
		type v struct {
			Key          string `xml:"Key"`
			VersionID    string `xml:"VersionId"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			IsLatest     bool   `xml:"IsLatest"`
			LastModified string `xml:"LastModified"`
		}
		var x struct {
			Entries []struct {
				XMLName xml.Name
				v
			} `xml:",any"`
			IsTruncated         bool   `xml:"IsTruncated"`
			NextKeyMarker       string `xml:"NextKeyMarker"`
			NextVersionIDMarker string `xml:"NextVersionIdMarker"`
		}
		if err := xml.Unmarshal(resp.body, &x); err != nil {
			return out, fmt.Errorf("s3: list versions: %w", err)
		}
		for _, e := range x.Entries {
			switch e.XMLName.Local {
			case "Version", "DeleteMarker":
				out = append(out, Version{Key: e.Key, VersionID: e.VersionID, ETag: e.ETag, Size: e.Size,
					IsLatest: e.IsLatest, DeleteMarker: e.XMLName.Local == "DeleteMarker", LastModified: e.LastModified})
			}
		}
		if !x.IsTruncated {
			return out, nil
		}
		if x.NextKeyMarker == "" {
			return out, errors.New("s3: list versions: truncated without a marker")
		}
		keyMarker, versionMarker = x.NextKeyMarker, x.NextVersionIDMarker
	}
}

// CreateMultipartUpload starts a multipart upload of key, and returns its
// upload id.
func (c *Client) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	resp, err := c.do(ctx, request{method: http.MethodPost, bucket: bucket, key: key, query: url.Values{"uploads": {""}}})
	if err != nil {
		return "", err
	}
	var x struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(resp.body, &x); err != nil || x.UploadID == "" {
		return "", fmt.Errorf("s3: create multipart upload answered %q", resp.body)
	}
	return x.UploadID, nil
}

// UploadPart uploads part number n of an upload, and returns its ETag.
func (c *Client) UploadPart(ctx context.Context, bucket, key, uploadID string, n int, body []byte) (string, error) {
	q := url.Values{"uploadId": {uploadID}, "partNumber": {strconv.Itoa(n)}}
	resp, err := c.do(ctx, request{method: http.MethodPut, bucket: bucket, key: key, query: q, body: body,
		header: http.Header{"Content-Md5": {contentMD5(body)}}})
	if err != nil {
		return "", err
	}
	return resp.header.Get("ETag"), nil
}

// CompleteMultipartUpload completes an upload from its parts' ETags, in
// part order from part 1, the completion conditional on cond.
func (c *Client) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, etags []string, cond Cond) (Written, error) {
	var b bytes.Buffer
	b.WriteString("<CompleteMultipartUpload>")
	for i, e := range etags {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, xmlEscape(e))
	}
	b.WriteString("</CompleteMultipartUpload>")
	h := http.Header{}
	cond.set(h)
	resp, err := c.do(ctx, request{method: http.MethodPost, bucket: bucket, key: key, query: url.Values{"uploadId": {uploadID}},
		body: b.Bytes(), header: h})
	if err != nil {
		return Written{}, err
	}
	var x struct {
		ETag    string `xml:"ETag"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.Unmarshal(resp.body, &x); err != nil {
		return Written{}, fmt.Errorf("s3: complete multipart upload answered %q", resp.body)
	}
	// A completion can fail after its 200 status, in the body.
	if x.Code != "" {
		return Written{}, &Error{Status: resp.status, Code: x.Code, Message: x.Message}
	}
	return Written{ETag: x.ETag, VersionID: resp.header.Get("x-amz-version-id")}, nil
}

// AbortMultipartUpload abandons an upload.
func (c *Client) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := c.do(ctx, request{method: http.MethodDelete, bucket: bucket, key: key, query: url.Values{"uploadId": {uploadID}}})
	return err
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
