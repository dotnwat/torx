//go:build unix

package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rustfs/qa/rustfs"
	"github.com/dotnwat/torx/examples/rustfs/qa/s3"
)

// smokeJob is rustfs.smoke: a cluster of four servers with two drives each,
// one bucket, and an object written through one server and read back
// through every other, then replaced only if unchanged, and deleted. It is
// the smallest job that proves the binary runs, the servers form a cluster,
// and the client reaches it.
type smokeJob struct {
	torx.JobBase
	fs *rustfs.Service
}

func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.fs = rustfs.New(serviceName, 4, 2)
	jc.Register(j.fs)
}

func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	nodes := j.fs.Nodes()
	clients := make([]*s3.Client, len(nodes))
	for i, n := range nodes {
		c, err := j.fs.Client(n)
		if err != nil {
			return err
		}
		defer c.Close()
		clients[i] = c
	}
	const bucket, key = "smoke", "hello"
	if err := clients[0].CreateBucket(ctx, bucket); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	body := bytes.Repeat([]byte("torx"), 100_000) // 400KB, larger than an inline object
	w, err := clients[0].PutObject(ctx, bucket, key, body, s3.Cond{IfNoneMatch: "*"})
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	for i, c := range clients {
		o, err := c.GetObject(ctx, bucket, key, "")
		if err != nil {
			return fmt.Errorf("get through %s: %w", nodes[i].Name(), err)
		}
		if !bytes.Equal(o.Body, body) || o.ETag != w.ETag {
			return fmt.Errorf("get through %s: %d bytes, etag %s; wrote %d bytes, etag %s", nodes[i].Name(), len(o.Body), o.ETag, len(body), w.ETag)
		}
	}
	if _, err := clients[1].PutObject(ctx, bucket, key, []byte("x"), s3.Cond{IfNoneMatch: "*"}); !s3.IsCode(err, "PreconditionFailed") {
		return fmt.Errorf("put if none match over an object: %w, want PreconditionFailed", err)
	}
	w2, err := clients[2].PutObject(ctx, bucket, key, []byte("small"), s3.Cond{IfMatch: w.ETag})
	if err != nil {
		return fmt.Errorf("put if match: %w", err)
	}
	if _, err := clients[3].PutObject(ctx, bucket, key, []byte("stale"), s3.Cond{IfMatch: w.ETag}); !s3.IsCode(err, "PreconditionFailed") {
		return fmt.Errorf("put if match a replaced etag: %w, want PreconditionFailed", err)
	}
	ls, err := clients[3].ListObjects(ctx, bucket, "")
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(ls) != 1 || ls[0].Key != key || ls[0].ETag != w2.ETag {
		return fmt.Errorf("list: %+v, want %s with etag %s", ls, key, w2.ETag)
	}
	if _, err := clients[0].DeleteObject(ctx, bucket, key, "", s3.Cond{}); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := clients[1].GetObject(ctx, bucket, key, ""); !s3.IsCode(err, "NoSuchKey") {
		return fmt.Errorf("get after delete: %w, want NoSuchKey", err)
	}
	jc.SetSummary(fmt.Sprintf("%d servers, %d drives each: put, conditional puts, list, and delete agree", len(nodes), j.fs.Drives()))
	return nil
}
