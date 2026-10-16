//go:build unix

package kudu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dotnwat/torx"
)

// NodeSource and NodeJars say where kudu-node is: its source, which java
// runs with no build step, and the directory of the Kudu Java client's
// jars. $KUDU_NODE_SRC and $KUDU_JARS name them; the harness sets both.
func NodeSource() string { return os.Getenv("KUDU_NODE_SRC") }

// NodeJars is the directory of the jars kudu-node runs with.
func NodeJars() string { return os.Getenv("KUDU_JARS") }

// ClientNode runs kudu-node on a node of its own, so a partition can cut
// the clients off from some servers as it would an application's. It runs
// several, each a JVM of its own: the Java client picks a tablet's
// "closest" replica by a number it draws once per JVM, so one JVM reads
// the same replica of a tablet every time.
type ClientNode struct {
	*torx.ServiceBase
	cluster *Cluster
	jvms    int

	mu    sync.Mutex
	procs []torx.Process
	addrs []string
	ports []int
}

// NewClientNode builds the client node of cluster, running jvms kudu-nodes.
func NewClientNode(name string, cluster *Cluster, jvms int) *ClientNode {
	c := &ClientNode{cluster: cluster, jvms: max(jvms, 1)}
	c.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(1, torx.NodeSpec{}), c)
	return c
}

// StartNode starts the kudu-nodes, told of the cluster's masters, which
// the cluster has started before them.
func (c *ClientNode) StartNode(ctx context.Context, n *torx.Node) error {
	src, jars := NodeSource(), NodeJars()
	if src == "" || jars == "" {
		return errors.New("kudu: KUDU_NODE_SRC and KUDU_JARS must name kudu-node's source and the client's jars")
	}
	for i := range c.jvms {
		port, err := n.AllocatePort()
		if err != nil {
			return err
		}
		addr := net.JoinHostPort(n.Addr(), strconv.Itoa(port))
		cmd := torx.Command("java", "-Xss1m", "-XX:+UseSerialGC", "-Xmx512m",
			"-cp", path.Join(jars, "*"), src,
			"--listen", addr, "--masters", c.cluster.MasterAddrs())
		p, err := c.StartCapturedAs(ctx, n, fmt.Sprintf("kudu-node-%d", i), cmd)
		if err != nil {
			n.ReleasePort(port)
			return err
		}
		c.mu.Lock()
		c.procs = append(c.procs, p)
		c.addrs = append(c.addrs, addr)
		c.ports = append(c.ports, port)
		c.mu.Unlock()
	}
	return nil
}

// WaitNode waits for the kudu-nodes to serve. One that exits first --
// java could not compile it, say -- fails the wait with what it said.
func (c *ClientNode) WaitNode(ctx context.Context, n *torx.Node) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	c.mu.Lock()
	procs := slices.Clone(c.procs)
	c.mu.Unlock()
	for i, u := range c.URLs() {
		exited := make(chan struct{})
		wctx, stop := context.WithCancel(ctx)
		go func() {
			if _, err := procs[i].Wait(wctx); err == nil || wctx.Err() == nil {
				close(exited)
			}
		}()
		err := torx.WaitUntil(wctx, func(ctx context.Context) (bool, error) {
			select {
			case <-exited:
				log := ""
				if res, err := n.Exec(ctx, torx.Command("tail", "-c", "4096", c.CapturePath(n, fmt.Sprintf("kudu-node-%d", i)))); err == nil {
					log = string(res.Stdout)
				}
				return false, fmt.Errorf("kudu: kudu-node-%d exited before serving:\n%s", i, log)
			default:
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return torx.WaitForHTTP(pctx, u+"/health") == nil, nil
		}, 200*time.Millisecond)
		stop()
		if err != nil {
			return err
		}
	}
	return nil
}

// StopNode stops the kudu-nodes, each with a thread dump if it hangs.
func (c *ClientNode) StopNode(ctx context.Context, n *torx.Node) error {
	c.mu.Lock()
	procs, ports := c.procs, c.ports
	c.procs, c.addrs, c.ports = nil, nil, nil
	c.mu.Unlock()
	var errs []error
	for i, p := range procs {
		_, err := torx.Stop(ctx, p, torx.StopPolicy{Signal: syscall.SIGTERM, Grace: 5 * time.Second, Dump: syscall.SIGQUIT})
		if err != nil && !errors.Is(err, torx.ErrShutdownTimeout) {
			errs = append(errs, err)
		}
		n.ReleasePort(ports[i])
	}
	return errors.Join(errs...)
}

// CleanNode has nothing to clean.
func (c *ClientNode) CleanNode(context.Context, *torx.Node) error { return nil }

// URLs are the kudu-nodes' APIs.
func (c *ClientNode) URLs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, a := range c.addrs {
		out = append(out, "http://"+a)
	}
	return out
}

// URL is the first kudu-node's API.
func (c *ClientNode) URL() string { return c.URLs()[0] }

// Node is the node the kudu-nodes run on.
func (c *ClientNode) Node() *torx.Node { return c.Nodes()[0] }

// Client calls kudu-node. Every call names a Kudu client of kudu-node's,
// which it makes on first use.
type Client struct {
	URL  string
	Name string
	HTTP *http.Client
}

// NewClient is a client of kudu-node at url, named name.
func NewClient(url, name string) *Client {
	return &Client{URL: url, Name: name, HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}}
}

// Op is one row operation of a write.
type Op struct {
	// Op is insert, insert_ignore, upsert, upsert_ignore, update,
	// update_ignore, delete, or delete_ignore.
	Op string `json:"op"`
	K  int64  `json:"k"`
	V  string `json:"v,omitempty"`
	P  string `json:"p,omitempty"`
}

// OpResult is Kudu's answer to one operation of a write.
type OpResult struct {
	// Code is OK, or the row error's status: AlreadyPresent, NotFound, ...;
	// Unknown if the operation went unanswered.
	Code string `json:"code"`
	Msg  string `json:"msg,omitempty"`
	// TS is the timestamp of the request the operation was flushed in.
	TS     int64  `json:"ts"`
	Server string `json:"server,omitempty"`
}

// WriteResp is Kudu's answer to a write.
type WriteResp struct {
	Results []OpResult `json:"results"`
	// Code and Error say why the write failed as a whole, if it did: its
	// operations may have taken effect or not.
	Code             string `json:"code,omitempty"`
	Error            string `json:"error,omitempty"`
	PropagatedBefore int64  `json:"propagated_before"`
	PropagatedAfter  int64  `json:"propagated_after"`
}

// Write applies ops in one session of the client's, in transaction txn if
// it is not "".
func (c *Client) Write(ctx context.Context, table, txn string, timeout time.Duration, ops []Op) (WriteResp, error) {
	var out WriteResp
	err := c.call(ctx, "/write", map[string]any{
		"client": c.Name, "table": table, "txn": nilIfEmpty(txn),
		"timeout_ms": timeout.Milliseconds(), "ops": ops,
	}, &out)
	return out, err
}

// ReadReq is a scan.
type ReadReq struct {
	Table string `json:"table"`
	// Mode is snapshot (READ_AT_SNAPSHOT), ryw (READ_YOUR_WRITES), latest
	// (READ_LATEST), or diff (a diff scan from StartTS to TS).
	Mode string `json:"mode"`
	// TS is a snapshot scan's timestamp, 0 to have the server pick one; or
	// a diff scan's end.
	TS            int64   `json:"ts,omitempty"`
	StartTS       int64   `json:"start_ts,omitempty"`
	Keys          []int64 `json:"keys,omitempty"`
	Lo            *int64  `json:"lo,omitempty"`
	Hi            *int64  `json:"hi,omitempty"`
	Replica       string  `json:"replica,omitempty"` // leader or closest
	FaultTolerant bool    `json:"fault_tolerant,omitempty"`
	TimeoutMS     int64   `json:"timeout_ms,omitempty"`
}

// Row is a row read: its key, value, and payload, nil if null, and the
// tablet server that served it, as an index into ReadResp.Servers.
type Row struct {
	K      int64
	V      *string
	P      *string
	Server int
	// Deleted marks a row a diff scan returns as deleted.
	Deleted bool
}

// UnmarshalJSON reads a row from kudu-node's [k, v, p].
func (r *Row) UnmarshalJSON(b []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	if len(a) != 5 {
		return fmt.Errorf("kudu: row of %d columns", len(a))
	}
	if err := json.Unmarshal(a[3], &r.Server); err != nil {
		return err
	}
	if err := json.Unmarshal(a[4], &r.Deleted); err != nil {
		return err
	}
	if err := json.Unmarshal(a[0], &r.K); err != nil {
		return err
	}
	if err := json.Unmarshal(a[1], &r.V); err != nil {
		return err
	}
	return json.Unmarshal(a[2], &r.P)
}

// ReadResp is Kudu's answer to a scan.
type ReadResp struct {
	Rows []Row `json:"rows"`
	// Servers are the UUIDs of the tablet servers that served the rows.
	Servers []string `json:"servers"`
	// SnapshotTS is the scan's snapshot timestamp, as asked or as the
	// first tablet server picked it; -1 for a READ_LATEST scan, or one that
	// failed before a server picked one.
	SnapshotTS       int64  `json:"snapshot_ts"`
	Code             string `json:"code,omitempty"`
	Error            string `json:"error,omitempty"`
	PropagatedBefore int64  `json:"propagated_before"`
	PropagatedAfter  int64  `json:"propagated_after"`
}

// Read scans as r says.
func (c *Client) Read(ctx context.Context, r ReadReq) (ReadResp, error) {
	var out ReadResp
	req := asMap(r)
	req["client"] = c.Name
	err := c.call(ctx, "/read", req, &out)
	return out, err
}

// Result is the answer of a call that only succeeds or fails.
type Result struct {
	OK               bool   `json:"ok"`
	Txn              string `json:"txn,omitempty"`
	Code             string `json:"code,omitempty"`
	Error            string `json:"error,omitempty"`
	PropagatedBefore int64  `json:"propagated_before"`
	PropagatedAfter  int64  `json:"propagated_after"`
}

// Err is the result's failure, or nil.
func (r Result) Err() error {
	if r.Code == "" && r.Error == "" {
		return nil
	}
	return fmt.Errorf("%s: %s", r.Code, r.Error)
}

// Table describes the suite's table: its partitions, replication, and the
// encodings and compression of its columns.
type Table struct {
	Name            string            `json:"table"`
	HashBuckets     int               `json:"hash_buckets,omitempty"`
	RangeSplits     []int64           `json:"range_splits,omitempty"`
	Replicas        int               `json:"replicas"`
	ValueEncoding   string            `json:"value_encoding,omitempty"`
	PayloadEncoding string            `json:"payload_encoding,omitempty"`
	Compression     string            `json:"compression,omitempty"`
	ExtraConfigs    map[string]string `json:"extra_configs,omitempty"`
}

// CreateTable creates table t.
func (c *Client) CreateTable(ctx context.Context, t Table, timeout time.Duration) (Result, error) {
	req := asMap(t)
	req["client"] = c.Name
	req["timeout_ms"] = timeout.Milliseconds()
	var out Result
	err := c.call(ctx, "/create_table", req, &out)
	return out, err
}

// TxnBegin begins a transaction, and returns its handle in Result.Txn.
func (c *Client) TxnBegin(ctx context.Context, timeout time.Duration) (Result, error) {
	var out Result
	err := c.call(ctx, "/txn/begin", map[string]any{"client": c.Name, "timeout_ms": timeout.Milliseconds()}, &out)
	return out, err
}

// TxnCommit commits transaction txn, and waits for the commit to finish.
func (c *Client) TxnCommit(ctx context.Context, txn string) (Result, error) {
	var out Result
	err := c.call(ctx, "/txn/commit", map[string]any{"client": c.Name, "txn": txn}, &out)
	return out, err
}

// TxnRollback rolls transaction txn back.
func (c *Client) TxnRollback(ctx context.Context, txn string) (Result, error) {
	var out Result
	err := c.call(ctx, "/txn/rollback", map[string]any{"client": c.Name, "txn": txn}, &out)
	return out, err
}

// Close closes the client's Kudu client: the next call makes a new one,
// with no timestamp propagated to it, as a restarted process would.
func (c *Client) Close(ctx context.Context) error {
	var out Result
	return c.call(ctx, "/client/close", map[string]any{"client": c.Name}, &out)
}

// Partition names the tablet each of keys is in.
func (c *Client) Partition(ctx context.Context, table string, keys []int64) ([]string, error) {
	var out struct {
		Tablets []string `json:"tablets"`
		Code    string   `json:"code"`
		Error   string   `json:"error"`
	}
	if err := c.call(ctx, "/partition", map[string]any{"client": c.Name, "table": table, "keys": keys}, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%s: %s", out.Code, out.Error)
	}
	return out.Tablets, nil
}

// Replica is a tablet's replica, as the master locates it.
type Replica struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Role string `json:"role"`
}

// Tablet is a tablet of a table, and its replicas.
type Tablet struct {
	ID       string    `json:"id"`
	Replicas []Replica `json:"replicas"`
}

// Tablets locates table's tablets.
func (c *Client) Tablets(ctx context.Context, table string) ([]Tablet, error) {
	var out struct {
		Tablets []Tablet `json:"tablets"`
		Code    string   `json:"code"`
		Error   string   `json:"error"`
	}
	if err := c.call(ctx, "/tablets", map[string]any{"client": c.Name, "table": table}, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%s: %s", out.Code, out.Error)
	}
	return out.Tablets, nil
}

// asMap is v's JSON object as a map, its numbers kept as written: a
// timestamp needs every one of its 64 bits.
func asMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	m := map[string]any{}
	if err := d.Decode(&m); err != nil {
		panic(err)
	}
	return m
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ErrNode is a call kudu-node did not answer: what Kudu did with it is
// unknown.
var ErrNode = errors.New("kudu: kudu-node did not answer")

func (c *Client) call(ctx context.Context, path string, req, out any) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(hreq)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrNode, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrNode, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Stack string `json:"stack"`
		}
		_ = json.Unmarshal(body, &e)
		return fmt.Errorf("kudu-node %s: %s: %s\n%s", path, resp.Status, e.Error, e.Stack)
	}
	return json.Unmarshal(body, out)
}
