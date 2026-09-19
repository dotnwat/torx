//go:build unix

// Package rqlite runs an rqlite cluster as a torx service and speaks its HTTP
// API.
//
// The client half is the minimum the suite's jobs need -- execute statements,
// query at a read consistency level, read the cluster membership -- written
// against net/http so the example depends on nothing beyond torx and the
// standard library. The service half (service.go) deploys one rqlited per node
// and forms them into a cluster.
package rqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Read consistency levels, spelled as the level query parameter takes them.
// rqlite serves a request with an unknown level at its default rather than
// rejecting it, so a suite that means to test a level must validate the
// spelling itself (ValidLevel) or it silently tests the wrong guarantee.
const (
	LevelNone         = "none"         // the node's own copy, no cluster check
	LevelWeak         = "weak"         // forwarded to the leader, served from its copy
	LevelLinearizable = "linearizable" // the leader confirms its leadership with a quorum first
	LevelStrong       = "strong"       // the read goes through the Raft log
)

// Levels lists every read consistency level, weakest first.
func Levels() []string {
	return []string{LevelNone, LevelWeak, LevelLinearizable, LevelStrong}
}

// ValidLevel reports whether level names a read consistency level.
func ValidLevel(level string) bool {
	return slices.Contains(Levels(), level)
}

const (
	// requestTimeout bounds one HTTP request. It is far longer than any request
	// the suite makes needs -- a membership view probes each member for at
	// most a couple of seconds -- and shorter than the waits the service
	// builds from repeated requests, so a request that stalls costs one
	// attempt, not the whole wait. Readiness probes do not go through this
	// client: the service hands torx.WaitForHTTP a URL, and torx bounds each
	// probe itself.
	requestTimeout = 10 * time.Second
	// maxResponse bounds a response body. The suite's databases are small, so
	// a backup of one stays far under it; a body that reaches it is refused
	// rather than cut short.
	maxResponse = 8 << 20
)

// Statement is one SQL statement with optional positional parameters.
type Statement struct {
	SQL  string
	Args []any
}

// Stmt builds a Statement.
func Stmt(sql string, args ...any) Statement {
	return Statement{SQL: sql, Args: args}
}

// MarshalJSON renders the statement as rqlite's request body expects: a bare
// string when it has no parameters, otherwise an array of the SQL followed by
// the parameters.
func (s Statement) MarshalJSON() ([]byte, error) {
	if len(s.Args) == 0 {
		return json.Marshal(s.SQL)
	}
	return json.Marshal(append([]any{s.SQL}, s.Args...))
}

// Result is the outcome of one statement. An execute fills LastInsertID and
// RowsAffected; a query fills Columns, Types, and Values, one entry per row
// with numbers decoded as float64. Error carries a statement's own failure,
// which rqlite reports inside an otherwise successful response.
type Result struct {
	LastInsertID int64    `json:"last_insert_id"`
	RowsAffected int64    `json:"rows_affected"`
	Columns      []string `json:"columns"`
	Types        []string `json:"types"`
	Values       [][]any  `json:"values"`
	Error        string   `json:"error"`
}

// Int returns the first cell of the first row as an integer: the shape of a
// COUNT(*) or any other single-value query.
func (r Result) Int() (int64, error) {
	if len(r.Values) == 0 || len(r.Values[0]) == 0 {
		return 0, errors.New("rqlite: result has no rows")
	}
	v, ok := r.Values[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("rqlite: first cell is %T, not a number", r.Values[0][0])
	}
	return int64(v), nil
}

// NodeInfo is one entry in the membership view a node serves at /nodes.
type NodeInfo struct {
	ID        string `json:"id"`
	APIAddr   string `json:"api_addr"`
	Addr      string `json:"addr"` // the Raft address
	Voter     bool   `json:"voter"`
	Reachable bool   `json:"reachable"`
	Leader    bool   `json:"leader"`
}

// LeaderOf returns the member a membership view marks as leader.
func LeaderOf(view []NodeInfo) (NodeInfo, bool) {
	for _, m := range view {
		if m.Leader {
			return m, true
		}
	}
	return NodeInfo{}, false
}

// Client talks to one rqlite node over its HTTP API.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the node whose HTTP API listens at addr, a
// host:port.
func NewClient(addr string) *Client {
	return &Client{base: "http://" + addr, http: &http.Client{Timeout: requestTimeout}}
}

// URL returns the absolute URL of path on this node, for an endpoint the
// client does not wrap, such as the readiness probe a service polls.
func (c *Client) URL(path string) string {
	return c.base + path
}

// Execute runs stmts as one transaction and returns one Result per statement.
// A follower forwards the request to the leader. A statement that fails fails
// the call: the suite's writes are assertions, never partial.
func (c *Client) Execute(ctx context.Context, stmts ...Statement) ([]Result, error) {
	return c.post(ctx, "/db/execute?transaction", stmts)
}

// Query runs one read-only statement at the given consistency level and
// returns its rows.
func (c *Client) Query(ctx context.Context, level string, stmt Statement) (Result, error) {
	if !ValidLevel(level) {
		return Result{}, fmt.Errorf("rqlite: unknown read level %q", level)
	}
	res, err := c.post(ctx, "/db/query?level="+url.QueryEscape(level), []Statement{stmt})
	if err != nil {
		return Result{}, err
	}
	if len(res) != 1 {
		return Result{}, fmt.Errorf("rqlite: query returned %d results, want 1", len(res))
	}
	return res[0], nil
}

// QueryInt runs a single-value query, such as a COUNT(*), at level.
func (c *Client) QueryInt(ctx context.Context, level string, stmt Statement) (int64, error) {
	res, err := c.Query(ctx, level, stmt)
	if err != nil {
		return 0, err
	}
	return res.Int()
}

// Backup returns a backup of the database as a SQLite database file, the form
// Load restores from. Whichever node is asked, the backup is the leader's: a
// follower forwards the request.
func (c *Client) Backup(ctx context.Context) ([]byte, error) {
	return c.get(ctx, "/db/backup")
}

// Dump returns the database as a SQL text dump, the schema and every row as
// statements, readable as it is. It is not what Load takes: rqlite executes a
// loaded dump's statements over what the database already holds, while a
// loaded Backup replaces it.
func (c *Client) Dump(ctx context.Context) ([]byte, error) {
	return c.get(ctx, "/db/backup?fmt=sql")
}

// Load replaces the whole database with db, a SQLite database file as Backup
// returns it. Any node takes the request; a follower forwards it to the
// leader. rqlite reports data it could not load as a statement error in an
// otherwise successful response, which fails the call.
func (c *Client) Load(ctx context.Context, db []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/db/load", bytes.NewReader(db))
	if err != nil {
		return fmt.Errorf("rqlite: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	data, err := c.do(req)
	if err != nil {
		return err
	}
	_, err = decodeResults(req.URL.Path, data, nil)
	return err
}

// Nodes returns the cluster membership as this node sees it. Each member's
// reachability is probed live, bounded by timeout, so a dead member is
// reported unreachable instead of stalling the call. A node that knows no
// leader answers with an empty view.
func (c *Client) Nodes(ctx context.Context, timeout time.Duration) ([]NodeInfo, error) {
	u := c.base + "/nodes?ver=2&timeout=" + url.QueryEscape(timeout.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	data, err := c.do(req)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Nodes []NodeInfo `json:"nodes"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("rqlite: decode /nodes response: %w", err)
	}
	return resp.Nodes, nil
}

// post sends stmts to path and decodes the results, turning an HTTP failure, a
// request-level error, or any statement's own error into an error.
func (c *Client) post(ctx context.Context, path string, stmts []Statement) ([]Result, error) {
	body, err := json.Marshal(stmts)
	if err != nil {
		return nil, fmt.Errorf("rqlite: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	data, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return decodeResults(req.URL.Path, data, stmts)
}

// decodeResults decodes the results envelope rqlite answers a statement
// request with, turning a request-level error or any statement's own error
// into an error; stmts, when given, name the statement a result belongs to.
func decodeResults(path string, data []byte, stmts []Statement) ([]Result, error) {
	var resp struct {
		Results []Result `json:"results"`
		Error   string   `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("rqlite: decode %s response: %w", path, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("rqlite: %s: %s", path, resp.Error)
	}
	for i, r := range resp.Results {
		if r.Error != "" {
			sql := ""
			if i < len(stmts) {
				sql = " (" + stmts[i].SQL + ")"
			}
			return nil, fmt.Errorf("rqlite: statement %d%s: %s", i, sql, r.Error)
		}
	}
	return resp.Results, nil
}

// get fetches path and returns the body.
func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	return c.do(req)
}

// do sends req and returns the body of a 200 response. Any other status is an
// error carrying the response text, which is where rqlite puts messages such
// as "leader not found". A body past maxResponse is an error rather than a
// truncated one: a backup cut short would be a corrupt file, not a shorter
// answer.
func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, fmt.Errorf("rqlite: read %s response: %w", req.URL.Path, err)
	}
	if len(data) > maxResponse {
		return nil, fmt.Errorf("rqlite: %s response exceeds %d bytes", req.URL.Path, maxResponse)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rqlite: %s: HTTP %d: %s", req.URL.Path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}
