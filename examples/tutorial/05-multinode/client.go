//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// requestTimeout bounds each request, so a server that hangs fails the job
// with a clear error rather than hanging it.
const requestTimeout = 10 * time.Second

// ErrNotFound is returned by Get for a key that has no value.
var ErrNotFound = errors.New("key not found")

// Client speaks kvd's HTTP API from the job's own process. The job does not
// run on a node; it reaches kvd over the network at the address the service
// advertises.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the kvd at addr (host:port).
func NewClient(addr string) *Client {
	return &Client{base: "http://" + addr, http: &http.Client{Timeout: requestTimeout}}
}

// Put writes value at key.
func (c *Client) Put(ctx context.Context, key, value string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("PUT %s: %s", key, statusLine(resp))
	}
	return nil
}

// Get reads the value at key, or ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		return string(body), err
	case http.StatusNotFound:
		return "", fmt.Errorf("GET %s: %w", key, ErrNotFound)
	default:
		return "", fmt.Errorf("GET %s: %s", key, statusLine(resp))
	}
}

// Stats is what kvd reports about itself.
type Stats struct {
	Keys int `json:"keys"`
	Puts int `json:"puts"`
	Gets int `json:"gets"`
}

// Stats fetches the server's counters.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/stats", nil)
	if err != nil {
		return Stats{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Stats{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Stats{}, fmt.Errorf("GET /stats: %s", statusLine(resp))
	}
	var st Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return Stats{}, fmt.Errorf("GET /stats: %w", err)
	}
	return st, nil
}

// statusLine renders an unexpected response, body included, for an error.
func statusLine(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if b := strings.TrimSpace(string(body)); b != "" {
		return resp.Status + ": " + b
	}
	return resp.Status
}
