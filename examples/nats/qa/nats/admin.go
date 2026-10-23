//go:build unix

package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// StreamState is a stream's state, as its leader, or a replica, holds it.
type StreamState struct {
	Msgs       uint64 `json:"messages"`
	Bytes      uint64 `json:"bytes"`
	FirstSeq   uint64 `json:"first_seq"`
	LastSeq    uint64 `json:"last_seq"`
	NumDeleted int    `json:"num_deleted,omitempty"`
	Subjects   int    `json:"num_subjects,omitempty"`
}

// PeerInfo is one of a group's replicas, as its leader sees it.
type PeerInfo struct {
	Name    string        `json:"name"`
	Current bool          `json:"current"`
	Offline bool          `json:"offline,omitempty"`
	Active  time.Duration `json:"active"`
	Lag     uint64        `json:"lag,omitempty"`
}

// ClusterInfo is a Raft group's leader and replicas.
type ClusterInfo struct {
	Leader   string     `json:"leader,omitempty"`
	Replicas []PeerInfo `json:"replicas,omitempty"`
}

// Peers are the group's servers, its leader first.
func (ci *ClusterInfo) Peers() []string {
	var out []string
	if ci.Leader != "" {
		out = append(out, ci.Leader)
	}
	for _, r := range ci.Replicas {
		out = append(out, r.Name)
	}
	return out
}

// SourceInfo is how a stream's copying of another -- as its mirror, or one
// of its sources -- stands.
type SourceInfo struct {
	Name   string        `json:"name"`
	Lag    uint64        `json:"lag"`
	Active time.Duration `json:"active"`
	Error  *APIError     `json:"error,omitempty"`
}

// StreamInfo is what a stream's leader says of it.
type StreamInfo struct {
	Config  map[string]any `json:"config"`
	State   StreamState    `json:"state"`
	Cluster *ClusterInfo   `json:"cluster,omitempty"`
	Mirror  *SourceInfo    `json:"mirror,omitempty"`
	Sources []*SourceInfo  `json:"sources,omitempty"`
}

// ReplicaState is a stream's state as one server holds it, from the
// server's JetStream report.
type ReplicaState struct {
	Server  string        `json:"server"`
	Leader  string        `json:"leader,omitempty"`
	State   StreamState   `json:"state"`
	Mirror  *SourceInfo   `json:"mirror,omitempty"`
	Sources []*SourceInfo `json:"sources,omitempty"`
}

// Replicas returns the state of each stream of account acc that the
// server at base (its monitoring URL), named server, holds a replica of,
// by stream.
func Replicas(ctx context.Context, base, server, acc string) (map[string]ReplicaState, error) {
	var jsz struct {
		Accounts []struct {
			Name    string `json:"name"`
			Streams []struct {
				Name    string        `json:"name"`
				Cluster *ClusterInfo  `json:"cluster"`
				State   StreamState   `json:"state"`
				Mirror  *SourceInfo   `json:"mirror"`
				Sources []*SourceInfo `json:"sources"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if err := Jsz(ctx, base, "?accounts=true&streams=true&acc="+acc, &jsz); err != nil {
		return nil, err
	}
	out := map[string]ReplicaState{}
	for _, a := range jsz.Accounts {
		if a.Name != acc {
			continue
		}
		for _, s := range a.Streams {
			rs := ReplicaState{Server: server, State: s.State, Mirror: s.Mirror, Sources: s.Sources}
			if s.Cluster != nil {
				rs.Leader = s.Cluster.Leader
			}
			out[s.Name] = rs
		}
	}
	return out, nil
}

// CreateStream creates a stream of config cfg, or answers with the
// stream if one of that config exists.
func (c *Client) CreateStream(ctx context.Context, cfg map[string]any) (*StreamInfo, error) {
	var si StreamInfo
	err := c.Request(ctx, "$JS.API.STREAM.CREATE."+cfg["name"].(string), cfg, &si)
	return &si, err
}

// UpdateStream updates stream cfg["name"] to config cfg.
func (c *Client) UpdateStream(ctx context.Context, cfg map[string]any) (*StreamInfo, error) {
	var si StreamInfo
	err := c.Request(ctx, "$JS.API.STREAM.UPDATE."+cfg["name"].(string), cfg, &si)
	return &si, err
}

// StreamInfo asks stream's leader about it.
func (c *Client) StreamInfo(ctx context.Context, stream string) (*StreamInfo, error) {
	var si StreamInfo
	err := c.Request(ctx, "$JS.API.STREAM.INFO."+stream, nil, &si)
	return &si, err
}

// StepDown has stream's leader step down, in favor of server preferred
// if it is not "".
func (c *Client) StepDown(ctx context.Context, stream, preferred string) error {
	var body any
	if preferred != "" {
		body = map[string]any{"placement": map[string]any{"preferred": preferred}}
	}
	return c.Request(ctx, "$JS.API.STREAM.LEADER.STEPDOWN."+stream, body, nil)
}

// ConsumerStepDown has a consumer's leader step down.
func (c *Client) ConsumerStepDown(ctx context.Context, stream, consumer string) error {
	return c.Request(ctx, "$JS.API.CONSUMER.LEADER.STEPDOWN."+stream+"."+consumer, nil, nil)
}

// MetaStepDown has the meta group's leader step down; it needs the system
// account.
func (c *Client) MetaStepDown(ctx context.Context) error {
	return c.Request(ctx, "$JS.API.META.LEADER.STEPDOWN", nil, nil)
}

// RemovePeer removes server peer from stream's replicas; the stream
// places a new replica elsewhere.
func (c *Client) RemovePeer(ctx context.Context, stream, peer string) error {
	return c.Request(ctx, "$JS.API.STREAM.PEER.REMOVE."+stream, map[string]string{"peer": peer}, nil)
}

// EvacuatePeer moves stream's replica off server peer.
func (c *Client) EvacuatePeer(ctx context.Context, stream, peer string) error {
	return c.Request(ctx, "$JS.API.STREAM.PEER.EVACUATE."+stream, map[string]string{"peer": peer}, nil)
}

// ConsumerInfo is what a consumer's leader says of it.
type ConsumerInfo struct {
	Cluster *ClusterInfo `json:"cluster,omitempty"`
}

// ConsumerInfo asks a consumer's leader about it.
func (c *Client) ConsumerInfo(ctx context.Context, stream, consumer string) (*ConsumerInfo, error) {
	var ci ConsumerInfo
	err := c.Request(ctx, "$JS.API.CONSUMER.INFO."+stream+"."+consumer, nil, &ci)
	return &ci, err
}

// RemoveConsumerPeer removes server peer from a consumer's replicas; the
// consumer places a new one elsewhere. With evacuate, it moves it there.
func (c *Client) RemoveConsumerPeer(ctx context.Context, stream, consumer, peer string, evacuate bool) error {
	op := "REMOVE"
	if evacuate {
		op = "EVACUATE"
	}
	return c.Request(ctx, "$JS.API.CONSUMER.PEER."+op+"."+stream+"."+consumer, map[string]string{"peer": peer}, nil)
}

// EvacuateServer moves every stream's replica off server, which stays in
// the cluster; it needs the system account.
func (c *Client) EvacuateServer(ctx context.Context, server string) error {
	return c.Request(ctx, "$JS.API.SERVER.EVACUATE", map[string]string{"peer": server}, nil)
}

// CancelMove rolls back stream's reconfiguration in progress.
func (c *Client) CancelMove(ctx context.Context, stream string) error {
	return c.Request(ctx, "$JS.API.STREAM.CANCEL_MOVE."+stream, nil, nil)
}

// Healthz reads a server's health endpoint at base (its monitoring URL),
// with query, and returns its status and what it said.
func Healthz(ctx context.Context, base, query string) (int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz"+query, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

// Jsz reads a server's JetStream report at base (its monitoring URL),
// with query, into out.
func Jsz(ctx context.Context, base, query string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/jsz"+query, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("jsz: %s: %s", resp.Status, b)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
