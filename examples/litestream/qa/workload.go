//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/dotnwat/torx/examples/litestream/model"
)

// genTxn draws a transaction over keys keys of each table, with values of
// at most maxValue bytes but for a rare huge one. Most are a few writes;
// some are large -- larger than a small page cache, so SQLite spills them
// to the WAL before they commit -- and some of those roll back; some
// delete ranges, roll back to savepoints, or create and drop tables.
func genTxn(rng *rand.Rand, uid uint64, keys int64, maxValue int) model.Txn {
	t := model.Txn{UID: uid}
	size := func() int {
		switch r := rng.IntN(100); {
		case r < 70:
			return model.MinValue + rng.IntN(200)
		case r < 95:
			return model.MinValue + rng.IntN(maxValue)
		default:
			return maxValue + rng.IntN(4*maxValue)
		}
	}
	put := func(table int) model.Op {
		return model.Op{Kind: model.Put, Table: table, Key: rng.Int64N(keys), Size: size()}
	}
	anyTable := func() int {
		if rng.IntN(10) == 0 {
			return model.Permanent + rng.IntN(model.Ephemeral)
		}
		return rng.IntN(model.Permanent)
	}
	switch r := rng.IntN(100); {
	case r < 65: // a few writes
		for range 1 + rng.IntN(5) {
			if rng.IntN(5) == 0 {
				t.Ops = append(t.Ops, model.Op{Kind: model.Del, Table: anyTable(), Key: rng.Int64N(keys)})
			} else {
				t.Ops = append(t.Ops, put(anyTable()))
			}
		}
	case r < 80: // a large transaction, which spills; some roll back
		for range 20 + rng.IntN(200) {
			t.Ops = append(t.Ops, put(rng.IntN(model.Permanent)))
		}
		t.Rollback = r >= 75
	case r < 87: // savepoints, some rolled back to
		for range 1 + rng.IntN(3) {
			t.Ops = append(t.Ops, model.Op{Kind: model.Savepoint})
			for range 1 + rng.IntN(30) {
				t.Ops = append(t.Ops, put(anyTable()))
			}
			if rng.IntN(2) == 0 {
				t.Ops = append(t.Ops, model.Op{Kind: model.RollbackTo})
			} else {
				t.Ops = append(t.Ops, model.Op{Kind: model.Release})
			}
		}
		t.Ops = append(t.Ops, put(anyTable()))
	case r < 93: // a range delete, which frees pages
		lo := rng.Int64N(keys)
		t.Ops = append(t.Ops, model.Op{Kind: model.DelRange, Table: anyTable(), Key: lo, Hi: lo + 1 + rng.Int64N(keys/4+1)})
	case r < 97: // create an ephemeral table and fill it
		e := model.Permanent + rng.IntN(model.Ephemeral)
		t.Ops = append(t.Ops, model.Op{Kind: model.Create, Table: e})
		for range 1 + rng.IntN(20) {
			t.Ops = append(t.Ops, put(e))
		}
	default: // drop one
		t.Ops = append(t.Ops, model.Op{Kind: model.Drop, Table: model.Permanent + rng.IntN(model.Ephemeral)})
	}
	return t
}

// appClient talks to lsapp.
type appClient struct {
	http *http.Client
}

func newAppClient(timeout time.Duration) *appClient {
	return &appClient{http: &http.Client{Timeout: timeout}}
}

// txnReply is lsapp's answer to a transaction.
type txnReply struct {
	N          int64  `json:"n"`
	RolledBack bool   `json:"rolledback"`
	Error      string `json:"error"`
	Outcome    string `json:"outcome"`
}

// errNoAnswer is a request whose answer was lost: the app may have
// crashed after or before it committed.
type errNoAnswer struct{ err error }

func (e errNoAnswer) Error() string { return "no answer: " + e.err.Error() }

func (c *appClient) post(ctx context.Context, url string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, out)
}

func (c *appClient) get(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *appClient) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return errNoAnswer{err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return errNoAnswer{err}
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusInternalServerError {
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}

// Txn sends txn. A reply that says the transaction failed is not an
// error: txnReply says how it ended.
func (c *appClient) Txn(ctx context.Context, base string, txn model.Txn) (txnReply, error) {
	var r txnReply
	err := c.post(ctx, base+"/txn", txn, &r)
	return r, err
}

// Lookup returns the txlog row of transaction uid, 0 if it did not commit.
func (c *appClient) Lookup(ctx context.Context, base string, uid uint64) (int64, error) {
	var r struct {
		N     int64  `json:"n"`
		Error string `json:"error"`
	}
	if err := c.get(ctx, base+"/lookup?uid="+strconv.FormatUint(uid, 10), &r); err != nil {
		return 0, err
	}
	if r.Error != "" {
		return 0, fmt.Errorf("lookup: %s", r.Error)
	}
	return r.N, nil
}

// Checkpoint has the app checkpoint the WAL in mode.
func (c *appClient) Checkpoint(ctx context.Context, base, mode string) (string, error) {
	var r map[string]any
	if err := c.post(ctx, base+"/checkpoint", map[string]string{"mode": mode}, &r); err != nil {
		return "", err
	}
	if e, ok := r["error"]; ok {
		return "", fmt.Errorf("checkpoint %s: %v", mode, e)
	}
	return fmt.Sprintf("busy=%v log=%v checkpointed=%v", r["busy"], r["log"], r["checkpointed"]), nil
}

// Vacuum has the app vacuum the database: in full, or n pages of an
// incremental vacuum.
func (c *appClient) Vacuum(ctx context.Context, base string, full bool, n int) error {
	var r map[string]any
	if err := c.post(ctx, base+"/vacuum", map[string]any{"full": full, "incremental": n}, &r); err != nil {
		return err
	}
	if e, ok := r["error"]; ok {
		return fmt.Errorf("vacuum: %v", e)
	}
	return nil
}

// Reopen has the app close every connection and open the database again.
func (c *appClient) Reopen(ctx context.Context, base string) error {
	var r map[string]any
	return c.post(ctx, base+"/reopen", struct{}{}, &r)
}
