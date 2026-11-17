//go:build unix

package main

import (
	"time"

	"github.com/dotnwat/torx/examples/litestream/model"
	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
)

// The outcomes of a transaction.
const (
	committed  = "committed"  // the app committed it, at row N of the txlog
	aborted    = "aborted"    // it failed, and the txlog does not hold it
	rolledBack = "rolledback" // it rolled back, as it was told to
	inDoubt    = "unknown"    // its answer was lost, and the app could not be asked since
)

// txnRec is a transaction a client sent the app.
type txnRec struct {
	UID    uint64    `json:"uid"`
	Client int       `json:"client"`
	Epoch  int       `json:"epoch"` // the primary it was sent to
	Call   time.Time `json:"call"`
	Return time.Time `json:"return"`
	// Outcome is one of the outcomes above; N its txlog row if committed.
	Outcome string `json:"outcome"`
	N       int64  `json:"n,omitempty"`
	Err     string `json:"error,omitempty"`
	// Resolved says the outcome of a transaction in doubt was learned
	// later, from the txlog of the app that ran it.
	Resolved bool `json:"resolved,omitempty"`
}

// ackRec is a `litestream sync -wait`: once it returns, the replica holds
// every transaction the primary committed before it was called -- rows 1
// through Prefix of the epoch's txlog.
type ackRec struct {
	Epoch       int       `json:"epoch"`
	Prefix      int64     `json:"prefix"`
	Call        time.Time `json:"call"`
	Return      time.Time `json:"return"`
	TXID        uint64    `json:"txid,omitempty"`
	ReplicaTXID uint64    `json:"replica_txid,omitempty"`
	Err         string    `json:"error,omitempty"`
}

// The kinds of observation.
const (
	obsRestore  = "restore"  // a restore on the standby, of the latest state, a TXID, or a time
	obsFollow   = "follow"   // a read of the follower's copy
	obsFailover = "failover" // the restore a new primary starts from
	obsFinal    = "final"    // the restore once every fault is healed and the replica caught up
	obsPrimary  = "primary"  // the primary's own database, at the end
)

// obsRec is a copy of the database the job read: what it was, when it was
// made and read, and what it held.
type obsRec struct {
	ID     int               `json:"id"`
	Kind   string            `json:"kind"`
	Target litestream.Target `json:"target,omitzero"`
	Node   string            `json:"node"`
	Epoch  int               `json:"epoch"` // the primary's epoch when the copy was made
	Call   time.Time         `json:"call"`
	Return time.Time         `json:"return"`
	// Life numbers the follower's lives: one restarted reads afresh.
	Life int `json:"life,omitempty"`
	// FollowErrors is how many polls the follower had failed in this life
	// by the time its copy was read.
	FollowErrors int `json:"follow_errors,omitempty"`
	// TXID is the TXID Litestream says it restored.
	TXID string `json:"txid,omitempty"`
	Err  string `json:"error,omitempty"`
	// Kept is where the copy was kept, for a copy found broken at once.
	Kept   string        `json:"kept,omitempty"`
	Report *model.Report `json:"-"`
	// Summary is what the history file keeps of the report.
	Summary *reportSummary `json:"report,omitempty"`
}

type reportSummary struct {
	Txns      int          `json:"txns"`
	Last      uint64       `json:"last,omitempty"`
	Digest    model.Digest `json:"digest"`
	Integrity []string     `json:"integrity,omitempty"`
	Bad       []string     `json:"bad,omitempty"`
	Gaps      []int64      `json:"gaps,omitempty"`
	Pages     int          `json:"pages"`
	PageSize  int          `json:"page_size"`
}

func summarize(r *model.Report) *reportSummary {
	if r == nil {
		return nil
	}
	s := &reportSummary{Txns: len(r.TxLog), Digest: r.Digest, Bad: r.BadValues, Gaps: r.TxLogGaps, Pages: r.PageCount, PageSize: r.PageSize}
	if len(r.TxLog) > 0 {
		s.Last = r.TxLog[len(r.TxLog)-1]
	}
	if !(len(r.Integrity) == 1 && r.Integrity[0] == "ok") {
		s.Integrity = r.Integrity
		if len(s.Integrity) > 10 {
			s.Integrity = s.Integrity[:10]
		}
	}
	return s
}

// anomaly is something the job found wrong.
type anomaly struct {
	Kind   string   `json:"kind"`
	Detail string   `json:"detail"`
	Obs    []int    `json:"obs,omitempty"`
	Txns   []uint64 `json:"txns,omitempty"`
}
