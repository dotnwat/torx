//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/objstore"
)

// report writes the record -- the transactions, acks, copies read, faults,
// and the store's requests -- checks it, and fails the job if anything is
// wrong.
func (j *chaosJob) report(_ context.Context, jc *torx.JobContext) error {
	j.mu.Lock()
	txns := make([]*txnRec, len(j.txns))
	for i, t := range j.txns {
		c := *t
		txns[i] = &c
	}
	acks := slices.Clone(j.acks)
	obs := slices.Clone(j.obs)
	notes := slices.Clone(j.notes)
	events := slices.Clone(j.events)
	specs := maps.Clone(j.specs)
	bases := maps.Clone(j.bases)
	epochs := j.epoch
	kills := slices.Clone(j.appKills)
	j.mu.Unlock()

	jsonl := func(name string, xs []any) {
		var b []byte
		for _, x := range xs {
			line, _ := json.Marshal(x)
			b = append(append(b, line...), '\n')
		}
		jc.WriteArtifact(name, b)
	}
	var xs []any
	for _, t := range txns {
		xs = append(xs, t)
	}
	jsonl("txns.jsonl", xs)
	xs = nil
	for _, a := range acks {
		xs = append(xs, a)
	}
	jsonl("acks.jsonl", xs)
	xs = nil
	for _, o := range obs {
		xs = append(xs, o)
	}
	jsonl("copies.jsonl", xs)
	type ev struct {
		Fault  string    `json:"fault"`
		Heal   bool      `json:"heal,omitempty"`
		Target string    `json:"target,omitempty"`
		Start  time.Time `json:"start"`
		End    time.Time `json:"end"`
		Err    string    `json:"error,omitempty"`
	}
	xs = nil
	for _, e := range events {
		x := ev{Fault: e.Fault, Heal: e.Heal, Target: e.Target, Start: e.Start, End: e.End}
		if e.Err != nil {
			x.Err = e.Err.Error()
		}
		xs = append(xs, x)
	}
	jsonl("faults.jsonl", xs)
	var sb strings.Builder
	_ = j.store.WriteHistory(&sb)
	jc.WriteArtifact("store-history.jsonl", []byte(sb.String()))

	var snapshots []time.Time
	for _, e := range j.store.History() {
		if e.Op == objstore.OpPut && e.Applied && strings.HasPrefix(e.Key, replicaPath+"/0009/") {
			snapshots = append(snapshots, e.End)
		}
	}
	c := newChecker(txns, specs, acks, obs, bases, epochs, kills, snapshots, j.allow)
	found, known := c.check()
	anomalies := append(notes, found...)
	b, _ := json.MarshalIndent(anomalies, "", "  ")
	jc.WriteArtifact("anomalies.json", b)
	b, _ = json.MarshalIndent(known, "", "  ")
	jc.WriteArtifact("known.json", b)
	for _, a := range known {
		jc.Log("warn", fmt.Sprintf("known issue %s: %s", a.Kind, a.Detail))
	}

	outcomes := map[string]int{}
	for _, t := range txns {
		outcomes[t.Outcome]++
	}
	var okAcks, okRestores, failedRestores int
	for _, a := range acks {
		if a.Err == "" {
			okAcks++
		}
	}
	kinds := map[string]int{}
	for _, o := range obs {
		if o.Report != nil {
			okRestores++
			kinds[o.Kind]++
		} else {
			failedRestores++
		}
	}
	faults := 0
	for _, e := range events {
		if !e.Heal && e.Err == nil {
			faults++
		}
	}
	summary := fmt.Sprintf("%d txns (%d committed, %d aborted, %d rolled back, %d in doubt), %d acks, %d copies read (%v) and %d failed, %d faults, %d epochs, %d anomalies, %d known",
		len(txns), outcomes[committed], outcomes[aborted], outcomes[rolledBack], outcomes[inDoubt],
		okAcks, okRestores, kinds, failedRestores, faults, epochs+1, len(anomalies), len(known))
	jc.SetSummary(summary)
	jc.Log("info", summary)
	if len(anomalies) == 0 {
		return nil
	}
	var errs []error
	for _, a := range anomalies {
		jc.Log("error", fmt.Sprintf("%s: %s", a.Kind, a.Detail))
		errs = append(errs, fmt.Errorf("%s: %s", a.Kind, a.Detail))
	}
	return errors.Join(errs...)
}
