package objstore

import (
	"net/http"
	"slices"
	"strings"
	"time"
)

// Action is what a fault does to a request.
type Action string

// The faults a rule can inject. "Before" faults stop the request before it
// takes effect, so the store is as it was; "after" faults let it take
// effect and then keep the answer from the client, which cannot tell the
// two apart -- the case a client's retries most often get wrong.
const (
	// Fail answers with the rule's Status (500 if unset) before the
	// request takes effect.
	Fail Action = "fail"
	// Drop closes the connection, with a reset, before the request takes
	// effect.
	Drop Action = "drop"
	// Delay holds the request for the rule's Delay before serving it. A
	// client that gives up meanwhile cancels it: it never takes effect.
	Delay Action = "delay"
	// Stall holds the request for the rule's Delay and then serves it
	// whether or not its client gave up meanwhile: a write the client
	// timed out on lands late, perhaps after the client's retry.
	Stall Action = "stall"
	// Hang holds the request until its client gives up; it never takes
	// effect. A client cut off from the store sees every request hang.
	Hang Action = "hang"
	// FailAfter lets the request take effect, then answers with the
	// rule's Status (500 if unset).
	FailAfter Action = "fail-after"
	// DropAfter lets the request take effect, then closes the connection
	// with a reset.
	DropAfter Action = "drop-after"
	// DelayAfter lets the request take effect, then holds the answer for
	// the rule's Delay; a client that gives up meanwhile hears nothing.
	DelayAfter Action = "delay-after"
)

// Rule injects a fault into the requests it matches: those of its Clients,
// of its Ops, on keys under its Prefix, each with probability Prob. The
// first rule, in the order they were added, that matches a request and
// fires injects its fault; a request gets at most one.
type Rule struct {
	// Name labels the rule's faults in the history and the log.
	Name string
	// Clients are the access key IDs whose requests the rule matches; all
	// clients' if empty.
	Clients []string
	// Ops are the kinds of request the rule matches; all if empty.
	Ops []Op
	// Prefix is the start of the keys the rule matches; all keys if
	// empty. A request to a bucket, a list, or a batch of deletes matches
	// on its prefix parameter, or its first key.
	Prefix string
	// Prob is the chance the rule fires on a request it matches; 0 is
	// never, 1 always.
	Prob float64
	// Action is the fault; Status and Delay qualify it.
	Action Action
	Status int
	Delay  time.Duration
}

func (r *Rule) status() int {
	if r.Status != 0 {
		return r.Status
	}
	return http.StatusInternalServerError
}

type rule struct {
	id int
	Rule
}

// AddRule adds a rule, after every rule there is, and returns its ID, to
// remove it by.
func (s *Server) AddRule(r Rule) int {
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	s.ruleSeq++
	s.rules = append(s.rules, &rule{id: s.ruleSeq, Rule: r})
	return s.ruleSeq
}

// RemoveRule removes the rule with ID id, if there is one. Requests it is
// already holding carry on as it said.
func (s *Server) RemoveRule(id int) {
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	s.rules = slices.DeleteFunc(s.rules, func(r *rule) bool { return r.id == id })
}

// ClearRules removes every rule.
func (s *Server) ClearRules() {
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	s.rules = nil
}

// Rules returns the rules in force, in order.
func (s *Server) Rules() []Rule {
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	out := make([]Rule, len(s.rules))
	for i, r := range s.rules {
		out[i] = r.Rule
	}
	return out
}

// draw returns the fault a request gets, if any.
func (s *Server) draw(req request) *Rule {
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	for _, r := range s.rules {
		if !r.matches(req) {
			continue
		}
		if r.Prob < 1 {
			s.rngMu.Lock()
			x := s.rng.Float64()
			s.rngMu.Unlock()
			if x >= r.Prob {
				continue
			}
		}
		rr := r.Rule
		return &rr
	}
	return nil
}

func (r *rule) matches(req request) bool {
	if r.Prob <= 0 {
		return false
	}
	if len(r.Clients) > 0 && !slices.Contains(r.Clients, req.client) {
		return false
	}
	if len(r.Ops) > 0 && !slices.Contains(r.Ops, req.op) {
		return false
	}
	return r.Prefix == "" || strings.HasPrefix(req.match, r.Prefix)
}
