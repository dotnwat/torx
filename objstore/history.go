package objstore

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"slices"
	"strings"
	"time"
)

// Event is one request the store served, as the store saw it.
type Event struct {
	// Seq numbers the requests in the order they arrived.
	Seq int64 `json:"seq"`
	// Start and End are when the store began and finished with it, on its
	// clock.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Client is the access key ID it was signed with.
	Client string `json:"client"`
	Op     Op     `json:"op"`
	Bucket string `json:"bucket"`
	Key    string `json:"key,omitempty"`
	// Keys are the keys a batch of deletes removed.
	Keys []string `json:"keys,omitempty"`
	// Cond is its If-None-Match and If-Match headers, if any; Range its
	// Range header.
	Cond  string `json:"cond,omitempty"`
	Range string `json:"range,omitempty"`
	// Fault is the fault injected, "rule:action", if any.
	Fault string `json:"fault,omitempty"`
	// Applied says the request changed what the store holds.
	Applied bool `json:"applied,omitempty"`
	// Status is the HTTP status the store answered with; -1 when it
	// answered nothing, because a fault hung up or the client gave up.
	Status int `json:"status"`
	// ETag is the ETag the answer carried; Size the bytes written or read.
	ETag string `json:"etag,omitempty"`
	Size int64  `json:"size,omitempty"`
}

// History returns every request served so far, in the order each ended.
func (s *Server) History() []Event {
	s.histMu.Lock()
	defer s.histMu.Unlock()
	return slices.Clone(s.history)
}

// WriteHistory writes the history to w, one JSON object per line, in the
// order the requests arrived.
func (s *Server) WriteHistory(w io.Writer) error {
	h := s.History()
	slices.SortFunc(h, func(a, b Event) int { return int(a.Seq - b.Seq) })
	enc := json.NewEncoder(w)
	for _, ev := range h {
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return nil
}

func parseDeletes(body []byte) (keys []string, quiet bool, err error) {
	var d struct {
		Quiet   bool `xml:"Quiet"`
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if err := xml.Unmarshal(body, &d); err != nil {
		return nil, false, err
	}
	for _, o := range d.Objects {
		keys = append(keys, o.Key)
	}
	return keys, d.Quiet, nil
}

type partRef struct {
	number int
	etag   string
}

func parseComplete(body []byte) ([]partRef, error) {
	var c struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(body, &c); err != nil {
		return nil, err
	}
	if len(c.Parts) == 0 {
		return nil, errors.New("no parts")
	}
	out := make([]partRef, len(c.Parts))
	for i, p := range c.Parts {
		out[i] = partRef{p.PartNumber, p.ETag}
	}
	return out, nil
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
