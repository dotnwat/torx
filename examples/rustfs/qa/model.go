//go:build unix

package main

import (
	"github.com/dotnwat/torx/linearize"
)

// The model of one key of a bucket: a register holding a value, or no
// object (""). S3 promises each key read-after-write consistency, a GET or
// a listing seeing every write that finished before it began, and
// preconditions that hold at the instant a write takes effect: each key
// must be linearizable on its own. Keys are independent, so each is checked
// alone; a listing is a read of every key at once, checked as one read per
// key.

// keyIn is an operation on a key: its kind, the value a write writes, and
// the value whose ETag a conditional operation expects.
type keyIn struct {
	f      string
	value  string
	expect string
}

// keyOut is how an operation on a key ended: a result, and the value a
// read saw.
type keyOut struct {
	result result
	read   string
}

type result byte

const (
	done         result = iota // the operation took effect
	precondition               // 412: a precondition did not hold
	noSuchKey                  // 404: the key holds no object
	unknown                    // no answer says whether it took effect
)

// marker is the state of a key whose current version, in a bucket with
// versioning, is a delete marker: it reads as no object, as a key never
// written does, but it is not nothing -- a known issue of RustFS treats it
// otherwise (see allowIfMatchOnMarker).
const marker = "<delete marker>"

// visible is what a read of a key in state s returns.
func visible(s string) string {
	if s == marker {
		return ""
	}
	return s
}

// allowed is the known issues of RustFS a model allows (knownIssues), so
// that a run that keeps finding them can find others.
type allowed struct {
	// ifMatchOnMarker: a write with If-Match takes effect on a delete
	// marker, whatever ETag it names.
	ifMatchOnMarker bool
	// deleteIfMatchMissing: in a bucket with versioning, a delete with
	// If-Match of a key never written succeeds, and leaves a delete marker.
	deleteIfMatchMissing bool
}

// keyModel is the model of a key, in a bucket with versioning or not,
// allowing the known issues in allow.
func keyModel(versioned bool, allow allowed) linearize.Model[string, keyIn, keyOut] {
	deleted := ""
	if versioned {
		deleted = marker
	}
	return linearize.Model[string, keyIn, keyOut]{
		Init: func() string { return "" },
		// Values are unique, so the unanswered operations that are the
		// same are deletes, conditional on the same value or not.
		Same: func(a keyIn, ao keyOut, b keyIn, bo keyOut) bool { return a == b && ao == bo },
		Step: func(s string, in keyIn, out keyOut) (bool, string) {
			switch in.f {
			case "get", "head", "list":
				return out.read == visible(s), s
			case "put", "multipart":
				return out.result == done || out.result == unknown, in.value
			case "delete":
				return out.result == done || out.result == unknown, deleted
			case "put-if-absent":
				return conditional(visible(s) == "", s, in.value, out.result)
			case "put-if-match":
				if allow.ifMatchOnMarker && s == marker && (out.result == done || out.result == unknown) {
					return true, in.value
				}
				return conditional(s == in.expect && s != "", s, in.value, out.result)
			case "delete-if-match":
				if allow.deleteIfMatchMissing && versioned && s == "" && (out.result == done || out.result == unknown) {
					return true, marker
				}
				return conditional(s == in.expect && s != "", s, deleted, out.result)
			}
			return false, s
		},
	}
}

// conditional steps a write that applies only when holds: to next if it
// took effect, which it must have exactly when holds, and staying at s if
// its precondition failed. A 404 says the key held no object. One whose
// outcome is unknown took effect if it could have: had it not, it changed
// nothing, and the search places it last.
func conditional(holds bool, s, next string, r result) (bool, string) {
	switch r {
	case done:
		return holds, next
	case precondition:
		return !holds, s
	case noSuchKey:
		return visible(s) == "", s
	case unknown:
		if holds {
			return true, next
		}
		return true, s
	}
	return false, s
}
