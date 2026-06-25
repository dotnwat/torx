// Error taxonomy for torx.
//
// Every error torx surfaces wraps exactly one category sentinel
// (ErrAllocation, ErrBackend, ErrReadinessTimeout, ErrService), so callers
// classify failures with errors.Is(err, ErrX) without depending on concrete
// error types. *Error attaches operation context to a sentinel and is
// retrievable with errors.As. MultiError aggregates failures from best-effort
// steps (such as service teardown) so that one failure does not mask the
// others.
package torx

import "errors"

// Category sentinels. Every error torx surfaces wraps one of these.
var (
	// ErrAllocation indicates a pool could not satisfy a resource request.
	ErrAllocation = errors.New("torx: resource allocation failed")
	// ErrBackend indicates a node transport operation (exec, file transfer,
	// signal) failed.
	ErrBackend = errors.New("torx: backend operation failed")
	// ErrReadinessTimeout indicates a readiness condition did not hold within
	// its deadline.
	ErrReadinessTimeout = errors.New("torx: readiness timed out")
	// ErrService indicates a service lifecycle operation (start, stop, clean)
	// failed.
	ErrService = errors.New("torx: service lifecycle operation failed")
)

// Error attaches an operation context and an optional underlying cause to a
// category sentinel. errors.Is matches the category (and the cause);
// errors.As extracts *Error so callers can read Op for reporting.
type Error struct {
	// Op names the operation that failed, e.g. "pool: allocate".
	Op string
	// Kind is the category sentinel this error belongs to; it must be non-nil.
	Kind error
	// Err is the underlying cause, or nil.
	Err error
}

// Wrap builds an *Error in the given category. cause may be nil.
func Wrap(kind error, op string, cause error) *Error {
	return &Error{Op: op, Kind: kind, Err: cause}
}

func (e *Error) Error() string {
	switch {
	case e.Op != "" && e.Err != nil:
		return e.Op + ": " + e.Kind.Error() + ": " + e.Err.Error()
	case e.Op != "":
		return e.Op + ": " + e.Kind.Error()
	case e.Err != nil:
		return e.Kind.Error() + ": " + e.Err.Error()
	default:
		return e.Kind.Error()
	}
}

// Unwrap exposes the category sentinel and the underlying cause so that
// errors.Is matches both.
func (e *Error) Unwrap() []error {
	if e.Err == nil {
		return []error{e.Kind}
	}
	return []error{e.Kind, e.Err}
}

// MultiError accumulates errors from best-effort steps so that a failure in
// one step does not mask the others. The zero value is ready to use.
type MultiError struct {
	errs []error
}

// Append records err if it is non-nil.
func (m *MultiError) Append(err error) {
	if err != nil {
		m.errs = append(m.errs, err)
	}
}

// Err returns nil if no errors were recorded, otherwise an errors.Join of
// them; every recorded cause stays discoverable with errors.Is / errors.As.
func (m *MultiError) Err() error {
	return errors.Join(m.errs...)
}
