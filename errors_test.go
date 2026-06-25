package torx

import (
	"errors"
	"testing"
)

func TestErrorIsMatchesKindAndCause(t *testing.T) {
	cause := errors.New("no free port")
	err := Wrap(ErrAllocation, "coordination: allocate port", cause)

	if !errors.Is(err, ErrAllocation) {
		t.Errorf("errors.Is(err, ErrAllocation) = false, want true")
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false, want true")
	}
	if errors.Is(err, ErrBackend) {
		t.Errorf("errors.Is(err, ErrBackend) = true, want false")
	}
}

func TestErrorAsExtractsStructured(t *testing.T) {
	err := Wrap(ErrService, "service: start", nil)

	var te *Error
	if !errors.As(err, &te) {
		t.Fatalf("errors.As(err, *Error) = false, want true")
	}
	if te.Op != "service: start" {
		t.Errorf("Op = %q, want %q", te.Op, "service: start")
	}
	if te.Kind != ErrService {
		t.Errorf("Kind = %v, want ErrService", te.Kind)
	}
}

func TestErrorMessage(t *testing.T) {
	cause := errors.New("boom")
	tests := []struct {
		name string
		err  *Error
		want string
	}{
		{"op and cause", Wrap(ErrBackend, "backend: exec", cause), "backend: exec: torx: backend operation failed: boom"},
		{"op only", Wrap(ErrBackend, "backend: exec", nil), "backend: exec: torx: backend operation failed"},
		{"cause only", Wrap(ErrBackend, "", cause), "torx: backend operation failed: boom"},
		{"kind only", Wrap(ErrBackend, "", nil), "torx: backend operation failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{ErrAllocation, ErrBackend, ErrReadinessTimeout, ErrService}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = true, want false", a, b)
			}
		}
	}
}

func TestMultiError(t *testing.T) {
	var m MultiError
	if err := m.Err(); err != nil {
		t.Errorf("empty MultiError.Err() = %v, want nil", err)
	}

	m.Append(nil) // nil is ignored
	if err := m.Err(); err != nil {
		t.Errorf("after Append(nil), Err() = %v, want nil", err)
	}

	m.Append(Wrap(ErrService, "stop a", nil))
	m.Append(Wrap(ErrBackend, "stop b", nil))

	err := m.Err()
	if err == nil {
		t.Fatalf("Err() = nil, want joined error")
	}
	if !errors.Is(err, ErrService) || !errors.Is(err, ErrBackend) {
		t.Errorf("joined error lost a cause: %v", err)
	}
}
