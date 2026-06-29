package torx

import (
	"encoding/json"
	"strings"
	"testing"
)

// markerBackend is a test Backend that records what it was built from. It embeds
// LocalBackend to satisfy the interface without reimplementing every method.
type markerBackend struct {
	LocalBackend
	id string
}

func TestBuildBackendUsesRegistry(t *testing.T) {
	RegisterBackend("regtest-marker", func(d BackendDescriptor) (Backend, error) {
		var cfg struct {
			ID string `json:"id"`
		}
		if len(d.Config) > 0 {
			if err := json.Unmarshal(d.Config, &cfg); err != nil {
				return nil, err
			}
		}
		return markerBackend{id: cfg.ID}, nil
	})

	b, err := buildBackend(BackendDescriptor{Kind: "regtest-marker", Config: json.RawMessage(`{"id":"xyz"}`)})
	if err != nil {
		t.Fatalf("buildBackend: %v", err)
	}
	m, ok := b.(markerBackend)
	if !ok {
		t.Fatalf("buildBackend returned %T, want markerBackend", b)
	}
	if m.id != "xyz" {
		t.Errorf("marker id = %q, want xyz (Config did not reach the builder)", m.id)
	}
}

func TestBuildBackendLocalIsBuiltIn(t *testing.T) {
	// The local backend needs no registration: the empty and "local" kinds both
	// build a LocalBackend directly.
	for _, kind := range []string{"", "local"} {
		b, err := buildBackend(BackendDescriptor{Kind: kind})
		if err != nil {
			t.Fatalf("buildBackend(%q): %v", kind, err)
		}
		if _, ok := b.(LocalBackend); !ok {
			t.Errorf("buildBackend(%q) = %T, want LocalBackend", kind, b)
		}
	}
}

func TestBuildBackendUnknownKind(t *testing.T) {
	_, err := buildBackend(BackendDescriptor{Kind: "ghost"})
	if err == nil {
		t.Fatal("expected an error for an unregistered backend kind")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error = %v, want it to name the unknown kind", err)
	}
}

func TestRegisterBackendRejectsReservedKinds(t *testing.T) {
	for _, kind := range []string{"", "local"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterBackend(%q, ...) should panic on a reserved kind", kind)
				}
			}()
			RegisterBackend(kind, func(BackendDescriptor) (Backend, error) { return LocalBackend{}, nil })
		}()
	}
}

func TestRegisterBackendRejectsDuplicate(t *testing.T) {
	build := func(BackendDescriptor) (Backend, error) { return LocalBackend{}, nil }
	RegisterBackend("regtest-dup", build)
	defer func() {
		if recover() == nil {
			t.Errorf("a duplicate RegisterBackend should panic")
		}
	}()
	RegisterBackend("regtest-dup", build)
}
