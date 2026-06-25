package torx

import "testing"

// TestName is a build-gate smoke test: it confirms the torx package compiles,
// links, and its tests run under the Go toolchain.
func TestName(t *testing.T) {
	if Name != "torx" {
		t.Fatalf("Name = %q, want %q", Name, "torx")
	}
}
