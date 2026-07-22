package torx

import (
	"context"
	"testing"
)

func init() {
	Register("disc.plain", func() Job { return &discPlainJob{} })
	Register("disc.matrix", func() Job { return &discMatrixJob{} })
	// Under a prefix the disc.* tests do not select, so they see a stable count.
	Register("discpanic.factory", func() Job { panic("factory-boom") })
	Register("discpanic.matrix", func() Job { return &discMatrixPanicJob{} })
}

// discMatrixPanicJob panics while enumerating its variants.
type discMatrixPanicJob struct{ JobBase }

func (*discMatrixPanicJob) Declare(*JobContext)                    {}
func (*discMatrixPanicJob) Run(context.Context, *JobContext) error { return nil }
func (*discMatrixPanicJob) Matrix() []Params                       { panic("matrix-boom") }

type discPlainJob struct{ JobBase }

func (*discPlainJob) Declare(*JobContext)                    {}
func (*discPlainJob) Run(context.Context, *JobContext) error { return nil }

type discMatrixJob struct{ JobBase }

func (*discMatrixJob) Declare(*JobContext)                    {}
func (*discMatrixJob) Run(context.Context, *JobContext) error { return nil }
func (*discMatrixJob) Matrix() []Params {
	return Matrix(map[string][]any{"n": {1, 2, 3}})
}

func TestMatrixCrossProduct(t *testing.T) {
	got := Matrix(map[string][]any{"a": {1, 2}, "b": {"x", "y"}})
	if len(got) != 4 {
		t.Fatalf("got %d variants, want 4: %v", len(got), got)
	}
	// Dimensions expand in sorted name order, so the first variant takes the
	// first value of every dimension.
	if got[0]["a"] != 1 || got[0]["b"] != "x" {
		t.Errorf("first variant = %v, want a=1 b=x", got[0])
	}
	// Every combination is present and distinct.
	seen := map[string]bool{}
	for _, p := range got {
		seen[variantID("j", p)] = true
	}
	for _, want := range []string{"j[a=1,b=x]", "j[a=1,b=y]", "j[a=2,b=x]", "j[a=2,b=y]"} {
		if !seen[want] {
			t.Errorf("missing variant %q", want)
		}
	}
}

func TestMatrixEmpty(t *testing.T) {
	if got := Matrix(nil); len(got) != 1 || len(got[0]) != 0 {
		t.Errorf("Matrix(nil) = %v, want a single empty params", got)
	}
}

func TestVariantID(t *testing.T) {
	if got := variantID("pkg.Job", nil); got != "pkg.Job" {
		t.Errorf("variantID(no params) = %q, want pkg.Job", got)
	}
	// Keys are sorted, so the id is independent of map iteration order.
	if got := variantID("pkg.Job", Params{"b": "x", "a": 1}); got != "pkg.Job[a=1,b=x]" {
		t.Errorf("variantID = %q, want pkg.Job[a=1,b=x]", got)
	}
}

func TestDiscoverExpandsVariants(t *testing.T) {
	// Scope to the disc.* jobs; the registry also holds other tests' jobs.
	reqs, err := Discover("^disc[.]")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	// disc.plain (1) + disc.matrix (3) == 4
	if len(reqs) != 4 {
		t.Fatalf("discovered %d requests, want 4: %+v", len(reqs), reqs)
	}
}

func TestDiscoverSelects(t *testing.T) {
	matrix, err := Discover("disc[.]matrix")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(matrix) != 3 {
		t.Errorf("matrix variants = %d, want 3: %+v", len(matrix), matrix)
	}
	for _, req := range matrix {
		if req.ID != "disc.matrix" {
			t.Errorf("variant has base id %q, want disc.matrix", req.ID)
		}
	}

	plain, err := Discover("disc[.]plain")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(plain) != 1 || plain[0].ID != "disc.plain" || len(plain[0].Params) != 0 {
		t.Errorf("plain = %+v, want one disc.plain with no params", plain)
	}
}

func TestDiscoverBadPattern(t *testing.T) {
	if _, err := Discover("["); err == nil {
		t.Errorf("expected an error for an invalid regexp pattern")
	}
}

func TestDiscoverRecoversFactoryPanic(t *testing.T) {
	reqs, err := Discover("^discpanic[.]factory$")
	if err != nil {
		t.Fatalf("Discover crashed on a panicking factory instead of recovering: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one request carrying a discovery error", reqs)
	}
}

func TestDiscoverRecoversMatrixPanic(t *testing.T) {
	reqs, err := Discover("^discpanic[.]matrix$")
	if err != nil {
		t.Fatalf("Discover crashed on a panicking Matrix instead of recovering: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one request carrying a discovery error", reqs)
	}
}

// TestRunReportsDiscoveryPanicAsFailure checks that a discovery-broken job
// becomes a FAIL in the run rather than being dropped, and does not prevent a
// healthy job discovered alongside it from running.
func TestRunReportsDiscoveryPanicAsFailure(t *testing.T) {
	reqs, err := Discover("^discpanic[.]factory$", "^disc[.]plain$")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	res := Run(context.Background(), testPool(1), InProcessLauncher{}, reqs, RunOptions{})
	byID := map[string]Status{}
	for _, j := range res.Jobs {
		byID[j.ID] = j.Status
	}
	if byID["discpanic.factory"] != StatusFail {
		t.Errorf("broken job status = %v, want FAIL", byID["discpanic.factory"])
	}
	if byID["disc.plain"] != StatusPass {
		t.Errorf("healthy job status = %v, want PASS (a broken sibling must not block it)", byID["disc.plain"])
	}
}
