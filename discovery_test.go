package torx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func init() {
	Register("disc.plain", func() Job { return &discPlainJob{} })
	Register("disc.matrix", func() Job { return &discMatrixJob{} })
	// Under prefixes the disc.* tests do not select, so they see a stable count.
	Register("discpanic.factory", func() Job { panic("factory-boom") })
	Register("discpanic.matrix", func() Job { return &discMatrixPanicJob{} })
	Register("discdup.matrix", func() Job { return &discDupJob{} })
	Register("discresolve.job", func() Job { return &discResolveJob{} })
	Register("discresolve.panic", func() Job { return &discResolvePanicJob{} })
}

// discDupJob returns the same parameter set twice, so its variants collide.
type discDupJob struct{ JobBase }

func (*discDupJob) Declare(*JobContext)                    {}
func (*discDupJob) Run(context.Context, *JobContext) error { return nil }
func (*discDupJob) Matrix() []Params                       { return []Params{{"n": 1}, {"n": 1}} }

// discMatrixPanicJob panics while enumerating its variants.
type discMatrixPanicJob struct{ JobBase }

func (*discMatrixPanicJob) Declare(*JobContext)                    {}
func (*discMatrixPanicJob) Run(context.Context, *JobContext) error { return nil }
func (*discMatrixPanicJob) Matrix() []Params                       { panic("matrix-boom") }

type discPlainJob struct{ JobBase }

func (*discPlainJob) Declare(*JobContext)                    {}
func (*discPlainJob) Run(context.Context, *JobContext) error { return nil }

// discResolveJob canonicalizes its parameters: "n" is required and must lie in
// 1..10, "m" defaults to 5, and any other key is rejected.
type discResolveJob struct{ JobBase }

func (*discResolveJob) Declare(*JobContext)                    {}
func (*discResolveJob) Run(context.Context, *JobContext) error { return nil }
func (*discResolveJob) Matrix() []Params {
	return []Params{{"n": 1}, {"n": 2}}
}

func (*discResolveJob) ResolveParams(p Params) (Params, error) {
	for k := range p {
		if k != "n" && k != "m" {
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	n := p.Int("n", 0)
	if n < 1 || n > 10 {
		return nil, fmt.Errorf("n out of range: %v", p["n"])
	}
	return Params{"n": n, "m": p.Int("m", 5)}, nil
}

// discResolvePanicJob panics while resolving its parameters.
type discResolvePanicJob struct{ JobBase }

func (*discResolvePanicJob) Declare(*JobContext)                    {}
func (*discResolvePanicJob) Run(context.Context, *JobContext) error { return nil }
func (*discResolvePanicJob) ResolveParams(Params) (Params, error)   { panic("resolve-boom") }

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
	for _, want := range []string{`j[a=1,b="x"]`, `j[a=1,b="y"]`, `j[a=2,b="x"]`, `j[a=2,b="y"]`} {
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
	// Keys are sorted, so the id is independent of map iteration order; string
	// values are quoted (JSON) so their type is preserved.
	if got := variantID("pkg.Job", Params{"b": "x", "a": 1}); got != `pkg.Job[a=1,b="x"]` {
		t.Errorf(`variantID = %q, want pkg.Job[a=1,b="x"]`, got)
	}
}

func TestVariantIDInjectiveAcrossTypes(t *testing.T) {
	// The reported collision: %v rendered both the number 1 and the string "1" as
	// "1". JSON encoding keeps them distinct.
	if num, str := variantID("j", Params{"v": 1}), variantID("j", Params{"v": "1"}); num == str {
		t.Errorf("number and string values collided: both %q", num)
	}
}

func TestVariantIDStableAcrossJSONRoundtrip(t *testing.T) {
	// The driver computes the id from native Matrix values (int, string, bool);
	// a worker recomputes it from params decoded from JSON, where the int is now a
	// float64. The two must agree or they would name different result directories.
	native := Params{"n": 1, "name": "x", "on": true}
	data, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Params
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if a, b := variantID("j", native), variantID("j", decoded); a != b {
		t.Errorf("variantID differs across a JSON round-trip: %q vs %q", a, b)
	}
}

func TestDiscoverRejectsDuplicateVariants(t *testing.T) {
	reqs, err := Discover("^discdup[.]matrix$")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one failing request for the duplicate variant", reqs)
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

func TestDiscoverResolvesParams(t *testing.T) {
	// Canonicalization runs before id construction: every variant carries the
	// default m=5 though no Matrix entry mentions m, and the ids encode it.
	reqs, err := Discover("^discresolve[.]job")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("discovered %d requests, want 2: %+v", len(reqs), reqs)
	}
	for _, req := range reqs {
		if req.discErr != nil {
			t.Fatalf("unexpected discovery error: %v", req.discErr)
		}
		if req.Params.Int("m", 0) != 5 {
			t.Errorf("canonical params missing the default: %+v", req.Params)
		}
		if id := variantID(req.ID, req.Params); !strings.Contains(id, "m=5") {
			t.Errorf("variant id %q not built from the canonical map", id)
		}
	}
}

func TestDiscoverSelectsOverCanonicalIDs(t *testing.T) {
	// Selection sees canonical ids: this exact-match pattern names the filled-in
	// default, which no raw Matrix entry contains.
	reqs, err := Discover(`^discresolve[.]job\[m=5,n=1\]$`)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("selected %d requests, want 1: %+v", len(reqs), reqs)
	}
}

func TestDiscoverResolverErrorFailsVariant(t *testing.T) {
	// A configuration the resolver rejects fails that variant loudly at
	// discovery -- before any node is allocated -- and leaves it attributable
	// via its raw parameters.
	ov := ParamsOverrides{"discresolve.job": {Configs: []Params{{"n": 99}, {"n": 3}}}}
	reqs, err := DiscoverWith(ov, "^discresolve[.]job")
	if err != nil {
		t.Fatalf("DiscoverWith: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("discovered %d requests, want 2: %+v", len(reqs), reqs)
	}
	var bad, good *JobRequest
	for i := range reqs {
		if reqs[i].discErr != nil {
			bad = &reqs[i]
		} else {
			good = &reqs[i]
		}
	}
	if bad == nil || good == nil {
		t.Fatalf("want one failing and one healthy request, got %+v", reqs)
	}
	if !strings.Contains(bad.discErr.Error(), "out of range") {
		t.Errorf("discErr = %v, want the resolver's rejection", bad.discErr)
	}
	if bad.Params.Int("n", 0) != 99 {
		t.Errorf("failing request params = %+v, want the raw n=99", bad.Params)
	}
	if good.Params.Int("n", 0) != 3 || good.Params.Int("m", 0) != 5 {
		t.Errorf("healthy request params = %+v, want canonical n=3 m=5", good.Params)
	}
}

func TestDiscoverResolverPanicFailsVariant(t *testing.T) {
	reqs, err := Discover("^discresolve[.]panic$")
	if err != nil {
		t.Fatalf("Discover crashed on a panicking resolver instead of recovering: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one request carrying a discovery error", reqs)
	}
	if !strings.Contains(reqs[0].discErr.Error(), "resolve-boom") {
		t.Errorf("discErr = %v, want it to mention resolve-boom", reqs[0].discErr)
	}
}

func TestDiscoverWithOverrideReplacesVariants(t *testing.T) {
	// disc.matrix compiles in n=1..3; the override replaces them entirely --
	// no merging -- with n=7 and n=8.
	ov := ParamsOverrides{"disc.matrix": {Matrix: map[string][]any{"n": {7, 8}}}}
	reqs, err := DiscoverWith(ov, "^disc[.]matrix")
	if err != nil {
		t.Fatalf("DiscoverWith: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("discovered %d requests, want 2: %+v", len(reqs), reqs)
	}
	seen := map[int]bool{}
	for _, req := range reqs {
		seen[req.Params.Int("n", 0)] = true
	}
	if !seen[7] || !seen[8] {
		t.Errorf("override variants = %+v, want n=7 and n=8", reqs)
	}
}

func TestDiscoverWithMatrixPlusConfigs(t *testing.T) {
	ov := ParamsOverrides{"disc.matrix": {
		Matrix:  map[string][]any{"n": {7}},
		Configs: []Params{{"n": 9}},
	}}
	reqs, err := DiscoverWith(ov, "^disc[.]matrix")
	if err != nil {
		t.Fatalf("DiscoverWith: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("discovered %d requests, want the expanded matrix plus the config: %+v", len(reqs), reqs)
	}
}

func TestDiscoverWithUnknownJob(t *testing.T) {
	ov := ParamsOverrides{"disc.nope": {Configs: []Params{{"n": 1}}}}
	if _, err := DiscoverWith(ov); err == nil {
		t.Errorf("expected an error for an override naming an unknown job")
	}
}

func TestDiscoverWithUnselectedJob(t *testing.T) {
	// The override names a real job, but the pattern selects none of its
	// variants: the externally supplied configuration would silently not run.
	ov := ParamsOverrides{"disc.matrix": {Configs: []Params{{"n": 7}}}}
	if _, err := DiscoverWith(ov, "^disc[.]plain$"); err == nil {
		t.Errorf("expected an error for an override whose job is never selected")
	}
}

func TestDiscoverWithDuplicateAcrossForms(t *testing.T) {
	// {n:1} via the matrix and {n:1,m:5} via configs resolve to the same
	// canonical map. Raw duplicate detection cannot see that; canonical
	// detection rejects the job no matter which form each point arrived
	// through.
	ov := ParamsOverrides{"discresolve.job": {
		Matrix:  map[string][]any{"n": {1}},
		Configs: []Params{{"n": 1, "m": 5}},
	}}
	reqs, err := DiscoverWith(ov, "^discresolve[.]job")
	if err != nil {
		t.Fatalf("DiscoverWith: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one failing request for the canonical duplicate", reqs)
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
	res := Run(context.Background(), testPool(t, 1), InProcessLauncher{}, reqs, RunOptions{})
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

// binaryDims builds n two-value dimensions, so the cross product is 2^n.
func binaryDims(n int) map[string][]any {
	dims := make(map[string][]any, n)
	for i := range n {
		dims[fmt.Sprintf("d%02d", i)] = []any{0, 1}
	}
	return dims
}

func TestMatrixLimit(t *testing.T) {
	// Exactly MaxVariants is allowed; MaxVariants is a power of two.
	if got := len(Matrix(binaryDims(16))); got != MaxVariants {
		t.Fatalf("Matrix at the limit produced %d variants, want %d", got, MaxVariants)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Matrix past the limit did not panic")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "131072 variants") || !strings.Contains(msg, fmt.Sprint(MaxVariants)) {
			t.Fatalf("panic does not name the count and the limit: %s", msg)
		}
	}()
	Matrix(binaryDims(17))
}

func TestMatrixSizeSaturates(t *testing.T) {
	// 70 binary dimensions overflow an int; the count must saturate, not wrap
	// to something small that would pass the limit.
	n, exact := matrixSize(binaryDims(70))
	if exact || n <= MaxVariants {
		t.Fatalf("matrixSize(2^70) = %d, exact=%v; want saturated above the limit", n, exact)
	}
	if n, exact := matrixSize(map[string][]any{"a": {1, 2}, "b": {}}); n != 0 || !exact {
		t.Fatalf("matrixSize with an empty dimension = %d, %v; want 0, true", n, exact)
	}
}

func TestMatrixEmptyDimensionBeatsOverflow(t *testing.T) {
	// An empty dimension makes the product zero even when the other dimensions
	// alone would overflow. Map iteration order varies per call, so the empty
	// one may come after the point of saturation; the size must not depend on
	// that. Each call is cheap, so many iterations cover the orders.
	dims := binaryDims(70)
	dims["a"] = nil
	for range 500 {
		if n, exact := matrixSize(dims); n != 0 || !exact {
			t.Fatalf("matrixSize(2^70 with an empty dimension) = %d, %v; want 0, true", n, exact)
		}
		if got := Matrix(dims); len(got) != 0 {
			t.Fatalf("Matrix with an empty dimension produced %d variants, want none", len(got))
		}
	}
	// Sorted last, the empty dimension must not cost building the prefix either:
	// 16 binary dimensions plus an empty "z" returns immediately.
	dims = binaryDims(16)
	dims["z"] = []any{}
	if got := Matrix(dims); len(got) != 0 {
		t.Fatalf("Matrix with a trailing empty dimension produced %d variants, want none", len(got))
	}
	// With no dimensions at all the product is one: a single empty parameter set.
	if got := Matrix(map[string][]any{}); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("Matrix(empty map) = %v, want one empty params", got)
	}
}

// oversizeJob returns more variants than MaxVariants from its own Matrix method
// without going through torx.Matrix, so only discovery's check can catch it.
type oversizeJob struct{ JobBase }

func (*oversizeJob) Declare(*JobContext)                    {}
func (*oversizeJob) Run(context.Context, *JobContext) error { return nil }
func (*oversizeJob) Matrix() []Params                       { return make([]Params, MaxVariants+1) }

func TestDiscoverConfinesOversizeMatrix(t *testing.T) {
	Register("discoversize.job", func() Job { return &oversizeJob{} })
	reqs, err := Discover("^discoversize[.]job$")
	if err != nil {
		t.Fatalf("Discover failed outright on an oversize job instead of confining it: %v", err)
	}
	if len(reqs) != 1 || reqs[0].discErr == nil {
		t.Fatalf("got %+v, want one request carrying a discovery error", reqs)
	}
	if msg := reqs[0].discErr.Error(); !strings.Contains(msg, fmt.Sprintf("%d variants", MaxVariants+1)) {
		t.Fatalf("discovery error does not name the count: %s", msg)
	}
}
