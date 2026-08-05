// Discovery and parametrization: turning the registered jobs into the concrete
// list of variants to run.
//
// A job that varies over parameters implements Parametrized, returning the
// parameter sets to run -- usually built with Matrix, which expands named
// dimensions into their cross product. Discover walks the registry, expands each
// job into its variants, and keeps those whose id matches the given patterns,
// producing the JobRequests the driver schedules. Each variant has a stable id
// -- the base id plus its sorted parameters -- used for selection and reporting.
// Variants may instead be supplied externally (DiscoverWith, fed by the -params
// file), and a job may validate and canonicalize each parameter set before ids
// are formed by implementing ParamResolver.
package torx

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Parametrized is implemented by a job that runs as several parameter variants.
type Parametrized interface {
	Matrix() []Params
}

// ParamResolver is implemented by a job that validates and canonicalizes its
// parameters. Discovery invokes it once per variant -- before selection,
// duplicate detection, and id construction -- with the raw parameter set from
// the job's Matrix or from a -params override. The job returns the complete
// canonical map (defaults filled in, values type- and range-checked, unknown
// keys rejected) or an error, which fails the variant loudly before any node
// is allocated.
//
// Canonical parameters make variant ids injective over resolved
// configurations: without the hook, {a:1} and {a:1,b:<default>} run the same
// configuration under two different ids, and a mistyped key silently runs the
// default value -- the worst failure mode for externally supplied
// configuration. One corollary: adding a dimension later changes every
// canonical id, since its default joins every map, so consumers should join
// runs on recorded parameters (JobResult.Params) rather than on id strings.
type ParamResolver interface {
	ResolveParams(p Params) (Params, error)
}

// Matrix expands named dimensions into the cross product of parameter sets,
// taking dimensions in sorted name order so the result is deterministic. With no
// dimensions it returns a single empty parameter set.
func Matrix(dims map[string][]any) []Params {
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := []Params{{}}
	for _, k := range keys {
		var next []Params
		for _, base := range result {
			for _, v := range dims[k] {
				p := make(Params, len(base)+1)
				for bk, bv := range base {
					p[bk] = bv
				}
				p[k] = v
				next = append(next, p)
			}
		}
		result = next
	}
	return result
}

// Discover returns the job variants to run: every registered job, expanded into
// its parameter variants, whose stable id matches one of the patterns (regular
// expressions). With no patterns every variant is returned.
func Discover(patterns ...string) ([]JobRequest, error) {
	return DiscoverWith(nil, patterns...)
}

// DiscoverWith is Discover with external parametrization: overrides -- usually
// a parsed -params file -- replace the named jobs' compiled-in variants with
// externally supplied ones. Replacement is total, never a merge. Every
// override must name a registered job, and at least one variant of every
// overridden job must end up selected; both are errors rather than silently
// running none of the configuration the caller supplied.
func DiscoverWith(overrides ParamsOverrides, patterns ...string) ([]JobRequest, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("discover: bad pattern %q: %w", p, err)
		}
		res = append(res, re)
	}
	for id := range overrides {
		if _, ok := lookupJob(id); !ok {
			return nil, fmt.Errorf("discover: params override names unknown job %q", id)
		}
	}

	selected := make(map[string]bool, len(overrides))
	var requests []JobRequest
	for _, id := range RegisteredJobs() {
		factory, ok := lookupJob(id)
		if !ok {
			continue
		}
		override, hasOverride := overrides[id]
		reqs, err := discoverJob(id, factory, override, hasOverride)
		if err != nil {
			// The factory or Matrix panicked, or two variants share a canonical id.
			// Both are job-supplied problems surfacing at the driver before any
			// worker isolation, so confine the failure to this job -- as a failing
			// variant when the job was selected -- rather than letting one bad job
			// crash discovery for the whole suite.
			if matchesAny(id, res) {
				requests = append(requests, JobRequest{ID: id, discErr: fmt.Errorf("discover: job %q: %w", id, err)})
				if hasOverride {
					selected[id] = true
				}
			}
			continue
		}
		for _, req := range reqs {
			if matchesAny(variantID(id, req.Params), res) {
				requests = append(requests, req)
				if hasOverride {
					selected[id] = true
				}
			}
		}
	}

	var unselected []string
	for id := range overrides {
		if !selected[id] {
			unselected = append(unselected, id)
		}
	}
	if len(unselected) > 0 {
		sort.Strings(unselected)
		return nil, fmt.Errorf("discover: params override selects no variant of: %s", strings.Join(unselected, ", "))
	}
	return requests, nil
}

// discoverJob expands one job into its variant requests. The raw parameter
// sets come from the override when one is present and from the job's Matrix
// otherwise; each set is then resolved through the job's ParamResolver when it
// implements one. A resolver failure is confined to its variant: the request
// carries the error (under the raw-parameter id, since no canonical form
// exists) and the driver records it as a failing result before any node is
// allocated. Duplicate detection runs over the resolved sets, so two
// configurations that canonicalize identically are rejected no matter which
// form each arrived through. The factory, Matrix, and resolver are all
// job-supplied code running at the driver; panics in them are recovered.
func discoverJob(id string, factory func() Job, override ParamsOverride, hasOverride bool) ([]JobRequest, error) {
	var job Job
	var raw []Params
	err := recovered(func() error {
		job = factory()
		if hasOverride {
			raw = override.variants()
		} else {
			raw = variantsOf(job)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	reqs := make([]JobRequest, 0, len(raw))
	resolved := make([]Params, 0, len(raw))
	for _, p := range raw {
		cp, rerr := resolveParams(job, p)
		if rerr != nil {
			reqs = append(reqs, JobRequest{ID: id, Params: p,
				discErr: fmt.Errorf("discover: job %q: resolve params: %w", id, rerr)})
			continue
		}
		reqs = append(reqs, JobRequest{ID: id, Params: cp})
		resolved = append(resolved, cp)
	}
	if err := checkDuplicateVariants(id, resolved); err != nil {
		return nil, err
	}
	return reqs, nil
}

// resolveParams canonicalizes p through the job's resolver, recovering a panic
// into an error. A job without a resolver keeps its parameters as they are.
func resolveParams(job Job, p Params) (Params, error) {
	r, ok := job.(ParamResolver)
	if !ok {
		return p, nil
	}
	var cp Params
	err := recovered(func() error {
		var rerr error
		cp, rerr = r.ResolveParams(p)
		return rerr
	})
	if err != nil {
		return nil, err
	}
	return cp, nil
}

// checkDuplicateVariants reports an error if two of a job's parameter sets encode
// to the same variant id -- a Matrix dimension that lists a value twice, or two
// externally supplied configurations that resolve to the same canonical map.
// Colliding variants are indistinguishable in selection and reporting and
// would share a per-variant result directory, letting concurrent workers truncate
// each other's traces and results, so discovery rejects the job outright.
func checkDuplicateVariants(id string, variants []Params) error {
	seen := make(map[string]struct{}, len(variants))
	for _, params := range variants {
		vid := variantID(id, params)
		if _, dup := seen[vid]; dup {
			return fmt.Errorf("duplicate variant %q", vid)
		}
		seen[vid] = struct{}{}
	}
	return nil
}

func variantsOf(job Job) []Params {
	if p, ok := job.(Parametrized); ok {
		if m := p.Matrix(); len(m) > 0 {
			return m
		}
	}
	return []Params{nil}
}

func matchesAny(id string, res []*regexp.Regexp) bool {
	if len(res) == 0 {
		return true
	}
	for _, re := range res {
		if re.MatchString(id) {
			return true
		}
	}
	return false
}

// variantID is the stable id for a job variant: the base id, plus its parameters
// in sorted order when present, e.g. "pkg.Job" or `pkg.Job[a=1,b="x"]`. Each
// value is JSON-encoded, which makes the id injective and type-preserving: the
// number 1 and the string "1" get distinct ids (v=1 versus v="1") instead of
// colliding, and the encoding is stable across the JSON round-trip a parameter
// set makes between the driver and a worker -- an integer and the float64 it
// decodes to both render as 1. Injectivity matters because these ids name
// per-variant result directories, so two variants sharing an id would write over
// each other's traces and results.
func variantID(base string, params Params) string {
	if len(params) == 0 {
		return base
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + encodeParamValue(params[k])
	}
	return base + "[" + strings.Join(parts, ",") + "]"
}

// encodeParamValue renders a parameter value as canonical JSON, so distinct
// values map to distinct strings. A value that cannot be JSON-encoded -- unusual,
// since parameters normally arrive as JSON -- falls back to %v, trading
// injectivity for a readable id rather than failing.
func encodeParamValue(v any) string {
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}
