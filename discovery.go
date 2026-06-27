// Discovery and parametrization: turning the registered jobs into the concrete
// list of variants to run.
//
// A job that varies over parameters implements Parametrized, returning the
// parameter sets to run -- usually built with Matrix, which expands named
// dimensions into their cross product. Discover walks the registry, expands each
// job into its variants, and keeps those whose id matches the given patterns,
// producing the JobRequests the driver schedules. Each variant has a stable id
// -- the base id plus its sorted parameters -- used for selection and reporting.
package torx

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Parametrized is implemented by a job that runs as several parameter variants.
type Parametrized interface {
	Matrix() []Params
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
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("discover: bad pattern %q: %w", p, err)
		}
		res = append(res, re)
	}

	var requests []JobRequest
	for _, id := range RegisteredJobs() {
		factory, ok := lookupJob(id)
		if !ok {
			continue
		}
		for _, params := range variantsOf(factory()) {
			if matchesAny(variantID(id, params), res) {
				requests = append(requests, JobRequest{ID: id, Params: params})
			}
		}
	}
	return requests, nil
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
// in sorted order when present, e.g. "pkg.Job" or "pkg.Job[a=1,b=x]".
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
		parts[i] = fmt.Sprintf("%s=%v", k, params[k])
	}
	return base + "[" + strings.Join(parts, ",") + "]"
}
