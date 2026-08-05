// Externally supplied parametrization: the -params file.
//
// A benchmarking or testing user arrives with a specific configuration or
// sweep in mind, and editing the suite to run it is unacceptable. A params
// file, keyed by job id, replaces the named jobs' compiled-in variants at
// discovery time: a "matrix" of dimensions expanded to its cross product, an
// explicit "configs" list for curated points a cross product cannot express,
// or both (the expanded matrix plus the configs). The envelope is strict --
// unknown fields, empty forms, empty dimensions, and null values are all
// rejected -- because externally supplied configuration must never degrade
// silently. torx never interprets a parameter value; the file only decides
// which opaque parameter sets exist.
package torx

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// ParamsOverride replaces one job's compiled-in variants with externally
// supplied ones. Matrix dimensions expand to their cross product (see Matrix);
// Configs lists explicit parameter sets taken verbatim. When both are present
// the job runs the expanded matrix plus the configs.
type ParamsOverride struct {
	Matrix  map[string][]any `json:"matrix,omitempty"`
	Configs []Params         `json:"configs,omitempty"`
}

// variants expands the override into the parameter sets it describes.
func (o ParamsOverride) variants() []Params {
	var out []Params
	if len(o.Matrix) > 0 {
		out = append(out, Matrix(o.Matrix)...)
	}
	return append(out, o.Configs...)
}

// ParamsOverrides is the parsed form of a -params file: job id to override.
type ParamsOverrides map[string]ParamsOverride

// LoadParamsOverrides reads and parses a -params file.
func LoadParamsOverrides(path string) (ParamsOverrides, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	return ParseParamsOverrides(data)
}

// ParseParamsOverrides parses the JSON body of a -params file, strictly: the
// top level must be an object keyed by job id, each entry must carry "matrix"
// and/or "configs" and nothing else, forms and dimensions must be non-empty,
// and null appears nowhere.
func ParseParamsOverrides(data []byte) (ParamsOverrides, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	if top == nil {
		// Unmarshal accepts a JSON null into a map by leaving it nil. Treating
		// that like an empty document would make a params file consisting of
		// "null" -- a templating hole, a broken generator -- behave exactly like
		// no overrides at all, silently running the compiled-in variants instead
		// of the configuration the caller meant to supply.
		return nil, errors.New("params: top level must be a JSON object keyed by job id")
	}
	// Walk entries in sorted order so which error surfaces is deterministic.
	ids := make([]string, 0, len(top))
	for id := range top {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make(ParamsOverrides, len(top))
	for _, id := range ids {
		ov, err := parseOverride(top[id])
		if err != nil {
			return nil, fmt.Errorf("params: job %q: %w", id, err)
		}
		out[id] = ov
	}
	return out, nil
}

func parseOverride(raw json.RawMessage) (ParamsOverride, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return ParamsOverride{}, errors.New(`entry must be a JSON object with "matrix" and/or "configs"`)
	}
	for k := range fields {
		if k != "matrix" && k != "configs" {
			return ParamsOverride{}, fmt.Errorf("unknown field %q", k)
		}
	}
	matrixRaw, hasMatrix := fields["matrix"]
	configsRaw, hasConfigs := fields["configs"]
	if !hasMatrix && !hasConfigs {
		return ParamsOverride{}, errors.New(`entry must carry "matrix" and/or "configs"`)
	}
	var ov ParamsOverride
	if hasMatrix {
		m, err := parseMatrix(matrixRaw)
		if err != nil {
			return ParamsOverride{}, err
		}
		ov.Matrix = m
	}
	if hasConfigs {
		c, err := parseConfigs(configsRaw)
		if err != nil {
			return ParamsOverride{}, err
		}
		ov.Configs = c
	}
	return ov, nil
}

func parseMatrix(raw json.RawMessage) (map[string][]any, error) {
	var dims map[string][]any
	if err := json.Unmarshal(raw, &dims); err != nil || dims == nil {
		return nil, errors.New(`"matrix" must be a JSON object of dimension value lists`)
	}
	if len(dims) == 0 {
		return nil, errors.New(`"matrix" is empty`)
	}
	for name, values := range dims {
		if values == nil {
			return nil, fmt.Errorf("dimension %q is null", name)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("dimension %q is empty", name)
		}
		for i, v := range values {
			if err := rejectNulls(v); err != nil {
				return nil, fmt.Errorf("dimension %q value %d: %w", name, i, err)
			}
		}
	}
	return dims, nil
}

func parseConfigs(raw json.RawMessage) ([]Params, error) {
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil || list == nil {
		return nil, errors.New(`"configs" must be a JSON array of parameter objects`)
	}
	if len(list) == 0 {
		return nil, errors.New(`"configs" is empty`)
	}
	out := make([]Params, len(list))
	for i, item := range list {
		var p Params
		if err := json.Unmarshal(item, &p); err != nil || p == nil {
			return nil, fmt.Errorf("config %d must be a JSON object", i)
		}
		for k, v := range p {
			if err := rejectNulls(v); err != nil {
				return nil, fmt.Errorf("config %d key %q: %w", i, k, err)
			}
		}
		out[i] = p
	}
	return out, nil
}

// rejectNulls walks a decoded JSON value and rejects any null within it. torx
// assigns null no meaning, and in externally supplied configuration it is
// virtually always a mistake -- a templating hole, a broken generator -- so it
// fails the file rather than degrading silently.
func rejectNulls(v any) error {
	switch t := v.(type) {
	case nil:
		return errors.New("null value")
	case map[string]any:
		for k, e := range t {
			if err := rejectNulls(e); err != nil {
				return fmt.Errorf("%q: %w", k, err)
			}
		}
	case []any:
		for i, e := range t {
			if err := rejectNulls(e); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	}
	return nil
}
