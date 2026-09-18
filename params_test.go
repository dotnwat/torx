package torx

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseParamsOverridesForms(t *testing.T) {
	ov, err := ParseParamsOverrides([]byte(`{
		"j.matrix":  {"matrix": {"a": [1, 2], "b": ["x"]}},
		"j.configs": {"configs": [{"a": 1}, {"a": 2, "b": "y"}]},
		"j.both":    {"matrix": {"a": [1]}, "configs": [{"a": 9}]}
	}`))
	if err != nil {
		t.Fatalf("ParseParamsOverrides: %v", err)
	}
	if got := len(mustVariants(t, ov["j.matrix"])); got != 2 {
		t.Errorf("matrix variants = %d, want 2 (cross product)", got)
	}
	if got := len(mustVariants(t, ov["j.configs"])); got != 2 {
		t.Errorf("configs variants = %d, want 2", got)
	}
	// Both forms present: the expanded matrix plus the explicit configs.
	if got := len(mustVariants(t, ov["j.both"])); got != 2 {
		t.Errorf("both variants = %d, want 2", got)
	}
}

func TestParseParamsOverridesEmptyConfigAllowed(t *testing.T) {
	// An empty parameter object is a legitimate explicit point: the job's
	// defaults. Only empty *forms* (no points at all) are rejected.
	ov, err := ParseParamsOverrides([]byte(`{"j": {"configs": [{}]}}`))
	if err != nil {
		t.Fatalf("ParseParamsOverrides: %v", err)
	}
	if got := len(mustVariants(t, ov["j"])); got != 1 {
		t.Errorf("variants = %d, want 1", got)
	}
}

func TestParseParamsOverridesRejects(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"top level not an object", `[1]`, ""},
		{"null document", `null`, "object"},
		{"trailing garbage", `{} x`, ""},
		{"null entry", `{"j": null}`, "object"},
		{"entry not an object", `{"j": [1]}`, "object"},
		{"unknown field", `{"j": {"martix": {"a": [1]}}}`, "unknown field"},
		{"neither form", `{"j": {}}`, "matrix"},
		{"null matrix", `{"j": {"matrix": null}}`, "matrix"},
		{"empty matrix", `{"j": {"matrix": {}}}`, "empty"},
		{"null dimension", `{"j": {"matrix": {"a": null}}}`, "null"},
		{"empty dimension", `{"j": {"matrix": {"a": []}}}`, "empty"},
		{"null dimension value", `{"j": {"matrix": {"a": [1, null]}}}`, "null"},
		{"dimension not a list", `{"j": {"matrix": {"a": 1}}}`, ""},
		{"null configs", `{"j": {"configs": null}}`, "configs"},
		{"empty configs", `{"j": {"configs": []}}`, "empty"},
		{"null config", `{"j": {"configs": [null]}}`, "object"},
		{"config not an object", `{"j": {"configs": [1]}}`, "object"},
		{"null config value", `{"j": {"configs": [{"a": null}]}}`, "null"},
		{"nested null", `{"j": {"configs": [{"a": {"b": [null]}}]}}`, "null"},
	}
	for _, tc := range cases {
		_, err := ParseParamsOverrides([]byte(tc.body))
		if err == nil {
			t.Errorf("%s: no error for %s", tc.name, tc.body)
			continue
		}
		if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.want)
		}
	}
}

func TestLoadParamsOverridesMissingFile(t *testing.T) {
	if _, err := LoadParamsOverrides(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Errorf("expected an error for a missing params file")
	}
}

// mustVariants expands an override that is expected to be within MaxVariants.
func mustVariants(t *testing.T, o ParamsOverride) []Params {
	t.Helper()
	v, err := o.variants()
	if err != nil {
		t.Fatalf("variants: %v", err)
	}
	return v
}

func TestParseParamsOverridesLimit(t *testing.T) {
	dims := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"d%02d": [0, 1]`, i)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	// 2^16 from the matrix alone is exactly the limit and is accepted.
	if _, err := ParseParamsOverrides([]byte(`{"j": {"matrix": ` + dims(16) + `}}`)); err != nil {
		t.Fatalf("an entry at the limit was rejected: %v", err)
	}
	// The limit covers matrix and configs together: one config more is too many.
	_, err := ParseParamsOverrides([]byte(`{"j": {"matrix": ` + dims(16) + `, "configs": [{"x": 1}]}}`))
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%d variants", MaxVariants+1)) {
		t.Fatalf("matrix+configs past the limit: err = %v, want the count named", err)
	}
	// Far past the limit, the count is still reported exactly.
	_, err = ParseParamsOverrides([]byte(`{"j": {"matrix": ` + dims(30) + `}}`))
	if err == nil || !strings.Contains(err.Error(), `job "j"`) || !strings.Contains(err.Error(), "1073741824 variants") {
		t.Fatalf("2^30 matrix: err = %v, want job id and exact count", err)
	}
	// Past int range, the count saturates rather than wrapping.
	_, err = ParseParamsOverrides([]byte(`{"j": {"matrix": ` + dims(70) + `}}`))
	if err == nil || !strings.Contains(err.Error(), "more than") {
		t.Fatalf("2^70 matrix: err = %v, want a saturated count", err)
	}
}
