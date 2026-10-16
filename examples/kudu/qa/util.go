//go:build unix

package main

import "encoding/json"

// countJSONArray is the length of the JSON array s, or -1 if s is not one.
func countJSONArray(s string) int {
	var a []json.RawMessage
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		return -1
	}
	return len(a)
}
