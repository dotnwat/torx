//go:build unix && !linux

package torx

// killTagged finds a worker's leftover processes through /proc, which only
// Linux has; elsewhere it kills nothing.
func killTagged(string) (int, error) { return 0, nil }
