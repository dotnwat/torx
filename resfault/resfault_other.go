//go:build unix && !linux

package resfault

import "errors"

// blockDevice is unreachable off Linux, where no node has a cgroup to
// throttle; it exists so the package builds on every unix.
func blockDevice(string) (string, error) {
	return "", errors.New("finding a directory's block device needs Linux's /proc/self/mountinfo")
}
