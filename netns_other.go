//go:build unix && !linux

package torx

import (
	"errors"
	"fmt"
	"os"
)

// errNoLab is why -netns fails off Linux: the lab is built from Linux's user
// and network namespaces.
var errNoLab = errors.New("-netns needs Linux's user and network namespaces")

func inLab() bool { return false }

func enterLab() int {
	fmt.Fprintln(os.Stderr, "torx:", errNoLab)
	return 2
}

type localLab struct{}

func newLabPool(int) (*Pool, *localLab, error) { return nil, nil, errNoLab }

func (*localLab) Close() {}
