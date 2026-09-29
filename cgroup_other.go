//go:build unix && !linux

package torx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// errNoCgroups is why -cgroups fails off Linux: nodes' cgroups are Linux's
// control groups, delegated by systemd.
var errNoCgroups = errors.New("-cgroups needs Linux's cgroup v2 and a systemd user manager")

func inScope() bool { return false }

func enterScope() int {
	fmt.Fprintln(os.Stderr, "torx:", errNoCgroups)
	return 2
}

func prepareCgroupRoot() (string, error) { return "", errNoCgroups }

func nodeCgroup(string, string) (string, error) { return "", errNoCgroups }

func resetCgroup(string) error { return errNoCgroups }

func intoCgroup(*syscall.SysProcAttr, string) (func(), error) { return nil, errNoCgroups }

type cgroupGate struct{}

func gateFor(string) *cgroupGate { return &cgroupGate{} }

func (*cgroupGate) enter(context.Context) (func(), error) { return nil, errNoCgroups }

func (*cgroupGate) freeze(context.Context, string) error { return errNoCgroups }

func (*cgroupGate) thaw(string) error { return errNoCgroups }
