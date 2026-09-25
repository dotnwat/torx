//go:build linux

package diskfault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dotnwat/torx"
)

// The lab test runs this test binary as a torx suite under -netns, whose
// nodes live in a mount namespace the lab's user namespace owns, so they may
// mount the tmpfs a limit is. TestMain sends the driver and worker
// invocations to torx.

const (
	// requiredEnv turns the lab test's skip, on a host that cannot build a
	// lab, into a failure.
	requiredEnv = "DISKFAULT_LAB_REQUIRED"
	suiteEnv    = "DISKFAULT_TEST_SUITE"
)

func TestMain(m *testing.M) {
	if (len(os.Args) > 1 && os.Args[1] == "worker") || os.Getenv(suiteEnv) != "" {
		torx.Main()
	}
	os.Exit(m.Run())
}

func init() {
	torx.Register("diskfault.lab", func() torx.Job { return &labJob{} })
}

// nothing is a service that asks for one node and runs nothing on it.
type nothing struct{ *torx.ServiceBase }

func newNothing() *nothing {
	s := &nothing{}
	s.ServiceBase = torx.NewServiceBase("nothing", torx.Homogeneous(1, torx.NodeSpec{}), nil)
	return s
}

func (*nothing) Start(context.Context) error { return nil }
func (*nothing) Stop(context.Context) error  { return nil }
func (*nothing) Clean(context.Context) error { return nil }
func (*nothing) Wait(context.Context) error  { return nil }

// labJob limits a directory, fills it, frees it, and unlimits it, checking
// what a write there does at each step.
type labJob struct {
	torx.JobBase
	s *nothing
}

func (j *labJob) Declare(jc *torx.JobContext) {
	j.s = newNothing()
	jc.Register(j.s)
}

func (j *labJob) Run(ctx context.Context, jc *torx.JobContext) error {
	n := j.s.Nodes()[0]
	root := n.Scratch().Root
	dir := filepath.Join(root, "limited")
	defer func() { _ = n.Rm(context.WithoutCancel(ctx), root) }()
	if err := Check(ctx, n, root); err != nil {
		return err
	}
	// write reports whether a write of size bytes to a new file in dir
	// succeeds.
	writes := 0
	write := func(size string) (bool, error) {
		writes++
		file := filepath.Join(dir, fmt.Sprintf("data-%d", writes))
		res, err := n.Exec(ctx, torx.Command("dd", "if=/dev/zero", "of="+file, "bs="+size, "count=1"))
		if err != nil {
			return false, err
		}
		if res.ExitCode != 0 {
			// A write that ran out of room keeps what it wrote; drop it.
			return false, n.Rm(ctx, file)
		}
		return true, nil
	}
	// expect checks a write of size bytes to dir succeeds or fails as want.
	expect := func(size string, want bool, state string) error {
		ok, err := write(size)
		if err != nil {
			return err
		}
		if ok != want {
			return fmt.Errorf("a %s write to %s: succeeded %t, want %t", size, state, ok, want)
		}
		return nil
	}
	steps := []struct {
		act   func() error
		size  string
		want  bool
		state string
	}{
		{func() error { return Limit(ctx, n, dir, 1<<20) }, "2M", false, "a 1 MiB directory"},
		{func() error { return nil }, "64k", true, "a 1 MiB directory"},
		{func() error { return Fill(ctx, n, dir) }, "64k", false, "a filled directory"},
		{func() error { return Free(ctx, n, dir) }, "64k", true, "a freed directory"},
		{func() error { return Unlimit(ctx, n, dir) }, "2M", true, "an unlimited directory"},
	}
	for _, st := range steps {
		if err := st.act(); err != nil {
			return err
		}
		if err := expect(st.size, st.want, st.state); err != nil {
			return err
		}
	}
	// Unlimiting a directory that is not limited, or not there, is no error.
	if err := Unlimit(ctx, n, dir); err != nil {
		return err
	}
	return Unlimit(ctx, n, filepath.Join(root, "absent"))
}

// labUsable says why this host cannot run a lab, or nil if it can.
func labUsable() error {
	for _, tool := range []string{"ip", "nsenter", "sleep", "mount", "umount", "dd"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("%s is not installed", tool)
		}
	}
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET | syscall.CLONE_NEWNS,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("this kernel does not let an unprivileged user create a user namespace: %w", err)
	}
	return nil
}

// TestFaultsInALab runs the lab job through the real driver and worker under
// -netns.
func TestFaultsInALab(t *testing.T) {
	if err := labUsable(); err != nil {
		if os.Getenv(requiredEnv) != "" {
			t.Fatalf("%v (%s is set)", err, requiredEnv)
		}
		t.Skip(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-netns", "-results-dir", "", "diskfault.lab")
	cmd.Env = append(os.Environ(), suiteEnv+"=1")
	out, err := cmd.CombinedOutput()
	if _, ok := errors.AsType[*exec.ExitError](err); ok || err != nil {
		t.Fatalf("suite under -netns failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "1 passed") {
		t.Errorf("suite output does not report the job passing:\n%s", out)
	}
}
