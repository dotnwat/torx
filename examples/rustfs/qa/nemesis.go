//go:build unix

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/rustfs/qa/rustfs"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
)

// faultWeights names the faults the nemesis injects and how often it picks
// each, relative to the others.
var faultWeights = map[string]int{
	"crash":       3, // SIGKILL a server; restart it after the hold
	"crash-two":   1, // SIGKILL two servers at once, which can take writes below quorum
	"crash-all":   1, // SIGKILL every server at once; restart them all
	"terminate":   1, // stop a server with SIGTERM, as an operator would; restart it
	"pause":       3, // SIGSTOP a server; SIGCONT it after the hold
	"pause-two":   1,
	"reconfigure": 1, // restart a server with settings drawn anew

	// Drive faults, on the fragile drives only: never more of them than
	// the erasure code can lose, so a read that comes back wrong, or a write
	// that is lost, is RustFS's doing.
	"wipe-drive":    1, // empty a drive under its running server, as a disk swapped hot
	"replace-drive": 1, // crash a server, empty one of its drives, and restart it
	"corrupt":       2, // overwrite part of a few of a drive's files with garbage

	// Disk faults, which need drives that can be limited (-netns).
	"disk-full": 2, // fill one drive of a server, or all of them, until the hold ends

	// Network faults, which need nodes netfault can act on (-netns).
	"partition-one":    3, // cut a server off from every other
	"partition-half":   2, // split the servers in two
	"partition-bridge": 1, // split them in two with one server in both halves
	"deafen":           1, // a server hears no other, which still hear it
	"slow":             1, // delay one server's packets
	"lossy":            1, // drop a share of one server's packets

	// Resource faults, which need nodes with cgroups (-cgroups).
	"freeze":       2, // freeze every process of a server's node; thaw it after the hold
	"cpu-starve":   1, // cap a server at a sliver of one CPU
	"mem-pressure": 1, // reclaim a server's memory down to a fraction of what it uses
}

var diskFaults = map[string]bool{"disk-full": true}

var netFaults = map[string]bool{
	"partition-one": true, "partition-half": true, "partition-bridge": true, "deafen": true, "slow": true, "lossy": true,
}

var resFaults = map[string]bool{"freeze": true, "cpu-starve": true, "mem-pressure": true}

// drive is one drive of one server.
type drive struct {
	n *torx.Node
	d int
}

func (d drive) String() string { return d.n.Name() + "/drive" + strconv.Itoa(d.d) }

// faults is what the nemeses share: the cluster, the faults in effect that
// more than one must know of, and the job's history.
type faults struct {
	fs     *rustfs.Service
	h      *History
	jc     *torx.JobContext
	bucket string
	// fragile is the drives the drive faults may hit.
	fragile []drive
	// draw draws a server's settings, for reconfigure.
	draw func(rng *rand.Rand) []string

	holds nemesis.Holds

	mu     sync.Mutex
	frozen map[string]bool // nodes frozen now
	paused map[string]bool // nodes a pause holds stopped now
}

// build returns the nemesis faults named in names.
func (f *faults) build(names []string) []nemesis.Fault {
	var out []nemesis.Fault
	for _, name := range names {
		out = append(out, nemesis.Fault{Name: name, Weight: faultWeights[name], Inject: func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			heal, targets, err := f.inject(ctx, rng, name)
			return heal, strings.Join(targets, " "), err
		}})
	}
	return out
}

// record puts a fault or heal into the history.
func (f *faults) record(ev nemesis.Event) {
	t0 := time.Now().Add(-f.h.Now())
	op := Op{Process: "nemesis", F: ev.Fault, Node: ev.Target, Start: ev.Start.Sub(t0), End: ev.End.Sub(t0), Outcome: Ok}
	if ev.Heal {
		op.F = "heal-" + ev.Fault
	}
	if ev.Err != nil {
		op.Outcome, op.Err = Info, ev.Err.Error()
	}
	f.h.Add(op)
	f.jc.Log("info", fmt.Sprintf("nemesis: %s %s %s", op.F, op.Node, op.Err))
}

// inject applies fault and returns how to heal it (nil for an instant
// fault) and what it hit.
func (f *faults) inject(ctx context.Context, rng *rand.Rand, fault string) (nemesis.Heal, []string, error) {
	switch fault {
	case "crash", "crash-two", "crash-all":
		up := f.responsive()
		k := map[string]int{"crash": 1, "crash-two": 2, "crash-all": len(up)}[fault]
		if len(up) < k || k == 0 {
			return nil, nil, nemesis.ErrNoTarget
		}
		rng.Shuffle(len(up), func(i, j int) { up[i], up[j] = up[j], up[i] })
		victims := up[:k]
		release := f.holds.Hold(names(victims...)...)
		heal := nemesis.Released(release, f.restart(victims...))
		for _, n := range victims {
			if err := f.fs.Crash(ctx, n); err != nil {
				return heal, names(victims...), err
			}
		}
		return heal, names(victims...), nil

	case "terminate":
		n, err := f.target(rng)
		if err != nil {
			return nil, nil, err
		}
		release := f.holds.Hold(n.Name())
		clean, err := f.fs.Terminate(ctx, n)
		heal := nemesis.Released(release, f.restart(n))
		if err == nil && !clean {
			err = fmt.Errorf("%s did not exit within its grace after SIGTERM", n.Name())
		}
		return heal, names(n), err

	case "pause", "pause-two":
		up := f.responsive()
		k := map[string]int{"pause": 1, "pause-two": 2}[fault]
		if len(up) < k {
			return nil, nil, nemesis.ErrNoTarget
		}
		rng.Shuffle(len(up), func(i, j int) { up[i], up[j] = up[j], up[i] })
		victims := up[:k]
		f.setPaused(true, victims...)
		heal := func(ctx context.Context) error {
			var errs []error
			for _, n := range victims {
				errs = append(errs, f.fs.Resume(ctx, n))
			}
			f.setPaused(false, victims...)
			return errors.Join(errs...)
		}
		for _, n := range victims {
			if err := f.fs.Pause(ctx, n); err != nil {
				return heal, names(victims...), err
			}
		}
		return heal, names(victims...), nil

	case "reconfigure":
		n, err := f.target(rng)
		if err != nil {
			return nil, nil, err
		}
		env := f.draw(rng)
		release := f.holds.Hold(n.Name())
		if err := f.fs.Crash(ctx, n); err != nil {
			release()
			return nil, names(n), err
		}
		f.fs.SetServerEnv(f.fs.Index(n), env...)
		return nemesis.Released(release, f.restart(n)), []string{n.Name() + " " + strings.Join(env, " ")}, nil

	case "wipe-drive":
		d, err := f.fragileDrive(rng)
		if err != nil {
			return nil, nil, err
		}
		return nil, []string{d.String()}, f.fs.WipeDrive(ctx, d.n, d.d)

	case "replace-drive":
		d, err := f.fragileDrive(rng)
		if err != nil {
			return nil, nil, err
		}
		release := f.holds.Hold(d.n.Name())
		heal := nemesis.Released(release, f.restart(d.n))
		if err := f.fs.Crash(ctx, d.n); err != nil {
			return heal, []string{d.String()}, err
		}
		return heal, []string{d.String()}, f.fs.WipeDrive(ctx, d.n, d.d)

	case "corrupt":
		d, err := f.fragileDrive(rng)
		if err != nil {
			return nil, nil, err
		}
		hit, err := f.corrupt(ctx, rng, d)
		return nil, append([]string{d.String()}, hit...), err

	case "disk-full":
		n, err := f.target(rng)
		if err != nil {
			return nil, nil, err
		}
		var ds []int
		if rng.IntN(2) == 0 {
			ds = []int{rng.IntN(f.fs.Drives())}
		} else {
			for d := range f.fs.Drives() {
				ds = append(ds, d)
			}
		}
		var hit []string
		for _, d := range ds {
			hit = append(hit, drive{n, d}.String())
		}
		release := f.holds.Hold(hit...)
		heal := nemesis.Released(release, func(ctx context.Context) error {
			var errs []error
			for _, d := range ds {
				errs = append(errs, diskfault.Free(ctx, n, f.fs.Drive(n, d)))
			}
			return errors.Join(errs...)
		})
		for _, d := range ds {
			if err := diskfault.Fill(ctx, n, f.fs.Drive(n, d)); err != nil {
				return heal, hit, err
			}
		}
		return heal, hit, nil

	case "partition-one":
		n := f.any(rng)
		return f.healNet, names(n), netfault.Isolate(ctx, n, f.fs.Nodes())

	case "partition-half", "partition-bridge":
		nodes := slices.Clone(f.fs.Nodes())
		rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		half := len(nodes) / 2
		a, b := nodes[:half], nodes[half:]
		if fault == "partition-bridge" && len(nodes) >= 3 {
			a = nodes[:half+1]
		}
		return f.healNet, []string{strings.Join(names(a...), "+") + "|" + strings.Join(names(b...), "+")},
			netfault.Partition(ctx, a, b)

	case "deafen":
		n := f.any(rng)
		return f.healNet, names(n), netfault.Block(ctx, n, f.others(n)...)

	case "slow", "lossy":
		n := f.any(rng)
		shape := netfault.Shape{Delay: time.Duration(20+rng.IntN(280)) * time.Millisecond}
		shape.Jitter = shape.Delay / 4
		if fault == "lossy" {
			shape = netfault.Shape{Loss: float64(10 + rng.IntN(40))}
		}
		heal := func(ctx context.Context) error { return netfault.Unshape(ctx, n) }
		return heal, []string{fmt.Sprintf("%s %+v", n.Name(), shape)}, netfault.SetShape(ctx, n, shape)

	case "freeze":
		n, err := f.target(rng)
		if err != nil {
			return nil, nil, err
		}
		f.setFrozen(n, true)
		heal := func(ctx context.Context) error {
			f.setFrozen(n, false)
			return resfault.Thaw(ctx, n)
		}
		return heal, names(n), resfault.Freeze(ctx, n)

	case "cpu-starve":
		n := f.any(rng)
		share := []float64{0.01, 0.02, 0.05, 0.1}[rng.IntN(4)]
		heal := func(context.Context) error { return resfault.LimitCPU(n, 0) }
		return heal, []string{fmt.Sprintf("%s %v", n.Name(), share)}, resfault.LimitCPU(n, share)

	case "mem-pressure":
		n := f.any(rng)
		used, err := resfault.MemoryUsage(n)
		if err != nil {
			return nil, names(n), err
		}
		high := used / 4 * int64(1+rng.IntN(3))
		heal := func(context.Context) error { return resfault.LimitMemory(n, 0) }
		return heal, []string{fmt.Sprintf("%s %dMiB of %dMiB", n.Name(), high>>20, used>>20)}, resfault.LimitMemory(n, high)
	}
	return nil, nil, fmt.Errorf("unknown fault %q", fault)
}

// corrupt overwrites part of up to three of a drive's object files -- the
// metadata of an object (xl.meta) or a shard of its data -- with garbage,
// and returns which and where.
func (f *faults) corrupt(ctx context.Context, rng *rand.Rand, d drive) ([]string, error) {
	dir := filepath.Join(f.fs.Drive(d.n, d.d), f.bucket)
	res, err := d.n.Exec(ctx, torx.Command("sh", "-c", `[ -d "$1" ] || exit 0; find "$1" -type f -exec wc -c {} +`, "sh", dir))
	if err != nil {
		return nil, err
	}
	type file struct {
		path string
		size int64
	}
	var files []file
	for line := range bytes.Lines(res.Stdout) {
		fields := strings.Fields(string(line))
		if len(fields) != 2 || fields[1] == "total" {
			continue
		}
		size, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || size == 0 {
			continue
		}
		files = append(files, file{fields[1], size})
	}
	if len(files) == 0 {
		return nil, nemesis.ErrNoTarget
	}
	var hit []string
	for range 1 + rng.IntN(3) {
		fl := files[rng.IntN(len(files))]
		size := min(fl.size, int64(1+rng.IntN(4096)))
		off := rng.Int64N(fl.size - size + 1)
		if err := diskfault.Corrupt(ctx, d.n, fl.path, off, size); err != nil {
			return hit, err
		}
		rel, _ := filepath.Rel(dir, fl.path)
		hit = append(hit, fmt.Sprintf("%s@%d+%d", rel, off, size))
	}
	return hit, nil
}

// supervise restarts the servers that exited on their own, as a supervisor
// would, unless a fault holds them down. It runs before each fault.
func (f *faults) supervise(ctx context.Context) {
	for _, n := range f.fs.Nodes() {
		if f.fs.Running(n) || f.holds.Held(n.Name()) {
			continue
		}
		err := f.fs.Restart(ctx, n)
		op := Op{Process: "nemesis", F: "supervise-restart", Node: n.Name(), Start: f.h.Now(), Outcome: Ok}
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		op.End = f.h.Now()
		f.h.Add(op)
	}
}

// restart returns a heal that restarts nodes.
func (f *faults) restart(nodes ...*torx.Node) nemesis.Heal {
	return func(ctx context.Context) error {
		var errs []error
		for _, n := range nodes {
			if !f.fs.Running(n) {
				errs = append(errs, f.fs.Restart(ctx, n))
			}
		}
		return errors.Join(errs...)
	}
}

func (f *faults) healNet(ctx context.Context) error {
	return netfault.Heal(ctx, f.fs.Nodes()...)
}

// target picks a running server that no fault has stopped.
func (f *faults) target(rng *rand.Rand) (*torx.Node, error) {
	up := f.responsive()
	if len(up) == 0 {
		return nil, nemesis.ErrNoTarget
	}
	return up[rng.IntN(len(up))], nil
}

// any picks any server.
func (f *faults) any(rng *rand.Rand) *torx.Node {
	nodes := f.fs.Nodes()
	return nodes[rng.IntN(len(nodes))]
}

// fragileDrive picks a fragile drive whose server no other fault holds.
func (f *faults) fragileDrive(rng *rand.Rand) (drive, error) {
	var ok []drive
	for _, d := range f.fragile {
		if !f.holds.Held(d.n.Name()) && !f.holds.Held(d.String()) && !f.isFrozen(d.n) {
			ok = append(ok, d)
		}
	}
	if len(ok) == 0 {
		return drive{}, nemesis.ErrNoTarget
	}
	return ok[rng.IntN(len(ok))], nil
}

func (f *faults) others(n *torx.Node) []*torx.Node {
	var out []*torx.Node
	for _, m := range f.fs.Nodes() {
		if m.Name() != n.Name() {
			out = append(out, m)
		}
	}
	return out
}

// responsive is the servers running, and neither paused nor frozen.
func (f *faults) responsive() []*torx.Node {
	var out []*torx.Node
	for _, n := range f.fs.Nodes() {
		if f.fs.Running(n) && !f.isPaused(n) && !f.isFrozen(n) && !f.holds.Held(n.Name()) {
			out = append(out, n)
		}
	}
	return out
}

func (f *faults) setFrozen(n *torx.Node, frozen bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if frozen {
		f.frozen[n.Name()] = true
	} else {
		delete(f.frozen, n.Name())
	}
}

func (f *faults) isFrozen(n *torx.Node) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frozen[n.Name()]
}

func (f *faults) setPaused(paused bool, nodes ...*torx.Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range nodes {
		if paused {
			f.paused[n.Name()] = true
		} else {
			delete(f.paused, n.Name())
		}
	}
}

func (f *faults) isPaused(n *torx.Node) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused[n.Name()]
}

// healAll undoes every fault there may be, and brings every server back:
// what the job does after the nemeses stop, before the final reads.
func (f *faults) healAll(ctx context.Context, net, disk, res bool) error {
	var errs []error
	if res {
		for _, n := range f.fs.Nodes() {
			errs = append(errs, resfault.Reset(ctx, n))
		}
	}
	if net {
		errs = append(errs, f.healNet(ctx))
		for _, n := range f.fs.Nodes() {
			errs = append(errs, netfault.Unshape(ctx, n))
		}
	}
	if disk {
		for _, n := range f.fs.Nodes() {
			for d := range f.fs.Drives() {
				errs = append(errs, diskfault.Free(ctx, n, f.fs.Drive(n, d)))
			}
		}
	}
	for _, n := range f.fs.Nodes() {
		if f.fs.Paused(n) {
			errs = append(errs, f.fs.Resume(ctx, n))
		}
		if !f.fs.Running(n) {
			errs = append(errs, f.fs.Restart(ctx, n))
		}
	}
	return errors.Join(errs...)
}

func names(nodes ...*torx.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Name()
	}
	return out
}
