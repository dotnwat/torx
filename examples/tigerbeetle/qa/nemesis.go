//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/tigerbeetle/qa/tigerbeetle"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// faultWeights names the faults the nemesis injects and how often it picks
// each, relative to the others. The -primary variants aim at the replica
// that leads the newest view, where a fault does the most damage; the others
// pick any replica.
var faultWeights = map[string]int{
	"crash":          2, // SIGKILL a replica; restart it after the hold
	"crash-primary":  3,
	"crash-majority": 1, // SIGKILL a majority at once, so the cluster loses quorum
	"crash-all":      1, // SIGKILL every replica at once, as a power loss would; restart them all
	"pause":          2, // SIGSTOP a replica; SIGCONT it after the hold
	"pause-primary":  3,
	"recover":        1, // a backup loses its data file, and gets a new one from `tigerbeetle recover`
	"reconfigure":    2, // restart a replica with a configuration drawn anew

	// Register more client sessions at once than the cluster keeps
	// (clients_max, 64), so it evicts the workload's, some mid-request.
	"session-storm": 1,

	// Restart a replica with another --limit-request, the batch size a
	// --development replica shrinks to 32KiB (README.md: batch-limit).
	"batch-limit": 1,

	// Overwrite parts of one replica's data file with garbage -- WAL headers
	// and prepares, client replies, grid blocks, and now and then a sector of
	// one superblock copy -- while it runs or while it is down. TigerBeetle
	// repairs a replica's corrupt data from the others. The WAL's headers are
	// corrupted only while the replica runs, when its own copy in memory
	// overwrites them in time: see corrupt-headers.
	"corrupt": 2,
	// Corrupt a sector of the WAL's headers of a replica while it is down.
	// It restarts uncertain of its log's head (recovering_head), and when two
	// replicas are at once, the cluster cannot recover without an operator
	// (README.md: finding 3, tigerbeetle#1376).
	"corrupt-headers": 1,

	// Disk faults, which need data directories that can be limited (-netns).
	"disk-full": 2, // fill a replica's data directory until the hold ends

	// Resource faults, which need nodes with cgroups (-cgroups): the gray
	// failures of a replica that is up but barely working.
	"freeze":             1, // freeze every process of a replica's node at once; thaw it after the hold
	"freeze-primary":     2,
	"cpu-starve":         1, // cap a replica at a sliver of one CPU
	"cpu-starve-primary": 2,
	"mem-pressure":       1, // reclaim a replica's memory down to a fraction of what it uses
	// A slow disk, which needs data files on a block device (storage=disk).
	"slow-disk":         1,
	"slow-disk-primary": 2,

	// Network faults, which need nodes netfault can act on (-netns).
	"partition-primary": 3, // cut the primary off from every other replica
	"partition-half":    2, // split the replicas in two at random
	"partition-bridge":  2, // split them in two with one replica in both halves
	"deafen-primary":    2, // the primary hears no other replica, which still hear it
	"slow":              1, // delay one replica's packets
	"lossy":             1, // drop a share of one replica's packets
	"mtu-blackhole":     2, // a backup loses the primary's large packets, and gets its small ones
}

var diskFaults = map[string]bool{"disk-full": true}

var resFaults = map[string]bool{
	"freeze": true, "freeze-primary": true, "cpu-starve": true, "cpu-starve-primary": true, "mem-pressure": true,
}

var blockFaults = map[string]bool{"slow-disk": true, "slow-disk-primary": true}

var netFaults = map[string]bool{
	"partition-primary": true, "partition-half": true, "partition-bridge": true,
	"deafen-primary": true, "slow": true, "lossy": true, "mtu-blackhole": true,
}

// blackholeSize is the smallest packet mtu-blackhole drops: larger than a
// ping or a prepare_ok, smaller than a prepare that carries a batch.
const blackholeSize = 512

const (
	nemesisQuietMin = 500 * time.Millisecond // between one fault's heal and the next fault
	nemesisQuietMax = 3 * time.Second
	nemesisHoldMin  = 500 * time.Millisecond // how long a fault lasts before it is healed
	nemesisHoldMax  = 8 * time.Second
	// nemesisLongHold is how long a tenth of the faults last instead: long
	// enough that a replica cut off falls further behind than its
	// write-ahead log reaches, and must sync its state from the others.
	nemesisLongHold = 30 * time.Second
)

var errNoTarget = errors.New("no replica to target")

// nemesis injects one fault at a time into the cluster until its context is
// done, holding each for a random time and then healing it, and records every
// fault and heal in the history.
type nemesis struct {
	db     *tigerbeetle.Service
	h      *History
	jc     *torx.JobContext
	rng    *rand.Rand
	faults []string
	net    bool // whether the network faults can act on the nodes
	disk   bool // whether the data directories are limited

	// draw draws a configuration for a replica, for reconfigure.
	draw func(rng *rand.Rand) []string

	res bool // whether the nodes have cgroups, for the resource faults

	// st is what the nemeses running at once share.
	st *faultState
}

// faultState is the faults in effect that more than one nemesis needs to
// know of.
type faultState struct {
	mu sync.Mutex
	// full is the replicas whose disk is full now, so an exit on ENOSPC is
	// expected of them.
	full map[string]bool
	// frozen is the nodes frozen now.
	frozen map[string]bool
	// down is the replicas a fault holds down until its heal, which the
	// nemeses must not restart before then.
	down map[string]int
}

// hold marks nodes as held down by a fault, and returns how to release them.
func (st *faultState) hold(nodes ...*torx.Node) func() {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, n := range nodes {
		st.down[n.Name()]++
	}
	return func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		for _, n := range nodes {
			if st.down[n.Name()]--; st.down[n.Name()] == 0 {
				delete(st.down, n.Name())
			}
		}
	}
}

func (m *nemesis) run(ctx context.Context) {
	for {
		if !sleep(ctx, m.between(nemesisQuietMin, nemesisQuietMax)) {
			return
		}
		m.supervise(ctx)
		fault := m.pick()
		op := Op{Process: "nemesis", F: fault, Start: m.h.Now()}
		heal, targets, err := m.inject(context.WithoutCancel(ctx), fault)
		op.End = m.h.Now()
		op.Node = strings.Join(targets, ",")
		op.Outcome = Ok
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		m.h.Add(op)
		m.jc.Log("info", fmt.Sprintf("nemesis: %s %s %s", fault, op.Node, op.Err))
		if heal == nil {
			continue
		}
		hold := m.between(nemesisHoldMin, nemesisHoldMax)
		if m.rng.IntN(10) == 0 {
			hold = m.between(nemesisHoldMax, nemesisLongHold)
		}
		sleep(ctx, hold)
		// A heal runs even once the run is over: the fault must not outlast it.
		hctx := context.WithoutCancel(ctx)
		op = Op{Process: "nemesis", F: "heal-" + fault, Node: op.Node, Start: m.h.Now()}
		err = heal(hctx)
		op.End = m.h.Now()
		op.Outcome = Ok
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		m.h.Add(op)
		m.jc.Log("info", fmt.Sprintf("nemesis: healed %s %s %s", fault, op.Node, op.Err))
	}
}

func (m *nemesis) pick() string {
	total := 0
	for _, f := range m.faults {
		total += faultWeights[f]
	}
	r := m.rng.IntN(total)
	for _, f := range m.faults {
		if r -= faultWeights[f]; r < 0 {
			return f
		}
	}
	return m.faults[len(m.faults)-1]
}

func (m *nemesis) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(m.rng.Int64N(int64(hi-lo)))
}

// inject applies fault and returns how to heal it (nil for an instant
// fault) and the replicas it hit.
func (m *nemesis) inject(ctx context.Context, fault string) (func(context.Context) error, []string, error) {
	switch fault {
	case "crash", "crash-primary":
		n, err := m.target(ctx, fault == "crash-primary")
		if err != nil {
			return nil, nil, err
		}
		release := m.st.hold(n)
		if err := m.db.Crash(ctx, n); err != nil {
			release()
			return nil, names(n), err
		}
		return m.released(release, m.restart(n)), names(n), nil

	case "crash-majority", "crash-all":
		up := m.responsive()
		k := m.db.Replicas()/2 + 1
		if fault == "crash-all" {
			k = len(up)
		}
		if len(up) < k || k == 0 {
			return nil, nil, errNoTarget
		}
		m.rng.Shuffle(len(up), func(i, j int) { up[i], up[j] = up[j], up[i] })
		victims := up[:k]
		release := m.st.hold(victims...)
		for _, n := range victims {
			if err := m.db.Crash(ctx, n); err != nil {
				return m.released(release, m.restart(victims...)), names(victims...), err
			}
		}
		return m.released(release, m.restart(victims...)), names(victims...), nil

	case "pause", "pause-primary":
		n, err := m.target(ctx, fault == "pause-primary")
		if err != nil {
			return nil, nil, err
		}
		if err := m.db.Pause(ctx, n); err != nil {
			return nil, names(n), err
		}
		return func(ctx context.Context) error { return m.db.Resume(ctx, n) }, names(n), nil

	case "recover":
		// A backup: the others must be up to recover from, and the primary's
		// loss would be a crash-primary first.
		n, err := m.backup(ctx)
		if err != nil {
			return nil, nil, err
		}
		release := m.st.hold(n)
		if err := m.db.Crash(ctx, n); err != nil {
			release()
			return nil, names(n), err
		}
		return m.released(release, func(ctx context.Context) error { return m.db.Recover(ctx, n) }), names(n), nil

	case "reconfigure", "batch-limit":
		n, err := m.target(ctx, m.rng.IntN(2) == 0)
		if err != nil {
			return nil, nil, err
		}
		flags := m.draw(m.rng)
		if fault == "batch-limit" {
			if !slices.Contains(flags, "--experimental") {
				flags = append(flags, "--experimental")
			}
			flags = append(flags, "--limit-request="+[]string{"32KiB", "256KiB", "1MiB"}[m.rng.IntN(3)])
		}
		release := m.st.hold(n)
		if err := m.db.Crash(ctx, n); err != nil {
			release()
			return nil, names(n), err
		}
		m.db.SetReplicaFlags(m.db.Index(n), flags...)
		return m.released(release, m.restart(n)), []string{n.Name() + " " + strings.Join(flags, " ")}, nil

	case "disk-full":
		n, err := m.target(ctx, m.rng.IntN(2) == 0)
		if err != nil {
			return nil, nil, err
		}
		dir := m.db.DataDir(n)
		m.st.mu.Lock()
		m.st.full[n.Name()] = true
		m.st.mu.Unlock()
		heal := func(ctx context.Context) error {
			if err := diskfault.Free(ctx, n, dir); err != nil {
				return err
			}
			m.st.mu.Lock()
			delete(m.st.full, n.Name())
			m.st.mu.Unlock()
			// A replica that hit the full disk exited; bring it back.
			if !m.db.Running(n) {
				return m.db.Restart(ctx, n)
			}
			return nil
		}
		return heal, names(n), diskfault.Fill(ctx, n, dir)

	case "session-storm":
		// The cluster evicts the session that committed least recently, so
		// the storm's sessions keep committing until the heal: with more of
		// them busy than it keeps, it evicts busy sessions, the workload's
		// among them.
		k := 64 + m.rng.IntN(32)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for range k {
			wg.Go(func() {
				c, err := m.db.NewClient()
				if err != nil {
					return
				}
				defer c.Close()
				for {
					select {
					case <-stop:
						return
					default:
					}
					// A real request: the client answers Nop itself.
					done := make(chan error, 1)
					go func() {
						_, err := c.LookupAccounts([]tb.Uint128{tb.ToUint128(1)})
						done <- err
					}()
					select {
					case err := <-done:
						if err != nil {
							return // evicted
						}
					case <-stop:
						return
					}
				}
			})
		}
		heal := func(context.Context) error {
			close(stop)
			wg.Wait()
			return nil
		}
		return heal, []string{fmt.Sprintf("%d sessions", k)}, nil

	case "corrupt", "corrupt-headers":
		n, err := m.target(ctx, m.rng.IntN(2) == 0)
		if err != nil {
			return nil, nil, err
		}
		var heal func(context.Context) error
		atRest := fault == "corrupt-headers" || m.rng.IntN(2) == 0
		if atRest {
			release := m.st.hold(n)
			if err := m.db.Crash(ctx, n); err != nil {
				release()
				return nil, names(n), err
			}
			heal = m.released(release, m.restart(n))
		}
		ranges := m.corruption(atRest && fault == "corrupt")
		if fault == "corrupt-headers" {
			ranges = [][2]int64{{walHeaders + m.rng.Int64N(64)*sector, sector}}
		}
		file := m.db.DataFile(n)
		for _, r := range ranges {
			if err := diskfault.Corrupt(ctx, n, file, r[0], r[1]); err != nil {
				return heal, names(n), err
			}
		}
		how := "live"
		if atRest {
			how = "at rest"
		}
		return heal, []string{fmt.Sprintf("%s %s %v", n.Name(), how, ranges)}, nil

	case "freeze", "freeze-primary":
		n, err := m.target(ctx, fault == "freeze-primary")
		if err != nil {
			return nil, nil, err
		}
		m.setFrozen(n, true)
		heal := func(ctx context.Context) error {
			m.setFrozen(n, false)
			return resfault.Thaw(ctx, n)
		}
		return heal, names(n), resfault.Freeze(ctx, n)

	case "cpu-starve", "cpu-starve-primary":
		n, err := m.target(ctx, fault == "cpu-starve-primary")
		if err != nil {
			return nil, nil, err
		}
		share := []float64{0.01, 0.02, 0.05, 0.1}[m.rng.IntN(4)]
		heal := func(context.Context) error { return resfault.LimitCPU(n, 0) }
		return heal, []string{fmt.Sprintf("%s %v", n.Name(), share)}, resfault.LimitCPU(n, share)

	case "mem-pressure":
		n, err := m.target(ctx, false)
		if err != nil {
			return nil, nil, err
		}
		// Down to between a quarter and three quarters of what it uses,
		// tmpfs pages of its data file included, which reclaim swaps out.
		used, err := resfault.MemoryUsage(n)
		if err != nil {
			return nil, names(n), err
		}
		high := used / 4 * int64(1+m.rng.IntN(3))
		heal := func(context.Context) error { return resfault.LimitMemory(n, 0) }
		return heal, []string{fmt.Sprintf("%s %dMiB of %dMiB", n.Name(), high>>20, used>>20)}, resfault.LimitMemory(n, high)

	case "slow-disk", "slow-disk-primary":
		n, err := m.target(ctx, fault == "slow-disk-primary")
		if err != nil {
			return nil, nil, err
		}
		limit := resfault.IOLimit{
			WriteBPS:  uint64(64+m.rng.IntN(4032)) << 10,
			WriteIOPS: uint64(5 + m.rng.IntN(200)),
			ReadIOPS:  uint64(5 + m.rng.IntN(200)),
		}
		dir := m.db.DataDir(n)
		heal := func(context.Context) error { return resfault.ThrottleIO(n, dir, resfault.IOLimit{}) }
		return heal, []string{n.Name() + " " + limit.String()}, resfault.ThrottleIO(n, dir, limit)

	case "partition-primary":
		p, err := m.target(ctx, true)
		if err != nil {
			return nil, nil, err
		}
		return m.healNet, names(p), netfault.Isolate(ctx, p, m.db.Nodes())

	case "partition-half", "partition-bridge":
		nodes := slices.Clone(m.db.Nodes())
		m.rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		half := len(nodes) / 2
		a, b := nodes[:half], nodes[half:]
		if fault == "partition-bridge" && len(nodes) >= 3 {
			a = nodes[:half+1]
		}
		return m.healNet, []string{strings.Join(names(a...), "+") + "|" + strings.Join(names(b...), "+")},
			netfault.Partition(ctx, a, b)

	case "deafen-primary":
		p, err := m.target(ctx, true)
		if err != nil {
			return nil, nil, err
		}
		return m.healNet, names(p), netfault.Block(ctx, p, m.others(p)...)

	case "mtu-blackhole":
		p, err := m.target(ctx, true)
		if err != nil {
			return nil, nil, err
		}
		others := m.others(p)
		f := others[m.rng.IntN(len(others))]
		return m.healNet, []string{f.Name() + "<" + p.Name()}, netfault.Blackhole(ctx, f, blackholeSize, p)

	case "slow", "lossy":
		n := m.db.Nodes()[m.rng.IntN(len(m.db.Nodes()))]
		shape := netfault.Shape{Delay: time.Duration(20+m.rng.IntN(280)) * time.Millisecond}
		shape.Jitter = shape.Delay / 4
		if fault == "lossy" {
			shape = netfault.Shape{Loss: float64(10 + m.rng.IntN(40))}
		}
		heal := func(ctx context.Context) error { return netfault.Unshape(ctx, n) }
		return heal, []string{fmt.Sprintf("%s %+v", n.Name(), shape)}, netfault.SetShape(ctx, n, shape)
	}
	return nil, nil, fmt.Errorf("unknown fault %q", fault)
}

// The data file's layout in a release build (`tigerbeetle inspect
// constants`): four copies of the superblock, the write-ahead log's headers
// and its prepares, the client replies, and the grid of 512KiB blocks,
// which grows from gridStart.
const (
	superblockCopy = 24 << 10
	walHeaders     = 96 << 10
	walPrepares    = walHeaders + 256<<10
	clientReplies  = walPrepares + 1<<30
	gridStart      = clientReplies + 64<<20 + 160<<10
	sector         = 4 << 10
)

// corruption draws up to four ranges of a data file to corrupt, as
// [offset, size] pairs: a sector or a whole slot of one zone each, and a
// sector of one superblock copy seldom, since a replica that loses every
// copy cannot open its data file at all, a loss TigerBeetle leaves to
// `tigerbeetle recover`. The WAL's headers are left alone at rest.
func (m *nemesis) corruption(atRest bool) [][2]int64 {
	var out [][2]int64
	for range 1 + m.rng.IntN(4) {
		var base, slot, slots int64
		switch r := m.rng.IntN(100); {
		case r < 5:
			base, slot, slots = int64(m.rng.IntN(4))*superblockCopy, sector, superblockCopy/sector
		case r < 30 && !atRest:
			base, slot, slots = walHeaders, sector, 64
		case r < 30:
			continue
		case r < 65:
			base, slot, slots = walPrepares, 1<<20, 1024
		case r < 75:
			base, slot, slots = clientReplies, 1<<20, 64
		default:
			base, slot, slots = gridStart, 512<<10, 64
		}
		off := base + m.rng.Int64N(slots)*slot
		size := int64(sector)
		if slot > sector && m.rng.IntN(3) == 0 {
			size = slot // the whole slot
		} else if slot > sector {
			off += m.rng.Int64N(slot/sector) * sector
		}
		out = append(out, [2]int64{off, size})
	}
	return out
}

// supervise restarts the replicas that exited on their own -- on a full
// disk the heal of which has passed, say -- as an operator's supervisor
// would, leaving those a fault holds down, or whose disk is still full.
// Each restart is recorded in the history.
func (m *nemesis) supervise(ctx context.Context) {
	for _, n := range m.db.Nodes() {
		m.st.mu.Lock()
		skip := m.st.down[n.Name()] > 0 || m.st.full[n.Name()]
		m.st.mu.Unlock()
		if skip || m.db.Running(n) {
			continue
		}
		op := Op{Process: "nemesis", F: "supervise-restart", Node: n.Name(), Start: m.h.Now()}
		err := m.db.Restart(context.WithoutCancel(ctx), n)
		op.End, op.Outcome = m.h.Now(), Ok
		if err != nil {
			op.Outcome, op.Err = Info, err.Error()
		}
		m.h.Add(op)
		m.jc.Log("info", fmt.Sprintf("nemesis: restarted %s, which had exited %s", n.Name(), op.Err))
	}
}

// released is heal, after which the replicas release held down are theirs
// to restart again.
func (m *nemesis) released(release func(), heal func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		defer release()
		return heal(ctx)
	}
}

// restart restarts the replicas that are not running, whether the fault or
// their own exit stopped them.
func (m *nemesis) restart(nodes ...*torx.Node) func(context.Context) error {
	return func(ctx context.Context) error {
		var errs []error
		for _, n := range nodes {
			if !m.db.Running(n) {
				errs = append(errs, m.db.Restart(ctx, n))
			}
		}
		return errors.Join(errs...)
	}
}

func (m *nemesis) healNet(ctx context.Context) error {
	return netfault.Heal(ctx, m.db.Nodes()...)
}

// target picks the replica a fault hits: the primary when primary is set
// and one can be told, otherwise any responsive replica.
func (m *nemesis) target(ctx context.Context, primary bool) (*torx.Node, error) {
	if primary {
		if p, err := m.db.Primary(ctx); err == nil && m.db.Running(p) && !m.db.Paused(p) {
			return p, nil
		}
	}
	up := m.responsive()
	if len(up) == 0 {
		return nil, errNoTarget
	}
	return up[m.rng.IntN(len(up))], nil
}

// backup picks a responsive replica other than the primary.
func (m *nemesis) backup(ctx context.Context) (*torx.Node, error) {
	p, _ := m.db.Primary(ctx)
	var candidates []*torx.Node
	for _, n := range m.responsive() {
		if (p == nil || n.Name() != p.Name()) && !m.db.Standby(n) {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return nil, errNoTarget
	}
	return candidates[m.rng.IntN(len(candidates))], nil
}

func (m *nemesis) others(n *torx.Node) []*torx.Node {
	var out []*torx.Node
	for _, o := range m.db.Nodes() {
		if o.Name() != n.Name() {
			out = append(out, o)
		}
	}
	return out
}

func (m *nemesis) responsive() []*torx.Node {
	var up []*torx.Node
	for _, n := range m.db.Nodes() {
		if m.db.Running(n) && !m.db.Paused(n) && !m.isFrozen(n) {
			up = append(up, n)
		}
	}
	return up
}

func (m *nemesis) setFrozen(n *torx.Node, frozen bool) {
	m.st.mu.Lock()
	defer m.st.mu.Unlock()
	if frozen {
		m.st.frozen[n.Name()] = true
	} else {
		delete(m.st.frozen, n.Name())
	}
}

func (m *nemesis) isFrozen(n *torx.Node) bool {
	m.st.mu.Lock()
	defer m.st.mu.Unlock()
	return m.st.frozen[n.Name()]
}

// healAll ends every fault still in effect: frees full disks, heals the
// network, resumes paused replicas, and restarts stopped ones, including any
// that exited on their own.
func (m *nemesis) healAll(ctx context.Context) error {
	var errs []error
	// Thawed first: a command run on a frozen node would freeze with it.
	if m.res {
		for _, n := range m.db.Nodes() {
			errs = append(errs, resfault.Reset(ctx, n))
			m.setFrozen(n, false)
		}
	}
	if m.disk {
		for _, n := range m.db.Nodes() {
			errs = append(errs, diskfault.Free(ctx, n, m.db.DataDir(n)))
		}
		m.st.mu.Lock()
		clear(m.st.full)
		m.st.mu.Unlock()
	}
	if m.net {
		errs = append(errs, m.healNet(ctx))
		for _, n := range m.db.Nodes() {
			errs = append(errs, netfault.Unshape(ctx, n))
		}
	}
	for _, n := range m.db.Nodes() {
		if m.db.Paused(n) {
			errs = append(errs, m.db.Resume(ctx, n))
		}
		if !m.db.Running(n) {
			errs = append(errs, m.db.Restart(ctx, n))
		}
	}
	return errors.Join(errs...)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func names(nodes ...*torx.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Name()
	}
	return out
}

// configSpace lists, per start flag, the values a replica's configuration
// draws from; "" keeps TigerBeetle's default. Every flag but --cache-grid is
// experimental. The choices lean toward the settings that put the replica's
// less-trodden paths under the faults: no object caches, a pipeline of one,
// short timeouts that fire under a slow network, and commit stalls injected
// on the primary.
var configSpace = map[string][]string{
	"cache-grid":                     {"64MiB", "128MiB", "256MiB"},
	"limit-pipeline-requests":        {"", "0", "1", "8"},
	"cache-accounts":                 {"", "0", "1MiB"},
	"cache-transfers":                {"", "0", "1MiB"},
	"cache-transfers-pending":        {"", "0", "1MiB"},
	"timeout-prepare-ms":             {"", "", "50", "500"},
	"timeout-grid-repair-message-ms": {"", "", "50", "500"},
	"commit-stall-probability":       {"", "", "1/10"},
}

// memoryFlags are the flags --memory replaces: it sizes the caches itself
// from one budget, and refuses to be given any of them.
var memoryFlags = map[string]bool{
	"cache-grid": true, "cache-accounts": true, "cache-transfers": true, "cache-transfers-pending": true,
}

// drawConfig draws a replica configuration, one value per flag in a fixed
// order so a seed always draws the same one. A fifth of the configurations
// size the caches with --memory instead, and a few more emit metrics to a
// StatsD address nobody listens on or log at debug level, paths a replica
// rarely takes under faults.
func drawConfig(rng *rand.Rand) []string {
	var flags, experimental []string
	memory := rng.IntN(5) == 0
	if memory {
		experimental = append(experimental, "--memory="+[]string{"256MiB", "512MiB", "1GiB"}[rng.IntN(3)])
	}
	if rng.IntN(6) == 0 {
		experimental = append(experimental, "--statsd=127.0.0.1:8125")
	}
	if rng.IntN(10) == 0 {
		experimental = append(experimental, "--log-debug")
	}
	for _, flag := range slices.Sorted(maps.Keys(configSpace)) {
		choices := configSpace[flag]
		v := choices[rng.IntN(len(choices))]
		switch {
		case v == "", memory && memoryFlags[flag]:
		case flag == "cache-grid":
			flags = append(flags, "--"+flag+"="+v)
		default:
			experimental = append(experimental, "--"+flag+"="+v)
		}
	}
	if len(experimental) > 0 {
		flags = append(append(flags, "--experimental"), experimental...)
	}
	return flags
}
