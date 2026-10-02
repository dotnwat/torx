package linearize

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// A register holds one int; 0 is its initial value. A write of an unknown
// outcome is legal anywhere.
type regIn struct {
	write bool
	v     int
}

type regOut struct {
	v       int  // what a read saw
	unknown bool // the operation's outcome is unknown
}

var register = Model[int, regIn, regOut]{
	Init: func() int { return 0 },
	Step: func(s int, in regIn, out regOut) (bool, int) {
		if in.write {
			return true, in.v
		}
		return out.unknown || out.v == s, s
	},
}

func w(client int, call, ret int64, v int) Op[regIn, regOut] {
	return Op[regIn, regOut]{Client: client, Call: call, Return: ret, Input: regIn{write: true, v: v}}
}

func r(client int, call, ret int64, v int) Op[regIn, regOut] {
	return Op[regIn, regOut]{Client: client, Call: call, Return: ret, Output: regOut{v: v}}
}

func check(t *testing.T, ops []Op[regIn, regOut]) Result[int] {
	t.Helper()
	res, err := Check(context.Background(), register, ops)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestRegister(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []Op[regIn, regOut]
		want Outcome
	}{
		{"empty", nil, Ok},
		{"read initial", []Op[regIn, regOut]{r(0, 0, 1, 0)}, Ok},
		{"read nothing written", []Op[regIn, regOut]{r(0, 0, 1, 7)}, Illegal},
		{"sequential", []Op[regIn, regOut]{w(0, 0, 1, 1), r(1, 2, 3, 1)}, Ok},
		{"stale read", []Op[regIn, regOut]{w(0, 0, 1, 1), r(1, 2, 3, 0)}, Illegal},
		{"concurrent read sees either", []Op[regIn, regOut]{w(0, 0, 10, 1), r(1, 2, 3, 0), r(2, 4, 5, 1)}, Ok},
		// Once a read has seen the new value, a later read must too.
		{"new then old", []Op[regIn, regOut]{w(0, 0, 10, 1), r(1, 2, 3, 1), r(2, 4, 5, 0)}, Illegal},
		{"writes in either order", []Op[regIn, regOut]{w(0, 0, 5, 1), w(1, 0, 5, 2), r(2, 6, 7, 1)}, Ok},
		{"ordered writes", []Op[regIn, regOut]{w(0, 0, 1, 1), w(1, 2, 3, 2), r(2, 4, 5, 1)}, Illegal},
		// A call and a return at one instant count as concurrent.
		{"tie is concurrent", []Op[regIn, regOut]{w(0, 0, 2, 1), r(1, 2, 3, 0)}, Ok},
		// A write with no answer may take effect late, or never.
		{"pending late", []Op[regIn, regOut]{w(0, 0, Pending, 1), r(1, 5, 6, 0), r(1, 7, 8, 1)}, Ok},
		{"pending never", []Op[regIn, regOut]{w(0, 0, Pending, 1), r(1, 5, 6, 0), r(1, 7, 8, 0)}, Ok},
		{"pending not before call", []Op[regIn, regOut]{r(1, 0, 1, 1), w(0, 2, Pending, 1)}, Illegal},
		{"pending flicker", []Op[regIn, regOut]{w(0, 0, Pending, 1), r(1, 5, 6, 1), r(1, 7, 8, 0)}, Illegal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := check(t, tc.ops).Outcome; got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIllegalExplains(t *testing.T) {
	// w1 then r1, then r0 cannot follow: the longest linearization is
	// w1 r1, in state 1, stuck on the read of 0.
	ops := []Op[regIn, regOut]{w(0, 0, 1, 1), r(1, 2, 3, 1), r(2, 4, 5, 0)}
	res := check(t, ops)
	if res.Outcome != Illegal {
		t.Fatalf("outcome %v", res.Outcome)
	}
	if !slices.Equal(res.Longest, []int{0, 1}) || res.State != 1 || res.Stuck != 2 {
		t.Fatalf("longest %v state %d stuck %d", res.Longest, res.State, res.Stuck)
	}
}

func TestBadOp(t *testing.T) {
	_, err := Check(context.Background(), register, []Op[regIn, regOut]{r(0, 5, 4, 0)})
	if !errors.Is(err, ErrBadOp) {
		t.Fatalf("err = %v", err)
	}
}

func TestCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Many concurrent writes, and a read no order explains, make the
	// search explore every subset of the writes.
	var ops []Op[regIn, regOut]
	for i := range 24 {
		ops = append(ops, w(i, 0, 100, i+1))
	}
	ops = append(ops, r(99, 101, 102, 1000))
	res, err := Check(ctx, register, ops)
	if res.Outcome != Unknown || !errors.Is(err, context.Canceled) {
		t.Fatalf("outcome %v err %v", res.Outcome, err)
	}
}

// A compare-and-set register: cas(from, to) succeeds when the value is
// from.
type casIn struct {
	kind     byte // 'r', 'w', 'c'
	from, to int
}

type casOut struct {
	v       int
	ok      bool
	unknown bool
}

var casRegister = Model[int, casIn, casOut]{
	Init: func() int { return 0 },
	Step: func(s int, in casIn, out casOut) (bool, int) {
		switch in.kind {
		case 'w':
			return true, in.to
		case 'c':
			if out.unknown {
				if s == in.from {
					return true, in.to
				}
				return true, s
			}
			if out.ok {
				return s == in.from, in.to
			}
			return s != in.from, s
		}
		return out.unknown || out.v == s, s
	},
}

func TestCAS(t *testing.T) {
	cas := func(c int, call, ret int64, from, to int, ok bool) Op[casIn, casOut] {
		return Op[casIn, casOut]{Client: c, Call: call, Return: ret, Input: casIn{'c', from, to}, Output: casOut{ok: ok}}
	}
	// Two concurrent cas(0, x) cannot both succeed.
	res, err := Check(context.Background(), casRegister, []Op[casIn, casOut]{cas(0, 0, 5, 0, 1, true), cas(1, 0, 5, 0, 2, true)})
	if err != nil || res.Outcome != Illegal {
		t.Fatalf("double cas: %v %v", res.Outcome, err)
	}
	// One of them may fail.
	res, err = Check(context.Background(), casRegister, []Op[casIn, casOut]{cas(0, 0, 5, 0, 1, true), cas(1, 0, 5, 0, 2, false)})
	if err != nil || res.Outcome != Ok {
		t.Fatalf("one cas: %v %v", res.Outcome, err)
	}
}

// TestRandomHistories checks histories of a simulated register against a
// brute-force search over every order of their operations: histories a
// correct register produces, which must be linearizable, and the same with
// a read's answer changed, which may be or not.
func TestRandomHistories(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	illegal := 0
	for trial := range 2000 {
		ops := simulate(rng, 1+rng.IntN(7))
		res := check(t, ops)
		if res.Outcome != Ok {
			t.Fatalf("trial %d: a correct register's history is %v: %+v", trial, res.Outcome, ops)
		}
		if brute(ops) != Ok {
			t.Fatalf("trial %d: brute force disagrees on a correct history", trial)
		}
		// Change one read's answer.
		var reads []int
		for i, op := range ops {
			if !op.Input.write && !op.Output.unknown {
				reads = append(reads, i)
			}
		}
		if len(reads) == 0 {
			continue
		}
		bad := slices.Clone(ops)
		i := reads[rng.IntN(len(reads))]
		bad[i].Output.v = rng.IntN(4)
		got, want := check(t, bad).Outcome, brute(bad)
		if got != want {
			t.Fatalf("trial %d: check says %v, brute force %v: %+v", trial, got, want, bad)
		}
		if got == Illegal {
			illegal++
		}
	}
	// The changed histories must test both verdicts.
	if illegal < 200 {
		t.Fatalf("only %d of the changed histories are illegal", illegal)
	}
}

// simulate runs n operations of a few clients against a register that takes
// each operation at a random instant inside its interval. Some writes lose
// their answer.
func simulate(rng *rand.Rand, n int) []Op[regIn, regOut] {
	type pt struct {
		at int64
		op int
	}
	ops := make([]Op[regIn, regOut], n)
	points := make([]pt, n)
	for i := range ops {
		call := int64(rng.IntN(20))
		ret := call + int64(rng.IntN(8))
		at := call + int64(rng.IntN(int(ret-call)+1))
		ops[i] = Op[regIn, regOut]{Client: i, Call: call, Return: ret, Input: regIn{write: rng.IntN(2) == 0, v: 1 + rng.IntN(3)}}
		points[i] = pt{at, i}
	}
	slices.SortStableFunc(points, func(a, b pt) int { return int(a.at - b.at) })
	v := 0
	for _, p := range points {
		op := &ops[p.op]
		if op.Input.write {
			v = op.Input.v
			if rng.IntN(5) == 0 {
				op.Return = Pending
				op.Output.unknown = true
			}
		} else {
			op.Output.v = v
		}
	}
	return ops
}

// brute decides linearizability by trying every order of the operations.
func brute(ops []Op[regIn, regOut]) Outcome {
	used := make([]bool, len(ops))
	var try func(done int, s int) bool
	try = func(done int, s int) bool {
		if done == len(ops) {
			return true
		}
		for i, op := range ops {
			if used[i] {
				continue
			}
			// op may go next only if no operation left out returned
			// before op was called.
			minimal := true
			for j, o := range ops {
				if !used[j] && j != i && o.Return < op.Call {
					minimal = false
					break
				}
			}
			if !minimal {
				continue
			}
			ok, next := register.Step(s, op.Input, op.Output)
			if !ok {
				continue
			}
			used[i] = true
			if try(done+1, next) {
				return true
			}
			used[i] = false
		}
		return false
	}
	if try(0, register.Init()) {
		return Ok
	}
	return Illegal
}

func TestPartition(t *testing.T) {
	type kin struct {
		key string
		regIn
	}
	ops := []Op[kin, regOut]{
		{Input: kin{"a", regIn{write: true, v: 1}}},
		{Input: kin{"b", regIn{}}},
		{Input: kin{"a", regIn{}}},
	}
	parts := Partition(ops, func(in kin) string { return in.key })
	if len(parts["a"]) != 2 || len(parts["b"]) != 1 {
		t.Fatalf("parts %v", parts)
	}
}

// BenchmarkConcurrent checks histories of 8 clients, each issuing 250
// operations back to back on one register, with a tenth of the writes
// unanswered.
func BenchmarkConcurrent(b *testing.B) {
	rng := rand.New(rand.NewPCG(3, 4))
	ops := simulateClients(rng, 8, 250)
	b.ResetTimer()
	for b.Loop() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		res, err := Check(ctx, register, ops)
		cancel()
		if err != nil || res.Outcome != Ok {
			b.Fatalf("%v %v", res.Outcome, err)
		}
	}
}

// simulateClients runs clients that each issue n operations one after
// another against a register.
func simulateClients(rng *rand.Rand, clients, n int) []Op[regIn, regOut] {
	type pt struct {
		at int64
		op int
	}
	var ops []Op[regIn, regOut]
	var points []pt
	for c := range clients {
		t := int64(rng.IntN(10))
		for range n {
			call := t
			ret := call + 1 + int64(rng.IntN(20))
			at := call + int64(rng.IntN(int(ret-call)+1))
			ops = append(ops, Op[regIn, regOut]{Client: c, Call: call, Return: ret, Input: regIn{write: rng.IntN(3) == 0, v: len(ops) + 1}})
			points = append(points, pt{at, len(ops) - 1})
			t = ret + int64(rng.IntN(3))
		}
	}
	slices.SortStableFunc(points, func(a, b pt) int { return int(a.at - b.at) })
	v := 0
	for _, p := range points {
		op := &ops[p.op]
		if op.Input.write {
			v = op.Input.v
			if rng.IntN(10) == 0 {
				op.Return = Pending
				op.Output.unknown = true
			}
		} else {
			op.Output.v = v
		}
	}
	return ops
}

// weakenReg weakens a register operation: a read can be left out, and a
// write becomes one whose outcome is unknown.
func weakenReg(op Op[regIn, regOut]) (Op[regIn, regOut], Weakening) {
	if !op.Input.write {
		return op, Drop
	}
	op.Output.unknown = true
	op.Return = Pending
	return op, Replace
}

func TestMinimize(t *testing.T) {
	// A stale read buried among unrelated operations: the core is the
	// write it missed, and itself.
	var ops []Op[regIn, regOut]
	for i := range 20 {
		ops = append(ops, r(1, int64(10*i), int64(10*i+1), 0))
	}
	ops = append(ops, w(0, 300, 301, 7))
	for i := range 20 {
		ops = append(ops, r(1, int64(400+10*i), int64(401+10*i), 7))
	}
	ops = append(ops, r(2, 700, 701, 0)) // stale
	for i := range 10 {
		ops = append(ops, w(3, int64(800+10*i), int64(801+10*i), 100+i))
	}
	core, err := Minimize(context.Background(), register, ops, weakenReg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(core, []int{20, 41}) {
		t.Fatalf("core %v", core)
	}
	if _, err := Minimize(context.Background(), register, ops[:20], weakenReg); err == nil {
		t.Fatal("minimized a legal history")
	}
}

func TestMinimizeRandom(t *testing.T) {
	// Every core keeps the history illegal, and weakening any one of its
	// operations as well makes it legal.
	rng := rand.New(rand.NewPCG(5, 6))
	for trial := 0; trial < 300; {
		ops := simulate(rng, 2+rng.IntN(10))
		var reads []int
		for i, op := range ops {
			if !op.Input.write && !op.Output.unknown {
				reads = append(reads, i)
			}
		}
		if len(reads) == 0 {
			continue
		}
		ops[reads[rng.IntN(len(reads))]].Output.v = rng.IntN(4)
		if check(t, ops).Outcome != Illegal {
			continue
		}
		trial++
		core, err := Minimize(context.Background(), register, ops, weakenReg)
		if err != nil {
			t.Fatal(err)
		}
		with := func(idx []int) []Op[regIn, regOut] {
			var out []Op[regIn, regOut]
			for i, op := range ops {
				if slices.Contains(idx, i) {
					out = append(out, op)
				} else if w, how := weakenReg(op); how == Replace {
					out = append(out, w)
				}
			}
			return out
		}
		if check(t, with(core)).Outcome != Illegal {
			t.Fatalf("core %v is legal", core)
		}
		for i := range core {
			if check(t, with(slices.Delete(slices.Clone(core), i, i+1))).Outcome == Illegal {
				t.Fatalf("core %v is not minimal: without %d it is illegal", core, core[i])
			}
		}
	}
}

func TestLimit(t *testing.T) {
	// The history TestCanceled searches, which a bound on configurations
	// stops instead.
	var ops []Op[regIn, regOut]
	for i := range 24 {
		ops = append(ops, w(i, 0, 100, i+1))
	}
	ops = append(ops, r(99, 101, 102, 1000))
	res, err := CheckWithin(context.Background(), register, ops, Limits{Configurations: 1000})
	if res.Outcome != Unknown || !errors.Is(err, ErrLimit) || res.Explored <= 1000 || res.Explored > 1001 {
		t.Fatalf("outcome %v err %v explored %d", res.Outcome, err, res.Explored)
	}
}

// registerSame is the register, told that two unanswered writes of one
// value are the same.
var registerSame = Model[int, regIn, regOut]{
	Init: register.Init,
	Step: register.Step,
	Same: func(a regIn, ao regOut, b regIn, bo regOut) bool {
		return a.write && b.write && a.v == b.v && ao.unknown && bo.unknown
	},
}

// TestSame checks histories with many unanswered writes of few values with
// Same and without, against each other and a brute-force search.
func TestSame(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	verdicts := map[Outcome]int{}
	for trial := range 3000 {
		ops := simulate(rng, 2+rng.IntN(7))
		// More unanswered writes than simulate makes.
		for i := range ops {
			if ops[i].Input.write && rng.IntN(2) == 0 {
				ops[i].Return, ops[i].Output.unknown = Pending, true
			}
		}
		var reads []int
		for i, op := range ops {
			if !op.Input.write {
				reads = append(reads, i)
			}
		}
		if len(reads) > 0 && rng.IntN(2) == 0 {
			ops[reads[rng.IntN(len(reads))]].Output.v = rng.IntN(4)
		}
		want := brute(ops)
		res, err := Check(context.Background(), registerSame, ops)
		if err != nil || res.Outcome != want {
			t.Fatalf("trial %d: with Same %v (%v), brute force %v: %+v", trial, res.Outcome, err, want, ops)
		}
		verdicts[want]++
	}
	if verdicts[Ok] < 300 || verdicts[Illegal] < 300 {
		t.Fatalf("verdicts %v: both must be tested", verdicts)
	}
}

func BenchmarkManyPending(b *testing.B) {
	// 40 unanswered writes of two values, interleaved with reads that see
	// them flip: without Same, the search tries their subsets.
	var ops []Op[regIn, regOut]
	for i := range 40 {
		ops = append(ops, Op[regIn, regOut]{Call: int64(i), Return: Pending, Input: regIn{write: true, v: 1 + i%2}, Output: regOut{unknown: true}})
	}
	for i := range 40 {
		ops = append(ops, r(1, int64(100+2*i), int64(101+2*i), 1+i%2))
	}
	for _, m := range []Model[int, regIn, regOut]{register, registerSame} {
		res, _ := CheckWithin(context.Background(), m, ops, Limits{Configurations: 1 << 20})
		b.Logf("same=%v: %v, %d configurations", m.Same != nil, res.Outcome, res.Explored)
	}
	for b.Loop() {
		if res, _ := Check(context.Background(), registerSame, ops); res.Outcome != Ok {
			b.Fatal(res.Outcome)
		}
	}
}

func TestMinimizeKeep(t *testing.T) {
	// The writes are kept whole, never in the core: the stale read alone
	// is the core.
	ops := []Op[regIn, regOut]{
		r(1, 0, 1, 0), w(0, 2, 3, 7), r(1, 4, 5, 7), r(2, 6, 7, 0), w(3, 8, 9, 9),
	}
	keepWrites := func(op Op[regIn, regOut]) (Op[regIn, regOut], Weakening) {
		if op.Input.write {
			return op, Keep
		}
		return op, Drop
	}
	core, err := Minimize(context.Background(), register, ops, keepWrites)
	if err != nil || !slices.Equal(core, []int{3}) {
		t.Fatalf("core %v, err %v", core, err)
	}
}
