//go:build unix

package main

import (
	"fmt"
	"math/big"
	"math/bits"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// u128 is an unsigned 128-bit integer, the width of TigerBeetle's ids and
// amounts, as a value the model can do arithmetic on.
type u128 struct{ hi, lo uint64 }

var maxU128 = u128{^uint64(0), ^uint64(0)}

func u(v uint64) u128 { return u128{lo: v} }

// fromTB decodes the client's little-endian Uint128.
func fromTB(x tb.Uint128) u128 {
	var lo, hi uint64
	for i := 7; i >= 0; i-- {
		lo = lo<<8 | uint64(x[i])
		hi = hi<<8 | uint64(x[8+i])
	}
	return u128{hi, lo}
}

func (a u128) tb() tb.Uint128 {
	var x tb.Uint128
	for i := range 8 {
		x[i] = byte(a.lo >> (8 * i))
		x[8+i] = byte(a.hi >> (8 * i))
	}
	return x
}

func (a u128) isZero() bool { return a.hi == 0 && a.lo == 0 }

func (a u128) cmp(b u128) int {
	switch {
	case a.hi < b.hi:
		return -1
	case a.hi > b.hi:
		return 1
	case a.lo < b.lo:
		return -1
	case a.lo > b.lo:
		return 1
	}
	return 0
}

func (a u128) less(b u128) bool { return a.cmp(b) < 0 }

// add returns a+b and whether it overflowed.
func (a u128) add(b u128) (u128, bool) {
	lo, c := bits.Add64(a.lo, b.lo, 0)
	hi, c := bits.Add64(a.hi, b.hi, c)
	return u128{hi, lo}, c != 0
}

// mustAdd is a+b where the caller has ruled out overflow.
func (a u128) mustAdd(b u128) u128 {
	s, o := a.add(b)
	if o {
		panic("u128 overflow")
	}
	return s
}

// sub is a-b where the caller has ruled out underflow.
func (a u128) sub(b u128) u128 {
	lo, br := bits.Sub64(a.lo, b.lo, 0)
	hi, br := bits.Sub64(a.hi, b.hi, br)
	if br != 0 {
		panic("u128 underflow")
	}
	return u128{hi, lo}
}

// satSub is a-b, or zero when b exceeds a: Zig's -|.
func (a u128) satSub(b u128) u128 {
	if a.less(b) {
		return u128{}
	}
	return a.sub(b)
}

func minU128(a, b u128) u128 {
	if a.less(b) {
		return a
	}
	return b
}

func (a u128) String() string {
	if a.hi == 0 {
		return fmt.Sprint(a.lo)
	}
	if a == maxU128 {
		return "max"
	}
	v := new(big.Int).Lsh(new(big.Int).SetUint64(a.hi), 64)
	return v.Or(v, new(big.Int).SetUint64(a.lo)).String()
}
