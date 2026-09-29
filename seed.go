//go:build unix

// Seeds: reproducible randomness for randomized jobs.
//
// A chaos or randomized-operations job makes random choices -- a
// configuration drawn from a space of options, which fault to inject next,
// which node a client talks to -- and a failure it finds is only worth as much
// as the ability to run it again. So a run has one seed, chosen by the driver
// (at random unless -seed names one) and recorded in run.json, and every
// variant gets a seed of its own derived from the run seed and its id. The
// derivation is what makes a rerun cheap: the same -seed and a selection of
// just the failing variant hand it the same seed it failed with, while the
// variants of one run, a matrix of trials among them, each draw differently.
// The variant seed is recorded on its result.
//
// A job reads its seed from the JobContext and draws from named streams
// (JobContext.Rand), so the choices one part of a job makes do not shift when
// another part draws more or fewer numbers: a configuration drawn from the
// "config" stream is the same whether or not the fault schedule changed.
// Replaying a seed repeats the decisions, not the timing -- a concurrent
// system still interleaves as it will -- which is usually what finding a bug
// again needs.

package torx

import (
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	mrand "math/rand/v2"
)

// VariantSeed derives a variant's seed from the run seed and the variant's id
// (its base id plus parameters, as results name it). It is a pure function, so
// a rerun with the same run seed hands a variant the same seed whatever else
// the run selects.
func VariantSeed(run uint64, id string) uint64 {
	return hash64(run, id)
}

// RandomSeed returns a seed drawn from the operating system's randomness, for
// a run that was not given one.
func RandomSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on the platforms torx supports; a seed is
		// not a secret, so a fixed fallback is harmless if it ever did.
		return 1
	}
	return binary.LittleEndian.Uint64(b[:])
}

// NewRand returns a generator for the named stream of seed. Distinct names
// give independent streams, and the same seed and name always give the same
// sequence. A *rand.Rand is not safe for concurrent use: give each goroutine
// its own stream.
func NewRand(seed uint64, stream string) *mrand.Rand {
	return mrand.New(mrand.NewPCG(seed, hash64(0, stream)))
}

// hash64 mixes a 64-bit value and a string into a 64-bit hash.
func hash64(v uint64, s string) uint64 {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
