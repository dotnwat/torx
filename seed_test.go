package torx

import (
	"context"
	"encoding/json"
	"testing"
)

func init() {
	Register("stest.seeded", func() Job { return &seededJob{} })
}

// seededJob records the seed it saw in Declare and in Run, and the first draw
// of a named stream, so a test can check what the driver handed it.
type seededJob struct {
	JobBase
	declared uint64
}

func (j *seededJob) Declare(jc *JobContext) {
	j.declared = jc.Seed()
	// The seed may shape the job: an odd seed asks for two nodes.
	jc.Register(newFakeServiceSpec("s", new([]string), 1+int(jc.Seed()%2)))
}

func (j *seededJob) Run(_ context.Context, jc *JobContext) error {
	return jc.Record(map[string]uint64{
		"declared": j.declared,
		"run":      jc.Seed(),
		"draw":     jc.Rand("config").Uint64(),
	})
}

func TestVariantSeedIsStableAndDistinct(t *testing.T) {
	// The derivation is part of the contract: a seed recorded by one build
	// must derive the same variant seeds in the next.
	if got, want := VariantSeed(7, "a[x=1]"), uint64(0xfaaa831f16484c53); got != want {
		t.Fatalf("VariantSeed(7, a[x=1]) = %#x, want %#x", got, want)
	}
	if VariantSeed(7, "a[x=1]") == VariantSeed(7, "a[x=2]") {
		t.Error("two variants of one run share a seed")
	}
	if VariantSeed(7, "a[x=1]") == VariantSeed(8, "a[x=1]") {
		t.Error("one variant gets the same seed under two run seeds")
	}
}

func TestNewRandStreams(t *testing.T) {
	a, b := NewRand(42, "nemesis"), NewRand(42, "nemesis")
	for range 10 {
		if a.Uint64() != b.Uint64() {
			t.Fatal("one seed and stream gave two sequences")
		}
	}
	if NewRand(42, "nemesis").Uint64() == NewRand(42, "client-0").Uint64() {
		t.Error("two streams of one seed start alike")
	}
	if NewRand(42, "nemesis").Uint64() == NewRand(43, "nemesis").Uint64() {
		t.Error("one stream of two seeds starts alike")
	}
}

// TestRunHandsEachVariantItsDerivedSeed runs two variants under a run seed and
// checks each saw the seed VariantSeed derives for it, the same in Declare as
// in Run, and that the seeds land on the results and the suite. A second run
// under the same seed must draw the same.
func TestRunHandsEachVariantItsDerivedSeed(t *testing.T) {
	const runSeed = 1234
	reqs := []JobRequest{
		{ID: "stest.seeded", Params: Params{"trial": 1}},
		{ID: "stest.seeded", Params: Params{"trial": 2}},
	}
	run := func() map[string]map[string]uint64 {
		res := Run(context.Background(), testPool(t, 2), InProcessLauncher{}, reqs, RunOptions{Seed: runSeed})
		if !res.Ok() {
			t.Fatalf("run failed:\n%s", res.Render())
		}
		if res.Seed != runSeed {
			t.Errorf("suite seed = %d, want %d", res.Seed, runSeed)
		}
		out := map[string]map[string]uint64{}
		for _, jr := range res.Jobs {
			want := VariantSeed(runSeed, jr.ID)
			if jr.Seed != want {
				t.Errorf("%s: result seed = %d, want %d", jr.ID, jr.Seed, want)
			}
			var data map[string]uint64
			if err := json.Unmarshal(jr.Data, &data); err != nil {
				t.Fatal(err)
			}
			if data["declared"] != want || data["run"] != want {
				t.Errorf("%s: saw seed %d in Declare and %d in Run, want %d in both", jr.ID, data["declared"], data["run"], want)
			}
			out[jr.ID] = data
		}
		return out
	}
	first, second := run(), run()
	if len(first) != 2 {
		t.Fatalf("got %d results, want 2", len(first))
	}
	var draws []uint64
	for id, d := range first {
		if second[id]["draw"] != d["draw"] {
			t.Errorf("%s drew %d, then %d under the same run seed", id, d["draw"], second[id]["draw"])
		}
		draws = append(draws, d["draw"])
	}
	if draws[0] == draws[1] {
		t.Error("two variants drew alike")
	}
}

// TestSizingSeesTheVariantSeed checks the driver sizes a job under the seed the
// worker will run it with: a seed-shaped job gets the nodes it declares.
func TestSizingSeesTheVariantSeed(t *testing.T) {
	req := JobRequest{ID: "stest.seeded"}
	for _, runSeed := range []uint64{1, 2, 3, 4} {
		spec, err := sizeJob(req, runSeed)
		if err != nil {
			t.Fatal(err)
		}
		want := 1 + int(VariantSeed(runSeed, "stest.seeded")%2)
		if spec.Size() != want {
			t.Errorf("run seed %d: sized to %d node(s), want %d", runSeed, spec.Size(), want)
		}
	}
}
