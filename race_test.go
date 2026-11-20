//go:build race

package torx

// raceEnabled is whether this binary, which is also the worker the tests
// launch, was built with the race detector.
const raceEnabled = true
