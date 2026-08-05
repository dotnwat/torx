package torx

import "testing"

func TestDriverMainRunDirExclusiveFlags(t *testing.T) {
	// Naming both an exact run directory and a results root is a contradiction,
	// rejected as a usage error before discovery or any run.
	if got := driverMain([]string{"-run-dir", t.TempDir(), "-results-dir", "elsewhere"}); got != 2 {
		t.Fatalf("driverMain = %d, want usage error 2", got)
	}
}
