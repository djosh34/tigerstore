//go:build !sim

package sim

import (
	"os"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestDiagCapture reuses a checkout's translated TestSeeds binary. Run this
// native binary from that checkout's internal/sim with GOSIMTOOL set there.
// The simulator is unchanged; SIM_KEEP asks it to retain the passing run too.
func TestDiagCapture(t *testing.T) {
	if os.Getenv("DIAG_CAPTURE") == "" {
		t.Skip("set DIAG_CAPTURE and SIM_OUT to capture seed 290")
	}
	r := simrun.Do(t, simrun.Run{Test: "TestSeeds", Seed: 290, Env: []string{
		"SIM_SEED=290", "SIM_SEEDS=1", "SIM_KEEP=1",
	}})
	if err := simrun.WriteFiles(os.Getenv("SIM_OUT"), r.Artifacts); err != nil {
		t.Fatal(err)
	}
}
