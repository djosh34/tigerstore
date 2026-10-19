package sim

import (
	"os"
	"testing"
)

// Same configuration as TestEndWaitsForCatchUp, seed 7004 only. Preserve
// -sim.out evidence even on success; the original failing history is also
// kept verbatim in check/testdata for stable checker-only comparisons.
func TestDiag289SaveSeed7004(t *testing.T) {
	if os.Getenv("DIAG289") == "" {
		t.Skip("diagnosis only")
	}
	cfg := DefaultConfig(7004)
	cfg.Faults = Faults{}
	cfg.LagAtEnd = true
	res := run(t, cfg)
	checkers(t, res)
	explain(t, cfg, res)
}
