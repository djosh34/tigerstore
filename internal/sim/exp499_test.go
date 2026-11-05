package sim

import (
	"os"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestExp499Busy (exp branch only) runs a busy no-fault seed of seed
// seconds (seed 320 or 1280) and saves its history, for timing the witness
// on it natively (#499, as #485 measured).
func TestExp499Busy(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("exp only")
	}
	simrun.Seeds(t, 320, 1, func(t *testing.T, seed uint64) int {
		cfg := DefaultConfig(1)
		cfg.Faults = Faults{}
		cfg.Duration = int64(seed) * Second
		res := run(t, cfg)
		save(t, replayCommand(t.Name(), seed), res, true)
		t.Logf("busy %d s: %d calls", seed, len(res.History))
		return len(res.History)
	}, nil)
	_ = strconv.Itoa
}
