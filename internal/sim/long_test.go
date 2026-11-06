package sim

import (
	"os"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestBusySeeds runs TestSeeds's seeds with their faults for 1,280 s
// instead of 20 s, every checker on each: the busy seed whose peak memory
// #500 tracks (the length #485 measured). It skips unless $SIM_SEEDS says
// how many seeds to run: a seed takes minutes.
func TestBusySeeds(t *testing.T) {
	longSeeds(t, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Duration = 1280 * Second
		return cfg
	})
}

// TestCampaignShapeSeeds runs seeds of the long campaign's shape: TestSeeds's
// faults about 16 times further apart, for 4 simulated hours, every
// checker on each (#500). It skips unless $SIM_SEEDS says how many seeds
// to run: a seed takes up to hours.
func TestCampaignShapeSeeds(t *testing.T) {
	longSeeds(t, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Duration = 4 * 3600 * Second
		spreadFaults(&cfg.Faults, 16)
		return cfg
	})
}

// longSeeds runs $SIM_SEEDS seeds of the configs that config gives, from
// $SIM_SEED or seed 1, with every checker.
func longSeeds(t *testing.T, config func(seed uint64) Config) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: a long seed takes minutes to hours")
	}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		res := run(t, config(seed))
		checkers(t, res)
		t.Logf("%+v, %d committed writes", res.Stats, len(res.Log))
		return res.Stats
	}, func(uint64, Stats) bool { return false })
}
