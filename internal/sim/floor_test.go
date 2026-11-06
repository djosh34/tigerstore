package sim

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// The simulator's speed is its answered calls per core-second on calm
// runs, against the floor of the same calls (floor.go, #494, #500):
//
//	go test -run '^$' -bench '^BenchmarkFloor$' -benchtime 5x ./internal/sim
//	SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^TestFloorUnderGosim$' -test.v
//	SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^TestCalm$' -test.v
//
// The first is the floor, the second the floor under gosim, which shows
// what gosim alone costs, and the third the simulator. Each prints its
// answered calls per core-second; TestCalm also prints the fixed cost of a
// seed, its CPU time outside the load: the cluster's formation and the
// checks.

// floorLoad is how long the floor's clients run: as long as a calm run's.
func floorLoad() time.Duration {
	cfg := CalmConfig(1)
	return time.Duration(cfg.Duration + cfg.Heal)
}

// add adds r's counts to a total.
func (r *FloorResult) add(o FloorResult) {
	r.Answered += o.Answered
	r.Failed += o.Failed
	r.CPU += o.CPU
	r.Wall += o.Wall
}

// TestFloorUnderGosim runs the floor under gosim, one seed per run.
func TestFloorUnderGosim(t *testing.T) {
	if simrun.Race {
		t.Skip("it measures gosim runs")
	}
	var total FloorResult
	seeds := 0
	if !simrun.Seeds(t, 1, 1, func(t *testing.T, seed uint64) FloorResult {
		var r FloorResult
		var err error
		simrun.Bubble(t, func(*testing.T) { r, err = Floor(seed, floorLoad()) })
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d calls answered, %d failed, %.3f s of CPU time: %.0f calls per core-second", r.Answered, r.Failed, r.CPU.Seconds(), r.CallsPerCoreSecond())
		return r
	}, func(_ uint64, r FloorResult) bool {
		total.add(r)
		seeds++
		return false
	}) {
		return
	}
	t.Logf("%d seeds of the floor under gosim: %d calls answered, %d failed, %.3f s of CPU time: %.0f calls per core-second",
		seeds, total.Answered, total.Failed, total.CPU.Seconds(), total.CallsPerCoreSecond())
}

// calmCost is what a calm seed cost: the calls answered, the CPU time of
// its load, and that of the whole seed. InRun is set for a seed that forms
// its cluster in the run, under its load, which the totals leave out.
type calmCost struct {
	Answered   int
	Load, Seed time.Duration
	InRun      bool
}

// TestCalm runs calm seeds with every check, and measures the simulator's
// answered calls per core-second of load and the fixed cost per seed. The
// load starts as the nodes start from the disks of their formed cluster,
// so it holds their first election, which the floor's does not; one seed
// in formUnderFaults forms its cluster in the run, under its load, and is
// left out.
func TestCalm(t *testing.T) {
	if simrun.Race {
		t.Skip("it measures gosim runs")
	}
	var total calmCost
	seeds := 0
	if !simrun.Seeds(t, 1, 1, func(t *testing.T, seed uint64) calmCost {
		if !formsFirst(CalmConfig(seed)) {
			t.Logf("seed %d forms its cluster under its load; it is left out", seed)
			return calmCost{InRun: true}
		}
		before, err := simrun.CPUTime()
		if err != nil {
			t.Fatal(err)
		}
		res := run(t, CalmConfig(seed))
		checkers(t, res)
		after, err := simrun.CPUTime()
		if err != nil {
			t.Fatal(err)
		}
		c := calmCost{Answered: res.Stats.OK, Load: res.LoadCPU, Seed: after - before}
		t.Logf("%d calls answered, %.3f s of CPU time in the load, %.3f s in the seed: %.0f calls per core-second, %.3f s fixed",
			c.Answered, c.Load.Seconds(), c.Seed.Seconds(), float64(c.Answered)/c.Load.Seconds(), (c.Seed - c.Load).Seconds())
		return c
	}, func(_ uint64, c calmCost) bool {
		if c.InRun {
			return false
		}
		total.Answered += c.Answered
		total.Load += c.Load
		total.Seed += c.Seed
		seeds++
		return false
	}) {
		return
	}
	t.Logf("%d calm seeds: %d calls answered, %.0f per core-second of load, %.3f s fixed per seed",
		seeds, total.Answered, float64(total.Answered)/total.Load.Seconds(), (total.Seed-total.Load).Seconds()/float64(seeds))
}
