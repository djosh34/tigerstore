package sim

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"testing/cryptotest"
	"time"

	"github.com/djosh34/tigerstore/internal/core"

	"github.com/djosh34/tigerstore/check"
)

var (
	firstSeed = flag.Uint64("sim.seed", 1, "first seed to run")
	numSeeds  = flag.Int("sim.seeds", 0, "number of seeds; 0 means 4 with -short and 16 without")
	traceFile = flag.String("sim.traces", "", "append one line per seed with its trace hash to this file")
)

// run runs one seed with Raft's randomness seeded from it.
func run(t *testing.T, cfg Config) *Result {
	t.Helper()
	cryptotest.SetGlobalRandom(t, cfg.Seed)
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestSeeds runs every seed twice and requires the same trace hash. A
// failing seed is reproduced with -run 'TestSeeds/<seed>$'.
func TestSeeds(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 16
		if testing.Short() {
			n = 4
		}
	}
	var out *os.File
	if *traceFile != "" {
		f, err := os.OpenFile(*traceFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		})
		out = f
	}
	var total Stats
	ran := 0
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			a := run(t, DefaultConfig(seed))
			b := run(t, DefaultConfig(seed))
			if a.Trace != b.Trace {
				t.Fatalf("seed %d is not deterministic: trace %016x, then %016x", seed, a.Trace, b.Trace)
			}
			st := a.Stats
			if st.Crashes == 0 || st.LinkChanges == 0 || st.Compactions == 0 || st.Revokes == 0 || st.ColdRestarts < 2 || st.DiskFaults == 0 || st.OK < 200 || len(a.Log) < 50 {
				t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
			}
			ran++
			total.Fenced += st.Fenced
			total.StalledStops += st.StalledStops
			checkers(t, a)
			t.Logf("trace %016x: %+v, %d committed writes", a.Trace, st, len(a.Log))
			if out != nil {
				if _, err := fmt.Fprintf(out, "%d %016x\n", seed, a.Trace); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	// Corruption and a leader stalled long enough to stop are rare in one
	// run, but a set of seeds must see both.
	if ran >= 64 && !t.Failed() && (total.Fenced == 0 || total.StalledStops == 0) {
		t.Fatalf("%d seeds fenced %d damaged nodes and stopped %d stalled leaders; both must happen", ran, total.Fenced, total.StalledStops)
	}
}

// checkers runs the independent checkers of package check on a run:
// durability, revisions and watches. Linearizability runs on the smaller
// histories of TestLinearizable.
func checkers(t *testing.T, res *Result) {
	t.Helper()
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Durable(h); err != nil {
		t.Errorf("durability: %v", err)
	}
	if err := check.Revisions(h); err != nil {
		t.Errorf("revisions: %v", err)
	}
	if err := check.Watches(h); err != nil {
		t.Errorf("watches: %v", err)
	}
}

// linearizableConfig is a short run whose history Porcupine can check:
// 5 virtual seconds with the same faults and 4 clients, then 5 s of
// healing, without leases.
func linearizableConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Duration = 5 * Second
	cfg.Heal = 5 * Second
	cfg.Clients = 4
	cfg.NoLeases = true
	return cfg
}

// TestLinearizable checks short runs with Porcupine. Not finishing in a
// minute is a failure.
func TestLinearizable(t *testing.T) {
	n := *numSeeds
	if n == 0 {
		n = 16
		if testing.Short() {
			n = 4
		}
	}
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			res := run(t, linearizableConfig(seed))
			h, err := res.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if err := check.Linearizable(h, time.Minute); err != nil {
				t.Fatalf("linearizability: %v", err)
			}
			t.Logf("%d ops checked in %v", len(h.Ops), time.Since(start))
		})
	}
}

// TestStalledLeaderIsReplaced stalls the leader's disk for good at 5 s,
// with no other fault. The leader still heartbeats, so only its own
// watchdog can make way. It must stop within core.StallTicks election
// timeouts of the stall, plus a tick for an append already queued and one
// for the next client write, and every call that starts 10 election
// timeouts after the stall must succeed.
func TestStalledLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.StallLeaderAt = 5 * Second
		cfg.LiveFrom = cfg.StallLeaderAt + 10*election
		cfg.Duration = 25 * Second
		res := run(t, cfg)
		if len(res.StallStops) == 0 {
			t.Fatalf("seed %d: the stalled leader never stopped", seed)
		}
		limit := cfg.StallLeaderAt + core.StallTicks*election + 2*TickTime
		if stop := res.StallStops[0]; stop < cfg.StallLeaderAt || stop > limit {
			t.Fatalf("seed %d: the stalled leader stopped at %d ms, want between %d and %d ms",
				seed, stop/Millisecond, cfg.StallLeaderAt/Millisecond, limit/Millisecond)
		}
	}
}
