package sim

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"testing/cryptotest"
	"time"

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
	for seed := *firstSeed; seed < *firstSeed+uint64(n); seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			a := run(t, DefaultConfig(seed))
			b := run(t, DefaultConfig(seed))
			if a.Trace != b.Trace {
				t.Fatalf("seed %d is not deterministic: trace %016x, then %016x", seed, a.Trace, b.Trace)
			}
			st := a.Stats
			if st.Crashes == 0 || st.LinkChanges == 0 || st.Compactions == 0 || st.Revokes == 0 || st.ColdRestarts < 2 || st.OK < 200 || len(a.Log) < 50 {
				t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
			}
			checkers(t, a)
			t.Logf("trace %016x: %+v, %d committed writes", a.Trace, st, len(a.Log))
			if out != nil {
				if _, err := fmt.Fprintf(out, "%d %016x\n", seed, a.Trace); err != nil {
					t.Fatal(err)
				}
			}
		})
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
// watchdog can make way: every call that starts 10 election timeouts after
// the stall must succeed.
func TestStalledLeaderIsReplaced(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.StallLeaderAt = 5 * Second
		cfg.LiveFrom = cfg.StallLeaderAt + 10*Second
		cfg.Duration = 25 * Second
		res := run(t, cfg)
		if res.Stats.StalledStops == 0 {
			t.Fatalf("seed %d: the stalled leader never stopped", seed)
		}
	}
}

// TestAllLinkMasks holds each of the 64 sets of directed links between the
// three nodes for 3 virtual seconds, from 2 s on, with no other fault,
// then heals. Every rule and checker must hold, and the cluster must serve
// again 2 s after healing.
func TestAllLinkMasks(t *testing.T) {
	for mask := range uint64(64) {
		cfg := DefaultConfig(1000 + mask)
		cfg.Faults = Faults{HoldMask: mask, HoldMaskAt: 2 * Second}
		cfg.Duration = 5 * Second
		cfg.Heal = 5 * Second
		cfg.Clients = 4
		cfg.NoLeases = true
		res := run(t, cfg)
		checkers(t, res)
		h, err := res.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		if err := check.Linearizable(h, time.Minute); err != nil {
			t.Fatalf("mask %06b: linearizability: %v", mask, err)
		}
	}
}

// TestSwitchingLinks switches to a random set of directed links about
// every 300 ms for 10 virtual seconds, then heals.
func TestSwitchingLinks(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(2000 + seed)
		cfg.Faults = Faults{SwitchEvery: 300 * Millisecond}
		cfg.Duration = 10 * Second
		res := run(t, cfg)
		checkers(t, res)
		if res.Stats.LinkChanges < 10 {
			t.Fatalf("seed %d switched only %d times", seed, res.Stats.LinkChanges)
		}
	}
}
