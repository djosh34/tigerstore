package sim

import (
	"errors"
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
			if st.Crashes == 0 || st.LinkChanges == 0 || st.Compactions == 0 || st.Revokes == 0 || st.ColdRestarts < 2 || st.DiskFaults == 0 ||
				st.Pauses == 0 || st.LinkStalls == 0 || st.ClockJumps == 0 || st.HalfOpens == 0 ||
				st.GrayNodes == 0 || st.GrayStreams == 0 || st.OK < 200 || len(a.Log) < 50 {
				t.Fatalf("seed %d did too little to test anything: %+v, %d committed writes", seed, st, len(a.Log))
			}
			ran++
			total.Fenced += st.Fenced
			total.StalledStops += st.StalledStops
			total.Asymmetric += st.Asymmetric
			total.CommitStops += st.CommitStops
			total.RecoveryCrashes += st.RecoveryCrashes
			checkers(t, a)
			t.Logf("trace %016x: %+v, %d committed writes", a.Trace, st, len(a.Log))
			if out != nil {
				if _, err := fmt.Fprintf(out, "%d %016x\n", seed, a.Trace); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	// Corruption, a leader stalled long enough to stop and a link 10 times
	// slower than its other direction are rare in one run, but a set of
	// seeds must see each.
	if ran >= 64 && !t.Failed() && (total.Fenced == 0 || total.StalledStops == 0 || total.Asymmetric == 0 ||
		total.CommitStops == 0 || total.RecoveryCrashes == 0) {
		t.Fatalf("%d seeds: %d fenced nodes, %d stopped stalled leaders, %d asymmetric links, %d stopped gray leaders, %d crashes in recovery; each must happen",
			ran, total.Fenced, total.StalledStops, total.Asymmetric, total.CommitStops, total.RecoveryCrashes)
	}
}

// checkers runs the independent checkers of package check on a run:
// durability, revisions and watches. Linearizability runs on the smaller
// histories of TestLinearizable.
func checkers(t *testing.T, res *Result) {
	t.Helper()
	if err := independent(res); err != nil {
		t.Error(err)
	}
}

// independent runs the durability, revision and watch checkers of package
// check on a run.
func independent(res *Result) error {
	h, err := res.Checkable()
	if err != nil {
		return err
	}
	var errs []error
	if err := check.Durable(h); err != nil {
		errs = append(errs, fmt.Errorf("durability: %w", err))
	}
	if err := check.Revisions(h); err != nil {
		errs = append(errs, fmt.Errorf("revisions: %w", err))
	}
	if err := check.Watches(h); err != nil {
		errs = append(errs, fmt.Errorf("watches: %w", err))
	}
	return errors.Join(errs...)
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
		// The mask went in at 2 s and, with no other link fault, held
		// until healing at 5 s.
		if res.Stats.LinkChanges != 1 {
			t.Fatalf("mask %06b: %d link changes, want the one", mask, res.Stats.LinkChanges)
		}
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

// TestEachFaultTakesEffect runs each network, process, gray and recovery
// fault alone
// for 5 virtual seconds and requires that it happened. The simulator
// itself fails a run in which a message overtakes an earlier one on its
// link or a paused node does any work.
func TestEachFaultTakesEffect(t *testing.T) {
	cases := []struct {
		name   string
		faults Faults
		count  func(Stats) int
	}{
		{"link stall", Faults{LinkStallEvery: Second}, func(st Stats) int { return st.LinkStalls }},
		{"pause", Faults{PauseEvery: Second}, func(st Stats) int { return st.Pauses }},
		{"clock jump", Faults{ClockJumpEvery: Second}, func(st Stats) int { return st.ClockJumps }},
		{"half-open link", Faults{HalfOpenEvery: Second}, func(st Stats) int { return st.HalfOpens }},
		{"link delays", Faults{SwitchEvery: 200 * Millisecond, MaxDelay: 5 * Millisecond}, func(st Stats) int { return st.Asymmetric }},
		{"gray node", Faults{GrayEvery: Second}, func(st Stats) int { return st.GrayNodes }},
		{"gray client", Faults{GrayClient: true}, func(st Stats) int { return st.GrayStreams }},
		{"crash in recovery", Faults{CrashEvery: 500 * Millisecond, MaxDown: Second, RecoveryCrash: true}, func(st Stats) int { return st.RecoveryCrashes }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig(3000)
			cfg.Faults = c.faults
			cfg.Duration = 5 * Second
			cfg.Heal = 5 * Second
			res := run(t, cfg)
			checkers(t, res)
			if c.count(res.Stats) == 0 {
				t.Fatalf("the fault never took effect: %+v", res.Stats)
			}
		})
	}
}

// TestHalfOpenLink makes the link from the leader to a follower half-open
// at 5 s, with no other fault, until the peer write timeout fails it 5 s
// later. The leader and the other follower are a quorum, so every call to
// them must succeed and take under a second; the cut-off follower must
// serve again 2 s after the connection fails.
func TestHalfOpenLink(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(4000 + seed)
		cfg.Faults = Faults{}
		cfg.HalfOpenAt = 5 * Second
		cfg.LiveFrom = cfg.HalfOpenAt + halfOpenTimeout + 2*Second
		cfg.Duration = 15 * Second
		res := run(t, cfg)
		if len(res.halfOpens) != 1 {
			t.Fatalf("seed %d: %d half-open links, want 1", seed, len(res.halfOpens))
		}
		h := res.halfOpens[0]
		healthy := 0
		for _, r := range res.History {
			if r.Call < h.start || r.Call >= h.end || r.Node == h.to {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed during the half-open link: %v", seed, r.Node+1, r.Call/Millisecond, r.Err)
			}
			if r.Return-r.Call >= Second {
				t.Fatalf("seed %d: call to node %d at %d ms took %d ms", seed, r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
			}
			healthy++
		}
		if healthy < 20 {
			t.Fatalf("seed %d: only %d calls to the healthy pair during the half-open link", seed, healthy)
		}
	}
}

// TestGrayLeaderIsReplaced makes the leader gray at 5 s until healing,
// with no other fault: its heartbeats get through, its appends do not, so
// Raft's quorum check keeps it leader while nothing commits. It must stop
// within core.CommitStallTicks election timeouts plus two ticks, and every
// call to another node that starts 3 election timeouts after that must
// succeed in under a second.
func TestGrayLeaderIsReplaced(t *testing.T) {
	election := int64(core.DefaultConfig(1, nil).ElectionTicks) * TickTime
	for seed := uint64(1); seed <= 3; seed++ {
		cfg := DefaultConfig(5000 + seed)
		cfg.Faults = Faults{}
		cfg.GrayLeaderAt = 5 * Second
		cfg.Duration = 20 * Second
		res := run(t, cfg)
		if len(res.CommitStops) == 0 {
			t.Fatalf("seed %d: the gray leader never stopped", seed)
		}
		stop := res.CommitStops[0]
		if limit := cfg.GrayLeaderAt + core.CommitStallTicks*election + 2*TickTime; stop > limit {
			t.Fatalf("seed %d: the gray leader stopped at %d ms, after %d ms", seed, stop/Millisecond, limit/Millisecond)
		}
		ok := 0
		for _, r := range res.History {
			if r.Call < stop+3*election || r.Call >= cfg.Duration || r.Node == res.grayNode {
				continue
			}
			if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
				t.Fatalf("seed %d: call to node %d at %d ms failed: %v", seed, r.Node+1, r.Call/Millisecond, r.Err)
			}
			if r.Return-r.Call >= Second {
				t.Fatalf("seed %d: call to node %d at %d ms took %d ms", seed, r.Node+1, r.Call/Millisecond, (r.Return-r.Call)/Millisecond)
			}
			ok++
		}
		if ok < 20 {
			t.Fatalf("seed %d: only %d calls to the healthy pair", seed, ok)
		}
	}
}

// TestDiskFaultAfterHealing cuts every link before the first election, so
// no node appends during the faults: the guaranteed disk fault must fire
// after healing, and the liveness rule must still hold.
func TestDiskFaultAfterHealing(t *testing.T) {
	cfg := DefaultConfig(6001)
	cfg.Faults = Faults{DiskErrorEvery: 1000 * Second, HoldMask: 0, HoldMaskAt: 1}
	cfg.Duration = 5 * Second
	cfg.Heal = 5 * Second
	res := run(t, cfg)
	if res.Stats.DiskFaults != 1 {
		t.Fatalf("%d disk faults, want the guaranteed one", res.Stats.DiskFaults)
	}
	checkers(t, res)
}
