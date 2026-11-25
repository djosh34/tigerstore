package sim

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A scenario draws its whole setup from its seed (#466): the weights are
// the constants below, and nothing at run time changes them, so that the
// seed and the commit decide a run. Scenario draws a fresh one: the
// cluster's size, the load and its clients, the faults and how their
// rates change, the links' delays, the memory budget, the nodes' limits
// and the values' sizes.

// The kinds of load a scenario draws, in percent: the small keys of
// client.go (60), the Kubernetes-shaped load of kube.go (28), and the
// membership work (membership.go) beside the small keys (the rest).
const (
	scenarioSmall      = 60
	scenarioKubernetes = scenarioSmall + 28
)

// scenarioLoseQuorum is the percentage of the scenarios with faults and
// no membership work that lose their quorum for good (Config.LoseQuorum).
const scenarioLoseQuorum = 3

// scenarioCertExpiry is the percentage of the scenarios without the
// membership work whose certificate expires (Config.CertExpiresAt).
const scenarioCertExpiry = 3

// scenarioSizes are the cluster sizes a scenario draws, with their
// weights: three and five voters much more often than one.
var scenarioSizes = []weighted[int]{{1, 1}, {3, 6}, {5, 3}}

// scenarioRates are the fault rates a scenario draws, as the scale of the
// mean times between the faults that come at random times, with their
// weights: none, calm (16 times further apart), DefaultConfig's, and
// storms (4 times closer).
var scenarioRates = []weighted[float64]{{0, 1}, {16, 3}, {1, 8}, {0.25, 3}}

// scenarioDelays are the most of a link's own one-way delay a scenario
// draws, with their weights: DefaultConfig's 2 ms, and up to 10 ms and
// beyond.
var scenarioDelays = []weighted[int64]{
	{2 * Millisecond, 4}, {5 * Millisecond, 2}, {10 * Millisecond, 3}, {25 * Millisecond, 1},
}

// scenarioCatchups are the applied entries the nodes keep for followers
// that lag, from the simulator's tiny window to wl's production one.
var scenarioCatchups = []weighted[uint64]{{wlCatchup, 4}, {1000, 2}, {100_000, 2}}

// scenarioDiskBudgets are the nodes' disk budgets, which size the log's
// ring and how much log comes between checkpoints (wlnode.go), from
// DefaultConfig's tiny one to 16 MiB.
var scenarioDiskBudgets = []weighted[int64]{{32 << 10, 4}, {1 << 20, 2}, {16 << 20, 1}}

// scenarioChunkKeys are the most keys a checkpoint writes in one write,
// from a few to wl's 4,096 (0).
var scenarioChunkKeys = []weighted[int]{{0, 2}, {16, 1}, {256, 1}}

// weighted is a choice and its weight.
type weighted[T any] struct {
	v T
	w int
}

// pick draws one of choices by weight.
func pick[T any](rng *rand.Rand, choices []weighted[T]) T {
	total := 0
	for _, c := range choices {
		total += c.w
	}
	n := rng.IntN(total)
	for _, c := range choices[:len(choices)-1] {
		if n < c.w {
			return c.v
		}
		n -= c.w
	}
	return choices[len(choices)-1].v
}

// logUniform draws from lo to hi, each power of two as likely.
func logUniform(rng *rand.Rand, lo, hi int64) int64 {
	return int64(float64(lo) * math.Pow(float64(hi)/float64(lo), rng.Float64()))
}

// Scenario is the fresh scenario of seed: 20 to 80 s of faults (#495),
// on wl nodes, every checker on it.
func Scenario(seed uint64) Config {
	rng := rand.New(rand.NewPCG(seed, 0x7363656e))
	cfg := DefaultConfig(seed)
	switch k := rng.IntN(100); {
	case k < scenarioSmall:
	case k < scenarioKubernetes:
		cfg = kubeConfig(seed, false)
		cfg.Objects = int(logUniform(rng, 200, 2000))
	default:
		cfg = membershipConfig(seed)
	}
	cfg.Nodes = pick(rng, scenarioSizes)
	cfg.Duration = 20*Second + rng.Int64N(60*Second)
	if cfg.Objects > 0 {
		cfg.Clients, cfg.V3Clients = 4+rng.IntN(13), rng.IntN(4)
	} else {
		cfg.Clients, cfg.V3Clients = 1+rng.IntN(10), rng.IntN(5)
		if rng.IntN(4) == 0 {
			cfg.ValueBytes = int(logUniform(rng, 64, 64<<10))
		}
	}
	// The membership work needs time between the faults that break
	// connections (membershipConfig), so its faults are never storms.
	rate := pick(rng, scenarioRates)
	if cfg.Membership {
		rate = max(rate, 1)
	}
	switch rate {
	case 0:
		cfg.Faults = Faults{}
	default:
		spreadFaults(&cfg.Faults, rate)
	}
	// Half the scenarios with faults change their rates during the run:
	// one to three waves, each from none to a storm.
	if rate != 0 && rng.IntN(2) == 0 {
		for i := range 1 + rng.IntN(3) {
			at := cfg.Duration * int64(i+1) / 4
			scale := pick(rng, scenarioRates) / rate
			if cfg.Membership {
				scale = max(scale, 1)
			}
			cfg.Waves = append(cfg.Waves, Wave{At: at + rng.Int64N(cfg.Duration/4), Scale: scale})
		}
	}
	if d := pick(rng, scenarioDelays); d != 2*Millisecond {
		cfg.LinkDelay = d
		cfg.Faults.MaxDelay = max(cfg.Faults.MaxDelay, 2*d)
	}
	// The budgets start at the least a wl node keeps: below about 255
	// MiB its buffers leave its store less than a quarter of the budget
	// (TestTheBuffersFitTheBudget), and it takes the budget anyway (#760).
	if rng.IntN(2) == 0 {
		cfg.MemoryBytes = logUniform(rng, 256<<20, 4<<30)
	}
	cfg.Catchup = pick(rng, scenarioCatchups)
	// The small keys' runs with a disk budget draw it, with room for
	// their values; the Kubernetes load sizes its own (kubeConfig).
	if cfg.Objects == 0 && cfg.Faults.DiskBudget > 0 {
		cfg.Faults.DiskBudget = max(pick(rng, scenarioDiskBudgets), 64*int64(cfg.ValueBytes))
		cfg.Faults.DiskCapacity = int(4 * cfg.Faults.DiskBudget)
	}
	cfg.ckptChunkKeys = pick(rng, scenarioChunkKeys)
	// An operator replaces a node fenced for good, but in the membership
	// work's runs, which damage no disk, and in the few that lose their
	// quorum for good.
	cfg.Faults.Replace = !cfg.Membership
	if !cfg.Membership && rate != 0 && rng.IntN(100) < scenarioLoseQuorum {
		cfg.LoseQuorum, cfg.Faults.Replace = true, false
	}
	// A few have their certificate expire during the faults, and an
	// operator rotate it 3 to 15 s later (Config.CertExpiresAt).
	if !cfg.Membership && rng.IntN(100) < scenarioCertExpiry {
		cfg.CertExpiresAt = cfg.Duration/4 + rng.Int64N(cfg.Duration/4)
		cfg.CertRotateAt = cfg.CertExpiresAt + 3*Second + rng.Int64N(12*Second)
	}
	return cfg
}

// scenarioRun is what a scenario's run tells the rules over a set: its
// kind and size, and what it did.
type scenarioRun struct {
	Kind  string
	Nodes int
	Stats Stats
}

// kind names the load of cfg.
func (c Config) kind() string {
	switch {
	case c.LoseQuorum:
		return "requested quorum loss"
	case c.CertExpiresAt > 0:
		return "certificate expiry"
	case c.Membership:
		return "membership"
	case c.Objects > 0:
		return "kubernetes"
	}
	return "small keys"
}

// describe is one line of what a scenario drew.
func (c Config) describe() string {
	return fmt.Sprintf("%s on %d voters, %d clients (%d clientv3), %s of faults, crashes every %s, waves %v, link delays up to %s, memory budget %d, disk budget %d, catch-up %d entries, checkpoint chunks of %d keys, values of %d bytes",
		c.kind(), c.voters(), c.Clients+c.V3Clients, c.V3Clients, seconds(c.Duration), seconds(c.Faults.CrashEvery),
		c.Waves, seconds(cmp.Or(c.LinkDelay, 2*Millisecond)), c.MemoryBytes, c.Faults.DiskBudget, cmp.Or(c.Catchup, wlCatchup), c.ckptChunkKeys, c.ValueBytes)
}

// TestScenarios runs $SIM_SEEDS fresh scenarios (Scenario) from $SIM_SEED
// or seed 1, every checker on each, and logs how many of each kind and
// size ran, the membership work's among them. It skips unless
// $SIM_SEEDS says how many: CI and the campaigns choose.
func TestScenarios(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: the campaigns and CI say how many scenarios to run")
	}
	ran := map[string]int{}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) scenarioRun {
		cfg := Scenario(seed)
		t.Logf("seed %d: %s", seed, cfg.describe())
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		t.Logf("seed %d: %d ok, %d failed, %d ambiguous, %d fenced, %d replaced, quorum lost at %s, %d cold restarts, membership changes %v",
			seed, st.OK, st.Failed, st.Ambiguous, st.Fenced, st.Replaced, seconds(st.QuorumLostAt), st.ColdRestarts, st.MembershipChanges)
		return scenarioRun{Kind: cfg.kind(), Nodes: cfg.voters(), Stats: st}
	}, func(_ uint64, r scenarioRun) bool {
		ran[fmt.Sprintf("%s on %d voters", r.Kind, r.Nodes)]++
		if r.Stats.QuorumLostAt > 0 {
			ran["quorum lost for good"]++
		}
		if r.Kind == "membership" {
			ran["membership work completed"] += min(r.Stats.MembershipChanges[3], 1)
		}
		return false
	})
	var counts []string
	for _, k := range slices.Sorted(maps.Keys(ran)) {
		counts = append(counts, fmt.Sprintf("%s: %d", k, ran[k]))
	}
	t.Logf("scenarios run: %s", strings.Join(counts, ", "))
}

// TestReplaceFenced fences nodes for good, as TestSeeds' runs do, and a
// follower a quarter into the run if none was yet (Config.FenceAt), so
// that a run under the race detector, with the host's timing, fences one
// too; an operator replaces each (Config.Replace): the replaced node
// must join as a new member and the run pass every checker.
func TestReplaceFenced(t *testing.T) {
	simrun.Each(t, []uint64{1, 2}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Duration = 40 * Second
		cfg.flipRecords, cfg.Faults.Replace, cfg.FenceAt = true, true, cfg.Duration/4
		res := run(t, cfg)
		checkers(t, res)
		if st := res.Stats; st.Replaced == 0 {
			t.Fatalf("seed %d: no node replaced: %d fenced", seed, st.Fenced)
		}
		return res.Stats
	}, nil)
}

// TestLoseQuorum fences a majority of the voters for good
// (Config.LoseQuorum) in clusters of three and five: every checker must
// pass on what committed, and no write may succeed after the loss.
func TestLoseQuorum(t *testing.T) {
	simrun.Each(t, []uint64{1, 2}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Nodes = 1 + 2*int(seed)
		cfg.Duration, cfg.LoseQuorum = 40*Second, true
		res := run(t, cfg)
		// The quorum goes only after a client's write succeeded (spare),
		// whatever the host's timing: the checks need a committed log.
		wrote := int64(math.MaxInt64)
		for _, r := range res.History {
			if r.Err == nil && isWrite(r.req) {
				wrote = min(wrote, r.Return)
			}
		}
		if lost := res.Stats.QuorumLostAt; lost > 0 && lost < wrote {
			t.Fatalf("seed %d: the cluster lost its quorum at %d ms, before a client's write succeeded (at %d ms)", seed, lost/Millisecond, wrote/Millisecond)
		}
		checkers(t, res)
		if st := res.Stats; st.QuorumLostAt == 0 {
			t.Fatalf("seed %d: the cluster of %d kept its quorum: %d fenced", seed, cfg.Nodes, st.Fenced)
		}
		return res.Stats
	}, nil)
}

// TestCertExpiry lets the nodes' certificate expire during the faults
// and an operator rotate it 6 s later (Config.CertExpiresAt), on three
// voters and on one: no write may succeed in between, every checker must
// pass, and the cluster must serve again after the rotation.
func TestCertExpiry(t *testing.T) {
	simrun.Each(t, []uint64{1, 2}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Nodes = 1 + 2*int(seed%2)
		cfg.CertExpiresAt, cfg.CertRotateAt = 8*Second, 14*Second
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		if st.CertsExpiredAt == 0 || st.ExpiredWrites == 0 {
			t.Fatalf("seed %d: the expiry took no effect: connections reset at %s, %d writes failed before the rotation", seed, seconds(st.CertsExpiredAt), st.ExpiredWrites)
		}
		t.Logf("seed %d: connections reset at %s, %d writes failed before the rotation, %d ok in all", seed, seconds(st.CertsExpiredAt), st.ExpiredWrites, st.OK)
		return st
	}, nil)
}
