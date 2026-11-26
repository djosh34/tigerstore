package sim

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A campaign's seeds (#495, #497) are about 80% fresh scenarios, 15% aged
// forks and 5% long seeds, counted by cost. These are the costs, in fresh
// scenarios: #497 measured an aged fork at about 40 and a long seed of 4
// simulated hours at about 150; an image's seed builds the image and runs
// forksPerImage forks of it, so each fork carries its share of the build.
const (
	costFresh = 1
	costImage = forksPerImage * 40
	costLong  = 150
)

// The shares by cost, in percent.
const (
	shareFresh = 80
	shareAged  = 15
	shareLong  = 5
)

// The kinds of a campaign's seeds.
const (
	kindFresh = "fresh"
	kindAged  = "aged"
	kindLong  = "long"
)

// campaignKind allocates seeds from 1 by running cost. Each seed takes
// the kind with the largest deficit after adding its cost: target cost
// minus that kind's cost, in hundredths of a fresh scenario's cost. Ties
// go to fresh, then aged, then long. This starts with 389 fresh seeds;
// seeds 1 to 2,404 are 2,400 fresh, 3 aged and 1 long (79.2/15.8/5.0 by
// cost). The deficits return to zero after 38,461 seeds, so a table of
// that cycle makes even a seed far from 1 a constant-time lookup, with
// no dependence on which seeds a run includes. Seed 0 is fresh too.
func campaignKind(seed uint64) string {
	if seed == 0 {
		return kindFresh
	}
	return campaignKinds[(seed-1)%uint64(len(campaignKinds))]
}

// campaignKinds holds one cycle of the running cost allocation.
var campaignKinds = campaignCycle()

// campaignCycle walks seeds until each kind's cost is exactly its target
// share of the total again. The deficits alone decide the next kind, so
// the same choices repeat from there. Costs or shares whose deficits do
// not return to zero within maxCampaignCycle seeds stop the walk there,
// rather than hang every test of the package at init, and fail
// TestCampaignKindCycleCloses.
func campaignCycle() []string {
	kinds := [...]string{kindFresh, kindAged, kindLong}
	costs := [...]int{costFresh, costImage, costLong}
	shares := [...]int{shareFresh, shareAged, shareLong}
	var deficit [3]int
	var cycle []string
	for {
		kind := 0
		for k := 1; k < len(kinds); k++ {
			if deficit[k]-(100-shares[k])*costs[k] > deficit[kind]-(100-shares[kind])*costs[kind] {
				kind = k
			}
		}
		cycle = append(cycle, kinds[kind])
		for k := range deficit {
			deficit[k] += shares[k] * costs[kind]
		}
		deficit[kind] -= 100 * costs[kind]
		if deficit == [3]int{} {
			return cycle
		}
		if len(cycle) == maxCampaignCycle {
			// TestCampaignKindCycleCloses fails on a cycle this long.
			return cycle
		}
	}
}

// maxCampaignCycle bounds campaignCycle's walk: today's cycle is 38,461
// seeds.
const maxCampaignCycle = 1 << 20

// longConfig is a long seed: TestSeeds's faults about 16 times further
// apart, for 4 simulated hours (TestCampaignShapeSeeds), on the cluster
// size, clients and limits a fresh scenario of its seed draws.
func longConfig(seed uint64) Config {
	cfg := Scenario(seed)
	if cfg.Membership || cfg.LoseQuorum || cfg.CertExpiresAt > 0 {
		cfg = DefaultConfig(seed)
	}
	def := DefaultConfig(seed).Faults
	spreadFaults(&def, 16)
	def.DiskBudget, def.DiskCapacity, def.Replace = cfg.Faults.DiskBudget, cfg.Faults.DiskCapacity, true
	cfg.Faults, cfg.Waves = def, nil
	cfg.Duration = 4 * 3600 * Second
	return cfg
}

// TestCampaign runs $SIM_SEEDS seeds from $SIM_SEED, each of the kind
// campaignKind allocates: a fresh scenario (Scenario), an aged image and its
// forks (TestAgedForks), or a long seed (longConfig), every checker on
// each, and logs how many of each kind ran and what they cost, the
// membership work's runs among them. It skips unless $SIM_SEEDS is set:
// the campaigns say how many.
func TestCampaign(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: the campaigns say how many seeds to run")
	}
	ran, cost := map[string]int{}, map[string]int{}
	images := map[uint64]bool{}
	// An aged seed's image and each of its forks run in gosim runs of
	// their own, as TestAgedForks's do (#794): all in one process, the
	// forks piled up memory to the runner's limit.
	forksOf := func(seed uint64) int {
		if campaignKind(seed) == kindAged {
			return forksPerImage
		}
		return 0
	}
	simrun.ImageSeedsBy(t, 1, 0, forksOf, func(t *testing.T, seed uint64) scenarioRun {
		switch kind := campaignKind(seed); kind {
		case kindAged:
			_, only := forkMode(t, forksPerImage)
			forks := forksPerImage
			if only >= 0 {
				forks = 1
			}
			st := agedForks(t, seed, agingConfig(seed), forksPerImage, forkConfig)
			return scenarioRun{Kind: kind, Stats: st, Forks: forks}
		case kindLong:
			cfg := longConfig(seed)
			res := run(t, cfg)
			checkers(t, res)
			t.Logf("seed %d: long, %s: %d ok, %d failed, %d ambiguous, %d fenced, %d replaced", seed, cfg.describe(), res.Stats.OK, res.Stats.Failed, res.Stats.Ambiguous, res.Stats.Fenced, res.Stats.Replaced)
			return scenarioRun{Kind: kind, Nodes: cfg.voters(), Stats: res.Stats}
		default:
			cfg := Scenario(seed)
			t.Logf("seed %d: %s", seed, cfg.describe())
			res := run(t, cfg)
			checkers(t, res)
			return scenarioRun{Kind: cfg.kind(), Nodes: cfg.voters(), Stats: res.Stats}
		}
	}, func(seed uint64, r scenarioRun) bool {
		switch r.Kind {
		case kindAged:
			// Each fork carries its share of the image's cost.
			images[seed] = true
			ran["aged images"] = len(images)
			ran["aged forks"] += r.Forks
			cost[kindAged] += costImage * r.Forks / forksPerImage
		case kindLong:
			ran["long seeds"]++
			cost[kindLong] += costLong
		default:
			ran["fresh: "+r.Kind]++
			cost[kindFresh] += costFresh
			if r.Kind == "membership" && r.Stats.MembershipChanges[3] > 0 {
				ran["membership work completed"]++
			}
		}
		if r.Stats.QuorumLostAt > 0 {
			ran["quorum lost for good"]++
			if r.Stats.Replaced > 0 {
				// Through a replacement (#850; Config.LoseQuorum replaces
				// none), which TestReplacementLosesQuorum builds directly.
				ran["quorum lost through a replacement"]++
			}
		}
		return false
	})
	var counts []string
	for _, k := range slices.Sorted(maps.Keys(ran)) {
		counts = append(counts, fmt.Sprintf("%s: %d", k, ran[k]))
	}
	total := cost[kindFresh] + cost[kindAged] + cost[kindLong]
	t.Logf("campaign: %s; by cost: fresh %d, aged %d, long %d of %d", strings.Join(counts, ", "), cost[kindFresh], cost[kindAged], cost[kindLong], total)
}
