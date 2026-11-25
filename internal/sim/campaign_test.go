package sim

import (
	"fmt"
	"maps"
	"math/rand/v2"
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

// campaignKind draws seed's kind, each with a weight by count that gives
// its share by cost: share/cost, scaled to whole numbers.
func campaignKind(seed uint64) string {
	const scale = costFresh * costImage * costLong
	weights := []weighted[string]{
		{kindFresh, shareFresh * scale / costFresh},
		{kindAged, shareAged * scale / costImage},
		{kindLong, shareLong * scale / costLong},
	}
	return pick(rand.New(rand.NewPCG(seed, 0x63616d70)), weights)
}

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

// TestCampaign runs $SIM_SEEDS seeds from $SIM_SEED, each of the kind it
// draws (campaignKind): a fresh scenario (Scenario), an aged image and its
// forks (TestAgedForks), or a long seed (longConfig), every checker on
// each, and logs how many of each kind ran and what they cost, the
// membership work's runs among them. It skips unless $SIM_SEEDS is set:
// the campaigns say how many.
func TestCampaign(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: the campaigns say how many seeds to run")
	}
	ran, cost := map[string]int{}, map[string]int{}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) scenarioRun {
		switch kind := campaignKind(seed); kind {
		case kindAged:
			st := agedForks(t, seed, agingConfig(seed), forksPerImage, forkConfig)
			return scenarioRun{Kind: kind, Stats: st}
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
	}, func(_ uint64, r scenarioRun) bool {
		switch r.Kind {
		case kindAged:
			ran["aged images"]++
			ran["aged forks"] += forksPerImage
			cost[kindAged] += costImage
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
