package sim

import (
	"testing"
	"time"
)

// These tests select policies only. They run no seed or simulation.
func TestLongSeedLimits(t *testing.T) {
	if witnessStepLimit != 350_000_000 || longSeedBudget(0) != witnessStepLimit {
		t.Fatal("fresh, aged or busy seeds lost their existing step limit")
	}
	if longWitnessStepLimit != 4_000_000_000 || longSeedBudget(longSeedWallLimit) != longWitnessStepLimit {
		t.Fatal("long seed budget is not twice the provisional full-count estimate")
	}
	if longBudgetTime != 133*time.Minute+20*time.Second {
		t.Fatalf("estimated budget time %s, want 133m20s", longBudgetTime)
	}
	if longSeedWallLimit != 155*time.Minute || longSeedWallLimit < longBudgetTime+longSeedSetupAllowance+longSeedWallMargin {
		t.Fatalf("long backstop %s does not cover the budget, setup and margin", longSeedWallLimit)
	}
	const grace = 10 * time.Minute
	const job = 340 * time.Minute
	if longSeedWallLimit+grace >= job {
		t.Fatal("long limit leaves no room in the campaign timeout")
	}
}

func TestCampaignWallLimits(t *testing.T) {
	seen := map[string]bool{}
	for seed := uint64(1); seed <= 100_000 && len(seen) < 3; seed++ {
		kind := campaignKind(seed)
		want := time.Duration(0)
		if kind == kindLong {
			want = longSeedWallLimit
		}
		if got := campaignWallLimit(seed); got != want {
			t.Fatalf("seed %d (%s): limit %s, want %s", seed, kind, got, want)
		}
		seen[kind] = true
	}
	if !seen[kindFresh] || !seen[kindAged] || !seen[kindLong] {
		t.Fatalf("policy test did not exercise all campaign kinds: %v", seen)
	}
}
