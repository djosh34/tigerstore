package check

import (
	"math/rand/v2"
	"testing"
)

func TestSurvivalGeneratedExactness(t *testing.T) {
	histories := 0
	for seed := range uint64(256) {
		for _, leased := range []bool{false, true} {
			for _, image := range []bool{false, true} {
				for _, corrupt := range []bool{false, true} {
					h := eligibilityHistory(seed, leased, image)
					if corrupt {
						rng := rand.New(rand.NewPCG(seed, 894))
						if !staleRead(rng, h) {
							h.Ops[len(h.Ops)-1].Resp.Range.Count++
						}
					}
					a, b := runSurvival(h, true, false, 2000000), runSurvival(h, false, false, 2000000)
					if _, err := survivalCheck(a, b); err != nil {
						t.Fatalf("seed=%d leased=%v image=%v corrupt=%v: %v", seed, leased, image, corrupt, err)
					}
					if a.err != b.err {
						t.Fatalf("generated explanation changed: old %s new %s", a.err, b.err)
					}
					histories++
				}
			}
		}
	}
	if histories != 2048 {
		t.Fatal(histories)
	}
	t.Logf("%d generated histories: full errors exact, construction allowance enforced", histories)
}

func TestSurvivalUnknownConstructionCost(t *testing.T) {
	h := soleMixedHistory(57)
	a, b := runSurvival(h, true, true, 2000000), runSurvival(h, false, true, 2000000)
	if _, err := survivalCheck(a, b); err != nil {
		t.Fatal(err)
	}
	if b.hits != 0 || a.steps != 14 || b.steps != 15 { // #905 enabled on both sides
		t.Fatalf("unknown fallback: steps %d/%d hits %d", a.steps, b.steps, b.hits)
	}
	t.Logf("unknown: old %d new %d construction %d items / <=%d steps", a.steps, b.steps, b.items, (b.items+255)/256)
}
