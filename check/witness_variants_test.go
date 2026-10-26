package check

import (
	"math/rand/v2"
	"testing"
	"time"
)

// All normal witness regressions run with all optimizations by default.
// Additionally compare each variant with the original search on valid and
// deliberately stale histories, with no log or Effect to help the search.
func TestRevisionSearchVariants(t *testing.T) {
	rng := rand.New(rand.NewPCG(441, 3))
	rejected := 0
	for i := range 300 {
		_, h := narrowingHistory(rng)
		for _, corrupt := range []bool{false, true} {
			if corrupt && !staleRead(rng, h) {
				continue
			}
			var baseline WitnessStats
			want := Witnessed(h, FromRevisions, time.Second, RevisionSearch("baseline"), WitnessMetrics(&baseline))
			if !corrupt && want != nil {
				t.Fatal(want)
			}
			if want != nil {
				rejected++
			}
			for _, variant := range []string{"checkpoints", "indexed", "both", "prefilter", "fast", "optional", "scoped", "tuned", "retired", "best", "undo", "ultimate"} {
				var stats WitnessStats
				got := Witnessed(h, FromRevisions, time.Second, RevisionSearch(variant), WitnessMetrics(&stats))
				if (got == nil) != (want == nil) {
					t.Fatalf("history %d corrupt=%v variant=%s: baseline=%v got=%v", i, corrupt, variant, want, got)
				}
				if (variant == "checkpoints" || variant == "undo") && (stats.Backtracks != baseline.Backtracks || stats.Fits != baseline.Fits) {
					t.Fatalf("%s changed search: %+v vs %+v", variant, stats, baseline)
				}
			}
		}
	}
	if rejected < 100 {
		t.Fatalf("only %d stale histories rejected", rejected)
	}
	t.Logf("%d stale histories rejected by every variant", rejected)
}
