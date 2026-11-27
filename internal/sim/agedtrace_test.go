package sim

import (
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A small lease-free image and its faulted fork pin the random stream:
// image seed 1, 300 objects for 40 s, fork 0 (seed 1000) for 30 s.
// Manager option A permits #897's deliberate routing change: production
// cursor routing stages hot watch output and changes socket flush batching,
// first at 1,847 ms of aging on node 2. Every semantic checker remains on.
// Findings: https://github.com/djosh34/tigerstore/pull/897#issuecomment-7222981838
// The earlier main pin was aa1af1b84b486b60 / 709b8736985eb23a (#885);
// #900 had changed its history ambiguity bits without changing its trace.
func TestLeaseFreeForkTrace(t *testing.T) {
	simrun.ImageEach(t, []uint64{1}, 1, func(t *testing.T, seed uint64) Stats {
		aging := agingConfig(seed)
		aging.Objects, aging.Duration = 300, 40*Second
		st := agedForks(t, seed, aging, 1, func(aging Config, seed uint64) Config {
			cfg := forkConfig(aging, seed)
			cfg.Duration = 30 * Second
			return cfg
		})
		if st.ImageLeases != 0 {
			t.Fatalf("lease-free fixture holds %d leases", st.ImageLeases)
		}
		if save, _ := forkMode(t, 1); !save && simrun.InGosim {
			const want = "seed 1000: trace cee1eb73ec6ac127 history df529d402093fa52"
			if got := hashLines[len(hashLines)-1]; !strings.HasPrefix(got, want+" ") {
				t.Errorf("lease-free fork differs from its pin:\n got %s\nwant %s", got, want)
			}
			if !t.Failed() {
				t.Log("aging and fork0 passed all existing checkers: durability, revisions, watches, witness-from-log, witness-from-revisions; routed trace pin matches")
			}
		}
		return st
	}, nil)
}
