package sim

import (
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A small lease-free image and its faulted fork pin the random stream:
// image seed 1, 300 objects for 40 s, fork 0 (seed 1000) for 30 s.
// Manager option A restores resident output to main's direct writer. The
// first divergence is aging at 1847.136018 ms, node 2/revision 2: resident
// watches 10/11/9 (streams 49/29/25, bodies 2712/3211/1443) now share the
// first client flush with the Txn reply: 7561 buffered bytes instead of 153.
// Reverting only resident routing restores cee1eb73ec6ac127/df529d402093fa52;
// reverting only P/window-bounded pieces retains the new hashes. All 10,789
// disk calls/CRCs through that output event match; later scheduling diverges.
// Every checker stays on: aging/fork witnesses use 7250/12224 steps of 360M.
// Findings: https://github.com/djosh34/tigerstore/pull/897#issuecomment-7230441342
// Full control hashes and neutral observers: docs/fetch-cursor-resident-pin.md.
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
			const want = "seed 1000: trace ec26ff5f37150752 history ab3e074948e3f9cb"
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
