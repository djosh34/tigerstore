package sim

import (
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// A small lease-free image and its faulted fork pin main's random stream:
// image seed 1, 300 objects for 40 s, fork 0 (seed 1000) for 30 s. The
// trace hash was recorded on origin/main at 70fc176d, under gosim. The
// history hash covers each call's ambiguity bit, so #900's definite
// refused dials changed it (401a6326a622c0c2 before); the trace did not.
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
			const want = "seed 1000: trace aa1af1b84b486b60 history 709b8736985eb23a"
			if got := hashLines[len(hashLines)-1]; !strings.HasPrefix(got, want+" ") {
				t.Errorf("lease-free fork differs from main:\n got %s\nwant %s", got, want)
			}
		}
		return st
	}, nil)
}
