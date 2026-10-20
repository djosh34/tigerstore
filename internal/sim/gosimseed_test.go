//go:build !sim

package sim

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestGosimDriver(t *testing.T) {
	mt := metatesting.ForOtherPackage(t, "github.com/djosh34/tigerstore/internal/sim")
	first, _ := strconv.Atoi(os.Getenv("FIRST"))
	n, _ := strconv.Atoi(os.Getenv("N"))
	for seed := first; seed < first+n; seed++ {
		var hashes []string
		for range 2 {
			start := time.Now()
			r, err := mt.RunQuiet(&metatesting.RunConfig{Test: "TestGosimSeed", Seed: int64(seed), ExtraEnv: []string{fmt.Sprintf("SIM_SEED=%d", seed)}})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("seed %d: failed=%v err=%q took %v\n%s", seed, r.Failed, r.Err, time.Since(start), r.Artifacts["hashes.txt"])
			if r.Failed {
				os.WriteFile(fmt.Sprintf("/tmp/w351/seed-%d.log", seed), r.LogOutput, 0o644)
			}
			hashes = append(hashes, string(r.Artifacts["hashes.txt"]))
		}
		if hashes[0] != hashes[1] {
			t.Errorf("seed %d differs", seed)
		}
	}
}
