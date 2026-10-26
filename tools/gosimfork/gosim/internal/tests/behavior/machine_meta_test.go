//go:build !sim

package behavior_test

import (
	"log"
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestMachineCrashRuns(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	counts := make(map[int]int)
	for seed := int64(0); seed < 50; seed++ {
		run, err := mt.Run(t, &metatesting.RunConfig{
			Test: "TestMachineCrash",
			Seed: seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if run.Failed {
			t.Fatal("run failed")
		}
		var n int
		for _, log := range metatesting.ParseLog(run.LogOutput) {
			if log["msg"] == "output" {
				n = int(log["n"].(float64))
			}
		}
		counts[n]++
	}

	for i := 0; i <= 3; i++ {
		if counts[i] == 0 {
			t.Errorf("expected some output %d, got %d", i, counts[i])
		}
	}

	log.Println("counts", counts)
}
