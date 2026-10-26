//go:build !sim

package behavior_test

/*
func TestNemesisDelay(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	counts := make(map[float64]int)
	for seed := int64(0); seed < 200; seed++ {
		run, err := mt.Run(t, &metatesting.RunConfig{
			Test: "TestNemesisDelayInner",
			Seed: seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if run.Failed {
			t.Fatal("run failed")
		}
		delay := metatesting.MustFindLogValue(metatesting.ParseLog(run.LogOutput), "delay", "val").(float64)
		counts[delay]++
	}

	t.Log(counts)
	if counts[-1] == 0 || counts[0] == 0 || counts[1] == 0 {
		t.Error("bad")
	}
}

func TestNemesisCrash(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	counts := make(map[float64]int)
	for seed := int64(0); seed < 200; seed++ {
		run, err := mt.Run(t, &metatesting.RunConfig{
			Test: "TestNemesisCrashInner",
			Seed: seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if run.Failed {
			t.Fatal("run failed")
		}
		logs := metatesting.ParseLog(run.LogOutput)
		start := metatesting.MustFindLogValue(logs, "start", "now").(float64)
		hello := metatesting.MustFindLogValue(logs, "hello", "now").(float64)
		counts[hello-start]++
	}

	t.Log(counts)
	// 5 seconds is the restart time
	if counts[0] == 0 || counts[5] == 0 {
		t.Error("bad")
	}
}
*/
