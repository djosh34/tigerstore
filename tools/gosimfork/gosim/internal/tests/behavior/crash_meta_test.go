//go:build !sim

package behavior_test

import (
	"log"
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestCrashRestartFilesystemPartial(t *testing.T) {
	contentsCount := make(map[string]int)
	mt := metatesting.ForCurrentPackage(t)
	for seed := int64(0); seed < 100; seed++ {
		// XXX: this testing API is clunky...
		// config := gosim.ConfigWithNSeeds(100)
		// config.CaptureLog = true
		// XXX: i don't like this log level override anymore
		// config.LogLevelOverride = "INFO"
		run, err := mt.Run(t, &metatesting.RunConfig{
			Test: "TestCrashRestartFilesystemPartialInner",
			Seed: seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if run.Failed {
			t.Fatal("run failed")
		}
		output := metatesting.ParseLog(run.LogOutput)
		contents := metatesting.MustFindLogValue(output, "read", "contents").(string)
		contentsCount[contents]++
	}
	log.Println(contentsCount)
	// expected either no file, a file that hasn't been resized, a resized file
	// with no contents, or a file with its expected contents
	for _, expected := range []string{"", "\x00\x00\x00\x00\x00", "hello", "<missing>"} {
		if contentsCount[expected] == 0 {
			t.Errorf("expected to see %q, got 0", expected)
		}
		delete(contentsCount, expected)
	}
	if len(contentsCount) > 0 {
		t.Fatalf("unexpected results %v", contentsCount)
	}
}
