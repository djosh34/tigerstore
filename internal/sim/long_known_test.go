//go:build !sim

package sim

import (
	"os"
	"strings"
	"testing"
)

// TestCampaignShapeKnownTimeout checks the campaign input, not the seed.
// TestCampaignShapeSeeds calls longSeeds, which uses simrun.Seeds, so the
// native driver's known-failure entry applies without starting a simulator.
func TestCampaignShapeKnownTimeout(t *testing.T) {
	data, err := os.ReadFile("known-failures.txt")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for line := range strings.SplitSeq(string(data), "\n") {
		text, _, _ := strings.Cut(line, "#")
		fields := strings.Fields(text)
		if len(fields) < 2 || fields[0] != "TestCampaignShapeSeeds" || fields[1] != "1" {
			continue
		}
		if len(fields) != 3 || fields[2] != "https://github.com/djosh34/tigerstore/issues/876" {
			t.Fatalf("seed 1 timeout has the wrong owner: %q", line)
		}
		found++
	}
	if found != 1 {
		t.Fatalf("found %d seed 1 known timeouts, want one under #876", found)
	}
}
