//go:build !sim

package sim

import (
	"os"
	"strings"
	"testing"
)

// TestCampaignShapeSeedOneRuns checks the campaign input, not the seed.
// The #876 witness regression must actually execute, not be reported as a
// passing workflow that only skipped seed 1 through the native driver.
func TestCampaignShapeSeedOneRuns(t *testing.T) {
	data, err := os.ReadFile("known-failures.txt")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		text, _, _ := strings.Cut(line, "#")
		fields := strings.Fields(text)
		if len(fields) < 2 || fields[0] != "TestCampaignShapeSeeds" || fields[1] != "1" {
			continue
		}
		t.Fatalf("seed 1 must run its full witness check, not be quarantined: %q", line)
	}
}
