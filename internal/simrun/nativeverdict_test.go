//go:build !sim

package simrun

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// This child runs only trivial subtests, never gosim or simulator seeds.
// Its parent kills it while Go still holds the subtest verdicts buffered.
func TestDriverVerdictChild(t *testing.T) {
	if os.Getenv("SIMRUN_VERDICT_CHILD") != "1" {
		t.Skip("only run as the verdict test's subprocess")
	}
	seedSubtest(t, "TestCampaign", 1, false, func(*testing.T) {})
	seedSubtest(t, "TestCampaign", 2, true, func(t *testing.T) { t.Skip("known failure") })
	seedSubtest(t, "TestCampaign", 3, false, func(t *testing.T) { t.Error("failed seed") })
	seedSubtest(t, "TestCampaign", 4, false, func(t *testing.T) { t.Skip("ordinary skip") })
	driverVerdict(t, "image", "TestCampaign/5", "PASS")
	driverVerdict(t, "fork", "TestCampaign/5/0", "PASS")
	if _, err := fmt.Fprintln(os.Stdout, "waiting for parent to kill this process"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Minute)
}

func TestDriverVerdictsSurviveKilledParent(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestDriverVerdictChild$", "-test.v")
	cmd.Env = append(os.Environ(), "SIMRUN_VERDICT_CHILD=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		line := scanner.Text()
		log.WriteString(line + "\n")
		if line == "waiting for parent to kill this process" {
			ready = true
			break
		}
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Error(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Error("child exited successfully instead of being killed")
	}
	if !ready || scanner.Err() != nil {
		t.Fatalf("child did not finish its subtests: %v\n%s", scanner.Err(), log.String())
	}
	for _, line := range []string{
		"simrun: seed TestCampaign/1 PASS\n",
		"simrun: seed TestCampaign/2 SKIP known-failure\n",
		"simrun: seed TestCampaign/3 FAIL\n",
		"simrun: seed TestCampaign/4 SKIP\n",
		"simrun: image TestCampaign/5 PASS\n",
		"simrun: fork TestCampaign/5/0 PASS\n",
	} {
		if !strings.Contains(log.String(), line) {
			t.Errorf("missing unbuffered verdict %q in:\n%s", line, log.String())
		}
	}
	if strings.Contains(log.String(), "--- PASS:") || strings.Contains(log.String(), "--- FAIL:") {
		t.Errorf("test did not exercise Go's buffered subtest verdicts:\n%s", log.String())
	}
}

func TestDoubleSeedBudget(t *testing.T) {
	for _, c := range []struct{ seeds, twice int }{
		{0, 0}, {1, 0}, {38, 0}, {83, 1}, {121, 1}, {299, 2}, {2417, 24}, {2500, 25}, {3, 10},
	} {
		got := 0
		for i := range c.seeds {
			if doubleSeed(i, c.seeds, c.twice) {
				got++
			}
		}
		if want := min(c.seeds, c.twice); got != want {
			t.Errorf("%d seeds, twice=%d: selected %d seeds, want %d", c.seeds, c.twice, got, want)
		}
	}
}
