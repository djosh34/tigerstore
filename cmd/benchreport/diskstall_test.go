package main

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// nodeLog writes a node's log of zap lines, each at base plus its offset.
func nodeLog(t *testing.T, path string, base time.Time, lines map[time.Duration]string) {
	t.Helper()
	var b strings.Builder
	for off, msg := range lines {
		fmt.Fprintf(&b, `{"level":"info","ts":%q,"caller":"node/node.go:960","msg":%q}`+"\n", base.Add(off).UTC().Format(time.RFC3339Nano), msg)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestDiskStall checks the test for a stall of the runner's disk: it takes
// the stall of A/A run 43625723837 (every node's syncs of about 2 s ending
// together, the leader silenced by #233's rule, a new leader after), and
// leaves as plain failures a stall on one node only, a leader change
// without the stall, slow syncs without the silent rule, a stall with no
// new leader after it, and a failure of another cause.
func TestDiskStall(t *testing.T) {
	base := time.Date(2026, 11, 14, 18, 42, 53, 0, time.UTC)
	stalled := func(skew time.Duration) map[time.Duration]string {
		return map[time.Duration]string{
			0:                          "leader is 3 at term 2",
			4*time.Second + skew:       "slow log sync: 1.917942911s",
			6*time.Second + skew:       "slow log sync: 1.993263284s",
			8*time.Second + skew:       "slow log sync: 2.001609606s",
			8*time.Second + skew + 3e6: "leader is 1 at term 3",
		}
	}
	quiet := map[time.Duration]string{0: "leader is 3 at term 2", 8 * time.Second: "leader is 1 at term 3"}
	silent := 6*time.Second - 1e7
	for _, tc := range []struct {
		name  string
		cause string
		nodes []map[time.Duration]string
		rule  bool
		want  bool
	}{
		{"the runner's disk", "leader check exited 1: the leader changed during the run", []map[time.Duration]string{stalled(0), stalled(time.Millisecond), stalled(-time.Millisecond)}, true, true},
		{"one node's disk", "leader check exited 1: the leader changed during the run", []map[time.Duration]string{stalled(0), quiet, quiet}, true, false},
		{"a leader change alone", "leader check exited 1: the leader changed during the run", []map[time.Duration]string{quiet, quiet, quiet}, false, false},
		{"slow syncs, no silent rule", "leader check exited 1: the leader changed during the run", []map[time.Duration]string{stalled(0), stalled(0), stalled(0)}, false, false},
		{"syncs a second apart", "leader check exited 1: the leader changed during the run", []map[time.Duration]string{stalled(0), stalled(time.Second / 2), stalled(-time.Second / 2)}, true, false},
		{"another cause", "loadgen exited 1: request timed out", []map[time.Duration]string{stalled(0), stalled(0), stalled(0)}, true, false},
	} {
		dir := filepath.Join(t.TempDir(), "rtt-0", "k8s")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		prefix := filepath.Join(dir, "candidate-max-994948578.1")
		if err := os.WriteFile(prefix+".failed", []byte(tc.cause), 0o644); err != nil {
			t.Fatal(err)
		}
		for i, lines := range tc.nodes {
			l := maps.Clone(lines)
			if tc.rule && i == 0 {
				l[silent] = "warning: the leader's disk write has taken 2 election timeouts and more than its size allows; " + silentRule
			}
			nodeLog(t, fmt.Sprintf("%s-n%d.log", prefix, i+1), base, l)
		}
		_, err := diskStall(prefix)
		if got := err == nil; got != tc.want {
			t.Errorf("%s: a stall of the runner's disk: %v, want %v (%v)", tc.name, got, tc.want, err)
		}
		if err != nil && !errors.Is(err, errNotDiskStall) {
			t.Errorf("%s: %v is not errNotDiskStall", tc.name, err)
		}
	}
}

// TestReportCountsRetries checks that the report names every pair measured
// again after a stall of the runner's disk, and fails past maxRetries.
func TestReportCountsRetries(t *testing.T) {
	for _, n := range []int{maxRetries, maxRetries + 1} {
		dir := t.TempDir()
		for i := range n {
			rtt := strconv.Itoa(i)
			writeAt(t, dir, rtt, "k8s", "baseline-max-1.1.json", `{"valid":true,"throughput":1000,"server_cpu_us_per_op":10}`)
			writeAt(t, dir, rtt, "k8s", "candidate-max-1.1.json", `{"valid":true,"throughput":1000,"server_cpu_us_per_op":10}`)
			writeAt(t, dir, rtt, "k8s", "retried", "the runner's disk stalled on every node")
		}
		var b strings.Builder
		err := report(&b, dir, "", nil)
		if !strings.Contains(b.String(), fmt.Sprintf("Pairs measured again after a stall of the runner's disk, %d of at most %d", n, maxRetries)) {
			t.Fatalf("%d retries: report:\n%s", n, b.String())
		}
		if failed := errors.Is(err, errTooManyRetries); failed != (n > maxRetries) {
			t.Fatalf("%d retries: %v", n, err)
		}
	}
}
