package main

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
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

// TestDiskStall checks the test for a stall of the runner's disk, one
// episode: the stall of A/A run 43625723837 (every node's syncs of about
// 2 s ending together, the leader silenced by #233's rule inside the
// stall, the new leader at the check's term after it) passes, and so does
// a valid episode followed by a later shared stall. Each of these stays a
// plain failure: a stall on one node only; a leader change alone; slow
// syncs without the silent rule; ends spread 480 ms; the silence after the
// election; the election before the stall ended; a new leader at another
// term; a new leader 85 s after the stall; slow syncs on every node a
// second apart; large writes (128 MiB) on every node; a large write
// logged just after the last sync; no write sizes on
// one node; no new leader; and another cause.
func TestDiskStall(t *testing.T) {
	base := time.Date(2026, 11, 14, 18, 42, 53, 0, time.UTC)
	const changed = "leader check exited 1: the leader changed during the run: 127.0.0.1:20030 term 2, now 127.0.0.1:20010 term 3"
	// stall is one node's log of the stall: syncs ending at 4, 6 and 8 s
	// plus skew, and the new leader 3 ms after the last.
	stall := func(skew time.Duration) map[time.Duration]string {
		return map[time.Duration]string{
			0:                          "leader is 3 at term 2",
			4*time.Second + skew:       "slow log sync: 1.917942911s",
			4*time.Second + skew + 1e5: "slow write to the log: 1 entries, 48 bytes in 1.918065119s",
			6*time.Second + skew:       "slow log sync: 1.993263284s",
			6*time.Second + skew + 1e5: "slow log flush: 1049 bytes in 1.993050513s",
			8*time.Second + skew:       "slow log sync: 2.001609606s",
			8*time.Second + skew + 3e6: "leader is 1 at term 3",
		}
	}
	with := func(m map[time.Duration]string, k time.Duration, v string) map[time.Duration]string {
		m = maps.Clone(m)
		m[k] = v
		return m
	}
	without := func(m map[time.Duration]string, k time.Duration) map[time.Duration]string {
		m = maps.Clone(m)
		delete(m, k)
		return m
	}
	quiet := map[time.Duration]string{0: "leader is 3 at term 2", 8 * time.Second: "leader is 1 at term 3"}
	const rule = "warning: the leader's disk write has taken 2 election timeouts and more than its size allows; " + silentRule
	in := 6*time.Second - 1e7 // inside the stall that ends at 6 s
	three := func(n0, n1, n2 map[time.Duration]string) []map[time.Duration]string {
		return []map[time.Duration]string{n0, n1, n2}
	}
	for _, tc := range []struct {
		name  string
		cause string
		nodes []map[time.Duration]string
		want  bool
	}{
		{"the runner's disk", changed, three(with(stall(0), in, rule), stall(time.Millisecond), stall(-time.Millisecond)), true},
		{"a later shared stall after a valid one", changed, three(with(with(stall(0), in, rule), 20*time.Second, "slow log sync: 1.5s"), with(stall(0), 20*time.Second, "slow log sync: 1.5s"), with(stall(0), 20*time.Second, "slow log sync: 1.5s")), true},
		{"one node's disk", changed, three(with(stall(0), in, rule), quiet, quiet), false},
		{"a leader change alone", changed, three(quiet, quiet, quiet), false},
		{"slow syncs, no silent rule", changed, three(stall(0), stall(0), stall(0)), false},
		{"ends spread 480 ms", changed, three(with(stall(0), in, rule), stall(240*time.Millisecond), stall(-240*time.Millisecond)), false},
		{"silence after the election", changed, three(with(stall(0), 50*time.Second, rule), stall(0), stall(0)), false},
		{"the election before the stall ended", changed, three(with(without(stall(0), 8*time.Second+3e6), in, rule), without(with(stall(0), 8*time.Second-2e8, "leader is 1 at term 3"), 8*time.Second+3e6), without(stall(0), 8*time.Second+3e6)), false},
		{"a new leader at another term", "leader check exited 1: the leader changed during the run: 127.0.0.1:20030 term 2, now 127.0.0.1:20010 term 4", three(with(stall(0), in, rule), stall(0), stall(0)), false},
		{"a new leader 85 s after the stall", changed, three(with(without(stall(0), 8*time.Second+3e6), in, rule), with(without(stall(0), 8*time.Second+3e6), 93*time.Second, "leader is 1 at term 3"), without(stall(0), 8*time.Second+3e6)), false},
		{"no shared stall, silence and slow syncs apart", changed, three(with(stall(0), in, rule), stall(time.Second), stall(-time.Second)), false},
		{"large writes on every node", changed, three(with(with(stall(0), in, rule), 6*time.Second+2e5, "slow log flush: 134217728 bytes in 1.99s"), with(stall(0), 6*time.Second+2e5, "slow log flush: 134217728 bytes in 1.99s"), with(stall(0), 6*time.Second+2e5, "slow log flush: 134217728 bytes in 1.99s")), false},
		{"a large write logged just after the last sync", changed, three(with(with(stall(0), in, rule), 8*time.Second+1e6, "slow write to the log: 1 entries, 134217728 bytes in 2.0s"), stall(0), stall(0)), false},
		{"no write sizes on one node", changed, three(with(stall(0), in, rule), stall(0), without(without(stall(0), 4*time.Second+1e5), 6*time.Second+1e5)), false},
		{"no new leader", changed, three(with(without(stall(0), 8*time.Second+3e6), in, rule), without(stall(0), 8*time.Second+3e6), without(stall(0), 8*time.Second+3e6)), false},
		{"another cause", "loadgen exited 1: request timed out", three(with(stall(0), in, rule), stall(0), stall(0)), false},
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
			nodeLog(t, fmt.Sprintf("%s-n%d.log", prefix, i+1), base, lines)
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

// TestReportCountsRetries checks that the report names every pair
// measured again after a stall of the runner's disk, seeds at one RTT
// included, and fails past maxRetries.
func TestReportCountsRetries(t *testing.T) {
	for _, n := range []int{maxRetries, maxRetries + 1} {
		dir := t.TempDir()
		for i := range n {
			seed := strconv.Itoa(i + 1)
			writeAt(t, dir, "0", "k8s", "baseline-max-"+seed+".1.json", `{"valid":true,"throughput":1000,"server_cpu_us_per_op":10}`)
			writeAt(t, dir, "0", "k8s", "candidate-max-"+seed+".1.json", `{"valid":true,"throughput":1000,"server_cpu_us_per_op":10}`)
			writeAt(t, dir, "0", "k8s", "retried-"+seed, "the runner's disk stalled on every node")
		}
		var b strings.Builder
		err := report(&b, dir, "", nil)
		if !strings.Contains(b.String(), fmt.Sprintf("Pairs measured again after a stall of the runner's disk, %d of at most %d", n, maxRetries)) || !strings.Contains(b.String(), "seed "+strconv.Itoa(n)+":") {
			t.Fatalf("%d retries: report:\n%s", n, b.String())
		}
		if failed := errors.Is(err, errTooManyRetries); failed != (n > maxRetries) {
			t.Fatalf("%d retries: %v", n, err)
		}
	}
}

// wlLog writes a node's log of internal/wl lines, each at base plus its
// offset, in order.
func wlLog(t *testing.T, path string, base time.Time, lines map[time.Duration]string) {
	t.Helper()
	offs := slices.Sorted(maps.Keys(lines))
	var b strings.Builder
	for _, off := range offs {
		fmt.Fprintf(&b, "tigerstore-wl %s %s\n", base.Add(off).UTC().Format("2006/01/02 15:04:05.000000"), lines[off])
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestDiskStallWL checks the test for a stall of the runner's disk on
// internal/wl's nodes (#747), as the stall of bench run 44202167112, seed
// 82087027, at 0 ms shows it: the leader silent from its oldest append's
// start for 3.48 s, every node's log writes 4.86 s of their 5 s window
// in about 110 syncs of about 200 KB, and a new leader 0.47 s after the
// leader caught up. Each of these stays a plain failure: one node's
// disk; no catching up; a wait under stallSync; large writes; the new
// leader past stallElected; no new leader but "0"; and another cause.
func TestDiskStallWL(t *testing.T) {
	base := time.Date(2026, 11, 17, 23, 49, 36, 825324462, time.UTC)
	const changed = "leader check exited 1: the leader changed during the run: 127.0.0.1:20030 term 2, now 127.0.0.1:20010 term 3"
	silent := fmt.Sprintf("the log's oldest append waited for the disk since %s: silent, so the followers elect another leader", base.Format(time.RFC3339Nano))
	window := func(writesMS int, perReply float64) map[time.Duration]string {
		return map[time.Duration]string{
			-950 * time.Millisecond: "stats 5.0s: replies/s 106 turns/s 1575 | per reply: turns 14.919 walBytes 173013.4 | keys 102393",
			-949 * time.Millisecond: "cpu ms: process 967, loop 642, others 324; log writes 1199 ms for 2291 syncs; kernel dirty 0 MB, writeback 0 MB",
			4080 * time.Millisecond: fmt.Sprintf("stats 5.0s: replies/s 23 turns/s 139 | per reply: turns 6.149 walBytes %.1f | keys 118675", perReply),
			4081 * time.Millisecond: "memory MB: rss 1091",
			4082 * time.Millisecond: fmt.Sprintf("cpu ms: process 127, loop 98, others 29; log writes %d ms for 111 syncs; kernel dirty 0 MB, writeback 0 MB", writesMS),
		}
	}
	follower := func() map[time.Duration]string {
		m := window(4863, 194981.3)
		m[3950*time.Millisecond] = "the leader is 1"
		return m
	}
	leader := func(caught, elected time.Duration, by string) map[time.Duration]string {
		m := window(4863, 194981.3)
		m[2174*time.Millisecond] = silent
		if caught > 0 {
			m[caught] = "the log caught up: no longer silent"
		}
		m[elected] = by
		return m
	}
	good := leader(3478*time.Millisecond, 3950*time.Millisecond, "the leader is 1")
	for _, tc := range []struct {
		name  string
		cause string
		nodes []map[time.Duration]string
		want  bool
	}{
		{"the runner's disk", changed, []map[time.Duration]string{good, follower(), follower()}, true},
		{"one node's disk", changed, []map[time.Duration]string{good, window(300, 194981.3), window(300, 194981.3)}, false},
		{"no catching up", changed, []map[time.Duration]string{leader(0, 3950*time.Millisecond, "the leader is 1"), follower(), follower()}, false},
		{"a wait under stallSync", changed, []map[time.Duration]string{leader(800*time.Millisecond, 1200*time.Millisecond, "the leader is 1"), follower(), follower()}, false},
		{"large writes", changed, []map[time.Duration]string{good, window(4863, 2e6), window(4863, 2e6)}, false},
		{"a new leader past stallElected", changed, []map[time.Duration]string{leader(3478*time.Millisecond, 6*time.Second, "the leader is 1"), window(4863, 194981.3), window(4863, 194981.3)}, false},
		{"no new leader but 0", changed, []map[time.Duration]string{leader(3478*time.Millisecond, 3950*time.Millisecond, "the leader is 0"), window(4863, 194981.3), window(4863, 194981.3)}, false},
		{"another cause", "loadgen exited 1: request timed out", []map[time.Duration]string{good, follower(), follower()}, false},
	} {
		dir := filepath.Join(t.TempDir(), "rtt-0", "k8s")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		prefix := filepath.Join(dir, "baseline-max-82087027.1")
		if err := os.WriteFile(prefix+".failed", []byte(tc.cause), 0o644); err != nil {
			t.Fatal(err)
		}
		for i, lines := range tc.nodes {
			wlLog(t, fmt.Sprintf("%s-n%d.log", prefix, i+1), base, lines)
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
