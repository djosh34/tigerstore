package main

import (
	"testing"
	"time"
)

// TestDiag276QuantizedCPU replays #276 at the stats/summarize boundary.
// /proc/<pid>/stat has 10 ms units. Successful CPU reads may therefore
// return the same value at both ends, even when calls complete in between.
// Completion timestamps and CPU samples are independent measurements.
func TestDiag276QuantizedCPU(t *testing.T) {
	const start = int64(time.Second)
	const duration = 500 * time.Millisecond
	wk := &worker{c: &conn{}}
	wk.stats.winStart, wk.stats.winStop = start, start+int64(duration)
	for range 65 {
		wk.stats.complete(&stream{
			intended: start + int64(time.Millisecond),
			sent:     start + int64(time.Millisecond),
			msgs:     1,
		}, start+int64(2*time.Millisecond), nil)
	}
	// Both are successful, equal, nonzero cumulative CPU samples. This
	// is not a missing pid, a failed sample, or a zero elapsed interval.
	tick := time.Second / userHZ
	samples := [2]cpuSample{
		{at: start, servers: []time.Duration{7 * tick}},
		{at: start + int64(duration), servers: []time.Duration{7 * tick}},
	}
	res := summarize(config{workload: "put", duration: duration}, []*worker{wk}, nil, samples)
	// run sets Valid after checking phase, response, pid and sample errors.
	// Those checks have succeeded in the reported failure; replay that input.
	res.Valid = true
	if res.Ops != 65 || res.Cohort != 65 || res.CohortFailed != 0 || res.Latency.Count != 65 || res.ServerCPUUsPerOp != 0 {
		t.Fatalf("reproduction changed: %+v", res)
	}
	t.Logf("CPU samples %s -> %s, elapsed %s; %d completed puts, %d cohort, %d latencies, CPU/op %.1f",
		samples[0].servers[0], samples[1].servers[0], duration, res.Ops, res.Cohort, res.Latency.Count, res.ServerCPUUsPerOp)
	if err := checkWindow(res); err != nil {
		t.Fatal(err)
	}
}
