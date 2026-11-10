package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write puts a result file for bench put-1k at RTT 0 into dir.
func write(t *testing.T, dir, name, json string) {
	t.Helper()
	writeAt(t, dir, "0", "put-1k", name, json)
}

// writeAt puts a file for a bench at an RTT into dir.
func writeAt(t *testing.T, dir, rtt, bench, name, content string) {
	t.Helper()
	d := filepath.Join(dir, "rtt-"+rtt, bench)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const valid = `{"valid": true, "throughput": 1000, "server_cpu_us_per_op": 10}`

func TestReport(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"etcd-max-1.json", "etcd-max-2.json", "main-max-1.json", "main-max-2.json"} {
		write(t, dir, n, valid)
	}
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "| put-1k | 0 | main | max | 0 / 0 / 0 | 1,000") {
		t.Fatalf("report:\n%s", b.String())
	}
}

func TestReportSpreads(t *testing.T) {
	dir := t.TempDir()
	for i, n := range []int{30, 10, 20} {
		for _, mode := range []string{"max", "rate"} {
			write(t, dir, fmt.Sprintf("etcd-%s-%d.json", mode, i+1), fmt.Sprintf(`{"valid":true,"throughput":%d,"offered_rate":100,"server_cpu_us_per_op":%d,"client_cpu_us_per_op":%d,"server_cores_by_node":[%d],"latency_us":{"mean":%d,"p50":%d,"p90":%d,"p99":%d,"p999":%d,"max":%d}}`, n, n, n, n, n*1000, n*1000, n*1000, n*1000, n*1000, n*1000))
		}
	}
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"10 / 20 / 30", "10.0 / 20.0 / 30.0", "10.00 / 20.00 / 30.00", "| rate |", "| 3 |"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %s in report:\n%s", want, b.String())
		}
	}
	if got := num(runs{{Throughput: 10}, {Throughput: 30}}, func(r result) float64 { return r.Throughput }, "%.0f"); got != "10 / 20 / 30" {
		t.Fatal(got)
	}
}

func TestReportWatchSpreads(t *testing.T) {
	dir := t.TempDir()
	for i, n := range []int{30, 10, 20} {
		write(t, dir, fmt.Sprintf("etcd-rate-%d.json", i+1), fmt.Sprintf(`{"valid":true,"watch":{"events_per_s":%d,"server_cpu_us_per_event":%d,"latency_us":{"p99":%d,"p999":%d}}}`, n, n, n*1000, n*1000))
	}
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"| Watch benchmark |", "10 / 20 / 30", "10.00 / 20.00 / 30.00", "10.0 / 20.0 / 30.0"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %s in watch report:\n%s", want, b.String())
		}
	}
}

func TestMixedWatchCPUUnset(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-rate-1.json", `{"valid":true,"watch":{"events_per_s":100}}`)
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "| - | 1 |") {
		t.Fatal("unset watch CPU was not a dash")
	}
}

func TestReportRefusesInvalidResults(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "main-max-1.json", `{"valid": false, "throughput": 1000}`)
	if err := report(io.Discard, dir, ""); !errors.Is(err, errInvalidResult) {
		t.Fatalf("an invalid result was reported: %v", err)
	}
}

func TestReportRefusesUnequalRuns(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "etcd-max-2.json", valid)
	write(t, dir, "main-max-1.json", valid)
	if err := report(io.Discard, dir, ""); !errors.Is(err, errUnequalRuns) {
		t.Fatalf("unequal runs were reported: %v", err)
	}
}

// TestReportCeilings puts each seed against the lowest of its limits. With
// every primitive free but a 1 µs lookup, the leader's floor is 1 µs a
// put. The disk allows 100,000 puts a second, loopback 50,000: the
// network is the ceiling. A generator that would need more than half its
// core at the target fails the report.
func TestReportCeilings(t *testing.T) {
	dir := t.TempDir()
	// Loopback bytes a put: 1,100 out, 30 in, and the entry of 1,048.576
	// bytes and its 16-byte header to both followers.
	loop := (1100 + 30 + 2*(1048.576+16)) * 50000
	ceiling := fmt.Sprintf(`{"kernel":"k","cpu":"c","disk":"sda1","disk_mib_per_s":300,"primitives":{"lookup_ns":1000,"loopback_bytes_per_s":%f}}`, loop)
	if err := os.WriteFile(filepath.Join(dir, "ceiling.json"), []byte(ceiling), 0o644); err != nil {
		t.Fatal(err)
	}
	run := `{"valid":true,"throughput":40000,"min_written_bytes_per_op":1048.576,"client_cpu_us_per_op":%d,"conns":6,"endpoints":["a"],"per_op":{"out_bytes":1100,"in_bytes":30,"writes":1}}`
	writeAt(t, dir, "0", "put-1k", "main-max-1.1.json", fmt.Sprintf(run, 5))
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| put-1k | 1 | - | 0 | 1049 | 100,000 | 50,000 | 1.00 | 1,000,000 | - | 50,000 | network |",
		"| put-1k | 1 | 0 | main | 40,000 | 80.0% | 40,000 | 0.20 |",
		"index lookups 1.000",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("no %q in\n%s", want, b.String())
		}
	}
	writeAt(t, dir, "0", "put-1k", "main-max-1.1.json", fmt.Sprintf(run, 20))
	if err := report(io.Discard, dir, ""); !errors.Is(err, errGeneratorLimit) || !strings.Contains(err.Error(), "0.80 cores") {
		t.Errorf("a generator at 0.8 cores gave %v", err)
	}
}

func TestReportRampClimbing(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "main-max-1.json", `{"valid":true,"throughput":100,"ramp_climbing":true}`)
	write(t, dir, "main-max-2.json", `{"valid":true,"throughput":100}`)
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	if want := "- RTT 0 ms, put-1k, main max: 1 of 2 runs"; !strings.Contains(b.String(), want) {
		t.Fatalf("missing %s in report:\n%s", want, b.String())
	}
}

func TestReportRefusesInvalidGenerator(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "main-max-1.json", valid)
	if err := os.WriteFile(filepath.Join(dir, "generator-put-1k.json"), []byte(`{"valid":false}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := report(io.Discard, dir, ""); !errors.Is(err, errInvalidResult) {
		t.Fatalf("an invalid generator run was reported: %v", err)
	}
}

// maxAt writes the max results of etcd and main at an RTT, with the given
// throughputs.
func maxAt(t *testing.T, dir, rtt, bench string, etcd, main float64) {
	t.Helper()
	writeAt(t, dir, rtt, bench, "etcd-max-1.json", fmt.Sprintf(`{"valid":true,"throughput":%g}`, etcd))
	writeAt(t, dir, rtt, bench, "main-max-1.json", fmt.Sprintf(`{"valid":true,"throughput":%g}`, main))
}

func TestReportAcrossRTTs(t *testing.T) {
	dir := t.TempDir()
	for _, rtt := range []string{"10", "0", "2"} {
		maxAt(t, dir, rtt, "put-1k", 4000, 80000)
		writeAt(t, dir, rtt, "put-1k", "etcd-rate-1.json", `{"valid":true,"offered_rate":2000,"latency_us":{"p50":6000,"p99":17000}}`)
		writeAt(t, dir, rtt, "put-1k", "main-rate-1.json", `{"valid":true,"offered_rate":2000,"latency_us":{"p50":3300,"p99":4500}}`)
	}
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	want := "| put-1k | etcd | 0 | 4,000 | - | 2,000 | 6.00 | 17.00 | 0 of 2 |\n" +
		"| put-1k | etcd | 2 | 4,000 | - | 2,000 | 6.00 | 17.00 | 0 of 2 |\n" +
		"| put-1k | etcd | 10 | 4,000 | - | 2,000 | 6.00 | 17.00 | 0 of 2 |\n" +
		"| put-1k | main | 0 | 80,000 | 20.0x | 2,000 | 3.30 | 4.50 | 0 of 2 |\n"
	if !strings.Contains(b.String(), want) {
		t.Fatalf("missing\n%s\nin report:\n%s", want, b.String())
	}
}

func TestReportFailedEtcdRunStandsAlone(t *testing.T) {
	dir := t.TempDir()
	maxAt(t, dir, "0", "mixed-k8s", 3000, 8000)
	// etcd failed its only max run at 10 ms, and left an invalid result.
	writeAt(t, dir, "10", "mixed-k8s", "etcd-max-1.json", `{"valid":false}`)
	writeAt(t, dir, "10", "mixed-k8s", "etcd-max-1.failed", "loadgen exited 1: loadgen: mixed preload: too many requests\n")
	writeAt(t, dir, "10", "mixed-k8s", "main-max-1.json", `{"valid":true,"throughput":7000}`)
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatalf("a failed etcd run failed the report: %v", err)
	}
	for _, want := range []string{
		"| mixed-k8s | etcd | 10 | - | - | - | - | - | 1 of 1 |",
		"| mixed-k8s | main | 10 | 7,000 | etcd failed |",
		"- RTT 10 ms, mixed-k8s, etcd max 1: loadgen exited 1: loadgen: mixed preload: too many requests",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %s in report:\n%s", want, b.String())
		}
	}
}

func TestReportFailedTigerstoreRun(t *testing.T) {
	dir := t.TempDir()
	maxAt(t, dir, "0", "put-1k", 4000, 80000)
	writeAt(t, dir, "0", "put-1k", "etcd-max-2.json", `{"valid":true,"throughput":4000}`)
	writeAt(t, dir, "0", "put-1k", "main-max-2.failed", "check exited 1: the leader changed during the run\n")
	var b strings.Builder
	if err := report(&b, dir, ""); !errors.Is(err, errRunFailed) {
		t.Fatalf("a failed tigerstore run passed: %v", err)
	}
	if !strings.Contains(b.String(), "| put-1k | main | 0 | 80,000 | 20.0x | - | - | - | 1 of 2 |") {
		t.Fatalf("report:\n%s", b.String())
	}
}

func TestReportNoRate(t *testing.T) {
	dir := t.TempDir()
	writeAt(t, dir, "0", "put-1k", "etcd-max-1.failed", "loadgen exited 1: loadgen: request timed out\n")
	writeAt(t, dir, "0", "put-1k", "main-max-1.json", `{"valid":true,"throughput":80000}`)
	writeAt(t, dir, "0", "put-1k", "norate", "etcd has no max result here or at the first RTT\n")
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "- RTT 0 ms, put-1k: no rate runs, no shared rate: etcd has no max result here or at the first RTT") {
		t.Fatalf("report:\n%s", b.String())
	}
}

// TestReportShapes judges the candidate seed by seed: two shapes of very
// different speed, the candidate 20% faster on both, is a gain beyond the
// spread of either, though the two shapes together spread far wider. Each
// run names the node that led it.
func TestReportShapes(t *testing.T) {
	dir := t.TempDir()
	run := func(target, seed string, rep int, ops float64, shape string, port int) {
		name := fmt.Sprintf("%s-max-%s.%d", target, seed, rep)
		writeAt(t, dir, "0", "k8s", name+".json", fmt.Sprintf(`{"valid":true,"throughput":%g,"k8s":{"shape":%q,"copies":64}}`, ops, shape))
		writeAt(t, dir, "0", "k8s", name+".placement", fmt.Sprintf(`{"leader":"127.0.0.1:%d"}`, port))
	}
	for rep, jitter := range []float64{0.99, 1, 1.01} {
		run("baseline", "7", rep+1, 10000*jitter, "small", 20010)
		run("candidate", "7", rep+1, 12000*jitter, "small", 20020)
		run("baseline", "42", rep+1, 100000*jitter, "large", 20030)
		run("candidate", "42", rep+1, 120000*jitter, "large", 20010)
	}
	var b strings.Builder
	if err := report(&b, dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| k8s | 7 | small | 0 | candidate | 11,880 (n2), 12,000 (n2), 12,120 (n2) | 12,000 | 2.0% | +20.0% |",
		"| k8s | 42 | large | 0 | baseline | 99,000 (n3), 100,000 (n3), 101,000 (n3) | 100,000 | 2.0% | - |",
		"| k8s | 42 | large | 0 | candidate | 118,800 (n1), 120,000 (n1), 121,200 (n1) | 120,000 | 2.0% | +20.0% |",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("no line %q in\n%s", want, b.String())
		}
	}
}

// TestGeneratorAt reads the generator's cores at a target off its ramp: a
// line through the steps, at least the most a step used, and in proportion
// to the rate without a ramp.
func TestGeneratorAt(t *testing.T) {
	ramp := func(steps ...[2]float64) result {
		var r result
		r.ClientCPUUsPerOp = 3
		for _, s := range steps {
			r.Ramp = append(r.Ramp, struct {
				Throughput  float64 `json:"throughput"`
				ClientCores float64 `json:"client_cores"`
			}{s[0], s[1]})
		}
		return r
	}
	for _, c := range []struct {
		name string
		r    result
		want float64
	}{
		{"cores grow with the rate", ramp([2]float64{50000, 0.2}, [2]float64{100000, 0.3}), 0.4},
		{"flat cores stay flat", ramp([2]float64{50000, 0.34}, [2]float64{100000, 0.34}), 0.34},
		{"falling cores count the most a step used", ramp([2]float64{50000, 0.4}, [2]float64{100000, 0.3}), 0.4},
		{"no ramp: in proportion", ramp([2]float64{100000, 0.3}), 0.45},
	} {
		if got := generatorAt(c.r, 150000); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: %.4f cores, want %.4f", c.name, got, c.want)
		}
	}
}
