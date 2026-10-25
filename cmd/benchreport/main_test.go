package main

import (
	"errors"
	"fmt"
	"io"
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
	if err := report(&b, dir); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "| put-1k | 0 | main | max | 1,000") {
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
	if err := report(&b, dir); err != nil {
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
	if err := report(&b, dir); err != nil {
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
	if err := report(&b, dir); err != nil {
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
	if err := report(io.Discard, dir); !errors.Is(err, errInvalidResult) {
		t.Fatalf("an invalid result was reported: %v", err)
	}
}

func TestReportRefusesUnequalRuns(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "etcd-max-2.json", valid)
	write(t, dir, "main-max-1.json", valid)
	if err := report(io.Discard, dir); !errors.Is(err, errUnequalRuns) {
		t.Fatalf("unequal runs were reported: %v", err)
	}
}

func TestReportCeilings(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "main-max-1.json", `{"valid": true, "throughput": 90000, "disk_bytes_per_op": 3495.25}`)
	write(t, dir, "main-tmpfs-1.json", `{"valid": true, "throughput": 120000}`)
	write(t, dir, "main-alone-1.json", `{"valid": true, "throughput": 110000, "disk_bytes_per_op": 1165.08}`)
	write(t, dir, "diskbench.jsonl", "{\"group_bytes\":1048576,\"mib_per_s\":333.3}\n{\"max_mib_per_s\":333.3}\n")
	// A second benchmark gets a table of its own.
	writeAt(t, dir, "0", "txn-1k", "etcd-max-1.json", valid)
	writeAt(t, dir, "0", "txn-1k", "main-max-1.json", `{"valid": true, "throughput": 60000, "disk_bytes_per_op": 3495.25}`)
	writeAt(t, dir, "0", "txn-1k", "main-tmpfs-1.json", `{"valid": true, "throughput": 80000}`)
	writeAt(t, dir, "0", "txn-1k", "main-alone-1.json", `{"valid": true, "throughput": 70000, "disk_bytes_per_op": 1165.08}`)
	writeAt(t, dir, "0", "txn-1k", "diskbench.jsonl", "{\"max_mib_per_s\":333.3}\n")
	var b strings.Builder
	if err := report(&b, dir); err != nil {
		t.Fatal(err)
	}
	// 333.3 MiB/s over 3,495.25 bytes is 99,990/s, under the 120,000 on
	// tmpfs; over 1,165.08 bytes it is 299,971/s, so tmpfs is the lower.
	for _, want := range []string{
		"| put-1k | main | shared disk | 90,000 | 3495 | 99,990 | 120,000 | 90.0% |",
		"| put-1k | main | leader alone | 110,000 | 1165 | 299,971 | 120,000 | 91.7% |",
		"| put-1k | 0 | main | tmpfs | 120,000",
		"txn-1k at RTT 0 ms: the disk takes 333.3 MiB/s",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %s in report:\n%s", want, b.String())
		}
	}
	if n := strings.Count(b.String(), "| Benchmark | Store | Layout |"); n != 2 {
		t.Fatalf("%d ceiling tables, want one per benchmark:\n%s", n, b.String())
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
	if err := report(&b, dir); err != nil {
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

func TestReportBars(t *testing.T) {
	dir := t.TempDir()
	// At RTT 0 there is no bar; at 2 ms, 9x misses put's 10x and 2.6x
	// meets mixed-k8s's 2.5x.
	maxAt(t, dir, "0", "put-1k", 4000, 4000)
	maxAt(t, dir, "2", "put-1k", 4000, 36000)
	maxAt(t, dir, "2", "mixed-k8s", 3000, 7800)
	var b strings.Builder
	err := report(&b, dir)
	if !errors.Is(err, errBarMissed) {
		t.Fatalf("a missed bar passed: %v", err)
	}
	if !strings.Contains(err.Error(), "put-1k at RTT 2 ms, main: 9.0x") || strings.Contains(err.Error(), "mixed-k8s") || strings.Contains(err.Error(), "RTT 0") {
		t.Fatalf("wrong bars missed: %v", err)
	}
	if !strings.Contains(b.String(), "| put-1k | main | 2 | 36,000 | 9.0x, below 10x |") {
		t.Fatalf("the miss is not in the table:\n%s", b.String())
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
	if err := report(&b, dir); err != nil {
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
	if err := report(&b, dir); !errors.Is(err, errRunFailed) {
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
	if err := report(&b, dir); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "- RTT 0 ms, put-1k: no rate runs, no shared rate: etcd has no max result here or at the first RTT") {
		t.Fatalf("report:\n%s", b.String())
	}
}
