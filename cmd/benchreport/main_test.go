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

// write puts a result file for bench put-1k into dir.
func write(t *testing.T, dir, name, json string) {
	t.Helper()
	writeIn(t, dir, "put-1k", name, json)
}

// writeIn puts a result file for a bench into dir.
func writeIn(t *testing.T, dir, bench, name, json string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, bench), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, bench, name), []byte(json), 0o644); err != nil {
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
	if !strings.Contains(b.String(), "| put-1k | main | max | 1,000") {
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
	writeIn(t, dir, "txn-1k", "etcd-max-1.json", valid)
	writeIn(t, dir, "txn-1k", "main-max-1.json", `{"valid": true, "throughput": 60000, "disk_bytes_per_op": 3495.25}`)
	writeIn(t, dir, "txn-1k", "main-tmpfs-1.json", `{"valid": true, "throughput": 80000}`)
	writeIn(t, dir, "txn-1k", "main-alone-1.json", `{"valid": true, "throughput": 70000, "disk_bytes_per_op": 1165.08}`)
	writeIn(t, dir, "txn-1k", "diskbench.jsonl", "{\"max_mib_per_s\":333.3}\n")
	var b strings.Builder
	if err := report(&b, dir); err != nil {
		t.Fatal(err)
	}
	// 333.3 MiB/s over 3,495.25 bytes is 99,990/s, under the 120,000 on
	// tmpfs; over 1,165.08 bytes it is 299,971/s, so tmpfs is the lower.
	for _, want := range []string{
		"| put-1k | main | shared disk | 90,000 | 3495 | 99,990 | 120,000 | 90.0% |",
		"| put-1k | main | leader alone | 110,000 | 1165 | 299,971 | 120,000 | 91.7% |",
		"| put-1k | main | tmpfs | 120,000",
		"txn-1k: the disk takes 333.3 MiB/s",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %s in report:\n%s", want, b.String())
		}
	}
	if n := strings.Count(b.String(), "| Benchmark | Store | Layout |"); n != 2 {
		t.Fatalf("%d ceiling tables, want one per benchmark:\n%s", n, b.String())
	}
}
