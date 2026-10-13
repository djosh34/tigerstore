package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write puts a result file for bench put-1k into dir.
func write(t *testing.T, dir, name, json string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "put-1k"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "put-1k", name), []byte(json), 0o644); err != nil {
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
	if !strings.Contains(b.String(), "| put-1k | main | 1,000") {
		t.Fatalf("report:\n%s", b.String())
	}
}

func TestReportRefusesInvalidResults(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "main-max-1.json", `{"valid": false, "throughput": 1000}`)
	if err := report(io.Discard, dir); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("an invalid result was reported: %v", err)
	}
}

func TestReportRefusesUnequalRuns(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "etcd-max-1.json", valid)
	write(t, dir, "etcd-max-2.json", valid)
	write(t, dir, "main-max-1.json", valid)
	if err := report(io.Discard, dir); err == nil || !strings.Contains(err.Error(), "runs") {
		t.Fatalf("unequal runs were reported: %v", err)
	}
}
