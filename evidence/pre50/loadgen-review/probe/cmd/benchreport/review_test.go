package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReviewInvalidRunIncluded(t *testing.T) {
	dir := t.TempDir()
	bench := filepath.Join(dir, "put-1k")
	if err := os.Mkdir(bench, 0700); err != nil {
		t.Fatal(err)
	}
	raw := `{"throughput":1234,"offered_rate":999,"server_cpu_us_per_op":0,"ops":1234,"latency_us":{"count":1234,"p50":0,"p99":0,"p999":0},"errors":{"grpc Unavailable: broken":100}}`
	if err := os.WriteFile(filepath.Join(bench, "etcd-rate-1.json"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := report(&out, dir); err != nil {
		t.Fatal(err)
	}
	t.Logf("report accepts failed run without error warning or checking rate:\n%s", out.String())
}
