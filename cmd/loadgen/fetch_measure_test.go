package main

import (
	"strings"
	"testing"
)

func TestFetchMeasurementSnapshotsPreserveNativeMetrics(t *testing.T) {
	m, err := parseMetrics(strings.NewReader("etcd_server_is_leader 1\netcd_network_peer_sent_bytes_total{To=\"all\"} 17\ntigerstore_fetch_measure_jobs_total 7\ntigerstore_fetch_measure_memory_budget_bytes 3221225472\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !m.leader || m.peerSent != 17 || m.fetchMeasure["tigerstore_fetch_measure_jobs_total"] != 7 || m.fetchMeasure["tigerstore_fetch_measure_memory_budget_bytes"] != 3<<30 {
		t.Fatalf("lost snapshot: %+v", m)
	}
}

func TestFetchMeasurementP95UsesTheExistingHistogram(t *testing.T) {
	var h hist
	for i := int64(1); i <= 100; i++ {
		h.record(i * 1000)
	}
	s := h.summary()
	if s.P95 != float64(h.quantile(.95))/1000 || s.P95 < s.P90 || s.P95 > s.P99 {
		t.Fatalf("wrong p95: %+v", s)
	}
}
