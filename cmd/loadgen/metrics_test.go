package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseMetricsSumsPeersAndReadsLeader(t *testing.T) {
	page := `# HELP etcd_network_peer_sent_bytes_total The total number of bytes sent to peers.
# TYPE etcd_network_peer_sent_bytes_total counter
etcd_network_peer_sent_bytes_total{To="a b"} 1.5e+06
etcd_network_peer_sent_bytes_total{To="c"} 500000
etcd_network_peer_sent_failures_total{To="c"} 7
etcd_server_is_leader 1
`
	m, err := parseMetrics(strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	if m.peerSent != 2e6 || !m.leader {
		t.Fatalf("got %+v, want 2e6 bytes and leader", m)
	}
}

func TestParseMetricsNeedsTheLeaderGauge(t *testing.T) {
	_, err := parseMetrics(strings.NewReader("etcd_network_peer_sent_bytes_total{To=\"c\"} 5\n"))
	if !errors.Is(err, errNoMetric) {
		t.Fatalf("got %v, want %v", err, errNoMetric)
	}
}

func TestPeerBytesPerOpPutsTheLeaderFirst(t *testing.T) {
	from := []nodeMetrics{{peerSent: 100}, {peerSent: 1000}, {peerSent: 10}}
	to := []nodeMetrics{{peerSent: 300}, {peerSent: 9000, leader: true}, {peerSent: 410}}
	got := peerBytesPerOp(from, to, 100)
	if want := []float64{80, 2, 4}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	to[0].leader = true
	if got := peerBytesPerOp(from, to, 100); got != nil {
		t.Fatalf("two leaders: got %v, want nothing", got)
	}
	if got := peerBytesPerOp(from, to[:2], 100); got != nil {
		t.Fatalf("a missing sample: got %v, want nothing", got)
	}
}

// TestWindowMetricsRefusesALateStart drops a window whose start was read
// only after its end: those counters would describe no window.
func TestWindowMetricsRefusesALateStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, "etcd_server_is_leader 1\n")
	}))
	defer srv.Close()
	ep := strings.TrimPrefix(srv.URL, "http://")
	start := now()
	if _, err := windowMetrics([]string{ep, ep}, start, start+int64(50*time.Millisecond)); !errors.Is(err, errLateMetrics) {
		t.Fatalf("got %v, want %v", err, errLateMetrics)
	}
	start = now()
	got, err := windowMetrics([]string{ep, ep}, start, start+int64(300*time.Millisecond))
	if err != nil || len(got[0]) != 2 || len(got[1]) != 2 || !got[1][0].leader {
		t.Fatalf("got %+v, %v; want both reads of both nodes", got, err)
	}
}
