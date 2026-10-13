package server_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestMetrics checks /metrics against a few calls: it answers in
// Prometheus' text format under etcd's names, and the counters follow the
// calls made.
func TestMetrics(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	for range 3 {
		if _, err := kv.Range(context.Background(), &pb.RangeRequest{Key: []byte("a")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Range(context.Background(), &pb.RangeRequest{}); err == nil {
		t.Fatal("a range without a key succeeded")
	}
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	resp, err := hc.Get("https://" + c.running[1].addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close() //nolint:errcheck,gosec // a test response
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("/metrics: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	text := string(b)
	value := func(series string) float64 {
		t.Helper()
		m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` (\S+)$`).FindStringSubmatch(text)
		if m == nil {
			t.Fatalf("no series %s in:\n%s", series, text)
		}
		v, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for series, want := range map[string]float64{
		`grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"}`:              3,
		`grpc_server_handled_total{grpc_code="InvalidArgument",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"}`: 1,
		`grpc_server_handled_total{grpc_code="OK",grpc_method="DeleteRange",grpc_service="etcdserverpb.KV",grpc_type="unary"}`:        1,
		`grpc_server_started_total{grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"}`:                             4,
		`etcd_server_has_leader`: 1,
		`etcd_mvcc_delete_total`: 1,
		`etcd_mvcc_range_total`:  3,
	} {
		if got := value(series); got != want {
			t.Errorf("%s = %g, want %g", series, got, want)
		}
	}
	for _, series := range []string{
		"etcd_mvcc_put_total", "etcd_server_proposals_committed_total", "etcd_server_proposals_applied_total",
		"etcd_server_leader_changes_seen_total", "etcd_network_client_grpc_received_bytes_total", "etcd_network_client_grpc_sent_bytes_total",
		"process_start_time_seconds",
	} {
		if value(series) <= 0 {
			t.Errorf("%s is not above 0", series)
		}
	}
}
