package server_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// TestMetrics checks /metrics: Prometheus' text format under etcd's
// names, a series for every code of every method, every started call
// counted once as handled (a call the client cancels as Canceled),
// protobuf bytes in and out, and mvcc ranges counted as etcd counts them.
func TestMetrics(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv := pb.NewKVClient(conn)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	scrape := func() map[string]float64 {
		t.Helper()
		resp, err := hc.Get("https://" + c.running[1].addr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // a test response
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
			t.Fatalf("/metrics: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		out := map[string]float64{}
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "#") {
				continue
			}
			i := strings.LastIndexByte(line, ' ')
			v, err := strconv.ParseFloat(line[i+1:], 64)
			if err != nil {
				t.Fatalf("line %q: %v", line, err)
			}
			out[line[:i]] = v
		}
		return out
	}
	const rangeOK = `grpc_server_handled_total{grpc_code="OK",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"}`

	// Bytes and handled counts follow one known call.
	before := scrape()
	req := &pb.RangeRequest{Key: []byte("a")}
	resp, err := kv.Range(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	after := scrape()
	if d := after["etcd_network_client_grpc_received_bytes_total"] - before["etcd_network_client_grpc_received_bytes_total"]; d != float64(proto.Size(req)) {
		t.Errorf("received %g bytes for a %d-byte request", d, proto.Size(req))
	}
	if d := after["etcd_network_client_grpc_sent_bytes_total"] - before["etcd_network_client_grpc_sent_bytes_total"]; d != float64(proto.Size(resp)) {
		t.Errorf("sent %g bytes for a %d-byte response", d, proto.Size(resp))
	}
	if d := after[rangeOK] - before[rangeOK]; d != 1 {
		t.Errorf("one range counted %g times", d)
	}
	if d := after["etcd_mvcc_range_total"] - before["etcd_mvcc_range_total"]; d != 1 {
		t.Errorf("one range counted %g mvcc ranges", d)
	}

	// A transaction with one compare and a put with prev_kv: etcd counts
	// one range for the compare and two for the put's previous value.
	before = scrape()
	txn := &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_VERSION, Result: pb.Compare_GREATER, TargetUnion: &pb.Compare_Version{Version: 0}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("2"), PrevKv: true}}}},
	}
	if _, err := kv.Txn(context.Background(), txn); err != nil {
		t.Fatal(err)
	}
	after = scrape()
	if d := after["etcd_mvcc_range_total"] - before["etcd_mvcc_range_total"]; d != 3 {
		t.Errorf("a transaction with one compare and a put with prev_kv counted %g ranges, want 3", d)
	}
	if d := after["etcd_mvcc_put_total"] - before["etcd_mvcc_put_total"]; d != 1 {
		t.Errorf("one put counted %g times", d)
	}

	// A failing call, a unary call the client cancels and a watch the
	// client cancels.
	if _, err := kv.Range(context.Background(), &pb.RangeRequest{}); err == nil {
		t.Fatal("a range without a key succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w, err := pb.NewWatchClient(conn).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("a")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, _ = kv.Range(ctx, &pb.RangeRequest{Key: []byte("a")})

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := scrape()
		bad := ""
		for series, v := range m {
			if !strings.HasPrefix(series, "grpc_server_started_total{") {
				continue
			}
			labels := strings.TrimPrefix(series, "grpc_server_started_total{")
			handled := 0.0
			codes := 0
			for s2, h := range m {
				if strings.HasPrefix(s2, "grpc_server_handled_total{") && strings.HasSuffix(s2, ","+labels) {
					handled += h
					codes++
				}
			}
			if codes != 17 {
				t.Fatalf("%s: %d code series, want 17", series, codes)
			}
			if handled != v {
				bad = series
			}
		}
		const watchCanceled = `grpc_server_handled_total{grpc_code="Canceled",grpc_method="Watch",grpc_service="etcdserverpb.Watch",grpc_type="bidi_stream"}`
		if bad == "" && m[watchCanceled] == 1 &&
			m[`grpc_server_handled_total{grpc_code="InvalidArgument",grpc_method="Range",grpc_service="etcdserverpb.KV",grpc_type="unary"}`] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("started and handled differ for %q, or the cancelled watch or failed range is missing:\n%v", bad, m)
		}
		time.Sleep(20 * time.Millisecond)
	}
	m := scrape()
	if m["etcd_server_has_leader"] != 1 || m["etcd_server_leader_changes_seen_total"] < 1 || m["etcd_server_proposals_committed_total"] < 1 ||
		m["etcd_server_proposals_applied_total"] < 1 || m["etcd_mvcc_delete_total"] != 0 || m["process_start_time_seconds"] <= 0 {
		t.Fatalf("server metrics: %v", m)
	}
}
