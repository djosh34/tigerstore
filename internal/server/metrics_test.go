package server_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// scrapeMetrics reads node id's /metrics into a map from series to value.
func scrapeMetrics(t *testing.T, c *cluster, id uint64, cert tls.Certificate) map[string]float64 {
	t.Helper()
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	resp, err := hc.Get("https://" + c.running[id].addr + "/metrics")
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

// TestVersionMetrics checks the versions of #145 on /metrics: the
// cluster versions the binary supports and the active one, and the peer
// protocol range and the version of each connection.
func TestVersionMetrics(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 96)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"tigerstore_cluster_version_min":       1,
		"tigerstore_cluster_version_max":       core.MaxVersion,
		"tigerstore_cluster_version":           core.MaxVersion,
		"tigerstore_peer_protocol_min_version": 1,
		"tigerstore_peer_protocol_max_version": 1,
	}
	for _, peer := range []string{"2", "3"} {
		for _, dir := range []string{"in", "out"} {
			want[`tigerstore_peer_protocol_version{direction="`+dir+`",peer="`+peer+`"}`] = 1
		}
	}
	// The connections and the version announcements need a moment.
	retry(t, func(context.Context) (map[string]float64, error) {
		got := scrapeMetrics(t, c, 1, cert)
		for k, v := range want {
			if got[k] != v {
				return nil, fmt.Errorf("%s is %g, want %g", k, got[k], v)
			}
		}
		return got, nil
	})
	// The connection from a stopped peer ends. The one to it ends only
	// when a write to it fails, and a follower may have nothing to send.
	c.running[3].stop()
	delete(c.running, 3)
	retry(t, func(context.Context) (map[string]float64, error) {
		got := scrapeMetrics(t, c, 1, cert)
		if k := `tigerstore_peer_protocol_version{direction="in",peer="3"}`; got[k] != 0 {
			return nil, fmt.Errorf("%s is %g with node 3 stopped", k, got[k])
		}
		return got, nil
	})
}

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
	scrape := func() map[string]float64 { return scrapeMetrics(t, c, 1, cert) }
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

	// A failing call and a watch the client cancels.
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
	// Counts are per connection; a closed connection's stay in the sum.
	before = scrape()
	other, err := grpc.NewClient(c.running[1].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pb.NewKVClient(other).Range(context.Background(), &pb.RangeRequest{Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for scrape()[rangeOK]-before[rangeOK] != 1 {
		if time.Now().After(deadline) {
			t.Fatal("a range on a closed connection is not counted")
		}
		time.Sleep(20 * time.Millisecond)
	}
	m := scrape()
	if m["etcd_server_has_leader"] != 1 || m["etcd_server_leader_changes_seen_total"] < 1 || m["etcd_server_proposals_committed_total"] < 1 ||
		m["etcd_server_proposals_applied_total"] < 1 || m["etcd_mvcc_delete_total"] != 0 || m["process_start_time_seconds"] <= 0 {
		t.Fatalf("server metrics: %v", m)
	}
}
