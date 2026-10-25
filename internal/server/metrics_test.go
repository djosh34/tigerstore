package server_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/server"
)

// scrapeMetrics reads node 1's /metrics into a map from series to value.
func scrapeMetrics(t *testing.T, c *cluster, cert tls.Certificate) map[string]float64 {
	t.Helper()
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	resp, err := hc.Get("https://" + c.running[1].addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // a test response
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8; escaping=underscores" {
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
		"tigerstore_peer_protocol_min_version": 2,
		"tigerstore_peer_protocol_max_version": 2,
	}
	for _, peer := range []string{"2", "3"} {
		for _, dir := range []string{"in", "out"} {
			want[`tigerstore_peer_protocol_version{direction="`+dir+`",peer="`+peer+`"}`] = 2
		}
	}
	// The connections and the version announcements need a moment.
	retry(t, func(context.Context) (map[string]float64, error) {
		got := scrapeMetrics(t, c, cert)
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
		got := scrapeMetrics(t, c, cert)
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
	scrape := func() map[string]float64 { return scrapeMetrics(t, c, cert) }
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

// TestEtcdMetrics checks etcd's metrics beyond the gRPC counts: request
// and apply times by type, client requests by API version, the core's
// counts of leases, keys and transactions, the log's fsyncs, the gRPC
// handling times of --metrics=extensive, /debug/vars and pprof.
func TestEtcdMetrics(t *testing.T) {
	c := newClusterWith(t, func(c *cluster) {
		c.serverConfig = func(cfg *server.Config) { cfg.ExtensiveMetrics, cfg.Pprof = true, true }
	})
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv, lease := pb.NewKVClient(conn), pb.NewLeaseClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "client-api-version", "3.7")
	retry(t, func(context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	if _, err := kv.Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("2")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 60}); err != nil {
		t.Fatal(err)
	}
	want := map[string]func(float64) bool{
		`etcd_server_client_requests_total{client_api_version="3.7",type="unary"}`:                               func(v float64) bool { return v >= 4 },
		`etcd_server_request_duration_seconds_count{success="true",type="Put"}`:                                  func(v float64) bool { return v >= 1 },
		`etcd_server_request_duration_seconds_count{success="true",type="ReadonlyTxn"}`:                          func(v float64) bool { return v == 1 },
		`etcd_server_request_duration_seconds_count{success="true",type="Txn"}`:                                  func(v float64) bool { return v == 1 },
		`etcd_server_request_duration_seconds_count{success="true",type="LeaseGrant"}`:                           func(v float64) bool { return v == 1 },
		`etcd_server_apply_duration_seconds_count{op="Put",success="true",version="v3"}`:                         func(v float64) bool { return v >= 1 },
		`etcd_server_apply_duration_seconds_count{op="LeaseGrant",success="true",version="v3"}`:                  func(v float64) bool { return v == 1 },
		`grpc_server_handling_seconds_count{grpc_method="Put",grpc_service="etcdserverpb.KV",grpc_type="unary"}`: func(v float64) bool { return v >= 1 },
		`grpc_server_msg_received_total{grpc_method="Txn",grpc_service="etcdserverpb.KV",grpc_type="unary"}`:     func(v float64) bool { return v == 2 },
		`grpc_server_msg_sent_total{grpc_method="Txn",grpc_service="etcdserverpb.KV",grpc_type="unary"}`:         func(v float64) bool { return v == 2 },
		`etcd_debugging_lease_granted_total`:                                                                     func(v float64) bool { return v == 1 },
		`etcd_debugging_lease_ttl_total_bucket{le="64"}`:                                                         func(v float64) bool { return v == 1 },
		`etcd_mvcc_txn_total`:                                func(v float64) bool { return v == 1 },
		`etcd_debugging_mvcc_keys_total`:                     func(v float64) bool { return v == 2 },
		`etcd_debugging_mvcc_total_put_size_in_bytes`:        func(v float64) bool { return v == 4 },
		`etcd_disk_wal_fsync_duration_seconds_count`:         func(v float64) bool { return v >= 1 },
		`etcd_disk_wal_write_bytes_total`:                    func(v float64) bool { return v > 0 },
		`etcd_server_version{server_version="3.7.2"}`:        func(v float64) bool { return v == 1 },
		`etcd_cluster_version{cluster_version="3.7"}`:        func(v float64) bool { return v == 1 },
		`etcd_server_quota_backend_bytes`:                    func(v float64) bool { return v == 2<<30 },
		`etcd_debugging_mvcc_current_revision`:               func(v float64) bool { return v == 3 },
		`go_goroutines`:                                      func(v float64) bool { return v > 0 },
		`process_open_fds`:                                   func(v float64) bool { return v > 0 },
		`promhttp_metric_handler_requests_total{code="200"}`: func(v float64) bool { return v >= 0 },
	}
	retry(t, func(context.Context) (map[string]float64, error) {
		got := scrapeMetrics(t, c, cert)
		for k, ok := range want {
			if v, found := got[k]; !found || !ok(v) {
				return nil, fmt.Errorf("%s is %g (found %v)", k, v, found)
			}
		}
		return got, nil
	})

	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	get := func(path string) (int, string) {
		resp, err := hc.Get("https://" + c.running[1].addr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // a test response
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	code, body := get("/debug/vars")
	var vars map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &vars); code != 200 || err != nil {
		t.Fatalf("/debug/vars: %d %v %q", code, err, body)
	}
	for _, k := range []string{"cmdline", "memstats", "file_descriptor_limit", "raft.status"} {
		if _, ok := vars[k]; !ok {
			t.Errorf("/debug/vars has no %s", k)
		}
	}
	var raft struct {
		ID    string `json:"id"`
		State string `json:"raftState"`
	}
	if err := json.Unmarshal(vars["raft.status"], &raft); err != nil || raft.ID != "1" {
		t.Errorf("raft.status %s: %v", vars["raft.status"], err)
	}
	if code, body := get("/debug/pprof/"); code != 200 || !strings.Contains(body, "goroutine") {
		t.Errorf("/debug/pprof/: %d %q", code, body)
	}
	if code, _ := get("/debug/pprof/goroutine?debug=1"); code != 200 {
		t.Errorf("/debug/pprof/goroutine: %d", code)
	}
}

// TestQuotaAlertRatio checks the ratio of etcd-mixin's
// etcdDatabaseQuotaLowSpace alert, etcd_mvcc_db_total_size_in_bytes over
// etcd_server_quota_backend_bytes. As in etcd v3.7.2, where the quota
// compares the database's size, the size is what tigerstore's quota
// compares: the ratio stays below 100% while writes go in, however large
// the log grows, and the write NOSPACE refuses is one that would take it
// to 100%. Status reports the same size.
func TestQuotaAlertRatio(t *testing.T) {
	const quota = 256 << 10
	c := newClusterWith(t, func(c *cluster) {
		c.coreConfig = func(cfg *core.Config) { cfg.QuotaBytes = quota }
	})
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	kv, m := c.client(1, cert), pb.NewMaintenanceClient(c.dial(1, cert))
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")})
	})
	ratio := func() (float64, map[string]float64) {
		t.Helper()
		got := scrapeMetrics(t, c, cert)
		st, err := m.Status(ctx, &pb.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		total, inUse := got["etcd_mvcc_db_total_size_in_bytes"], got["etcd_mvcc_db_total_size_in_use_in_bytes"]
		if got["etcd_server_quota_backend_bytes"] != quota || st.DbSizeQuota != quota || total != inUse ||
			float64(st.DbSize) != total || float64(st.DbSizeInUse) != total {
			t.Fatalf("quota %v, size %v, in use %v; Status: quota %d, size %d, in use %d",
				got["etcd_server_quota_backend_bytes"], total, inUse, st.DbSizeQuota, st.DbSize, st.DbSizeInUse)
		}
		return total / quota, got
	}
	value := make([]byte, 8<<10)
	// One key written over and compacted each time: the log grows past
	// the quota, the database does not.
	for i := range 64 {
		r, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: value})
		if err != nil {
			t.Fatalf("put %d of k: %v", i, err)
		}
		if _, err := kv.Compact(ctx, &pb.CompactionRequest{Revision: r.Header.Revision}); err != nil {
			t.Fatal(err)
		}
	}
	if r, got := ratio(); r >= 0.5 || got["tigerstore_disk_bytes"] < 2*quota {
		t.Fatalf("after 64 compacted writes of one key: ratio %.2f, log %v bytes", r, got["tigerstore_disk_bytes"])
	}
	// New keys until the quota refuses one.
	for i := 0; ; i++ {
		key := fmt.Appendf(nil, "k%03d", i)
		before, _ := ratio()
		_, err := kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			cost := float64(256+len(key)+len(value)) / quota
			if !errors.Is(err, rpctypes.ErrGRPCNoSpace) || before >= 1 || before+cost < 1 {
				t.Fatalf("put %s at ratio %.4f with cost %.4f: %v", key, before, cost, err)
			}
			break
		}
		if after, _ := ratio(); after >= 1 {
			t.Fatalf("put %s went in, and the ratio is %.4f", key, after)
		}
		if i > quota/len(value) {
			t.Fatal("the quota never refused a put")
		}
	}
	alarms, err := m.Alarm(ctx, &pb.AlarmRequest{})
	if err != nil || len(alarms.Alarms) != 1 || alarms.Alarms[0].Alarm != pb.AlarmType_NOSPACE {
		t.Fatalf("alarms after the refusal: %v, %v", alarms, err)
	}
}
