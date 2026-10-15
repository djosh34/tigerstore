//go:build difftest

package difftest

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestDebug runs on a server with --enable-pprof, --metrics extensive
// and --listen-metrics-urls, which TIGERSTORE_DIFF_METRICS names.
func TestDebug(t *testing.T) {
	runCases(t, "etcd-v3.7.2-debug.json", []testCase{{"pprof", pprofCase}, {"extensive", extensiveCase}, {"metrics-url", metricsURLCase}})
}

// clusterOnly are metric families that only a server with peers has:
// the difftest's etcd runs alone.
func clusterOnly(name string) bool {
	if !strings.HasPrefix(name, "etcd_network_") {
		return false
	}
	switch name {
	case "etcd_network_client_grpc_received_bytes_total", "etcd_network_client_grpc_sent_bytes_total", "etcd_network_known_peers", "etcd_network_server_stream_failures_total":
		return false
	}
	return true
}

// scrape reads /metrics of the endpoint.
func (r *run) scrape(hc *http.Client, url string) map[string]*dto.MetricFamily {
	r.t.Helper()
	resp, err := hc.Get(url)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // closing a test response
	p := expfmt.NewTextParser(model.LegacyValidation)
	fams, err := p.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	return fams
}

// family is what the record keeps of a metric family: its type, help
// and label names.
type family struct {
	Type   string   `json:"type"`
	Help   string   `json:"help"`
	Labels []string `json:"labels,omitempty"`
}

// families records the families of a scrape, but tigerstore's own and
// those of a cluster.
func (r *run) families(do string, fams map[string]*dto.MetricFamily) {
	out := map[string]family{}
	for name, f := range fams {
		if strings.HasPrefix(name, "tigerstore_") || clusterOnly(name) {
			continue
		}
		labels := map[string]bool{}
		for _, m := range f.Metric {
			for _, l := range m.Label {
				labels[l.GetName()] = true
			}
		}
		out[name] = family{Type: f.GetType().String(), Help: f.GetHelp(), Labels: slices.Sorted(maps.Keys(labels))}
	}
	r.record(do, out, nil)
}

// series finds the value of the series of a family with the labels.
func series(fams map[string]*dto.MetricFamily, name string, labels ...string) float64 {
	f := fams[name]
	if f == nil {
		return -1
	}
	for _, m := range f.Metric {
		match := true
		for i := 0; i+1 < len(labels); i += 2 {
			found := false
			for _, l := range m.Label {
				if l.GetName() == labels[i] && l.GetValue() == labels[i+1] {
					found = true
				}
			}
			match = match && found
		}
		if !match {
			continue
		}
		switch {
		case m.Counter != nil:
			return m.Counter.GetValue()
		case m.Gauge != nil:
			return m.Gauge.GetValue()
		case m.Histogram != nil:
			return float64(m.Histogram.GetSampleCount())
		case m.Untyped != nil:
			return m.Untyped.GetValue()
		}
	}
	return 0
}

// delta is one series to compare before and after the case's calls.
type delta struct {
	name   string
	labels []string
}

// metricsCase checks /metrics and /debug/vars on a server with etcd's
// defaults: the families, the gRPC methods with series, and how known
// calls move etcd's counters.
func metricsCase(r *run) {
	fams := r.scrape(r.c.http2, r.c.url+"/metrics")
	r.families("families", fams)
	var methods []string
	for _, m := range fams["grpc_server_started_total"].GetMetric() {
		l := map[string]string{}
		for _, p := range m.Label {
			l[p.GetName()] = p.GetValue()
		}
		methods = append(methods, l["grpc_service"]+"/"+l["grpc_method"]+" "+l["grpc_type"])
	}
	slices.Sort(methods)
	r.record("gRPC methods with series", methods, nil)

	deltas := []delta{
		{"etcd_mvcc_put_total", nil},
		{"etcd_mvcc_range_total", nil},
		{"etcd_mvcc_txn_total", nil},
		{"etcd_debugging_mvcc_total_put_size_in_bytes", nil},
		{"etcd_debugging_mvcc_keys_total", nil},
		{"etcd_debugging_mvcc_current_revision", nil},
		{"etcd_debugging_lease_granted_total", nil},
		{"etcd_debugging_lease_revoked_total", nil},
		{"etcd_debugging_lease_ttl_total", nil},
		{"etcd_network_client_grpc_received_bytes_total", nil},
		{"etcd_server_range_duration_seconds", []string{"success", "true"}},
		{"grpc_server_msg_received_total", []string{"grpc_method", "Txn"}},
		{"grpc_server_msg_sent_total", []string{"grpc_method", "Txn"}},
		{"grpc_server_started_total", []string{"grpc_method", "Put"}},
		{"grpc_server_handled_total", []string{"grpc_method", "LeaseRevoke", "grpc_code", "OK"}},
		{"etcd_server_client_requests_total", []string{"type", "unary", "client_api_version", "unknown"}},
	}
	for _, t := range []string{"Put", "Range", "ReadonlyTxn", "Txn", "LeaseGrant", "LeaseRevoke"} {
		deltas = append(deltas, delta{"etcd_server_request_duration_seconds", []string{"type", t, "success", "true"}})
	}
	for _, op := range []string{"Put", "Txn", "LeaseGrant", "LeaseRevoke"} {
		deltas = append(deltas, delta{"etcd_server_apply_duration_seconds", []string{"op", op, "success", "true"}})
	}
	before := r.scrape(r.c.http2, r.c.url+"/metrics")
	a, b := r.key("a"), r.key("b")
	r.put("put", &pb.PutRequest{Key: a, Value: []byte("12")})
	_, err := r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: a})
	r.record("range", nil, err)
	_, err = r.c.kv.Txn(r.ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: a}}}}})
	r.record("read-only txn", nil, err)
	_, err = r.c.kv.Txn(r.ctx, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: b, Value: []byte("345")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: a}}},
	}})
	r.record("txn with a put and a range", nil, err)
	g, err := r.c.lease.LeaseGrant(r.ctx, &pb.LeaseGrantRequest{TTL: 60})
	r.record("lease grant", nil, err)
	if err == nil {
		_, err = r.c.lease.LeaseRevoke(r.ctx, &pb.LeaseRevokeRequest{ID: g.ID})
		r.record("lease revoke", nil, err)
	}
	after := r.scrape(r.c.http2, r.c.url+"/metrics")
	moved := map[string]float64{}
	for _, d := range deltas {
		moved[d.name+"{"+strings.Join(d.labels, ",")+"}"] = series(after, d.name, d.labels...) - series(before, d.name, d.labels...)
	}
	r.record("counters moved by the calls", moved, nil)

	resp, err := r.c.http2.Post(r.c.url+"/metrics", "text/plain", nil)
	if err != nil {
		r.t.Fatal(err)
	}
	_ = resp.Body.Close()
	r.record("POST /metrics", struct {
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
	}{resp.StatusCode, resp.Header.Get("Content-Type")}, nil)
	r.http("HEAD /health", r.c.http2, http.MethodHead, "/health")
	r.http("HEAD /version", r.c.http2, http.MethodHead, "/version")
	r.http("POST /debug/vars", r.c.http2, http.MethodPost, "/debug/vars")
	r.http("pprof is off", r.c.http2, http.MethodGet, "/debug/pprof/")
	r.debugVars()
}

// debugVars records the status, content type and variable names of
// /debug/vars, the fields of raft.status and whether its state is a
// voter's: the difftest's etcd leads, while the member tigerstore's
// difftest asks may follow.
func (r *run) debugVars() {
	resp, err := r.c.http2.Get(r.c.url + "/debug/vars")
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // closing a test response
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	var vars map[string]json.RawMessage
	if err := json.Unmarshal(b, &vars); err != nil {
		r.t.Fatalf("/debug/vars: %v", err)
	}
	var raft map[string]json.RawMessage
	if err := json.Unmarshal(vars["raft.status"], &raft); err != nil {
		r.t.Fatalf("raft.status: %v", err)
	}
	r.record("GET /debug/vars", struct {
		Status      int      `json:"status"`
		ContentType string   `json:"contentType"`
		Vars        []string `json:"vars"`
		Raft        []string `json:"raft"`
		Voter       bool     `json:"voter"`
	}{resp.StatusCode, resp.Header.Get("Content-Type"), slices.Sorted(maps.Keys(vars)), slices.Sorted(maps.Keys(raft)), slices.Contains([]string{`"StateLeader"`, `"StateFollower"`}, string(raft["raftState"]))}, nil)
}

// status records the status and content type of a GET.
func (r *run) status(do string, hc *http.Client, url string) {
	resp, err := hc.Get(url)
	if err != nil {
		r.t.Fatalf("%s: %v", do, err)
	}
	_ = resp.Body.Close()
	r.record(do, struct {
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
		CORS        string `json:"cors,omitempty"`
	}{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Access-Control-Allow-Origin")}, nil)
}

// pprofCase reads the profiles that --enable-pprof serves.
func pprofCase(r *run) {
	resp, err := r.c.http2.Get(r.c.url + "/debug/pprof/")
	if err != nil {
		r.t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		r.t.Fatal(err)
	}
	r.record("index", struct {
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
		Profiles    bool   `json:"profiles"`
	}{resp.StatusCode, resp.Header.Get("Content-Type"), strings.Contains(string(b), "Types of profiles available")}, nil)
	for _, p := range []string{"/debug/pprof", "/debug/pprof/cmdline", "/debug/pprof/goroutine?debug=1", "/debug/pprof/heap?debug=1", "/debug/pprof/mutex?debug=1", "/debug/pprof/nope", "/debug/pprof/symbol"} {
		r.status(p, r.c.http2, r.c.url+p)
	}
}

// extensiveCase checks the gRPC handling times of --metrics=extensive.
func extensiveCase(r *run) {
	before := r.scrape(r.c.http2, r.c.url+"/metrics")
	f := before["grpc_server_handling_seconds"]
	r.record("family", family{Type: f.GetType().String(), Help: f.GetHelp()}, nil)
	r.put("put", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
	after := r.scrape(r.c.http2, r.c.url+"/metrics")
	r.record("a put's handling time", series(after, "grpc_server_handling_seconds", "grpc_method", "Put")-series(before, "grpc_server_handling_seconds", "grpc_method", "Put"), nil)
}

// metricsURLCase reads the endpoints of --listen-metrics-urls.
func metricsURLCase(r *run) {
	addr := os.Getenv("TIGERSTORE_DIFF_METRICS")
	if addr == "" {
		r.t.Fatal("TIGERSTORE_DIFF_METRICS is not set")
	}
	url := "http://" + addr
	hc := &http.Client{Timeout: r.c.http1.Timeout}
	r.status("/metrics", hc, url+"/metrics")
	r.families("families", r.scrape(hc, url+"/metrics"))
	for _, p := range []string{"/health", "/livez", "/readyz", "/readyz?verbose", "/version", "/debug/vars", "/debug/pprof/", "/v3/kv/range"} {
		resp, err := hc.Get(url + p)
		if err != nil {
			r.t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			r.t.Fatal(err)
		}
		// etcd runs the checks of /readyz in map order.
		lines := strings.SplitAfter(string(b), "\n")
		slices.Sort(lines)
		r.record(p, struct {
			Status      int    `json:"status"`
			ContentType string `json:"contentType"`
			CORS        string `json:"cors,omitempty"`
			Body        string `json:"body"`
		}{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Access-Control-Allow-Origin"), strings.Join(lines, "")}, nil)
	}
}
