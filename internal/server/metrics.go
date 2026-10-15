package server

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
)

// /metrics serves the member's registry (internal/metrics) as etcd's
// promhttp.Handler serves the default one: the Go and process
// collectors, etcd's metrics and grpc-prometheus' server metrics, and
// tigerstore's own, named tigerstore_*. Before each scrape it reads the
// core's counts. The gRPC counts come from counters of each connection,
// added up at a scrape, so that a call writes no counter that another
// connection writes.

// counter is a count with one writer at a time, read by scrapes: the
// writer adds with a plain load and store, cheaper than an atomic add on
// every call, and a scrape still reads a whole value.
type counter struct{ v atomic.Int64 }

func (c *counter) add(n int64) { c.v.Store(c.v.Load() + n) }
func (c *counter) load() int64 { return c.v.Load() }

// callMetrics counts the calls of one method. started and received are
// written by the connection's reader, sent and handled under the
// connection's lock.
type callMetrics struct {
	started, received, sent counter
	handled                 [codes.Unauthenticated + 1]counter
}

// connMetrics are the gRPC counters of one connection; the server keeps
// the sum of closed connections.
type connMetrics struct {
	// calls are indexed by method.idx.
	calls []callMetrics
	// received and sent are protobuf bytes of client messages, without
	// the gRPC prefix, as etcd's codec counts them. The reader writes
	// received; sent is written under the connection's lock. They sit on
	// separate cache lines.
	received counter
	_        [56]byte
	sent     counter
}

func newConnMetrics() *connMetrics {
	return &connMetrics{calls: make([]callMetrics, len(methods))}
}

// add adds o's counts to m. Nothing counts on o or m meanwhile, or the
// sum may miss a call in flight, as any scrape may.
func (m *connMetrics) add(o *connMetrics) {
	m.received.add(o.received.load())
	m.sent.add(o.sent.load())
	for i := range o.calls {
		m.calls[i].started.add(o.calls[i].started.load())
		m.calls[i].received.add(o.calls[i].received.load())
		m.calls[i].sent.add(o.calls[i].sent.load())
		for code := range o.calls[i].handled {
			m.calls[i].handled[code].add(o.calls[i].handled[code].load())
		}
	}
}

// total sums the counts of the closed connections and the open ones.
func (s *Server) total() *connMetrics {
	t := newConnMetrics()
	s.mu.Lock()
	defer s.mu.Unlock()
	t.add(s.gone)
	for c := range s.conns {
		if hc, ok := c.(*conn); ok {
			t.add(hc.metrics)
		}
	}
	return t
}

// serverMetrics are what the server observes into the member's metrics.
type serverMetrics struct {
	set *metrics.Set
	// handler serves /metrics.
	handler http.Handler
	// request holds the observers of etcd_server_request_duration_seconds
	// by method.idx, failed and succeeded, readonlyTxn those of a
	// read-only Txn; handling those of grpc_server_handling_seconds by
	// method.idx, with --metrics=extensive only.
	request     [][2]prometheus.Observer
	readonlyTxn [2]prometheus.Observer
	handling    []prometheus.Observer
	handlingVec *prometheus.HistogramVec
	// scrape lets one /metrics request at a time set the core's metrics
	// and read them back.
	scrape sync.Mutex
}

// lazyService reports whether etcd's gRPC metrics leave a service's
// series out until it is called: etcd registers the lock and election
// services after it initializes the metrics of the others.
func lazyService(service string) bool {
	return service == "v3lockpb.Lock" || service == "v3electionpb.Election"
}

func (s *Server) newServerMetrics() error {
	m, cs := buildServerMetrics(s.n.MetricSet(), s.cfg.ExtensiveMetrics)
	for _, c := range append(cs, &grpcCollector{s: s}) {
		if err := m.set.Registry.Register(c); err != nil {
			return err
		}
	}
	s.metrics = m
	return nil
}

// buildServerMetrics returns the server's metrics in set, and the
// collectors to register for them.
func buildServerMetrics(set *metrics.Set, extensive bool) (*serverMetrics, []prometheus.Collector) {
	m := &serverMetrics{set: set, request: make([][2]prometheus.Observer, len(methodPaths))}
	for _, p := range methodPaths {
		if t := methods[p].reqType; t != "" {
			m.request[methods[p].idx] = [2]prometheus.Observer{set.Request.WithLabelValues(t, "false"), set.Request.WithLabelValues(t, "true")}
		}
	}
	m.readonlyTxn = [2]prometheus.Observer{set.Request.WithLabelValues("ReadonlyTxn", "false"), set.Request.WithLabelValues("ReadonlyTxn", "true")}
	var cs []prometheus.Collector
	if extensive {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.",
			Buckets: prometheus.DefBuckets,
		}, []string{"grpc_type", "grpc_service", "grpc_method"})
		m.handling, m.handlingVec = make([]prometheus.Observer, len(methodPaths)), h
		for _, p := range methodPaths {
			if typ, service, name := callType(p); !lazyService(service) {
				m.handling[methods[p].idx] = h.WithLabelValues(typ, service, name)
			}
		}
		cs = append(cs, h)
	}
	m.handler = promhttp.InstrumentMetricHandler(set.Registry, promhttp.HandlerFor(set.Registry, promhttp.HandlerOpts{}))
	return m, cs
}

// countStart counts the start of a gRPC call, and notes its time if it
// is timed. c.mu is held.
func (st *stream) countStart() {
	st.calls.started.add(1)
	if st.rpc.reqType != "" || st.c.s.metrics.handling != nil {
		st.began = time.Now()
	}
}

// clientRequests is the counter of etcd_server_client_requests_total for
// unary calls or streams from a client with API version v. The
// connection keeps the last version's counters: a client sends the same
// one every time. c.mu is held.
func (c *conn) clientRequests(stream bool, v string) prometheus.Counter {
	if v == "" {
		v = "unknown"
	}
	if c.apiVersion != v || c.apiCounters[0] == nil {
		c.apiVersion = v
		set := c.s.metrics.set
		c.apiCounters = [2]prometheus.Counter{set.ClientRequests.WithLabelValues("unary", v), set.ClientRequests.WithLabelValues("stream", v)}
	}
	if stream {
		return c.apiCounters[1]
	}
	return c.apiCounters[0]
}

// validAPIVersion is etcd's check of the client-api-version metadata.
func validAPIVersion(v string) error {
	if !utf8.ValidString(v) {
		return rpctypes.ErrGRPCInvalidClientAPIVersion
	}
	return nil
}

// canceled is the status of a call that the client reset or whose
// connection closed.
var canceled = status.New(codes.Canceled, context.Canceled.Error())

// countEnd counts the end of a gRPC call with its status, nil for OK,
// once: a call ends when its status is queued, or, if the client reset
// it or the connection closed first, as canceled, which is what grpc-go's
// interceptor sees in etcd. c.mu is held.
func (st *stream) countEnd(s *status.Status) {
	if st.calls == nil || st.counted {
		return
	}
	st.counted = true
	st.endTrace(s)
	code := s.Code()
	if code <= codes.Unauthenticated {
		st.calls.handled[code].add(1)
	}
	if st.began.IsZero() {
		return
	}
	d := time.Since(st.began).Seconds()
	m := st.c.s.metrics
	ok := 0
	if code == codes.OK {
		ok = 1
	}
	switch {
	case st.readonlyTxn:
		m.readonlyTxn[ok].Observe(d)
	case st.rpc.reqType != "":
		m.request[st.rpc.idx][ok].Observe(d)
	}
	if m.handling != nil {
		if o := m.handling[st.rpc.idx]; o != nil {
			o.Observe(d)
		} else {
			m.handlingVec.WithLabelValues(st.rpc.labels[:]...).Observe(d)
		}
	}
}

// serveMetrics answers /metrics with the core's counts read just before.
// If the node does not answer, the core's metrics are left out.
func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	nm, err := s.n.Metrics(ctx)
	s.metrics.scrape.Lock()
	defer s.metrics.scrape.Unlock()
	if err == nil {
		s.metrics.set.SetNode(&nm)
	} else {
		s.metrics.set.SetNode(nil)
	}
	s.metrics.handler.ServeHTTP(w, r)
}

// grpcCollector reports grpc-prometheus' server counters, etcd's client
// byte counts, and tigerstore's own metrics.
type grpcCollector struct{ s *Server }

var (
	grpcStarted     = prometheus.NewDesc("grpc_server_started_total", "Total number of RPCs started on the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	grpcHandled     = prometheus.NewDesc("grpc_server_handled_total", "Total number of RPCs completed on the server, regardless of success or failure.", []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code"}, nil)
	grpcMsgReceived = prometheus.NewDesc("grpc_server_msg_received_total", "Total number of RPC stream messages received on the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	grpcMsgSent     = prometheus.NewDesc("grpc_server_msg_sent_total", "Total number of gRPC stream messages sent by the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	clientReceived  = prometheus.NewDesc("etcd_network_client_grpc_received_bytes_total", "The total number of bytes received from grpc clients.", nil, nil)
	clientSent      = prometheus.NewDesc("etcd_network_client_grpc_sent_bytes_total", "The total number of bytes sent to grpc clients.", nil, nil)
)

// tigerstoreGauges are tigerstore's own metrics from the core.
var tigerstoreGauges = []struct {
	desc  *prometheus.Desc
	typ   prometheus.ValueType
	value func(m *core.Metrics) int64
}{
	{prometheus.NewDesc("tigerstore_index_bytes", "Estimated memory of the key index.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.IndexBytes }},
	{prometheus.NewDesc("tigerstore_window_bytes", "Data bytes of the Raft entries in memory.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.WindowBytes }},
	{prometheus.NewDesc("tigerstore_map_bytes", "Estimated memory of the map of where entries and values are on disk.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.MapBytes }},
	{prometheus.NewDesc("tigerstore_disk_bytes", "Size of the log's files: segments, packs and checkpoints.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.DiskBytes }},
	{prometheus.NewDesc("tigerstore_disk_budget_bytes", "The disk budget of the log's files; 0 means none.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.DiskBudget }},
	{prometheus.NewDesc("tigerstore_cleaned_bytes_total", "Bytes that cleaning wrote into packs.", nil, nil), prometheus.CounterValue, func(m *core.Metrics) int64 { return m.CleanedBytes }},
	{prometheus.NewDesc("tigerstore_cache_bytes", "Bytes of values in the value cache.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.CacheBytes }},
	{prometheus.NewDesc("tigerstore_cache_limit_bytes", "Bytes the value cache may hold now: the memory that the index and the entries in memory leave.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return m.CacheLimit }},
	{prometheus.NewDesc("tigerstore_cache_hits_total", "Values read from the value cache.", nil, nil), prometheus.CounterValue, func(m *core.Metrics) int64 { return m.CacheHits }},
	{prometheus.NewDesc("tigerstore_cache_misses_total", "Values read from the log on disk.", nil, nil), prometheus.CounterValue, func(m *core.Metrics) int64 { return m.CacheMisses }},
	{prometheus.NewDesc("tigerstore_cluster_version_min", "The oldest cluster version this binary supports.", nil, nil), prometheus.GaugeValue, func(*core.Metrics) int64 { return 1 }},
	{prometheus.NewDesc("tigerstore_cluster_version_max", "The newest cluster version this binary supports.", nil, nil), prometheus.GaugeValue, func(*core.Metrics) int64 { return core.MaxVersion }},
	{prometheus.NewDesc("tigerstore_cluster_version", "The cluster version this member has applied, which sets the entry format.", nil, nil), prometheus.GaugeValue, func(m *core.Metrics) int64 { return int64(m.ClusterVersion) }},
}

var (
	peerMinVersion = prometheus.NewDesc("tigerstore_peer_protocol_min_version", "The oldest peer protocol version this binary speaks.", nil, nil)
	peerMaxVersion = prometheus.NewDesc("tigerstore_peer_protocol_max_version", "The newest peer protocol version this binary speaks.", nil, nil)
	peerVersion    = prometheus.NewDesc("tigerstore_peer_protocol_version", "The protocol version of the connection to (out) or from (in) a peer, 0 while there is none.", []string{"direction", "peer"}, nil)
)

func (c *grpcCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{grpcStarted, grpcHandled, grpcMsgReceived, grpcMsgSent, clientReceived, clientSent, peerMinVersion, peerMaxVersion, peerVersion} {
		ch <- d
	}
	for _, g := range tigerstoreGauges {
		ch <- g.desc
	}
}

// constMetric sends a metric, or an invalid metric if it cannot be made.
func constMetric(ch chan<- prometheus.Metric, d *prometheus.Desc, typ prometheus.ValueType, v float64, labels ...string) {
	m, err := prometheus.NewConstMetric(d, typ, v, labels...)
	if err != nil {
		m = prometheus.NewInvalidMetric(d, err)
	}
	ch <- m
}

func (c *grpcCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	sm := s.total()
	constMetric(ch, clientReceived, prometheus.CounterValue, float64(sm.received.load()))
	constMetric(ch, clientSent, prometheus.CounterValue, float64(sm.sent.load()))
	for _, p := range methodPaths {
		typ, service, name := callType(p)
		cm := &sm.calls[methods[p].idx]
		// Every code of every method has a series, at 0 too, as etcd's
		// InitializeMetrics makes them, so that rate() sees a series'
		// first call; those of lazy services appear as they are counted.
		all := !lazyService(service)
		count := func(d *prometheus.Desc, c *counter, labels ...string) {
			if v := c.load(); all || v > 0 {
				constMetric(ch, d, prometheus.CounterValue, float64(v), labels...)
			}
		}
		count(grpcStarted, &cm.started, typ, service, name)
		count(grpcMsgReceived, &cm.received, typ, service, name)
		count(grpcMsgSent, &cm.sent, typ, service, name)
		for code := range cm.handled {
			count(grpcHandled, &cm.handled[code], typ, service, name, codes.Code(code).String())
		}
	}
	if nm := s.metrics.set.Node(); nm != nil {
		for _, g := range tigerstoreGauges {
			constMetric(ch, g.desc, g.typ, float64(g.value(nm)))
		}
	}
	if s.cfg.PeerVersions == nil {
		return
	}
	v := s.cfg.PeerVersions()
	constMetric(ch, peerMinVersion, prometheus.GaugeValue, float64(v.Min))
	constMetric(ch, peerMaxVersion, prometheus.GaugeValue, float64(v.Max))
	for _, id := range slices.Sorted(maps.Keys(v.Out)) {
		peer := strconv.FormatUint(id, 16)
		constMetric(ch, peerVersion, prometheus.GaugeValue, float64(v.Out[id]), "out", peer)
		constMetric(ch, peerVersion, prometheus.GaugeValue, float64(v.In[id]), "in", peer)
	}
}

// callType is grpc-prometheus' type, service and method of the method
// at path.
func callType(path string) (typ, service, name string) {
	l := methods[path].labels
	return l[0], l[1], l[2]
}

// labels are grpc-prometheus' type, service and method of m, the method
// at path.
func labels(path string, m *method) [3]string {
	service, name, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	typ := "unary"
	switch {
	case m.serverStream:
		typ = "server_stream"
	case m.stream != nil:
		typ = "bidi_stream"
	}
	return [3]string{typ, service, name}
}

// serveVars answers /debug/vars as etcd's serveVars does: the expvar
// variables, which are cmdline and memstats in tigerstore, and etcd's
// file_descriptor_limit and raft.status, of this member, sorted by name.
func (s *Server) serveVars(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	// The command links etcd's server package, which publishes its own
	// file_descriptor_limit and raft.status; this member's replace them.
	vars := map[string]string{}
	expvar.Do(func(kv expvar.KeyValue) { vars[kv.Key] = kv.Value.String() })
	vars["file_descriptor_limit"], vars["raft.status"] = strconv.FormatUint(metrics.FDLimit(), 10), "null"
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	if nm, err := s.n.Metrics(ctx); err == nil {
		if b, err := json.Marshal(nm.Raft); err == nil {
			vars["raft.status"] = string(b)
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	var b strings.Builder
	b.WriteString("{\n")
	for i, k := range slices.Sorted(maps.Keys(vars)) {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, "%q: %s", k, vars[k])
	}
	b.WriteString("\n}\n")
	_, _ = w.Write([]byte(b.String()))
}
