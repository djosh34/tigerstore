package wl

import (
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
)

// The node's Prometheus metrics, under etcd v3.7.2's names, help texts
// and buckets: internal/metrics' set of etcd's families, fed at each
// scrape from what the loop counted. The loop counts in plain fields of
// its own, with no atomic and no clock read per request (a request's
// time is the turns it took); a scrape copies them out through a post.

// loopStats is what the loop counts for the metrics.
type loopStats struct {
	calls                [kinds]callCounts
	recvBytes, sentBytes int64
	// unary and streams count the calls by type, etcd's
	// etcd_server_client_requests_total.
	unary, streams int64
	// request times calls by etcd's request type and success, handling
	// every call by kind (grpc_server_handling_seconds, with --metrics
	// extensive), apply the applied ops, ranges the served ranges.
	request  [len(requestTypes)][2]hist
	handling [kinds]hist
	apply    [len(applyOps)][2]hist
	ranges   [2]hist
	// mvcc's counters, as etcd's store counts them.
	core.Counters
	puts, deletes, rangesN, txns, putBytes int64
	leaderChanges                          int64
}

// callCounts are grpc-prometheus' counters of one method.
type callCounts struct {
	started, received, sent int64
	handled                 [17]int64
}

// hist is a histogram the loop observes into: counts per bucket, the
// last over every bound, and the sum.
type hist struct {
	counts []uint64
	sum    float64
}

func (h *hist) observe(bounds []float64, v float64) {
	if h.counts == nil {
		h.counts = make([]uint64, len(bounds)+1)
	}
	i := 0
	for i < len(bounds) && v > bounds[i] {
		i++
	}
	h.counts[i]++
	h.sum += v
}

func (h *hist) clone() hist { return hist{counts: slices.Clone(h.counts), sum: h.sum} }

// The buckets of etcd's histograms.
var (
	requestBuckets  = prometheus.ExponentialBuckets(0.001, 2, 14)
	applyBuckets    = prometheus.ExponentialBuckets(0.0001, 2, 20)
	handlingBuckets = prometheus.DefBuckets
)

// requestTypes are etcd's request types of etcd_server_request_duration_seconds.
var requestTypes = [...]string{"Range", "Put", "DeleteRange", "Txn", "ReadonlyTxn", "Compaction", "LeaseGrant", "LeaseRevoke", "Alarm"}

// requestType is the index in requestTypes of a call of kind, -1 for none.
func requestType(kind uint8, readonly bool) int {
	switch kind {
	case kRange:
		return 0
	case kPut:
		return 1
	case kDel:
		return 2
	case kTxn:
		if readonly {
			return 4
		}
		return 3
	case kCompact:
		return 5
	case kLeaseGrant:
		return 6
	case kLeaseRevoke:
		return 7
	case kAlarm:
		return 8
	}
	return -1
}

// applyOps are the ops of etcd_server_apply_duration_seconds.
var applyOps = [...]string{"Put", "DeleteRange", "Txn", "Compaction", "LeaseGrant", "LeaseRevoke", "Alarm"}

func applyOp(kind uint8) int {
	switch kind {
	case kPut:
		return 0
	case kDel:
		return 1
	case kTxn:
		return 2
	case kCompact:
		return 3
	case kLeaseGrant:
		return 4
	case kLeaseRevoke:
		return 5
	case kAlarm:
		return 6
	}
	return -1
}

// callStarted counts a call that opened.
func (n *node) callStarted(s *stream) {
	n.gs.calls[s.kind].started++
	if streamKind(s.kind) {
		n.gs.streams++
	} else {
		n.gs.unary++
	}
}

// callEnded counts a call's end with its gRPC code, and its time from
// its request's turn to this one.
func (n *node) callEnded(s *stream, code int) {
	if code < 0 || code >= len(n.gs.calls[s.kind].handled) {
		return
	}
	n.gs.calls[s.kind].handled[code]++
	if s.began.IsZero() {
		return
	}
	d := n.now.Sub(s.began).Seconds()
	ok := b2i(code == 0)
	if t := requestType(s.kind, s.readonly); t >= 0 {
		n.gs.request[t][ok].observe(requestBuckets, d)
	}
	if n.extensive {
		n.gs.handling[s.kind].observe(handlingBuckets, d)
	}
}

// streamKind reports whether calls of kind stream.
func streamKind(kind uint8) bool {
	switch kind {
	case kWatch, kLeaseKeepAlive, kRangeStream, kSnapshot:
		return true
	}
	return sideKind(kind) && sideMethods[kind-kSide].stream != nil
}

// membershipSnapshotEntries is etcd's memorySnapshotCount: it snapshots
// the membership into a fresh v2 store once more entries than this
// applied since the last time.
const membershipSnapshotEntries = 100

// membershipSnapshot counts the v2 store reads and writes of etcd's
// GetMembershipInfoInV2Format: a recursive get of the members, a create
// of each member's Raft attributes and of each removed ID, a set of the
// other attributes of each member that published them, and a set of the
// cluster version.
func (n *node) membershipSnapshot() {
	n.gs.StoreGets++
	n.gs.StoreCreates += int64(len(n.members) + len(n.removed))
	for i := range n.members {
		if len(n.members[i].clientURLs) > 0 {
			n.gs.StoreSets++
		}
	}
	n.gs.StoreSets++
}

// alarmGauge adds d to etcd_debugging_server_alarms of an alarm: each
// activation applied adds one, even of a raised alarm, and each
// deactivation of a raised one takes one away.
func (n *node) alarmGauge(a alarm, d int64) {
	for i := range n.gs.AlarmGauges {
		if g := &n.gs.AlarmGauges[i]; g.MemberID == a.member && uint64(g.Alarm) == a.typ {
			g.Value += d
			return
		}
	}
	n.gs.AlarmGauges = append(n.gs.AlarmGauges, core.AlarmGauge{MemberID: a.member, Alarm: etcdpb.AlarmType(a.typ), Value: d})
}

// promMethod is one method of etcd's gRPC services, as grpc-prometheus
// names it.
type promMethod struct {
	typ, service, name string
	kind               uint8 // kUnknown for one the node does not serve
	lazy               bool  // its series appear once counted
}

// promMethods are every method of the services etcd serves.
var promMethods = func() []promMethod {
	var out []promMethod
	descs := []*grpc.ServiceDesc{
		&etcdpb.KV_ServiceDesc, &etcdpb.Watch_ServiceDesc, &etcdpb.Lease_ServiceDesc, &etcdpb.Cluster_ServiceDesc,
		&etcdpb.Maintenance_ServiceDesc, &etcdpb.Auth_ServiceDesc,
	}
	for _, s := range sideServices {
		descs = append(descs, s.desc)
	}
	for _, d := range descs {
		lazy := d.ServiceName == "v3lockpb.Lock" || d.ServiceName == "v3electionpb.Election"
		for _, m := range d.Methods {
			out = append(out, promMethod{"unary", d.ServiceName, m.MethodName, kindOf("/" + d.ServiceName + "/" + m.MethodName), lazy})
		}
		for _, m := range d.Streams {
			typ := "server_stream"
			if m.ClientStreams {
				typ = "bidi_stream"
			}
			out = append(out, promMethod{typ, d.ServiceName, m.StreamName, kindOf("/" + d.ServiceName + "/" + m.StreamName), lazy})
		}
	}
	return out
}()

// kindOf is the kind of the method at path, its side method's too, which
// paths takes only at init.
func kindOf(path string) uint8 {
	for i, m := range sideMethods {
		if m.path == path {
			return kSide + uint8(i)
		}
	}
	return paths[path]
}

// promSnap is a scrape's copy of what the loop counted, and the node's
// state as main's metric set reads it.
type promSnap struct {
	stats loopStats
	node  core.Metrics
}

// scrape copies the loop's counts for a scrape.
func (n *node) scrape() *promSnap {
	p := &promSnap{stats: n.gs}
	g := &p.stats
	for i := range g.request {
		for j := range g.request[i] {
			g.request[i][j] = n.gs.request[i][j].clone()
		}
	}
	for i := range g.apply {
		for j := range g.apply[i] {
			g.apply[i][j] = n.gs.apply[i][j].clone()
		}
	}
	for i := range g.handling {
		g.handling[i] = n.gs.handling[i].clone()
	}
	g.ranges = [2]hist{n.gs.ranges[0].clone(), n.gs.ranges[1].clone()}
	m := &p.node
	m.HasLeader, m.IsLeader, m.IsLearner = n.lead != 0, n.leading(), n.isLearner()
	m.LeaderChanges = g.leaderChanges
	m.Committed, m.Applied = n.commit, n.applied
	m.Counters = g.Counters
	m.Puts, m.Deletes, m.Ranges, m.Txns, m.PutBytes = g.puts, g.deletes, g.rangesN, g.txns, g.putBytes
	m.Keys = int64(n.st.liveKeys())
	m.ID, m.EtcdVersion = n.id, "3.7.0"
	for i := range n.members {
		m.Members = append(m.Members, n.members[i].id)
	}
	m.Rev, m.CompactRev = n.st.rev, n.st.compactRev
	m.QuotaBytes = n.quotaBytes
	return p
}

// promCollector reports what the set's own collectors do not: gRPC's
// counters, the client bytes, and the histograms the loop keeps.
type promCollector struct {
	mu        sync.Mutex
	snap      *promSnap
	extensive bool
}

var (
	grpcStarted     = prometheus.NewDesc("grpc_server_started_total", "Total number of RPCs started on the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	grpcHandled     = prometheus.NewDesc("grpc_server_handled_total", "Total number of RPCs completed on the server, regardless of success or failure.", []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code"}, nil)
	grpcMsgReceived = prometheus.NewDesc("grpc_server_msg_received_total", "Total number of RPC stream messages received on the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	grpcMsgSent     = prometheus.NewDesc("grpc_server_msg_sent_total", "Total number of gRPC stream messages sent by the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	grpcHandling    = prometheus.NewDesc("grpc_server_handling_seconds", "Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.", []string{"grpc_type", "grpc_service", "grpc_method"}, nil)
	clientReceived  = prometheus.NewDesc("etcd_network_client_grpc_received_bytes_total", "The total number of bytes received from grpc clients.", nil, nil)
	clientSent      = prometheus.NewDesc("etcd_network_client_grpc_sent_bytes_total", "The total number of bytes sent to grpc clients.", nil, nil)
	clientRequests  = prometheus.NewDesc("etcd_server_client_requests_total", "The total number of client requests per client version.", []string{"type", "client_api_version"}, nil)
	requestDuration = prometheus.NewDesc("etcd_server_request_duration_seconds", "Response latency distribution in seconds for each type.", []string{"type", "success"}, nil)
	applyDuration   = prometheus.NewDesc("etcd_server_apply_duration_seconds", "The latency distributions of v2 apply called by backend.", []string{"version", "op", "success"}, nil)
	rangeDuration   = prometheus.NewDesc("etcd_server_range_duration_seconds", "The latency distributions of txn.Range", []string{"success"}, nil)
)

func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{grpcStarted, grpcHandled, grpcMsgReceived, grpcMsgSent, clientReceived, clientSent, clientRequests, requestDuration, applyDuration, rangeDuration} {
		ch <- d
	}
	if c.extensive {
		ch <- grpcHandling
	}
}

// constCounter sends a counter, or an invalid metric if it cannot be
// made.
func constCounter(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	m, err := prometheus.NewConstMetric(d, prometheus.CounterValue, v, labels...)
	if err != nil {
		m = prometheus.NewInvalidMetric(d, err)
	}
	ch <- m
}

// constHist sends h as a histogram with bounds, if it has samples.
func constHist(ch chan<- prometheus.Metric, d *prometheus.Desc, bounds []float64, h *hist, labels ...string) {
	if h.counts == nil {
		return
	}
	buckets := make(map[float64]uint64, len(bounds))
	var n uint64
	for i, b := range bounds {
		n += h.counts[i]
		buckets[b] = n
	}
	n += h.counts[len(bounds)]
	m, err := prometheus.NewConstHistogram(d, n, h.sum, buckets, labels...)
	if err != nil {
		m = prometheus.NewInvalidMetric(d, err)
	}
	ch <- m
}

var successLabel = [2]string{"false", "true"}

func (c *promCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	p := c.snap
	c.mu.Unlock()
	if p == nil {
		return
	}
	g := &p.stats
	constCounter(ch, clientReceived, float64(g.recvBytes))
	constCounter(ch, clientSent, float64(g.sentBytes))
	constCounter(ch, clientRequests, float64(g.unary), "unary", "unknown")
	constCounter(ch, clientRequests, float64(g.streams), "stream", "unknown")
	for _, m := range promMethods {
		var cc callCounts
		if m.kind != kUnknown {
			cc = g.calls[m.kind]
		}
		// Every method of a service has its series, at 0 too, as etcd's
		// InitializeMetrics makes them; Lock's and Election's appear once
		// counted.
		count := func(d *prometheus.Desc, v int64, labels ...string) {
			if !m.lazy || v > 0 {
				constCounter(ch, d, float64(v), labels...)
			}
		}
		count(grpcStarted, cc.started, m.typ, m.service, m.name)
		count(grpcMsgReceived, cc.received, m.typ, m.service, m.name)
		count(grpcMsgSent, cc.sent, m.typ, m.service, m.name)
		for code := range cc.handled {
			count(grpcHandled, cc.handled[code], m.typ, m.service, m.name, codes.Code(code).String())
		}
		if c.extensive && m.kind != kUnknown {
			constHist(ch, grpcHandling, handlingBuckets, &g.handling[m.kind], m.typ, m.service, m.name)
		}
	}
	for t := range g.request {
		for ok := range g.request[t] {
			constHist(ch, requestDuration, requestBuckets, &g.request[t][ok], requestTypes[t], successLabel[ok])
		}
	}
	for o := range g.apply {
		for ok := range g.apply[o] {
			constHist(ch, applyDuration, applyBuckets, &g.apply[o][ok], "v3", applyOps[o], successLabel[ok])
		}
	}
	for ok := range g.ranges {
		constHist(ch, rangeDuration, applyBuckets, &g.ranges[ok], successLabel[ok])
	}
}

// promHandler serves /metrics: it has the loop copy its counts, then
// gathers the set.
type promHandler struct {
	set     *metrics.Set
	col     *promCollector
	scrape  func() *promSnap
	handler http.Handler
	// peerSent counts the bytes sent to peers, which the set's
	// etcd_network_peer_sent_bytes_total takes, as one peer "all";
	// lastSent is what it took last.
	peerSent *atomic.Uint64
	mu       sync.Mutex
	lastSent uint64
}

func newPromHandler(set *metrics.Set, extensive bool, scrape func() *promSnap, peerSent *atomic.Uint64) (*promHandler, error) {
	col := &promCollector{extensive: extensive}
	// The set's own histograms of these, which the loop does not
	// observe into, give way to the collector's.
	set.Registry.Unregister(set.Request)
	set.Registry.Unregister(set.Apply)
	set.Registry.Unregister(set.Range)
	set.Registry.Unregister(set.ClientRequests)
	if err := set.Registry.Register(col); err != nil {
		return nil, err
	}
	h := &promHandler{set: set, col: col, scrape: scrape, peerSent: peerSent}
	h.handler = promhttp.InstrumentMetricHandler(set.Registry, promhttp.HandlerFor(set.Registry, promhttp.HandlerOpts{}))
	return h, nil
}

func (h *promHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	sent := h.peerSent.Load()
	h.set.PeerSent.WithLabelValues("all").Add(float64(sent - h.lastSent))
	h.lastSent = sent
	h.mu.Unlock()
	p := h.scrape()
	h.col.mu.Lock()
	h.col.snap = p
	h.col.mu.Unlock()
	if p != nil {
		h.set.SetNode(&p.node)
	} else {
		h.set.SetNode(nil)
	}
	h.handler.ServeHTTP(w, r)
}

// scrape has the loop copy its counts, or nil if it does not answer in
// time.
func (l *loop) scrape() *promSnap {
	got := make(chan *promSnap, 1)
	l.post(func() error {
		got <- l.n.scrape()
		return nil
	})
	select {
	case p := <-got:
		return p
	case <-time.After(healthTimeout):
		return nil
	}
}
