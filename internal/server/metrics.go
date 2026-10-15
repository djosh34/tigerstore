package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
)

// The /metrics endpoint answers in Prometheus' text format with the etcd
// metrics that scrapers of etcd v3.7.2 read most: gRPC calls by method and
// code, client message bytes, proposals, leader changes and mvcc
// operations. It is not every etcd metric.

// startTime is when the process started, for process_start_time_seconds.
var startTime = time.Now()

// counter is a count with one writer at a time, read by scrapes: the
// writer adds with a plain load and store, cheaper than an atomic add on
// every call, and a scrape still reads a whole value.
type counter struct{ v atomic.Int64 }

func (c *counter) add(n int64) { c.v.Store(c.v.Load() + n) }
func (c *counter) load() int64 { return c.v.Load() }

// callMetrics counts the calls of one method. started is written by the
// connection's reader, handled under the connection's lock.
type callMetrics struct {
	started counter
	handled [codes.Unauthenticated + 1]counter
}

// metrics are the gRPC counters of one connection, so that connections
// never write the same counter; a scrape adds them up. The server keeps
// the sum of closed connections.
type metrics struct {
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

func newMetrics() *metrics {
	return &metrics{calls: make([]callMetrics, len(methods))}
}

// add adds o's counts to m. Nothing counts on o or m meanwhile, or the
// sum may miss a call in flight, as any scrape may.
func (m *metrics) add(o *metrics) {
	m.received.add(o.received.load())
	m.sent.add(o.sent.load())
	for i := range o.calls {
		m.calls[i].started.add(o.calls[i].started.load())
		for code := range o.calls[i].handled {
			m.calls[i].handled[code].add(o.calls[i].handled[code].load())
		}
	}
}

// total sums the counts of the closed connections and the open ones.
func (s *Server) total() *metrics {
	t := newMetrics()
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

// countEnd counts the end of a gRPC call with its code, once: a call ends
// when its status is queued, or, if the client reset it or the
// connection closed first, as Canceled, which is what grpc-go's
// interceptor sees in etcd. c.mu is held.
func (st *stream) countEnd(code codes.Code) {
	if st.calls == nil || st.counted {
		return
	}
	st.counted = true
	if code <= codes.Unauthenticated {
		st.calls.handled[code].add(1)
	}
}

// metricsText answers /metrics.
func (s *Server) metricsText(ctx context.Context, _ url.Values) httpAnswer {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	nm, err := s.n.Metrics(ctx)
	if err != nil {
		return httpError(http.StatusServiceUnavailable, err.Error())
	}
	sm := s.total()
	var b strings.Builder
	metric := func(name, typ, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	metric("process_start_time_seconds", "gauge", "Start time of the process since unix epoch in seconds.")
	fmt.Fprintf(&b, "process_start_time_seconds %g\n", float64(startTime.UnixNano())/1e9)
	hasLeader := 0
	if nm.HasLeader {
		hasLeader = 1
	}
	for _, g := range []struct {
		name, typ, help string
		v               int64
	}{
		{"etcd_server_has_leader", "gauge", "Whether or not a leader exists. 1 is existence, 0 is not.", int64(hasLeader)},
		{"etcd_server_leader_changes_seen_total", "counter", "The number of leader changes seen.", nm.LeaderChanges},
		{"etcd_server_proposals_committed_total", "gauge", "The total number of consensus proposals committed.", int64(nm.Committed)},
		{"etcd_server_proposals_applied_total", "gauge", "The total number of consensus proposals applied.", int64(nm.Applied)},
		{"etcd_mvcc_put_total", "counter", "Total number of puts seen by this member.", nm.Puts},
		{"etcd_mvcc_delete_total", "counter", "Total number of deletes seen by this member.", nm.Deletes},
		{"etcd_mvcc_range_total", "counter", "Total number of ranges seen by this member.", nm.Ranges},
		{"etcd_network_client_grpc_received_bytes_total", "counter", "The total number of bytes received from grpc clients.", sm.received.load()},
		{"etcd_network_client_grpc_sent_bytes_total", "counter", "The total number of bytes sent to grpc clients.", sm.sent.load()},
	} {
		metric(g.name, g.typ, g.help)
		fmt.Fprintf(&b, "%s %d\n", g.name, g.v)
	}
	metric("grpc_server_started_total", "counter", "Total number of RPCs started on the server.")
	for _, p := range methodPaths {
		fmt.Fprintf(&b, "grpc_server_started_total{%s} %d\n", callLabels(p), sm.calls[methods[p].idx].started.load())
	}
	metric("grpc_server_handled_total", "counter", "Total number of RPCs completed on the server, regardless of success or failure.")
	for _, p := range methodPaths {
		// Every code of every method, at 0 too, as etcd's
		// InitializeMetrics does, so that rate() sees a series' first call.
		for code := range sm.calls[methods[p].idx].handled {
			n := sm.calls[methods[p].idx].handled[code].load()
			fmt.Fprintf(&b, "grpc_server_handled_total{grpc_code=%q,%s} %d\n", codes.Code(code).String(), callLabels(p), n)
		}
	}
	return httpAnswer{code: http.StatusOK, contentType: "text/plain; version=0.0.4; charset=utf-8", body: b.String()}
}

// callLabels are grpc-prometheus' labels for the method at path.
func callLabels(path string) string {
	service, name, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	typ := "unary"
	switch m := methods[path]; {
	case m.serverStream:
		typ = "server_stream"
	case m.stream != nil:
		typ = "bidi_stream"
	}
	return fmt.Sprintf("grpc_method=%q,grpc_service=%q,grpc_type=%q", name, service, typ)
}
