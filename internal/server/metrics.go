package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
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

// callMetrics counts the calls of one method.
type callMetrics struct {
	started atomic.Int64
	handled [codes.Unauthenticated + 1]atomic.Int64
}

// metrics are the server's gRPC counters.
type metrics struct {
	calls map[*method]*callMetrics
	// received and sent are protobuf bytes of client messages, without
	// the gRPC prefix, as etcd's codec counts them. They sit on separate
	// cache lines: readers write one, handlers the other.
	received atomic.Int64
	_        [56]byte
	sent     atomic.Int64
}

func newMetrics() *metrics {
	m := &metrics{calls: map[*method]*callMetrics{}}
	for _, rpc := range methods {
		m.calls[rpc] = &callMetrics{}
	}
	return m
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
		st.calls.handled[code].Add(1)
	}
}

// metricsText answers /metrics.
func (s *Server) metricsText(ctx context.Context, _ url.Values) (int, string, string) {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	nm, err := s.n.Metrics(ctx)
	if err != nil {
		return http.StatusServiceUnavailable, textPlain, err.Error() + "\n"
	}
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
		{"etcd_mvcc_db_total_size_in_bytes", "gauge", "Total size of the underlying database physically allocated in bytes.", nm.LogBytes},
		{"etcd_mvcc_put_total", "counter", "Total number of puts seen by this member.", nm.Puts},
		{"etcd_mvcc_delete_total", "counter", "Total number of deletes seen by this member.", nm.Deletes},
		{"etcd_mvcc_range_total", "counter", "Total number of ranges seen by this member.", nm.Ranges},
		{"etcd_network_client_grpc_received_bytes_total", "counter", "The total number of bytes received from grpc clients.", s.metrics.received.Load()},
		{"etcd_network_client_grpc_sent_bytes_total", "counter", "The total number of bytes sent to grpc clients.", s.metrics.sent.Load()},
	} {
		metric(g.name, g.typ, g.help)
		fmt.Fprintf(&b, "%s %d\n", g.name, g.v)
	}
	paths := make([]string, 0, len(methods))
	for p := range methods {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	metric("grpc_server_started_total", "counter", "Total number of RPCs started on the server.")
	for _, p := range paths {
		fmt.Fprintf(&b, "grpc_server_started_total{%s} %d\n", callLabels(p), s.metrics.calls[methods[p]].started.Load())
	}
	metric("grpc_server_handled_total", "counter", "Total number of RPCs completed on the server, regardless of success or failure.")
	for _, p := range paths {
		// Every code of every method, at 0 too, as etcd's
		// InitializeMetrics does, so that rate() sees a series' first call.
		for code := range s.metrics.calls[methods[p]].handled {
			n := s.metrics.calls[methods[p]].handled[code].Load()
			fmt.Fprintf(&b, "grpc_server_handled_total{grpc_code=%q,%s} %d\n", codes.Code(code).String(), callLabels(p), n)
		}
	}
	return http.StatusOK, "text/plain; version=0.0.4; charset=utf-8", b.String()
}

// callLabels are grpc-prometheus' labels for the method at path.
func callLabels(path string) string {
	service, name, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	typ := "unary"
	if methods[path].stream != nil {
		typ = "bidi_stream"
	}
	return fmt.Sprintf("grpc_method=%q,grpc_service=%q,grpc_type=%q", name, service, typ)
}
