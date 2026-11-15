package wl

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// The node's metrics in Prometheus' text format, as main's node names
// them: the HTTP/1.1 metrics endpoint reads them off the loop.

// nodeMetrics are what the metrics endpoint, served off the loop, reads.
type nodeMetrics struct {
	isLeader  atomic.Int64
	hasLeader atomic.Int64
	peerSent  atomic.Uint64
	// handled counts finished calls by kind and gRPC status code.
	handled [kinds][17]atomic.Uint64
}

// rpc names a call kind as gRPC's metrics do.
type rpc struct{ service, method, typ string }

var rpcs = func() [kinds]rpc {
	var r [kinds]rpc
	for path, k := range paths {
		svc, method, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
		typ := "unary"
		switch k {
		case kWatch, kLeaseKeepAlive:
			typ = "bidi_stream"
		case kRangeStream, kSnapshot:
			typ = "server_stream"
		}
		r[k] = rpc{svc, method, typ}
	}
	for i, m := range sideMethods {
		svc, method, _ := strings.Cut(strings.TrimPrefix(m.path, "/"), "/")
		typ := "unary"
		if m.stream != nil {
			typ = "server_stream"
		}
		r[kSide+uint8(i)] = rpc{svc, method, typ}
	}
	return r
}()

var codeNames = [17]string{
	"OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied",
	"ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated",
}

// handled counts a finished call of kind with code.
func (m *nodeMetrics) count(kind uint8, code int) {
	if int(kind) < len(m.handled) && code >= 0 && code < len(codeNames) {
		m.handled[kind][code].Add(1)
	}
}

// text writes the metrics.
func (m *nodeMetrics) text() []byte {
	b := fmt.Appendf(nil, "# TYPE etcd_server_has_leader gauge\netcd_server_has_leader %d\n", m.hasLeader.Load())
	b = fmt.Appendf(b, "# TYPE etcd_server_is_leader gauge\netcd_server_is_leader %d\n", m.isLeader.Load())
	b = fmt.Appendf(b, "# TYPE etcd_network_peer_sent_bytes_total counter\netcd_network_peer_sent_bytes_total{To=\"all\"} %d\n", m.peerSent.Load())
	b = append(b, "# TYPE grpc_server_handled_total counter\n"...)
	for k := range m.handled {
		r := rpcs[k]
		if r.method == "" {
			continue
		}
		for code := range m.handled[k] {
			if v := m.handled[k][code].Load(); v > 0 {
				b = fmt.Appendf(b, "grpc_server_handled_total{grpc_code=%q,grpc_method=%q,grpc_service=%q,grpc_type=%q} %d\n",
					codeNames[code], r.method, r.service, r.typ, v)
			}
		}
	}
	return b
}
