package server

import (
	"context"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
)

// Distributed tracing follows etcd v3.7.2 with --enable-distributed-tracing.
// Every gRPC call gets the server span of otelgrpc's stats handler, which
// etcd installs in grpc-go: the server feeds the same handler the events
// grpc-go would, so the span's name, attributes, status and parent, from
// the call's traceparent, tracestate and baggage metadata, are etcd's.
// Inside it come the spans etcd's server starts, named and with the
// attributes etcd gives them: range, put, delete_range, txn, compact,
// lease_grant and lease_revoke for the unary calls the node runs,
// range_streaming for KV.RangeStream and lease_renew for each keepalive.
// etcd's watch span is never ended, so never exported, and tigerstore has
// none.

// tracing holds the server span handler and the tracer of the inner
// spans; a Server without tracing has none.
type tracing struct {
	handler stats.Handler
	tracer  trace.Tracer
}

// newTracing makes the tracing of a TracerProvider, nil without one.
func newTracing(tp trace.TracerProvider) *tracing {
	if tp == nil {
		return nil
	}
	return &tracing{
		handler: otelgrpc.NewServerHandler(
			otelgrpc.WithPropagators(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})),
			otelgrpc.WithTracerProvider(tp),
		),
		// etcd's traceutil.Tracer.
		tracer: tp.Tracer("go.etcd.io/etcd"),
	}
}

// traceKey reports whether a metadata key is one the propagators read.
func traceKey(name string) bool {
	return name == "traceparent" || name == "tracestate" || name == "baggage"
}

// beginTrace starts the server span of a gRPC call. c.mu is held.
func (st *stream) beginTrace(h *reqHeaders) {
	t := st.c.s.tracing
	if t == nil {
		return
	}
	ctx := t.handler.TagRPC(metadata.NewIncomingContext(context.Background(), h.traceMD), &stats.RPCTagInfo{FullMethodName: h.path})
	t.handler.HandleRPC(ctx, &stats.InHeader{FullMethod: h.path, RemoteAddr: st.c.nc.RemoteAddr(), LocalAddr: st.c.nc.LocalAddr()})
	st.trace, st.traceBegan = ctx, time.Now()
}

// checkErrors are the answers of etcd's gRPC layer to requests it
// refuses before its server sees them, so before the server's span
// starts. A unary call that fails with one leaves its inner span
// unended, so never exported.
var checkErrors = map[string]bool{}

func init() {
	for _, err := range []error{
		rpctypes.ErrGRPCEmptyKey, rpctypes.ErrGRPCInvalidSortOption, rpctypes.ErrGRPCValueProvided, rpctypes.ErrGRPCLeaseProvided,
		rpctypes.ErrGRPCTooManyOps, rpctypes.ErrGRPCDuplicateKey, rpctypes.ErrGRPCKeyNotFound,
	} {
		checkErrors[status.Convert(err).Message()] = true
	}
}

// endTrace ends the inner span of a unary call, if it has one, and the
// server span with the call's status (nil for OK). c.mu is held.
func (st *stream) endTrace(s *status.Status) {
	if st.trace == nil {
		return
	}
	if st.span != nil && !checkErrors[s.Message()] {
		st.span.End()
	}
	st.c.s.tracing.handler.HandleRPC(st.trace, &stats.End{BeginTime: st.traceBegan, EndTime: time.Now(), Error: s.Err()})
}

// startSpan starts an inner span of a streaming call, which the handler
// ends; without tracing, it is a span that records nothing.
func (st *stream) startSpan(name string, attrs ...attribute.KeyValue) trace.Span {
	if st.trace == nil {
		return trace.SpanFromContext(context.Background())
	}
	_, span := st.c.s.tracing.tracer.Start(st.trace, name, trace.WithAttributes(attrs...))
	return span
}

// startOpSpan starts the inner span of a unary call the node runs, which
// endTrace ends.
func (c *conn) startOpSpan(st *stream, req *core.Request) {
	if st.trace == nil {
		return
	}
	name, attrs := opSpan(req)
	if name == "" {
		return
	}
	_, span := c.s.tracing.tracer.Start(st.trace, name, trace.WithAttributes(attrs...))
	c.mu.Lock()
	st.span = span
	c.mu.Unlock()
}

// opSpan is the name and attributes of etcd's span for a request, none
// for the requests etcd starts no span for.
func opSpan(req *core.Request) (string, []attribute.KeyValue) {
	switch r := req.Op.(type) {
	case nil:
		return "put", []attribute.KeyValue{attribute.String("key", string(req.Put.Key))}
	case *pb.RangeRequest:
		return "range", []attribute.KeyValue{
			attribute.String("range_begin", string(r.GetKey())),
			attribute.String("range_end", string(r.GetRangeEnd())),
			attribute.Int64("rev", r.GetRevision()),
			attribute.Int64("limit", r.GetLimit()),
			attribute.Bool("count_only", r.GetCountOnly()),
			attribute.Bool("keys_only", r.GetKeysOnly()),
		}
	case *pb.PutRequest:
		return "put", []attribute.KeyValue{attribute.String("key", string(r.GetKey()))}
	case *pb.DeleteRangeRequest:
		return "delete_range", []attribute.KeyValue{
			attribute.String("range_begin", string(r.GetKey())),
			attribute.String("range_end", string(r.GetRangeEnd())),
		}
	case *pb.TxnRequest:
		return "txn", []attribute.KeyValue{
			attribute.String("compare_first_key", firstCompareKey(r.GetCompare())),
			attribute.String("success_first_key", firstOpKey(r.GetSuccess())),
			attribute.String("success_first_type", firstOpType(r.GetSuccess())),
			attribute.Int64("success_first_lease", firstOpLease(r.GetSuccess())),
			attribute.Int("compare_len", len(r.GetCompare())),
			attribute.Int("success_len", len(r.GetSuccess())),
			attribute.Int("failure_len", len(r.GetFailure())),
			attribute.Bool("read_only", readonlyTxn(r)),
		}
	case *pb.CompactionRequest:
		return "compact", []attribute.KeyValue{
			attribute.Bool("is_physical", r.GetPhysical()),
			attribute.Int64("rev", r.GetRevision()),
		}
	case *pb.LeaseGrantRequest:
		// A lease without an ID gets its ID set once the node granted
		// it (complete); etcd picks it before the span starts.
		return "lease_grant", []attribute.KeyValue{
			attribute.Int64("id", r.GetID()),
			attribute.Int64("ttl", r.GetTTL()),
		}
	case *pb.LeaseRevokeRequest:
		return "lease_revoke", []attribute.KeyValue{attribute.Int64("id", r.GetID())}
	}
	return "", nil
}

// firstCompareKey is etcd's: the first non-empty key of the compares.
func firstCompareKey(c []*pb.Compare) string {
	for _, op := range c {
		if key := string(op.GetKey()); key != "" {
			return key
		}
	}
	return ""
}

// firstOpKey is etcd's: the first non-empty key of the operations.
func firstOpKey(ops []*pb.RequestOp) string {
	for _, operation := range ops {
		var key string
		switch op := operation.GetRequest().(type) {
		case *pb.RequestOp_RequestPut:
			key = string(op.RequestPut.GetKey())
		case *pb.RequestOp_RequestRange:
			key = string(op.RequestRange.GetKey())
		case *pb.RequestOp_RequestDeleteRange:
			key = string(op.RequestDeleteRange.GetKey())
		}
		if key != "" {
			return key
		}
	}
	return ""
}

// firstOpType is etcd's: the type of the first operation of a known
// type.
func firstOpType(ops []*pb.RequestOp) string {
	for _, operation := range ops {
		switch operation.GetRequest().(type) {
		case *pb.RequestOp_RequestPut:
			return "put"
		case *pb.RequestOp_RequestRange:
			return "range"
		case *pb.RequestOp_RequestDeleteRange:
			return "delete_range"
		case *pb.RequestOp_RequestTxn:
			return "txn"
		}
	}
	return ""
}

// firstOpLease is etcd's: the lease of the first put, -1 without one.
func firstOpLease(ops []*pb.RequestOp) int64 {
	for _, operation := range ops {
		if op, ok := operation.GetRequest().(*pb.RequestOp_RequestPut); ok {
			return op.RequestPut.GetLease()
		}
	}
	return -1
}
