package server_test

import (
	"context"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/djosh34/tigerstore/internal/server"
)

// TestTracing checks the spans of etcd's distributed tracing with its
// sampler at rate 0: a call whose traceparent is sampled gets otelgrpc's
// server span under the client's and etcd's span for the request under
// that; a call without one gets none.
func TestTracing(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
	c := newClusterWith(t, func(c *cluster) {
		c.serverConfig = func(cfg *server.Config) { cfg.TracerProvider = tp }
	})
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv, lease := pb.NewKVClient(conn), pb.NewLeaseClient(conn)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("untraced"), Value: []byte("1")})
	})
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("%d spans of calls without a sampled parent", n)
	}

	const traceID, parentID = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	ctx := metadata.AppendToOutgoingContext(context.Background(), "traceparent", "00-"+traceID+"-"+parentID+"-01")
	if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, &pb.PutRequest{}); err == nil {
		t.Fatal("a put without a key went through")
	}
	if _, err := kv.Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}}}); err != nil {
		t.Fatal(err)
	}
	g, err := lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 60})
	if err != nil {
		t.Fatal(err)
	}
	ka, err := lease.LeaseKeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ka.Send(&pb.LeaseKeepAliveRequest{ID: g.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := ka.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := ka.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := ka.Recv(); err == nil {
		t.Fatal("the keepalive stream went on after CloseSend")
	}

	type want struct {
		name, parent string
		kind         trace.SpanKind
		attrs        map[attribute.Key]attribute.Value
		status       codes.Code
	}
	server := func(method, code string) want {
		return want{name: method, parent: "client", kind: trace.SpanKindServer, attrs: map[attribute.Key]attribute.Value{
			"rpc.system.name": attribute.StringValue("grpc"), "rpc.method": attribute.StringValue(method), "rpc.response.status_code": attribute.StringValue(code),
		}}
	}
	inner := func(name, parent string, kv ...attribute.KeyValue) want {
		w := want{name: name, parent: parent, kind: trace.SpanKindInternal, attrs: map[attribute.Key]attribute.Value{}}
		for _, a := range kv {
			w.attrs[a.Key] = a.Value
		}
		return w
	}
	wants := []want{
		inner("put", "etcdserverpb.KV/Put", attribute.String("key", "a")),
		server("etcdserverpb.KV/Put", "OK"),
		// etcd's gRPC layer refuses the put without a key before its
		// server's span starts.
		server("etcdserverpb.KV/Put", "INVALID_ARGUMENT"),
		inner("txn", "etcdserverpb.KV/Txn", attribute.String("success_first_key", "a"), attribute.String("success_first_type", "range"),
			attribute.Int64("success_first_lease", -1), attribute.Int("success_len", 1), attribute.Bool("read_only", true)),
		server("etcdserverpb.KV/Txn", "OK"),
		inner("lease_grant", "etcdserverpb.Lease/LeaseGrant", attribute.Int64("id", g.ID), attribute.Int64("ttl", 60)),
		server("etcdserverpb.Lease/LeaseGrant", "OK"),
		inner("lease_renew", "etcdserverpb.Lease/LeaseKeepAlive", attribute.Int64("id", g.ID)),
		server("etcdserverpb.Lease/LeaseKeepAlive", "OK"),
	}
	spans := rec.Ended()
	if len(spans) != len(wants) {
		for _, s := range spans {
			t.Logf("span %s %v", s.Name(), s.Attributes())
		}
		t.Fatalf("%d spans, want %d", len(spans), len(wants))
	}
	for i, w := range wants {
		s := spans[i]
		if s.Name() != w.name || s.SpanKind() != w.kind || s.Status().Code != w.status {
			t.Fatalf("span %d: %s, kind %v, status %v; want %s, kind %v, status %v", i, s.Name(), s.SpanKind(), s.Status(), w.name, w.kind, w.status)
		}
		if got := s.SpanContext().TraceID().String(); got != traceID {
			t.Errorf("span %d %s: trace %s, want the client's %s", i, s.Name(), got, traceID)
		}
		parent := s.Parent().SpanID()
		if w.parent == "client" {
			if parent.String() != parentID || !s.Parent().IsRemote() {
				t.Errorf("span %d %s: parent %s, want the client's %s", i, s.Name(), parent, parentID)
			}
		} else if server := spans[i+1]; server.Name() != w.parent || parent != server.SpanContext().SpanID() {
			// An inner span ends just before its server span.
			t.Errorf("span %d %s: parent %s, want %s of %s", i, s.Name(), parent, server.SpanContext().SpanID(), w.parent)
		}
		attrs := map[attribute.Key]attribute.Value{}
		for _, a := range s.Attributes() {
			attrs[a.Key] = a.Value
		}
		for k, v := range w.attrs {
			if attrs[k] != v {
				t.Errorf("span %d %s: %s = %v, want %v", i, s.Name(), k, attrs[k].String(), v.String())
			}
		}
	}
}
