package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type kvTest struct{ pb.UnimplementedKVServer }

func (kvTest) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	grpc.SetHeader(ctx, metadata.Pairs("test-header", "yes"))
	grpc.SetTrailer(ctx, metadata.Pairs("test-trailer", "yes"))
	return &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 41}, Count: r.Limit, More: true}, nil
}
func TestProxyUnaryAndRedaction(t *testing.T) {
	backend := grpc.NewServer()
	pb.RegisterKVServer(backend, kvTest{})
	b, _ := net.Listen("tcp", "127.0.0.1:0")
	go backend.Serve(b)
	defer backend.Stop()
	cc, err := grpc.NewClient(b.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	var buf bytes.Buffer
	r := &recorder{enc: json.NewEncoder(&buf)}
	front := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(r.proxy(cc)), grpc.StatsHandler(r))
	f, _ := net.Listen("tcp", "127.0.0.1:0")
	go front.Serve(f)
	defer front.Stop()
	c, err := grpc.NewClient(f.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var head, tail metadata.MD
	res, err := pb.NewKVClient(c).Range(context.Background(), &pb.RangeRequest{Key: []byte("/registry/pods/"), RangeEnd: []byte("/registry/pods0"), Limit: 1}, grpc.Header(&head), grpc.Trailer(&tail))
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 1 || res.Header.Revision != 41 || head.Get("test-header")[0] != "yes" || tail.Get("test-trailer")[0] != "yes" {
		t.Fatalf("forwarding changed: %v %v %v", res, head, tail)
	}
	r.mu.Lock()
	logged := buf.String()
	r.mu.Unlock()
	if strings.Contains(logged, `"conn":null`) || !strings.Contains(logged, `"conn":1`) {
		t.Fatalf("connection context lost: %s", logged)
	}
	if _, err = pb.NewKVClient(c).Compact(context.Background(), &pb.CompactionRequest{Revision: 1}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("status changed: %v", err)
	}
	p := &pb.PutRequest{Key: []byte("/registry/secrets/test"), Value: []byte("SECRET_NEVER_LOG"), Lease: 123}
	wire, _ := proto.Marshal(p)
	m := decode("/etcdserverpb.KV/Put", wire, false)
	j, _ := json.Marshal(m)
	if strings.Contains(string(j), "SECRET_NEVER_LOG") || m["value_bytes"] != 16 {
		t.Fatalf("bad redaction %s", j)
	}
}

type watchTest struct{ pb.UnimplementedWatchServer }

func (watchTest) Watch(s grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchResponse]) error {
	for {
		r, err := s.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if r.GetCreateRequest() != nil {
			err = s.Send(&pb.WatchResponse{WatchId: 9, Created: true, Header: &pb.ResponseHeader{Revision: 42}})
		} else {
			err = s.Send(&pb.WatchResponse{WatchId: 9, Header: &pb.ResponseHeader{Revision: 43}})
		}
		if err != nil {
			return err
		}
	}
}
func TestWatchStreamingAndEventLengths(t *testing.T) {
	backend := grpc.NewServer()
	pb.RegisterWatchServer(backend, watchTest{})
	b, _ := net.Listen("tcp", "127.0.0.1:0")
	go backend.Serve(b)
	defer backend.Stop()
	cc, _ := grpc.NewClient(b.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cc.Close()
	r := &recorder{enc: json.NewEncoder(io.Discard)}
	front := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(r.proxy(cc)))
	f, _ := net.Listen("tcp", "127.0.0.1:0")
	go front.Serve(f)
	defer front.Stop()
	c, _ := grpc.NewClient(f.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer c.Close()
	s, err := pb.NewWatchClient(c).Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("/registry/pods/"), ProgressNotify: true, PrevKv: true}}}
	if err = s.Send(req); err != nil {
		t.Fatal(err)
	}
	resp, err := s.Recv()
	if err != nil || !resp.Created || resp.WatchId != 9 {
		t.Fatalf("create: %v %v", resp, err)
	}
	if err = s.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
		t.Fatal(err)
	}
	resp, err = s.Recv()
	if err != nil || resp.Header.Revision != 43 {
		t.Fatalf("progress: %v %v", resp, err)
	}
	s.CloseSend()
	if _, err = s.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	wire, _ := proto.Marshal(&pb.WatchResponse{Events: []*mvccpb.Event{{Kv: &mvccpb.KeyValue{Key: []byte("/registry/pods/ns/p"), Value: []byte("SECRET_NEVER_LOG")}}}})
	j, _ := json.Marshal(decode("/etcdserverpb.Watch/Watch", wire, true))
	if strings.Contains(string(j), "SECRET_NEVER_LOG") || !strings.Contains(string(j), "value_bytes") {
		t.Fatalf("event redaction %s", j)
	}
}

func TestRangeStreamPartialChunksAreSummarized(t *testing.T) {
	wire, _ := proto.Marshal(&pb.RangeStreamResponse{RangeResponse: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("/registry/pods/ns/p"), Value: []byte("NEVER_STORE")}}}})
	d := decode("/etcdserverpb.KV/RangeStream", wire, true)
	rr := d["range_response"].(map[string]any)
	if rr["kvs_count"] != 1 || rr["header"] != nil {
		t.Fatalf("partial chunk: %v", d)
	}
	j, _ := json.Marshal(d)
	if strings.Contains(string(j), "NEVER_STORE") {
		t.Fatal("value leaked")
	}
}

// Keep the stats import tied to the expected context propagation API.
var _ stats.Handler = (*recorder)(nil)
