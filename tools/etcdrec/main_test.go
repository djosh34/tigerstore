package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
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
	p := &pb.PutRequest{Key: []byte("/registry/secrets/test"), Value: []byte("SECRET_NEVER_LOG"), Lease: 123}
	wire, _ := proto.Marshal(p)
	m := decode("/etcdserverpb.KV/Put", wire, false)
	j, _ := json.Marshal(m)
	if strings.Contains(string(j), "SECRET_NEVER_LOG") || m["value_bytes"] != 16 {
		t.Fatalf("bad redaction %s", j)
	}
}

// Keep the stats import tied to the expected context propagation API.
var _ stats.Handler = (*recorder)(nil)
