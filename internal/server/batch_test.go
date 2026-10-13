package server

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// TestPutResponseEncoding compares the hand-written PutResponse encoding
// with protobuf's.
func TestPutResponseEncoding(t *testing.T) {
	prev := &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("old"), CreateRevision: 2, ModRevision: 5, Version: 3, Lease: 7}
	for _, c := range []struct {
		cluster, member, term uint64
		rev                   int64
		prev                  *mvccpb.KeyValue
	}{
		{0x7469676572, 1, 2, 10, nil},
		{1<<64 - 1, 1<<63 + 5, 1 << 40, 1<<63 - 1, prev},
		{0, 0, 0, 0, nil},
		{3, 2, 0, 1, prev},
	} {
		got, err := appendPutResponse(nil, c.cluster, c.member, c.rev, c.term, c.prev)
		if err != nil {
			t.Fatal(err)
		}
		if c.prev == nil && len(got) > putResponseSize {
			t.Fatalf("%d bytes, above putResponseSize", len(got))
		}
		want, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb.PutResponse{
			Header: &pb.ResponseHeader{ClusterId: c.cluster, MemberId: c.member, Revision: c.rev, RaftTerm: c.term},
			PrevKv: c.prev,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%+v:\n got %x\nwant %x", c, got, want)
		}
	}
}

// TestHeaderDecodeAllocs checks that a gRPC request's header block, once
// its fields are in the HPACK dynamic table, decodes without allocating.
func TestHeaderDecodeAllocs(t *testing.T) {
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"},
		{Name: ":authority", Value: "127.0.0.1:2379"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "user-agent", Value: "grpc-go/1.84.0"},
		{Name: "te", Value: "trailers"},
	}
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	encode := func() []byte {
		block.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		return bytes.Clone(block.Bytes())
	}
	first := encode()
	// The same fields again are all indexed.
	again := encode()
	c := &conn{}
	c.hdec = hpack.NewDecoder(4096, c.onField)
	decode := func(b []byte) {
		c.hdr = reqHeaders{}
		if _, err := c.hdec.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := c.hdec.Close(); err != nil {
			t.Fatal(err)
		}
	}
	decode(first)
	if c.hdr.bad || c.hdr.path != "/etcdserverpb.KV/Put" || c.hdr.contentType != "application/grpc" || c.hdr.method != "POST" {
		t.Fatalf("decoded %+v", c.hdr)
	}
	if n := testing.AllocsPerRun(100, func() { decode(again) }); n != 0 {
		t.Fatalf("%v allocations per header block", n)
	}
}
