package server

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
