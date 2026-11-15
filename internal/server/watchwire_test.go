package server

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// TestWatchHeadIsMarshal checks that the fields the server writes before
// a watch response's events are those protobuf's Marshal writes.
func TestWatchHeadIsMarshal(t *testing.T) {
	for i, r := range []*pb.WatchResponse{
		{},
		{Header: &pb.ResponseHeader{}},
		{Header: &pb.ResponseHeader{ClusterId: 1 << 60, MemberId: 3, Revision: 99, RaftTerm: 4}, WatchId: 5},
		{Header: &pb.ResponseHeader{Revision: 7}, WatchId: -1, Created: true},
		{WatchId: 2, Canceled: true, CompactRevision: 12, CancelReason: "mvcc: required revision has been compacted"},
		{WatchId: 2, Fragment: true},
	} {
		want, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := appendWatchHead(nil, r); !bytes.Equal(got, want) {
			t.Fatalf("response %d:\n got %x\nwant %x", i, got, want)
		}
	}
}
