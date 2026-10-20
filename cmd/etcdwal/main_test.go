package main

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestInternalRequest(t *testing.T) {
	for _, tc := range []struct {
		in   proto.Message
		want *pb.InternalRaftRequest
	}{
		{&pb.PutRequest{Key: []byte("k")}, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("k")}}},
		{&pb.DeleteRangeRequest{Key: []byte("k")}, &pb.InternalRaftRequest{DeleteRange: &pb.DeleteRangeRequest{Key: []byte("k")}}},
		{&pb.TxnRequest{}, &pb.InternalRaftRequest{Txn: &pb.TxnRequest{}}},
		{&pb.CompactionRequest{Revision: 3}, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 3}}},
		{&pb.LeaseGrantRequest{ID: 7}, &pb.InternalRaftRequest{LeaseGrant: &pb.LeaseGrantRequest{ID: 7}}},
		{&pb.LeaseRevokeRequest{ID: 7}, &pb.InternalRaftRequest{LeaseRevoke: &pb.LeaseRevokeRequest{ID: 7}}},
		{&pb.LeaseCheckpoint{ID: 7}, &pb.InternalRaftRequest{LeaseRevoke: &pb.LeaseRevokeRequest{ID: 7}}},
		{&pb.LeaseKeepAliveRequest{ID: 7}, nil},
		{&pb.LeaseCheckpointRequest{}, nil},
		{&pb.Member{}, nil},
	} {
		got, err := internalRequest(tc.in)
		if err != nil {
			t.Fatalf("%T: %v", tc.in, err)
		}
		if !proto.Equal(got, tc.want) {
			t.Errorf("%T: got %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, m := range []proto.Message{&pb.AlarmRequest{}, &pb.InternalRaftRequest{}, wrapperspb.UInt32(1)} {
		if _, err := internalRequest(m); err == nil {
			t.Errorf("%T: no error", m)
		}
	}
}
