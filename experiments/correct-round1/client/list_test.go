package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func validChunks() []*pb.RangeResponse {
	chunks := []*pb.RangeResponse{{}, {}, {Header: &pb.ResponseHeader{Revision: 33}, Count: 32}}
	for i := range 32 {
		n := 0
		if i >= 10 {
			n = 1
		}
		if i >= 30 {
			n = 2
		}
		chunks[n].Kvs = append(chunks[n].Kvs, &mvccpb.KeyValue{Key: []byte(fmt.Sprintf("%sk%02d", listPrefix, i)), Value: []byte("value"), Version: 1, CreateRevision: int64(i + 2), ModRevision: int64(i + 2)})
	}
	return chunks
}

// A self-consistent historical snapshot is still illegal when it precedes an
// acknowledged delete that completed before the stream invocation.
func TestRecordedListRejectsStaleLatestSnapshot(t *testing.T) {
	h := offlineFixture()
	q := history.Request{Range: &history.RangeRequest{Key: "a", End: "c"}}
	h.Ops = append(h.Ops, history.Op{Client: 60000, Call: 100, Return: 110, Req: q,
		Resp: &history.Response{Revision: 5, Range: &history.RangeResponse{}}})
	if err := check.Linearizable(h, time.Second); err != nil {
		t.Fatalf("current snapshot rejected: %v", err)
	}
	h.Ops[len(h.Ops)-1].Resp = &history.Response{Revision: 3, Range: &history.RangeResponse{Count: 2, KVs: []history.KeyValue{
		{Key: "a", Value: "first", CreateRevision: 2, ModRevision: 2, Version: 1},
		{Key: "b", Value: "second", CreateRevision: 3, ModRevision: 3, Version: 1},
	}}}
	if err := check.Linearizable(h, time.Second); err == nil {
		t.Fatal("accepted stale latest LIST after acknowledged delete")
	}
}

func TestMergeListRequiresRealMultichunkSnapshot(t *testing.T) {
	chunks := validChunks()
	got, err := mergeList(chunks)
	if err != nil || got.Count != 32 || len(got.Kvs) != 32 {
		t.Fatalf("valid LIST: %v %v", got, err)
	}
	for _, corrupt := range []struct {
		name   string
		change func([]*pb.RangeResponse)
	}{
		{"early header", func(c []*pb.RangeResponse) { c[0].Header = &pb.ResponseHeader{Revision: 33} }},
		{"early count", func(c []*pb.RangeResponse) { c[0].Count = 32 }},
		{"early more", func(c []*pb.RangeResponse) { c[0].More = true }},
		{"missing header", func(c []*pb.RangeResponse) { c[2].Header = nil }},
		{"wrong count", func(c []*pb.RangeResponse) { c[2].Count = 31 }},
		{"duplicate", func(c []*pb.RangeResponse) { c[1].Kvs[0] = c[0].Kvs[0] }},
		{"lost key", func(c []*pb.RangeResponse) { c[2].Kvs = c[2].Kvs[:1] }},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			c := validChunks()
			corrupt.change(c)
			if _, err := mergeList(c); err == nil {
				t.Fatal("accepted corrupt LIST")
			}
		})
	}
	if _, err := mergeList([]*pb.RangeResponse{{Header: &pb.ResponseHeader{Revision: 33}, Count: 32}}); err == nil {
		t.Fatal("accepted single chunk")
	}
}
