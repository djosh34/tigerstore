package sim

import (
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"
)

// TestRecordLog keeps calls with values in every place a value goes, and
// checks that they come back equal, that each value is kept once, and
// that keeping a call changed none of its messages.
func TestRecordLog(t *testing.T) {
	big := make([]byte, 3*arenaChunk/2)
	big[7] = 1
	kv := func(k, v string) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(k), Value: []byte(v), ModRevision: 3, Version: 1}
	}
	calls := []*Record{
		{
			Client: 1, Node: 2, Call: 10, Return: 20,
			req:  &pb.PutRequest{Key: []byte("a"), Value: []byte("v1"), PrevKv: true},
			Resp: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 4}, PrevKv: kv("a", "v0")},
		},
		{
			Client: 2, Call: 11, Return: 30,
			req: &pb.TxnRequest{
				Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_VALUE, TargetUnion: &pb.Compare_Value{Value: []byte("v1")}}},
				Success: []*pb.RequestOp{
					{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: big}}},
					{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}},
					{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: []*pb.RequestOp{
						{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("c"), Value: []byte("v1")}}},
					}}}},
				},
			},
			Resp: &pb.TxnResponse{Header: &pb.ResponseHeader{Revision: 5}, Succeeded: true, Responses: []*pb.ResponseOp{
				{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 5}}}},
				{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{kv("a", "v1")}, Count: 1}}},
				{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: &pb.TxnResponse{Responses: []*pb.ResponseOp{
					{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Deleted: 1, PrevKvs: []*mvccpb.KeyValue{kv("d", "")}}}},
				}}}},
			}},
		},
		{Client: 3, Call: 12, Return: 40, req: &pb.RangeRequest{Key: []byte("a")}, Err: rpctypes.ErrGRPCNoLeader, Ambiguous: false},
		{Client: 4, Call: 13, Return: 50, v3: true, req: &pb.LeaseGrantRequest{TTL: 3}, Resp: (*pb.LeaseGrantResponse)(nil), Err: rpctypes.ErrGRPCNoLeader},
		// Two sentinels with the same type and text stay apart.
		{Client: 6, Call: 15, Return: 70, req: &pb.RangeRequest{Key: []byte("b")}, Err: errRefused},
		{Client: 7, Call: 16, Return: 80, req: &pb.RangeRequest{Key: []byte("b")}, Err: errConnRefused},
		{
			Client: 5, Call: 14, Return: 60, req: &pb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true},
			Resp: &pb.DeleteRangeResponse{Deleted: 1, PrevKvs: []*mvccpb.KeyValue{kv("a", "v1")}},
		},
	}
	l, err := newRecordLog()
	if err != nil {
		t.Fatal(err)
	}
	var before [][]byte
	for _, r := range calls {
		for _, m := range []proto.Message{r.req, r.Resp} {
			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			before = append(before, b)
		}
		if err := l.add(r); err != nil {
			t.Fatal(err)
		}
	}
	var got []*Record
	for i, r := range calls {
		for j, m := range []proto.Message{r.req, r.Resp} {
			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != string(before[2*i+j]) {
				t.Errorf("keeping call %d changed its message %d", i, j)
			}
		}
	}
	got, err = l.records()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(calls) {
		t.Fatalf("%d calls back, want %d", len(got), len(calls))
	}
	for i, want := range calls {
		g := got[i]
		if g.Client != want.Client || g.Node != want.Node || g.Call != want.Call || g.Return != want.Return || g.Ambiguous != want.Ambiguous || g.v3 != want.v3 || !errors.Is(g.Err, want.Err) || !errors.Is(want.Err, g.Err) {
			t.Errorf("call %d: got %+v, want %+v", i, g, want)
		}
		if !proto.Equal(g.req, want.req) || !proto.Equal(g.Resp, want.Resp) || fmt.Sprintf("%T", g.Resp) != fmt.Sprintf("%T", want.Resp) {
			t.Errorf("call %d: got %v / %T %v, want %v / %T %v", i, g.req, g.Resp, g.Resp, want.req, want.Resp, want.Resp)
		}
	}
	// "v1" goes in five places, the big value in one, and "v0" and ""
	// in one each.
	if n := len(l.vals.spans); n != 4 {
		t.Errorf("%d values kept, want 4", n)
	}
	if &got[1].req.(*pb.TxnRequest).Success[2].GetRequestTxn().Success[0].GetRequestPut().Value[0] != &got[0].req.(*pb.PutRequest).Value[0] {
		t.Error("two calls with the same value hold two copies of it")
	}
}
