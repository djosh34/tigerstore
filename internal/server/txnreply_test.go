package server

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

func randHeader(r *rand.Rand) *pb.ResponseHeader {
	if r.IntN(4) == 0 {
		return nil
	}
	return &pb.ResponseHeader{ClusterId: r.Uint64N(3) << r.IntN(60), MemberId: r.Uint64(), Revision: r.Int64N(3) << r.IntN(40), RaftTerm: r.Uint64N(3)}
}

func randKV(r *rand.Rand) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key: bytes.Repeat([]byte{'k'}, r.IntN(3)), Value: bytes.Repeat([]byte{'v'}, r.IntN(200)),
		CreateRevision: r.Int64N(1 << 33), ModRevision: r.Int64N(3), Version: r.Int64N(4), Lease: r.Int64N(5) - 2,
	}
}

func randKVs(r *rand.Rand) []*mvccpb.KeyValue {
	var kvs []*mvccpb.KeyValue
	for range r.IntN(4) {
		kvs = append(kvs, randKV(r))
	}
	return kvs
}

func randTxnResp(r *rand.Rand, depth int) *pb.TxnResponse {
	t := &pb.TxnResponse{Header: randHeader(r), Succeeded: r.IntN(2) == 0}
	for range r.IntN(4) {
		op := &pb.ResponseOp{}
		switch k := r.IntN(6); {
		case k == 0:
			op.Response = &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: randHeader(r), Kvs: randKVs(r), More: r.IntN(2) == 0, Count: r.Int64N(3) - 1}}
		case k == 1:
			p := &pb.PutResponse{Header: randHeader(r)}
			if r.IntN(2) == 0 {
				p.PrevKv = randKV(r)
			}
			op.Response = &pb.ResponseOp_ResponsePut{ResponsePut: p}
		case k == 2:
			op.Response = &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Header: randHeader(r), Deleted: r.Int64N(3), PrevKvs: randKVs(r)}}
		case k == 3 && depth > 0:
			op.Response = &pb.ResponseOp_ResponseTxn{ResponseTxn: randTxnResp(r, depth-1)}
		case k == 4:
			// A set member that is nil, which protobuf writes empty.
			switch r.IntN(4) {
			case 0:
				op.Response = &pb.ResponseOp_ResponseRange{}
			case 1:
				op.Response = &pb.ResponseOp_ResponsePut{}
			case 2:
				op.Response = &pb.ResponseOp_ResponseDeleteRange{}
			default:
				op.Response = &pb.ResponseOp_ResponseTxn{}
			}
		}
		t.Responses = append(t.Responses, op)
	}
	return t
}

// TestTxnReplyEncoding compares the hand-written TxnResponse with
// protobuf's deterministic encoding on random answers.
func TestTxnReplyEncoding(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 5000 {
		resp := randTxnResp(r, 2)
		got, err := appendTxnReply(resp)
		if err != nil {
			t.Fatal(err)
		}
		body, err := proto.MarshalOptions{Deterministic: true}.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		want := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(body)))
		want = append(want, body...)
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d:\n got %x\nwant %x", i, got, want)
		}
	}
}
