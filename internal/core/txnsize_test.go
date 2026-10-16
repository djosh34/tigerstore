package core

import (
	"bytes"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// unknown adds an unknown field to m now and then.
func unknown(r *rand.Rand, m proto.Message) {
	if r.IntN(4) == 0 {
		m.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), r.Uint64N(1<<20)))
	}
}

func randBytes(r *rand.Rand) []byte { return bytes.Repeat([]byte{'b'}, r.IntN(3)*r.IntN(200)) }

func randTxn(r *rand.Rand, depth int) *pb.TxnRequest {
	t := &pb.TxnRequest{}
	for range r.IntN(3) {
		c := &pb.Compare{Result: pb.Compare_CompareResult(r.IntN(5) - 1), Target: pb.Compare_CompareTarget(r.IntN(6) - 1), Key: randBytes(r), RangeEnd: randBytes(r)}
		switch r.IntN(6) {
		case 0:
			c.TargetUnion = &pb.Compare_Version{Version: r.Int64N(3) - 1}
		case 1:
			c.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: r.Int64N(1 << 40)}
		case 2:
			c.TargetUnion = &pb.Compare_ModRevision{ModRevision: r.Int64N(2)}
		case 3:
			c.TargetUnion = &pb.Compare_Value{Value: randBytes(r)}
		case 4:
			c.TargetUnion = &pb.Compare_Lease{Lease: r.Int64N(3) - 1}
		}
		unknown(r, c)
		t.Compare = append(t.Compare, c)
	}
	for _, ops := range []*[]*pb.RequestOp{&t.Success, &t.Failure} {
		for range r.IntN(3) {
			op := &pb.RequestOp{}
			switch r.IntN(5) {
			case 0:
				g := &pb.RangeRequest{Key: randBytes(r), RangeEnd: randBytes(r), Limit: r.Int64N(3) - 1, Revision: r.Int64N(1 << 30),
					SortOrder: pb.RangeRequest_SortOrder(r.IntN(4) - 1), SortTarget: pb.RangeRequest_SortTarget(r.IntN(4)),
					Serializable: r.IntN(2) == 0, KeysOnly: r.IntN(2) == 0, CountOnly: r.IntN(2) == 0,
					MinModRevision: r.Int64N(3), MaxModRevision: r.Int64N(3), MinCreateRevision: r.Int64N(3), MaxCreateRevision: r.Int64N(3) - 1}
				unknown(r, g)
				op.Request = &pb.RequestOp_RequestRange{RequestRange: g}
			case 1:
				p := &pb.PutRequest{Key: randBytes(r), Value: randBytes(r), Lease: r.Int64N(3) - 1, PrevKv: r.IntN(2) == 0, IgnoreValue: r.IntN(2) == 0, IgnoreLease: r.IntN(2) == 0}
				unknown(r, p)
				op.Request = &pb.RequestOp_RequestPut{RequestPut: p}
			case 2:
				d := &pb.DeleteRangeRequest{Key: randBytes(r), RangeEnd: randBytes(r), PrevKv: r.IntN(2) == 0}
				unknown(r, d)
				op.Request = &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: d}
			case 3:
				if depth > 0 {
					op.Request = &pb.RequestOp_RequestTxn{RequestTxn: randTxn(r, depth-1)}
				}
			}
			unknown(r, op)
			*ops = append(*ops, op)
		}
	}
	unknown(r, t)
	return t
}

// TestTxnSize compares the written-out size of a transaction with
// protobuf's, also as the request that checkSize limits.
func TestTxnSize(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for i := range 5000 {
		txn := randTxn(r, 2)
		if got, want := txnSize(txn), proto.Size(txn); got != want {
			t.Fatalf("case %d: size %d, protobuf %d: %v", i, got, want, txn)
		}
		irr := &pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}, Txn: txn}
		if got, want := txnRequestBytes+protowire.SizeBytes(txnSize(txn)), proto.Size(irr); got != want {
			t.Fatalf("case %d: request size %d, protobuf %d", i, got, want)
		}
	}
}
