package core

import (
	"bytes"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// randomTxn builds a transaction with every kind of compare and
// operation, nested transactions included.
func randomTxn(r *rand.Rand, depth int) *pb.TxnRequest {
	key := func() []byte { return []byte{'k', byte('a' + r.IntN(4))} }
	t := &pb.TxnRequest{}
	for range r.IntN(3) {
		c := &pb.Compare{Key: key(), Result: pb.Compare_CompareResult(r.IntN(4)), Target: pb.Compare_CompareTarget(r.IntN(5))}
		switch r.IntN(6) {
		case 0:
			c.TargetUnion = &pb.Compare_Version{Version: r.Int64N(9) - 2}
		case 1:
			c.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: r.Int64N(9)}
		case 2:
			c.TargetUnion = &pb.Compare_ModRevision{ModRevision: r.Int64N(9)}
		case 3:
			c.TargetUnion = &pb.Compare_Value{Value: bytes.Repeat([]byte("v"), r.IntN(3))}
		case 4:
			c.TargetUnion = &pb.Compare_Lease{Lease: r.Int64N(9)}
		}
		if r.IntN(2) == 0 {
			c.RangeEnd = key()
		}
		t.Compare = append(t.Compare, c)
	}
	ops := func() []*pb.RequestOp {
		var out []*pb.RequestOp
		for range r.IntN(3) {
			switch r.IntN(4) {
			case 0:
				out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{
					Key: key(), RangeEnd: key(), Limit: r.Int64N(3), Revision: r.Int64N(3), SortOrder: pb.RangeRequest_SortOrder(r.IntN(3)),
					SortTarget: pb.RangeRequest_SortTarget(r.IntN(5)), Serializable: r.IntN(2) == 0, KeysOnly: r.IntN(2) == 0, CountOnly: r.IntN(2) == 0,
					MinModRevision: r.Int64N(3), MaxModRevision: r.Int64N(3), MinCreateRevision: r.Int64N(3), MaxCreateRevision: r.Int64N(3),
				}}})
			case 1:
				out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{
					Key: key(), Value: bytes.Repeat([]byte("x"), r.IntN(40)), Lease: r.Int64N(3), PrevKv: r.IntN(2) == 0,
				}}})
			case 2:
				out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{
					Key: key(), RangeEnd: key(), PrevKv: r.IntN(2) == 0,
				}}})
			case 3:
				if depth < 2 {
					out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: randomTxn(r, depth+1)}})
				}
			}
		}
		return out
	}
	t.Success, t.Failure = ops(), ops()
	return t
}

// TestUnmarshalAliasMatchesProtobuf decodes random transactions,
// concatenations of two, which protobuf merges, and ones with unknown and
// mistyped fields with unmarshalAlias and
// proto.Unmarshal, and requires the same message, also on every
// truncation, which must fail in both or neither.
func TestUnmarshalAliasMatchesProtobuf(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 3000 {
		a, err := proto.Marshal(randomTxn(r, 0))
		if err != nil {
			t.Fatal(err)
		}
		in := a
		if i%2 == 1 {
			b, err := proto.Marshal(randomTxn(r, 0))
			if err != nil {
				t.Fatal(err)
			}
			in = append(append([]byte{}, a...), b...)
		}
		if i%3 == 2 {
			// An unknown field 7, and field 1 (compare) as a varint, which
			// protobuf keeps as unknown too.
			in = append(append([]byte{}, in...), 0x3a, 2, 'u', 'v', 0x08, 0x05)
		}
		for _, cut := range []int{len(in), r.IntN(len(in) + 1)} {
			want, got := &pb.TxnRequest{}, &pb.TxnRequest{}
			wantErr := proto.Unmarshal(in[:cut], want)
			err := UnmarshalAlias(in[:cut], got)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("case %d cut %d: unmarshalAlias error %v, protobuf error %v", i, cut, err, wantErr)
			}
			if err == nil && !proto.Equal(got, want) {
				t.Fatalf("case %d cut %d:\n got %v\nwant %v", i, cut, got, want)
			}
		}
	}
}

// TestUnmarshalAliasShares checks that values are slices of the input.
func TestUnmarshalAliasShares(t *testing.T) {
	in, err := proto.Marshal(&pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k"), Value: []byte("value")}}}}})
	if err != nil {
		t.Fatal(err)
	}
	got := &pb.TxnRequest{}
	if err := UnmarshalAlias(in, got); err != nil {
		t.Fatal(err)
	}
	i := bytes.Index(in, []byte("value"))
	if i < 0 {
		t.Fatal("the encoding does not hold the value")
	}
	copy(in[i:], "VALUE")
	if v := got.Success[0].GetRequestPut().Value; string(v) != "VALUE" || cap(v) != len(v) {
		t.Fatalf("value %q with capacity %d is not a clipped slice of the input", v, cap(v))
	}
}
