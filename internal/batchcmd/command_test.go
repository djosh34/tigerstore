package batchcmd

import (
	"bytes"
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func mustBody(t *testing.T, op proto.Message) []byte {
	t.Helper()
	body, err := proto.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestPutFieldsAndAliases(t *testing.T) {
	for _, op := range []*pb.PutRequest{
		{Key: []byte("key"), Value: []byte("payload"), Lease: -7, PrevKv: true},
		{Key: []byte("key"), PrevKv: true, IgnoreValue: true, IgnoreLease: true},
	} {
		body := mustBody(t, op)
		c, err := Decode(PutPath, body)
		if err != nil {
			t.Fatal(err)
		}
		got := c.Legacy().(*pb.PutRequest)
		if !proto.Equal(op, got) || c.Size() != proto.Size(op) || !c.Valid() {
			t.Fatalf("decoded %v, want %v", got, op)
		}
		if &got.Key[0] != &body[bytes.Index(body, op.Key)] {
			t.Fatal("key was copied")
		}
		if len(op.Value) > 0 && &got.Value[0] != &body[bytes.Index(body, op.Value)] {
			t.Fatal("value was copied")
		}
	}
}

func TestTxnFieldsAndAliases(t *testing.T) {
	cs := []*pb.Compare{
		{Key: []byte("k"), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 1}},
		{Key: []byte("k"), Target: pb.Compare_CREATE, Result: pb.Compare_LESS, TargetUnion: &pb.Compare_CreateRevision{CreateRevision: 2}},
		{Key: []byte("k"), Target: pb.Compare_MOD, Result: pb.Compare_GREATER, TargetUnion: &pb.Compare_ModRevision{ModRevision: 3}},
		{Key: []byte("k"), Target: pb.Compare_VALUE, Result: pb.Compare_NOT_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("old")}},
		{Key: []byte("k"), Target: pb.Compare_LEASE, TargetUnion: &pb.Compare_Lease{Lease: -8}},
	}
	for _, cmp := range cs {
		op := &pb.TxnRequest{
			Compare: []*pb.Compare{cmp},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k"), Value: []byte("new"), PrevKv: true}}}},
			Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k"), Revision: 1, Limit: 1, KeysOnly: true, CountOnly: true, Serializable: true}}}},
		}
		body := mustBody(t, op)
		c, err := Decode(TxnPath, body)
		if err != nil {
			t.Fatal(err)
		}
		got := c.Legacy().(*pb.TxnRequest)
		if !proto.Equal(op, got) || c.ReadOnly() || c.Serializable() || c.Size() != proto.Size(op) {
			t.Fatalf("decoded %v, want %v", got, op)
		}
		v := got.Success[0].GetRequestPut().Value
		if &v[0] != &body[bytes.Index(body, []byte("new"))] {
			t.Fatal("txn value was copied")
		}
	}
}

func TestValidationAndUnsupported(t *testing.T) {
	for _, op := range []*pb.TxnRequest{
		{Compare: []*pb.Compare{{Key: []byte("a"), RangeEnd: []byte("z")}}},
		{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a")}}}}},
		{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{}}}}},
		{Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}}}}},
		{Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a"), SortTarget: pb.RangeRequest_VALUE}}}}},
	} {
		if _, err := Decode(TxnPath, mustBody(t, op)); status.Code(err) != codes.Unimplemented {
			t.Fatalf("unsupported %v: %v", op, err)
		}
	}
	if _, err := Decode("/unknown", nil); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	for _, body := range [][]byte{{0x0a, 0xff}, {0x08, 1}, {0x0d, 1}, {0}} {
		if _, err := Decode(PutPath, body); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%x: %v", body, err)
		}
	}
	if _, err := Decode(PutPath, nil); !errors.Is(err, rpctypes.ErrGRPCEmptyKey) {
		t.Fatal(err)
	}
	op := &pb.PutRequest{Key: []byte("a"), Value: []byte("v"), IgnoreValue: true}
	if _, err := Decode(PutPath, mustBody(t, op)); !errors.Is(err, rpctypes.ErrGRPCValueProvided) {
		t.Fatal(err)
	}
	put := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a")}}}
	dup := &pb.TxnRequest{Success: []*pb.RequestOp{put, put}}
	if _, err := Decode(TxnPath, mustBody(t, dup)); !errors.Is(err, rpctypes.ErrGRPCDuplicateKey) {
		t.Fatal(err)
	}
}

func TestNoncanonicalPutKeepsCanonicalSize(t *testing.T) {
	body := mustBody(t, &pb.PutRequest{Key: []byte("first"), Value: []byte("old")})
	body = protowire.AppendTag(body, 1, protowire.BytesType)
	body = protowire.AppendBytes(body, []byte("last"))
	body = protowire.AppendTag(body, 2, protowire.BytesType)
	body = protowire.AppendBytes(body, []byte("new"))
	body = protowire.AppendTag(body, 4, protowire.VarintType)
	body = append(body, 0x82, 0x00) // Noncanonical varint, bool value 2 is true.
	c, err := Decode(PutPath, body)
	if err != nil {
		t.Fatal(err)
	}
	want := &pb.PutRequest{Key: []byte("last"), Value: []byte("new"), PrevKv: true}
	if !proto.Equal(c.Legacy(), want) || c.Size() != proto.Size(want) || len(c.Body()) != len(body) {
		t.Fatalf("got %v size %d", c.Legacy(), c.Size())
	}
}
