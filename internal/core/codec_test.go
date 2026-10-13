package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestBatchRoundTrip(t *testing.T) {
	ops := []*pending{
		{req: Request{ID: 7, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}}},
		{req: Request{ID: 8, Op: &pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte("c")}}},
		{req: Request{ID: 9, Op: &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a")}}}}},
	}
	data, err := encodeBatch(2, 99, ops)
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	err = DecodeBatch(data, func(origin, boot, id uint64, op proto.Message) error {
		if origin != 2 || boot != 99 || id != ops[i].req.ID || !proto.Equal(op, ops[i].req.Op) {
			t.Errorf("write %d decoded as %d/%d/%d %v", i, origin, boot, id, op)
		}
		i++
		return nil
	})
	if err != nil || i != len(ops) {
		t.Fatalf("decoded %d writes, err %v", i, err)
	}
	for cut := range len(data) {
		if err := DecodeBatch(data[:cut], func(_, _, _ uint64, _ proto.Message) error { return nil }); err == nil {
			t.Fatalf("batch cut at %d of %d bytes decoded without error", cut, len(data))
		}
	}
	if _, err := encodeBatch(1, 1, []*pending{{req: Request{Op: &pb.RangeRequest{}}}}); err == nil {
		t.Fatal("a range was encoded as a write")
	}
}

func TestReadCtxNeedsSameBoot(t *testing.T) {
	ctx := readCtx(5, 42)
	if seq, ok := parseReadCtx(5, ctx); !ok || seq != 42 {
		t.Fatalf("parse = %d, %v", seq, ok)
	}
	if _, ok := parseReadCtx(6, ctx); ok {
		t.Fatal("a read context from another boot was accepted")
	}
}

// TestRequestSizeLimitMatchesEtcd checks the largest values etcd v3.7.2
// accepts for a 39-byte key, measured in issue #62.
func TestRequestSizeLimitMatchesEtcd(t *testing.T) {
	key := make([]byte, 39)
	put := func(n int) proto.Message { return &pb.PutRequest{Key: key, Value: make([]byte, n)} }
	txn := func(n int) proto.Message {
		return &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: make([]byte, n)}}}}}
	}
	for _, c := range []struct {
		name    string
		mk      func(int) proto.Message
		largest int
	}{{"put", put, 1572801}, {"txn", txn, 1572793}} {
		if err := check(c.mk(c.largest)); err != nil {
			t.Errorf("%s of %d bytes: %v", c.name, c.largest, err)
		}
		if err := check(c.mk(c.largest + 1)); err == nil {
			t.Errorf("%s of %d bytes was accepted", c.name, c.largest+1)
		}
	}
}
