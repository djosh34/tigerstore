package sim

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
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

// chunkStore is a Store in memory for TestArenaSpills, which fails every
// read once fail is set.
type chunkStore struct {
	slots map[int64][]byte
	fail  error
}

func (c *chunkStore) Put(_ any, p []byte) (int64, error) {
	if c.slots == nil {
		c.slots = map[int64][]byte{}
	}
	at := int64(len(c.slots)) * int64(len(p))
	c.slots[at] = slices.Clone(p)
	return at, nil
}

func (c *chunkStore) Get(off int64, p []byte) error {
	if c.fail != nil {
		return c.fail
	}
	for at, b := range c.slots {
		if off >= at && off+int64(len(p)) <= at+int64(len(b)) {
			copy(p, b[off-at:])
			return nil
		}
	}
	return fmt.Errorf("no slot holds %d bytes at %d", len(p), off)
}

// TestArenaSpills checks that an arena with a store keeps only its newest
// arenaKeep chunks in memory, sends the older ones of arenaChunk bytes to
// the store and reads back what was added from either, that a slice larger
// than a chunk stays in memory, and that a failed read of the store is the
// run's error (keptPages).
func TestArenaSpills(t *testing.T) {
	store := &chunkStore{}
	kept := &keptPages{pages: store}
	a := arena{store: kept}
	var spans []span
	var want [][]byte
	for i := range 3 * (arenaKeep + 4) {
		n := arenaChunk / 3
		if i == 5 {
			n = arenaChunk + 1
		}
		b := bytes.Repeat([]byte{byte(i)}, n)
		spans, want = append(spans, a.add(b)), append(want, b)
	}
	if inMem := slices.IndexFunc(a.chunks, func(c []byte) bool { return c != nil }); len(store.slots) == 0 || inMem < 0 {
		t.Fatalf("%d chunks in the store, %d in all", len(store.slots), len(a.chunks))
	}
	if n := len(a.chunks) - len(store.slots); n > arenaKeep+1 {
		t.Errorf("%d chunks of %d stay in memory, want %d at most and the large one", n, len(a.chunks), arenaKeep)
	}
	for i, s := range spans {
		if !bytes.Equal(a.bytes(s), want[i]) {
			t.Fatalf("slice %d reads otherwise than it was added", i)
		}
	}
	if kept.err != nil {
		t.Fatal(kept.err)
	}
	errGone := errors.New("the runner's disk is gone")
	store.fail = errGone
	a.bytes(spans[0])
	if !errors.Is(kept.err, errGone) {
		t.Errorf("a failed read left %v, want %v", kept.err, errGone)
	}
}
