package sim

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
)

// TestConversionRefusesWhatTheFormatCannotHold makes sure the converter
// never hands a checker a request or event that differs from what the
// cluster saw.
func TestConversionRefusesWhatTheFormatCannotHold(t *testing.T) {
	reqs := []proto.Message{
		&pb.RangeRequest{Key: []byte("a"), SortOrder: pb.RangeRequest_DESCEND},
		&pb.RangeRequest{Key: []byte("a"), MinModRevision: 2},
		&pb.PutRequest{Key: []byte("a"), IgnoreValue: true},
		&pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a"), RangeEnd: []byte("b")}}},
		&pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{}}}}},
		&pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), IgnoreLease: true}}}}},
	}
	for _, r := range reqs {
		if _, err := request(r); err == nil {
			t.Errorf("%T %v converted without error", r, r)
		}
	}
	bad := WatchRecord{Resp: &pb.WatchResponse{
		Header: &pb.ResponseHeader{Revision: 2},
		Events: []*mvccpb.Event{{Type: mvccpb.Event_EventType(2), Kv: &mvccpb.KeyValue{Key: []byte("a"), ModRevision: 2}}},
	}}
	if _, err := watchResponse(bad); err == nil {
		t.Error("a watch event of unknown type converted without error")
	}
}

// TestAmbiguousWritesFromTheLog leaves out an ambiguous write that is not
// in the committed log, and gives one that is the revision the log gives it
// and the time the next write in the log was acknowledged. Two calls with
// the same request get no effect: the log entry may be either one's.
func TestAmbiguousWritesFromTheLog(t *testing.T) {
	put := func(v string) *pb.PutRequest { return &pb.PutRequest{Key: []byte("a"), Value: []byte(v)} }
	timeout := status.Error(codes.DeadlineExceeded, "timeout")
	del := &pb.DeleteRangeRequest{Key: []byte("a")}
	res := &Result{
		Clients: 3,
		History: []*Record{
			{Client: 0, Call: 1, Return: 2, req: put("in"), Err: timeout, Ambiguous: true},
			{Client: 1, Call: 1, Return: 2, req: put("out"), Err: timeout, Ambiguous: true},
			{Client: 2, Call: 3, Return: 4, req: put("next"), Resp: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 3}}},
			{Client: 0, Call: 5, Return: 6, req: del, Err: timeout, Ambiguous: true},
			{Client: 1, Call: 5, Return: 6, req: del, Err: timeout, Ambiguous: true},
		},
		Log:      []proto.Message{put("in"), put("next"), del},
		revs:     []int64{2, 3, 4},
		FinalRev: 4,
	}
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) != 4 || h.Ops[0].Req.Put.Value != "in" || h.Ops[1].Req.Put.Value != "next" {
		t.Fatalf("got ops %+v, want only the committed writes", h.Ops)
	}
	if h.Ops[2].Effect != nil || h.Ops[3].Effect != nil {
		t.Fatalf("two calls with one request have effects %+v and %+v", h.Ops[2].Effect, h.Ops[3].Effect)
	}
	if e := h.Ops[0].Effect; e == nil || *e != (history.Effect{By: 4, Revision: 2}) {
		t.Fatalf("the committed ambiguous put has effect %+v, want by 4 at revision 2", e)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
}
