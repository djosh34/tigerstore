package sim

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// TestUncommittedAmbiguousWritesAreLeftOut keeps an ambiguous write that
// is in the committed log and leaves out one that is not.
func TestUncommittedAmbiguousWritesAreLeftOut(t *testing.T) {
	put := func(v string) *pb.PutRequest { return &pb.PutRequest{Key: []byte("a"), Value: []byte(v)} }
	timeout := status.Error(codes.DeadlineExceeded, "timeout")
	res := &Result{
		Clients: 2,
		History: []*Record{
			{Client: 0, Call: 1, Return: 2, req: put("in"), Err: timeout, Ambiguous: true},
			{Client: 1, Call: 1, Return: 2, req: put("out"), Err: timeout, Ambiguous: true},
		},
		Log:      []proto.Message{put("in")},
		FinalRev: 2,
	}
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) != 1 || h.Ops[0].Req.Put.Value != "in" {
		t.Fatalf("got ops %+v, want only the committed put", h.Ops)
	}
}
