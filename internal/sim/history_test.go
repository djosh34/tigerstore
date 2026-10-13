package sim

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
