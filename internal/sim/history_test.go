package sim

import (
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
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
		if _, err := frometcd.Request(r); err == nil {
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

// TestRotStop accepts only the stop of a rotted node on the damaged
// record itself, of its entry in its segment, or on its value (#160).
func TestRotStop(t *testing.T) {
	n := &node{rotEntry: 7, rotSeg: 3}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("core: read entry: %w", &wal.RecordError{Entry: 7, Segment: 3, Reason: "does not verify"}), true},
		{fmt.Errorf("core: %w", mvcc.ErrBadValue), true},
		{&wal.RecordError{Entry: 8, Segment: 3, Reason: "does not verify"}, false},
		{&wal.RecordError{Entry: 7, Segment: 4, Reason: "does not verify"}, false},
		{fmt.Errorf("%w: the record of entry 7 in segment 3 does not verify", wal.ErrCorrupt), false},
		{errors.New("node stopped"), false},
	} {
		if got := rotStop(n, tc.err); got != tc.want {
			t.Errorf("rotStop(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// TestHashesCoverHistory checks that the history hash changes with every
// part of the history: a call's exact times and answer, and a watch
// response and its time.
func TestHashesCoverHistory(t *testing.T) {
	result := func() *Result {
		return &Result{
			History: []*Record{{Client: 1, Node: 2, Call: 1_000_001, Return: 2_000_002, req: &pb.PutRequest{Key: []byte("a")}, Resp: &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 2}}}},
			Watches: []WatchResult{{Key: []byte("a"), Responses: []WatchRecord{{Time: 1, Resp: &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 2}}}}}},
		}
	}
	base := hashes(result())
	if again := hashes(result()); again != base {
		t.Fatalf("the same history hashes as %s and %s", base, again)
	}
	for name, change := range map[string]func(*Result){
		"call time":      func(r *Result) { r.History[0].Call++ },
		"return time":    func(r *Result) { r.History[0].Return++ },
		"answer":         func(r *Result) { r.History[0].Resp.(*pb.PutResponse).Header.Revision = 3 },
		"error":          func(r *Result) { r.History[0].Err = errors.New("lost") },
		"watch time":     func(r *Result) { r.Watches[0].Responses[0].Time = 2 },
		"watch response": func(r *Result) { r.Watches[0].Responses[0].Resp.Header.Revision = 3 },
	} {
		r := result()
		change(r)
		if hashes(r) == base {
			t.Errorf("changing the %s left the hashes at %s", name, base)
		}
	}
}
