package sim

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/check"
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

// An ambiguous Txn whose failure branch only reads matches its log entry
// without that branch, as a wl node logs it: it stays in the history with
// its effect, and the durability check finds a client for the entry
// (#703; seed 2 of TestWLFaultSeeds).
func TestAnAmbiguousTxnMatchesItsEntryWithoutItsReads(t *testing.T) {
	cmp := []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: 1}}}
	put := []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("v")}}}}
	read := []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}}
	reset := status.Error(codes.Unavailable, "error reading from server: connection reset")
	res := &Result{
		Clients:  1,
		History:  []*Record{{Client: 0, Call: 1, Return: 2, req: &pb.TxnRequest{Compare: cmp, Success: put, Failure: read}, Err: reset, Ambiguous: true}},
		Log:      []proto.Message{&pb.TxnRequest{Compare: cmp, Success: put}},
		revs:     []int64{2},
		FinalRev: 2,
	}
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) != 1 || h.Ops[0].Effect == nil || *h.Ops[0].Effect != (history.Effect{By: math.MaxInt64, Revision: 2}) {
		t.Fatalf("got ops %+v, want the Txn with its effect at revision 2", h.Ops)
	}
	if err := check.Durable(h); err != nil {
		t.Fatal(err)
	}
}

// logKey leaves out only a failure branch that changes nothing: any
// other difference in what a write can change keeps two requests apart,
// so an ambiguous call whose writes no entry holds is still dropped as
// never committed, and still pins no entry (#703).
func TestLogKeyKeepsWhatAWriteChanges(t *testing.T) {
	cmp := func(rev int64) []*pb.Compare {
		return []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}}
	}
	put := func(v string) []*pb.RequestOp {
		return []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte(v)}}}}
	}
	read := func(k string) []*pb.RequestOp {
		return []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(k)}}}}
	}
	logged := &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: slices.Concat(read("a"), put("w"))}
	for _, tc := range []struct {
		name string
		call *pb.TxnRequest
		same bool
	}{
		{"the same request", &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: slices.Concat(read("a"), put("w"))}, true},
		{"another compare", &pb.TxnRequest{Compare: cmp(2), Success: put("v"), Failure: slices.Concat(read("a"), put("w"))}, false},
		{"another success branch", &pb.TxnRequest{Compare: cmp(1), Success: put("x"), Failure: slices.Concat(read("a"), put("w"))}, false},
		{"another write in the failure branch", &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: slices.Concat(read("a"), put("y"))}, false},
		{"another read beside a write in the failure branch", &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: slices.Concat(read("b"), put("w"))}, false},
		{"no failure branch where the entry writes in it", &pb.TxnRequest{Compare: cmp(1), Success: put("v")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := logKey(tc.call)
			if err != nil {
				t.Fatal(err)
			}
			b, err := logKey(logged)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(a) == string(b); got != tc.same {
				t.Fatalf("the call matches the entry: %v, want %v", got, tc.same)
			}
			res := &Result{
				Clients:  1,
				History:  []*Record{{Client: 0, Call: 1, Return: 2, req: tc.call, Err: status.Error(codes.Unavailable, "connection reset"), Ambiguous: true}},
				Log:      []proto.Message{logged},
				revs:     []int64{2},
				FinalRev: 2,
			}
			h, err := res.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			if kept := len(h.Ops) == 1; kept != tc.same {
				t.Fatalf("the ambiguous call kept in the history: %v, want %v", kept, tc.same)
			}
			if tc.same && h.Ops[0].Effect == nil {
				t.Fatal("the call that matches its one entry has no effect")
			}
		})
	}
	// Two calls that differ only in a failure branch of reads share one key:
	// neither pins the one entry, as the log cannot tell whose it is.
	res := &Result{
		Clients: 2,
		History: []*Record{
			{Client: 0, Call: 1, Return: 2, req: &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: read("a")}, Err: status.Error(codes.Unavailable, "connection reset"), Ambiguous: true},
			{Client: 1, Call: 1, Return: 2, req: &pb.TxnRequest{Compare: cmp(1), Success: put("v"), Failure: read("b")}, Err: status.Error(codes.Unavailable, "connection reset"), Ambiguous: true},
		},
		Log:      []proto.Message{&pb.TxnRequest{Compare: cmp(1), Success: put("v")}},
		revs:     []int64{2},
		FinalRev: 2,
	}
	h, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range h.Ops {
		if op.Effect != nil {
			t.Fatalf("client %d's call pins the entry another call may own", op.Client)
		}
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

// Decoded watch protobufs are not retained after long-run conversion.
// Encoded references reconstruct identical checker input, hashes and
// report files, including after a conversion error.
func TestReleasedWatchResponsesReconstructExactly(t *testing.T) {
	for _, broken := range []bool{false, true} {
		l, err := newRecordLog()
		if err != nil {
			t.Fatal(err)
		}
		response := &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 2}, WatchId: 17, Events: []*mvccpb.Event{{
			Kv: &mvccpb.KeyValue{Key: []byte("a"), Value: []byte("value"), CreateRevision: 2, ModRevision: 2, Version: 1},
		}}}
		msg, err := l.encode(response)
		if err != nil {
			t.Fatal(err)
		}
		r := &Result{
			records: l, long: true, logged: true, FinalRev: 2,
			Watches:   []WatchResult{{Key: []byte("a"), PrevKV: true, Start: 2, Responses: []WatchRecord{{Time: 11, Resp: response}}}},
			watchRefs: [][]loggedWatch{{{at: 11, msg: msg}}},
		}
		var before, after bytes.Buffer
		writeHistory(&before, r)
		want, err := r.Checkable()
		if err != nil {
			t.Fatal(err)
		}
		files := runFilesWithin("/sim", "replay", r, true, &saveRoom{left: 1 << 20})
		if broken {
			r.History = []*Record{{req: &pb.RangeRequest{Key: []byte("a"), SortOrder: pb.RangeRequest_ASCEND}}}
		}
		got, err := r.checkable(true)
		if broken != (err != nil) {
			t.Fatalf("broken=%v conversion error %v", broken, err)
		}
		if r.History != nil || r.Watches[0].Responses != nil {
			t.Fatal("conversion retained a decoded client/watch response root")
		}
		if !broken && !reflect.DeepEqual(got, want) {
			t.Fatal("release changed the checker history")
		}
		writeHistory(&after, r)
		if !bytes.Equal(before.Bytes(), after.Bytes()) || r.Watches[0].Responses != nil {
			t.Fatal("streamed reconstruction changed the hash bytes or re-pinned decoded watches")
		}
		again, err := r.Checkable()
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("checker input reconstruction differs: %v", err)
		}
		if gotFiles := runFilesWithin("/sim", "replay", r, true, &saveRoom{left: 1 << 20}); !reflect.DeepEqual(gotFiles, files) {
			t.Fatal("reconstructed failure report differs byte-for-byte")
		}
	}
}

// A calls-only report must not touch watch-only chunks: they may be
// unreadable, and healthy watches should not be re-pinned either.
func TestCallsOnlyReportDoesNotDecodeWatches(t *testing.T) {
	run := func(t *testing.T, broken bool) {
		l, err := newRecordLog()
		if err != nil {
			t.Fatal(err)
		}
		req := &pb.PutRequest{Key: []byte("readable-client-put"), Value: []byte("value")}
		if err := l.add(&Record{req: req, Err: errConnRefused}); err != nil {
			t.Fatal(err)
		}
		// Put the watch in a separate encoded chunk and move only that
		// chunk to the backing store; the client's message stays readable.
		last := len(l.data.chunks) - 1
		chunk := l.data.chunks[last]
		l.data.chunks[last] = slices.Clip(chunk)
		msg, err := l.encode(&pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 2}})
		if err != nil {
			t.Fatal(err)
		}
		store := &chunkStore{}
		l.data.store = store
		at, err := store.Put(nil, l.data.chunks[msg.at.chunk])
		if err != nil {
			t.Fatal(err)
		}
		l.data.at[msg.at.chunk], l.data.chunks[msg.at.chunk] = at, nil
		if broken {
			store.fail = errors.New("unreadable watch-only chunk")
		}
		r := &Result{
			records: l, long: true, logged: true, hashed: "recorded hashes",
			Log:       []proto.Message{&pb.PutRequest{Key: []byte("log"), Value: make([]byte, 1024)}},
			Watches:   []WatchResult{{Key: []byte("a"), Start: 1}},
			watchRefs: [][]loggedWatch{{{msg: msg}}},
		}
		callsMax, historyMax := r.saveBounds()
		if historyMax <= callsMax {
			t.Fatal("test must admit calls.txt but not history.json")
		}
		files := runFilesWithin("/sim", "replay", r, true, &saveRoom{left: callsMax})
		if !strings.Contains(files["calls.txt"], "readable-client-put") || len(r.History) != 1 {
			t.Fatalf("broken=%v calls-only report lost readable client calls: %q", broken, files["calls.txt"])
		}
		if r.Watches[0].Responses != nil || store.gets != 0 {
			t.Fatalf("calls-only report read/re-held watches: reads %d, responses %d", store.gets, len(r.Watches[0].Responses))
		}
	}
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken=%v", broken), func(t *testing.T) { run(t, broken) })
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
