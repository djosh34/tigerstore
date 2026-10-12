package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

func create(c *pb.WatchCreateRequest) *pb.WatchRequest {
	return &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: c}}
}

// responses returns and forgets the watch responses gathered so far.
func (s *single) responses() []*pb.WatchResponse {
	var out []*pb.WatchResponse
	for _, w := range s.watch {
		out = append(out, w.Resp)
	}
	s.watch = nil
	return out
}

// put writes key "a".
func (s *single) put(id uint64, val string) {
	s.t.Helper()
	if r := s.do(id, &pb.PutRequest{Key: []byte("a"), Value: []byte(val)}); r.Err != nil {
		s.t.Fatal(r.Err)
	}
}

// TestWatchCatchUpKeepsRevisionsWhole writes 1,100 revisions of three
// keys each and checks that catching up splits them into
// responses of at most 1,000 revisions and never splits a revision.
func TestWatchCatchUpKeepsRevisionsWhole(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	id := uint64(1)
	for i := range 1100 {
		id++
		// Three keys per revision: an event-count boundary would land
		// inside a revision.
		var ops []*pb.RequestOp
		for _, k := range []string{"a", "b", "bb"} {
			ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(k), Value: fmt.Appendf(nil, "%d", i)}}})
		}
		if r := s.do(id, &pb.TxnRequest{Success: ops}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	s.n.Watch(1, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("c"), StartRevision: 1, PrevKv: true}))
	s.run()
	rs := s.responses()
	if !rs[0].Created || rs[0].WatchId != 0 {
		t.Fatalf("first response = %v", rs[0])
	}
	seen := map[int64]int{} // revision to response index
	events, prev := 0, int64(0)
	for i, r := range rs[1:] {
		revs := map[int64]bool{}
		for _, ev := range r.Events {
			rev := ev.Kv.ModRevision
			if j, ok := seen[rev]; ok && j != i {
				t.Fatalf("revision %d is split between responses %d and %d", rev, j, i)
			}
			if rev < prev {
				t.Fatalf("revision %d after %d", rev, prev)
			}
			seen[rev], prev = i, rev
			revs[rev] = true
			events++
			if ev.Kv.Version > 1 && ev.PrevKv == nil {
				t.Fatalf("update at %d without previous value", rev)
			}
		}
		if len(revs) > catchUpRevs || r.Header.Revision != s.n.kv.Rev() {
			t.Fatalf("response %d has %d revisions and header %d", i, len(revs), r.Header.Revision)
		}
	}
	if len(rs) != 3 || events != len(s.n.kv.History([]byte("a"), []byte("c"), 1)) {
		t.Fatalf("%d responses with %d events", len(rs), events)
	}
}

func TestWatchCreateRules(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.put(1, "1")
	reqs := []*pb.WatchCreateRequest{
		{Key: []byte("a")},
		{Key: []byte("a")},
		{Key: []byte("a"), WatchId: 7},
		{Key: []byte("a"), WatchId: 7},
		{Key: []byte("z"), RangeEnd: []byte("a")},
		{Key: []byte("a"), RangeEnd: []byte("a")},
		{Key: []byte("a"), RangeEnd: []byte{0}},
		{Key: []byte("a"), StartRevision: -1},
	}
	for _, r := range reqs {
		s.n.Watch(1, create(r))
	}
	s.run()
	var got []string
	for _, r := range s.responses() {
		got = append(got, fmt.Sprintf("%d %v %q", r.WatchId, r.Canceled, r.CancelReason))
	}
	want := []string{
		`0 false ""`, `1 false ""`, `7 false ""`,
		`-1 true "mvcc: duplicate watch ID provided on the WatchStream"`,
		`-1 true "mvcc: watcher range is empty"`, `-1 true "mvcc: watcher range is empty"`,
		`2 false ""`,
		`-1 true "etcdserver: mvcc: required revision has been compacted"`,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("responses\n%v\nwant\n%v", got, want)
	}
}

func TestWatchFiltersAndCancel(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.n.Watch(1, create(&pb.WatchCreateRequest{Key: []byte("a"), Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NOPUT}}))
	s.n.Watch(1, create(&pb.WatchCreateRequest{Key: []byte("a"), Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NODELETE}}))
	s.run()
	s.responses()
	s.put(1, "1")
	if r := s.do(2, &pb.DeleteRangeRequest{Key: []byte("a")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	for _, r := range s.responses() {
		for _, ev := range r.Events {
			if (r.WatchId == 0) != (ev.Type == mvccpb.Event_DELETE) {
				t.Fatalf("watch %d got a %s", r.WatchId, ev.Type)
			}
		}
	}
	s.n.Watch(1, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{CancelRequest: &pb.WatchCancelRequest{WatchId: 0}}})
	s.run()
	if rs := s.responses(); len(rs) != 1 || !rs[0].Canceled || rs[0].WatchId != 0 {
		t.Fatalf("cancel = %v", rs)
	}
	s.put(3, "2")
	for _, r := range s.responses() {
		if r.WatchId == 0 {
			t.Fatalf("canceled watch got %v", r)
		}
	}
}

func TestWatchProgress(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	progress := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}
	// A stream without watchers gets no answer.
	s.n.Watch(1, progress)
	s.run()
	if rs := s.responses(); len(rs) != 0 {
		t.Fatalf("progress on an empty stream = %v", rs)
	}
	s.put(1, "1")
	rev := s.n.kv.Rev()
	s.n.Watch(1, create(&pb.WatchCreateRequest{Key: []byte("a"), ProgressNotify: true}))
	s.n.Watch(1, progress)
	s.run()
	if rs := s.responses(); len(rs) != 2 || rs[1].WatchId != -1 || rs[1].Header.Revision != rev {
		t.Fatalf("progress = %v", rs)
	}
	// A watcher that starts above the current revision holds back the
	// stream's progress, so a client never resumes below its start.
	s.n.Watch(2, create(&pb.WatchCreateRequest{Key: []byte("a"), StartRevision: rev + 10, ProgressNotify: true}))
	s.n.Watch(2, progress)
	s.run()
	if rs := s.responses(); len(rs) != 1 || !rs[0].Created {
		t.Fatalf("progress with a watcher ahead = %v", rs)
	}
	// Periodic notifications go to watchers that asked, are synced, are
	// not ahead, and got no events since the last one.
	s.n.cfg.ProgressTicks = 1
	s.n.Tick()
	s.run()
	if rs := s.responses(); len(rs) != 1 || rs[0].WatchId != 0 || len(rs[0].Events) != 0 {
		t.Fatalf("notifications = %v", rs)
	}
	s.put(2, "2")
	s.responses()
	s.n.Tick()
	s.run()
	if rs := s.responses(); len(rs) != 0 {
		t.Fatalf("notification right after events = %v", rs)
	}
}
