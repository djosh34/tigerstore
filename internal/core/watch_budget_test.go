package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestOneTurnWatchResponseIsBounded applies 3,000 writes in one turn for
// a watcher with prev_kv: no response may hold more revisions than a
// catch-up's, nor much more than a piece of events (#429).
func TestOneTurnWatchResponseIsBounded(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 64 << 10 })
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), PrevKv: true}))
	s.run()
	s.responses()
	for i := range 3000 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%05d", i%300), Value: make([]byte, 100)}})
	}
	s.run()
	events := 0
	for _, r := range s.responses() {
		revs := map[int64]bool{}
		for _, ev := range r.Events {
			revs[ev.Kv.ModRevision] = true
		}
		events += len(r.Events)
		if len(revs) > catchUpRevs || proto.Size(r) > 2*s.n.cfg.PieceBytes {
			t.Fatalf("one watch response holds %d revisions, %d bytes", len(revs), proto.Size(r))
		}
	}
	if events != 3000 {
		t.Fatalf("the watcher got %d events, want 3000", events)
	}
}

// TestCatchUpWaitsForRoom starts a watch from revision 1 over 4.5 MB of
// history on a stream with room for 64 KiB: the node hands out about a
// piece more than the room, and the rest only as the stream gets room
// again. The events come whole and in order (#420).
func TestCatchUpWaitsForRoom(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) {
		c.PieceBytes = 32 << 10
		c.WindowBytes, c.WindowMaxBytes = 512, 1024
	})
	for i := range 3000 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%04d", i%1000), Value: make([]byte, 1500)}})
		if i%100 == 99 {
			s.run()
		}
	}
	s.run()
	s.room = 64 << 10
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), StartRevision: 1, PrevKv: true}))
	s.n.WatchRoom(1, s.room, 1<<20)
	s.run()
	out := 0
	for _, w := range s.watch {
		out += proto.Size(w.Resp)
	}
	if out > int(s.room)+2*s.n.cfg.PieceBytes {
		t.Fatalf("a stream with room for %d bytes got %d", s.room, out)
	}
	events, last := 0, int64(0)
	for range 400 {
		for _, r := range s.responses() {
			for _, ev := range r.Events {
				if ev.Kv.ModRevision < last {
					t.Fatalf("revision %d after %d", ev.Kv.ModRevision, last)
				}
				last = ev.Kv.ModRevision
				events++
			}
		}
		if events == 3000 {
			return
		}
		// The client took what it had.
		s.n.WatchRoom(1, s.room, 1<<20)
		s.run()
	}
	t.Fatalf("the watcher got %d events of 3000", events)
}

// TestBigRevisionGoesInParts deletes 300 keys of 1 KiB at one revision
// for a watcher with prev_kv: the revision passes a piece, so it goes out
// as one response in parts, the first saying how large the events are,
// and the stream stays open (#428).
func TestBigRevisionGoesInParts(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 32 << 10 })
	for i := range 300 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%03d", i), Value: make([]byte, 1024)}})
	}
	s.run()
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), PrevKv: true}))
	s.run()
	s.responses()
	if r := s.do(500, &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	parts := s.watch
	if len(parts) < 2 || parts[0].EventBytes == 0 || !parts[0].More || parts[len(parts)-1].More {
		t.Fatalf("a revision of 300 KiB of events went out in %d parts", len(parts))
	}
	var events int64
	for i, p := range parts {
		if p.Cont != (i > 0) || proto.Size(p.Resp) > 2*s.n.cfg.PieceBytes {
			t.Fatalf("part %d: cont %v, %d bytes", i, p.Cont, proto.Size(p.Resp))
		}
		for _, ev := range p.Resp.Events {
			events += int64(proto.Size(&pb.WatchResponse{Events: []*mvccpb.Event{ev}}))
		}
	}
	if events != parts[0].EventBytes {
		t.Fatalf("the events take %d bytes, the first part said %d", events, parts[0].EventBytes)
	}
	rs := s.responses()
	if len(rs) != 1 || len(rs[0].Events) != 300 || rs[0].Events[0].PrevKv == nil {
		t.Fatalf("%d responses, the first with %d events", len(rs), len(rs[0].Events))
	}
}

// TestPartsDoNotInterleave deletes 300 keys of 1 KiB at one revision for
// two watchers with prev_kv on one stream: each gets the revision as a
// response in parts, and no other response comes between the parts of
// one, as a gRPC message takes nothing in its middle (#505).
func TestPartsDoNotInterleave(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 32 << 10 })
	for i := range 300 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%03d", i), Value: make([]byte, 1024)}})
	}
	s.run()
	for range 2 {
		s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), PrevKv: true}))
	}
	s.run()
	s.responses()
	if r := s.do(500, &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	open := int64(-1)
	for i, p := range s.watch {
		switch {
		case open >= 0 && p.Resp.WatchId != open:
			t.Fatalf("response %d, of watch %d, comes inside the response in parts of watch %d", i, p.Resp.WatchId, open)
		case p.More:
			open = p.Resp.WatchId
		default:
			open = -1
		}
	}
	rs := s.responses()
	if len(rs) != 2 || len(rs[0].Events) != 300 || len(rs[1].Events) != 300 {
		t.Fatalf("%d responses for the two watchers", len(rs))
	}
}

// TestPartsSizeTheirFilteredEvents writes a revision of 100 puts and 300
// deletes with values of 1 KiB, for a watcher with prev_kv that filters
// out deletes: its response in parts says the size of the events it
// gets, not of every change.
func TestPartsSizeTheirFilteredEvents(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 32 << 10 })
	for i := range 300 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "b%03d", i), Value: make([]byte, 1024)}})
	}
	s.run()
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("c"), PrevKv: true, Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NODELETE}}))
	s.run()
	s.responses()
	// The deletes come after the puts, so the sizing walks over them.
	txn := &pb.TxnRequest{}
	for i := range 100 {
		txn.Success = append(txn.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: fmt.Appendf(nil, "a%03d", i), Value: make([]byte, 1024)}}})
	}
	txn.Success = append(txn.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte("c")}}})
	if r := s.do(500, txn); r.Err != nil {
		t.Fatal(r.Err)
	}
	var events int64
	for _, p := range s.watch {
		for _, ev := range p.Resp.Events {
			if ev.Type == mvccpb.DELETE {
				t.Fatal("a watcher with NODELETE got a delete")
			}
			events += int64(proto.Size(&pb.WatchResponse{Events: []*mvccpb.Event{ev}}))
		}
	}
	if len(s.watch) < 2 || s.watch[0].EventBytes != events {
		t.Fatalf("%d parts; the first says %d bytes of events, they take %d", len(s.watch), s.watch[0].EventBytes, events)
	}
}
