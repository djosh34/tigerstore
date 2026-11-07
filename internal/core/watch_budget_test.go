package core

import (
	"fmt"
	"strings"
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
// again, the catch-up's response of up to 1,000 revisions in parts. The
// events come whole and in order (#420).
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
		// Parts of a response may come over several turns.
		parts := s.watch
		s.watch = nil
		for _, w := range parts {
			for _, ev := range w.Resp.Events {
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

// bigDeleteSetup puts 300 keys of 1 KiB with pieces of 32 KiB, and a
// watcher with prev_kv, and fragment if asked, over them.
func bigDeleteSetup(t *testing.T, fragment bool) *single {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 32 << 10 })
	for i := range 300 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%03d", i), Value: make([]byte, 1024)}})
	}
	s.run()
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), PrevKv: true, Fragment: fragment}))
	s.run()
	s.responses()
	return s
}

// TestCompactionAtPartsBreaksTheStream compacts at the revision of a
// delete of 300 keys right after its first part went out to a watcher
// with prev_kv. The previous values of the parts still to come are gone,
// so the stream must break rather than send them without, or send fewer
// bytes than the first part said.
func TestCompactionAtPartsBreaksTheStream(t *testing.T) {
	s := bigDeleteSetup(t, false)
	compacted := false
	s.afterTurn = func(eff Effects) {
		for _, w := range eff.Watch {
			if w.More && !compacted {
				compacted = true
				s.n.Submit(Request{ID: 900, Op: &pb.CompactionRequest{Revision: s.n.kv.Rev()}})
			}
		}
	}
	s.do(500, &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
	s.run()
	var declared, sent int64
	for _, p := range s.watch {
		if p.Broken {
			return
		}
		declared = max(declared, p.EventBytes)
		for _, ev := range p.Resp.Events {
			if ev.PrevKv == nil {
				t.Fatal("a delete went to a watcher with prev_kv without its previous value")
			}
			sent += int64(proto.Size(&pb.WatchResponse{Events: []*mvccpb.Event{ev}}))
		}
	}
	t.Fatalf("the stream did not break: the first part said %d bytes, %d went out", declared, sent)
}

// TestFragmentsDoNotInterleave deletes 300 keys at one revision for a
// watcher that asked for fragments, and creates another watch on the
// stream after the first fragment: nothing may come between the
// fragments, or clientv3 drops it while it waits for the rest.
func TestFragmentsDoNotInterleave(t *testing.T) {
	s := bigDeleteSetup(t, true)
	created := false
	s.afterTurn = func(eff Effects) {
		for _, w := range eff.Watch {
			if w.Fragmented && w.Resp.Fragment && !created {
				created = true
				s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("z")}))
			}
		}
	}
	s.do(500, &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
	s.run()
	if !created {
		t.Fatal("the revision went in no fragments")
	}
	open := false
	for i, w := range s.watch {
		if open && !w.Fragmented {
			t.Fatalf("response %d, of watch %d, comes between the fragments of watch 0", i, w.Resp.WatchId)
		}
		open = w.Fragmented && w.Resp.Fragment
	}
}

// TestCatchUpGroupsAsEtcd writes three revisions that each pass a piece
// and watches from the first, as the difftest's fragments case does with
// 1 MiB: etcd sends the three in one response, or as three fragments to a
// watcher that asked for them. The response goes in parts.
func TestCatchUpGroupsAsEtcd(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 32 << 10 })
	var first int64
	for i := range 3 {
		r := s.do(uint64(i+1), &pb.PutRequest{Key: []byte("f"), Value: make([]byte, 33<<10)})
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if i == 0 {
			first = r.Resp.(*pb.PutResponse).Header.Revision
		}
	}
	for id, fragment := range []bool{false, true} {
		s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{WatchId: int64(id + 1), Key: []byte("f"), StartRevision: first, Fragment: fragment}))
		for range 10 {
			s.run()
		}
		var shape []string
		for _, w := range s.watch {
			if len(w.Resp.Events) == 0 {
				continue
			}
			part := strings.Repeat("e", len(w.Resp.Events))
			switch {
			case w.Cont:
				shape[len(shape)-1] += part
				continue
			case w.Resp.Fragment:
				part += "+"
			}
			shape = append(shape, part)
		}
		s.watch = nil
		want := "eee"
		if fragment {
			want = "e+ e+ e"
		}
		if got := strings.Join(shape, " "); got != want {
			t.Errorf("fragment %v: the catch-up came as %q, want %q", fragment, got, want)
		}
	}
}

// TestPartsHoldNothingPastTheRoom sends a delete of 300 keys in parts on a
// stream that has room for two responses a turn, while another watcher on
// the stream gets a write after each part. Its events must wait in the
// history, not pile up behind the parts and go out at once with the
// last, past the stream's room.
func TestPartsHoldNothingPastTheRoom(t *testing.T) {
	s := bigDeleteSetup(t, false)
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("z")}))
	s.run()
	s.responses()
	const slots = 2
	s.room = 1 << 30
	s.n.WatchRoom(1, s.room, slots)
	puts, most := 0, 0
	s.afterTurn = func(eff Effects) {
		most = max(most, len(eff.Watch))
		for _, w := range eff.Watch {
			if w.More {
				puts++
				s.n.Submit(Request{ID: uint64(1000 + puts), Op: &pb.PutRequest{Key: []byte("z"), Value: []byte("v")}})
			}
		}
		s.n.WatchRoom(1, s.room, slots)
	}
	s.do(500, &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
	for range 20 {
		s.run()
	}
	if puts < 2 {
		t.Fatalf("setup: %d writes during the parts", puts)
	}
	if most > slots {
		t.Fatalf("a turn sent %d responses on a stream with room for %d", most, slots)
	}
	events := 0
	for _, r := range s.responses() {
		if r.WatchId == 1 {
			events += len(r.Events)
		}
	}
	if events != puts {
		t.Fatalf("the other watcher got %d events of %d", events, puts)
	}
}

// TestFilteredCatchUpReadsNoValues catches up a NOPUT watcher over 100
// puts of 64 KiB whose values left memory, and one delete: it gets only
// the delete, so it must read none of the values it leaves out.
func TestFilteredCatchUpReadsNoValues(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) {
		c.PieceBytes = 32 << 10
		c.WindowBytes, c.WindowMaxBytes = 512, 1024
	})
	for i := range 100 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "p%03d", i), Value: make([]byte, 64<<10)}})
		s.run()
	}
	if r := s.do(500, &pb.DeleteRangeRequest{Key: []byte("p000")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	read := 0
	s.afterTurn = func(eff Effects) {
		for _, r := range eff.Reads {
			for _, f := range r.fills {
				read += int(f.Ref.Len)
			}
		}
	}
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("p"), RangeEnd: []byte("q"), StartRevision: 1, Filters: []pb.WatchCreateRequest_FilterType{pb.WatchCreateRequest_NOPUT}}))
	for range 10 {
		s.run()
	}
	events := 0
	for _, r := range s.responses() {
		events += len(r.Events)
	}
	if events != 1 {
		t.Fatalf("the NOPUT watcher got %d events, want the delete", events)
	}
	if read > 0 {
		t.Fatalf("the NOPUT watcher's catch-up read %d bytes of values it leaves out", read)
	}
}

// TestCatchUpSendsEachChangeOfAKey deletes the keys from "a" on and puts
// "a" again in one Txn, as the difffuzz case delete-then-put does,
// then watches from that revision: the watcher gets the delete and the
// put, in order, as etcd sends them, not the put twice; after a
// compaction at that revision, the put alone.
func TestCatchUpSendsEachChangeOfAKey(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.put(1, "old")
	r := s.do(2, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte{0}}}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("new")}}},
	}})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	rev := r.Resp.(*pb.TxnResponse).Header.Revision
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), StartRevision: rev, PrevKv: true}))
	for range 5 {
		s.run()
	}
	var got []string
	for _, r := range s.responses() {
		for _, ev := range r.Events {
			got = append(got, fmt.Sprintf("%v %s", ev.Type, ev.Kv.Value))
		}
	}
	if want := "DELETE  PUT new"; strings.Join(got, " ") != want {
		t.Fatalf("the watcher got %q, want %q", strings.Join(got, " "), want)
	}
	// A compaction at the revision removes the delete, as etcd's does:
	// a watcher from there gets the put alone.
	s.ok(3, &pb.CompactionRequest{Revision: rev})
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{WatchId: 5, Key: []byte("a"), StartRevision: rev}))
	for range 5 {
		s.run()
	}
	got = nil
	for _, r := range s.responses() {
		for _, ev := range r.Events {
			if r.WatchId == 5 {
				got = append(got, fmt.Sprintf("%v %s", ev.Type, ev.Kv.Value))
			}
		}
	}
	if want := "PUT new"; strings.Join(got, " ") != want {
		t.Fatalf("after a compaction at the revision the watcher got %q, want %q", strings.Join(got, " "), want)
	}
}
