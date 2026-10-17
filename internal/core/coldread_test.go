package core

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// coldKeys are the keys coldSingle writes, each with value "v" + key.
var coldKeys = []string{"a", "b", "c", "d", "e"}

// coldSingle starts a single node with a tiny window, writes coldKeys,
// then enough other keys that their entries leave memory.
func coldSingle(t *testing.T, change func(*Config)) (*single, *simfs.Disk) {
	t.Helper()
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		if change != nil {
			change(cfg)
		}
	})
	for i, k := range coldKeys {
		if r := s.do(uint64(i+1), &pb.PutRequest{Key: []byte(k), Value: []byte("v" + k)}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	last := s.n.Applied()
	fill(t, s, 100, 20)
	if s.n.FirstIndex() <= last {
		t.Fatalf("the values' entries are still in memory: first index %d, last value at %d", s.n.FirstIndex(), last)
	}
	return s, disk
}

// fill writes n keys f0, f1... of 64 bytes each, with request IDs from
// id on.
func fill(t *testing.T, s *single, id uint64, n int) {
	t.Helper()
	for i := range n {
		if r := s.do(id+uint64(i), &pb.PutRequest{Key: fmt.Appendf(nil, "f%d", i), Value: make([]byte, 64)}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
}

// replies returns the replies run gives, by request ID.
func replies(s *single) map[uint64]Reply {
	out := map[uint64]Reply{}
	for _, r := range s.run() {
		out[r.ID] = r
	}
	return out
}

// release runs the held value reads and returns the replies after them.
func release(t testing.TB, s *single) map[uint64]Reply {
	t.Helper()
	held := s.held
	s.held, s.hold = nil, false
	for _, r := range held {
		s.n.ValuesRead(r, r.Run())
	}
	return replies(s)
}

// TestColdRepliesWait sends every kind of request whose response carries
// values while those values are on disk only: reads of d and e, writes of
// a, b and c. No reply comes while their reads are held, the node goes on
// with other writes, ticks and a compaction meanwhile, and every reply is
// right once the reads are done.
func TestColdRepliesWait(t *testing.T) {
	s, _ := coldSingle(t, nil)
	wire, err := proto.Marshal(&pb.PutRequest{Key: []byte("b"), Value: []byte("wire"), PrevKv: true})
	if err != nil {
		t.Fatal(err)
	}
	put, err := ParsePut(wire)
	if err != nil {
		t.Fatal(err)
	}
	// A missing key has mod revision 0.
	mod := func(rev int64) []*pb.Compare {
		return []*pb.Compare{{Key: []byte("zz"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}}
	}
	// ops ranges over k, after a put if write is set.
	ops := func(k string, write bool) []*pb.RequestOp {
		var out []*pb.RequestOp
		if write {
			out = append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("t"), Value: []byte("1")}}})
		}
		return append(out, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(k)}}})
	}
	all := &pb.RangeRequest{Key: []byte("d"), RangeEnd: []byte("f")}
	reqs := []Request{
		{ID: 201, Op: &pb.RangeRequest{Key: []byte("d")}},
		{ID: 202, Op: all},
		{ID: 203, Op: all, Compact: true},
		{ID: 204, Op: all, Chunk: true},
		{ID: 205, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("new"), PrevKv: true}},
		{ID: 206, Put: put},
		{ID: 207, Op: &pb.DeleteRangeRequest{Key: []byte("c"), PrevKv: true}},
		{ID: 208, Op: &pb.TxnRequest{Compare: mod(0), Success: ops("d", false), Failure: ops("e", false)}},
		{ID: 209, Op: &pb.TxnRequest{Compare: mod(9), Success: ops("d", false), Failure: ops("e", false)}},
		{ID: 210, Op: &pb.TxnRequest{Compare: mod(0), Success: ops("d", true), Failure: ops("e", true)}},
		{ID: 211, Op: &pb.TxnRequest{Compare: mod(9), Success: ops("d", true), Failure: ops("e", true)}},
	}
	s.hold = true
	for _, r := range reqs {
		s.n.Submit(r)
	}
	if got := replies(s); len(got) > 0 {
		t.Fatalf("%d replies before their values were read", len(got))
	}
	if len(s.held) == 0 {
		t.Fatal("no value reads")
	}
	// The committer goes on: a write, ticks and a compaction.
	if r := s.do(301, &pb.PutRequest{Key: []byte("x"), Value: []byte("1")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	for range 30 {
		s.n.Tick()
		s.run()
	}
	if s.n.Leader() != 1 {
		t.Fatal("the node lost its leadership while reads waited")
	}
	if r := s.do(302, &pb.CompactionRequest{Revision: s.n.kv.Rev()}); r.Err != nil {
		t.Fatal(r.Err)
	}
	got := release(t, s)
	value := func(kvs []*mvccpb.KeyValue) string {
		var out []string
		for _, kv := range kvs {
			out = append(out, string(kv.Key)+"="+string(kv.Value))
		}
		return fmt.Sprint(out)
	}
	for id, want := range map[uint64]string{
		201: "[d=vd]",
		202: "[d=vd e=ve]",
		204: "[d=vd e=ve]",
	} {
		if r := got[id]; r.Err != nil || value(r.Resp.(*pb.RangeResponse).Kvs) != want {
			t.Fatalf("range %d: %v, want %s", id, r, want)
		}
	}
	if r := got[203]; r.Err != nil || len(r.Range.KVs) != 2 || string(r.Range.KVs[1].Value) != "ve" {
		t.Fatalf("range without messages: %+v", r)
	}
	if r := got[205]; r.Err != nil || string(r.Resp.(*pb.PutResponse).PrevKv.Value) != "va" {
		t.Fatalf("put with prev_kv: %v", r)
	}
	if r := got[206]; r.Err != nil || string(r.PrevKv.Value) != "vb" {
		t.Fatalf("put in wire form with prev_kv: %+v", r)
	}
	if r := got[207]; r.Err != nil || value(r.Resp.(*pb.DeleteRangeResponse).PrevKvs) != "[c=vc]" {
		t.Fatalf("delete with prev_kv: %v", r)
	}
	for id, want := range map[uint64]string{208: "[d=vd]", 209: "[e=ve]", 210: "[d=vd]", 211: "[e=ve]"} {
		r := got[id]
		if r.Err != nil {
			t.Fatalf("transaction %d: %v", id, r.Err)
		}
		resps := r.Resp.(*pb.TxnResponse).Responses
		if v := value(resps[len(resps)-1].GetResponseRange().Kvs); v != want {
			t.Fatalf("transaction %d's range: %s, want %s", id, v, want)
		}
	}
}

// TestColdWatchEventsWait watches with previous values, writes keys whose
// previous values are on disk only and compacts while those reads are
// held. A progress notification asked for meanwhile waits behind the
// events, and once the reads are done every event holds the values and
// revisions from when it was applied.
func TestColdWatchEventsWait(t *testing.T) {
	s, _ := coldSingle(t, nil)
	create := func(stream uint64, c *pb.WatchCreateRequest) {
		s.n.Watch(stream, Caller{}, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: c}})
	}
	create(1, &pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("d"), PrevKv: true})
	s.run()
	s.hold = true
	if r := s.do(201, &pb.PutRequest{Key: []byte("a"), Value: []byte("new")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	rev := s.n.kv.Rev()
	if r := s.do(202, &pb.DeleteRangeRequest{Key: []byte("b")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	// A watch from the start catches up on values on disk only.
	create(2, &pb.WatchCreateRequest{Key: []byte("c"), StartRevision: 1})
	s.n.Watch(1, Caller{}, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}})
	if r := s.do(203, &pb.CompactionRequest{Revision: s.n.kv.Rev()}); r.Err != nil {
		t.Fatal(r.Err)
	}
	s.run()
	for _, w := range s.watch {
		if !w.Resp.Created {
			t.Fatalf("stream %d got %v before the values were read", w.Stream, w.Resp)
		}
	}
	s.watch = nil
	release(t, s)
	var on1 []*pb.WatchResponse
	var on2 []*mvccpb.Event
	for _, w := range s.watch {
		if w.Stream == 1 {
			on1 = append(on1, w.Resp)
		} else {
			on2 = append(on2, w.Resp.Events...)
		}
	}
	var evs []*mvccpb.Event
	for _, r := range on1[:len(on1)-1] {
		evs = append(evs, r.Events...)
	}
	if len(evs) != 2 || string(evs[0].Kv.Value) != "new" || string(evs[0].PrevKv.Value) != "va" || evs[0].Kv.ModRevision != rev ||
		evs[1].Type != mvccpb.Event_DELETE || string(evs[1].PrevKv.Value) != "vb" || evs[1].Kv.ModRevision != rev+1 {
		t.Fatalf("events after the reads: %v", evs)
	}
	if p := on1[len(on1)-1]; p.WatchId != invalidWatchID || len(p.Events) > 0 {
		t.Fatalf("the last response on the stream is %v, want the progress notification", p)
	}
	if len(on2) != 1 || string(on2[0].Kv.Value) != "vc" {
		t.Fatalf("catch-up events: %v", on2)
	}
}

// TestColdValueCompareWaitsInOrder sends a transaction that compares a
// value on disk only, and a write after it. Both wait for the read, in
// order, while the node goes on ticking and serving reads of the state
// before them. A read that sorts by values on disk waits too.
func TestColdValueCompareWaitsInOrder(t *testing.T) {
	s, _ := coldSingle(t, nil)
	if r := s.do(200, &pb.PutRequest{Key: []byte("h"), Value: []byte("in memory")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	s.hold = true
	s.n.Submit(Request{ID: 201, Op: &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("va")}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("swapped")}}}},
	}})
	s.n.Submit(Request{ID: 202, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("after")}})
	s.n.Submit(Request{ID: 203, Op: &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("d"), SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_DESCEND, Serializable: true}})
	if got := replies(s); len(got) > 0 {
		t.Fatalf("replies before the values were read: %v", got)
	}
	before := s.n.Applied()
	for range 30 {
		s.n.Tick()
		s.run()
	}
	if s.n.Leader() != 1 || s.n.Applied() != before {
		t.Fatalf("leader %d, applied %d, want 1 and %d", s.n.Leader(), s.n.Applied(), before)
	}
	if r := s.do(301, &pb.RangeRequest{Key: []byte("h"), Serializable: true}); r.Err != nil || len(r.Resp.(*pb.RangeResponse).Kvs) != 1 {
		t.Fatalf("read while apply waits: %v", r)
	}
	got := release(t, s)
	if r := got[201]; r.Err != nil || !r.Resp.(*pb.TxnResponse).Succeeded {
		t.Fatalf("value compare: %v", r)
	}
	if r := got[202]; r.Err != nil || r.Resp.(*pb.PutResponse).Header.Revision != got[201].Resp.(*pb.TxnResponse).Header.Revision+1 {
		t.Fatalf("write after the compare: %v", r)
	}
	var keys []string
	for _, kv := range got[203].Resp.(*pb.RangeResponse).GetKvs() {
		keys = append(keys, string(kv.Key))
	}
	if !slices.Equal(keys, []string{"c", "b", "a"}) {
		t.Fatalf("sort by value: %v, %v", keys, got[203].Err)
	}
	r := s.do(302, &pb.RangeRequest{Key: []byte("a")})
	if v := r.Resp.(*pb.RangeResponse).Kvs[0].Value; string(v) != "after" {
		t.Fatalf("a is %q", v)
	}
}

// TestColdReadKeepsItsSegment holds a read of an old value, overwrites
// every key, compacts the old values away and cuts checkpoints: the
// segment with the value stays until the read is done.
func TestColdReadKeepsItsSegment(t *testing.T) {
	s, disk := coldSingle(t, func(cfg *Config) {
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	})
	first := disk.DurableNames()[0]
	s.hold = true
	s.n.Submit(Request{ID: 201, Op: &pb.RangeRequest{Key: []byte("a"), Revision: 2, Serializable: true}})
	s.run()
	if len(s.held) != 1 {
		t.Fatalf("%d value reads, want 1", len(s.held))
	}
	s.hold = false
	id := uint64(300)
	churn := func() {
		for range 5 {
			for i, k := range coldKeys {
				if r := s.do(id+60+uint64(i), &pb.PutRequest{Key: []byte(k), Value: []byte("newer")}); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			fill(t, s, id+1, 40)
			if r := s.do(id+50, &pb.CompactionRequest{Revision: s.n.kv.Rev()}); r.Err != nil {
				t.Fatal(r.Err)
			}
			id += 100
		}
	}
	churn()
	if s.n.DurableCheckpoint().Index == 0 || disk.DurableNames()[0] != first {
		t.Fatalf("checkpoint %d, first file %s, want a checkpoint and %s", s.n.DurableCheckpoint().Index, disk.DurableNames()[0], first)
	}
	got := release(t, s)
	if r := got[201]; r.Err != nil || string(r.Resp.(*pb.RangeResponse).Kvs[0].Value) != "va" {
		t.Fatalf("old value: %v", r)
	}
	churn()
	if disk.DurableNames()[0] == first {
		t.Fatal("the segment stayed after its read was done")
	}
}

// TestPersistedInstallEndsTheCore checks that a durable snapshot install
// ends the core with ErrInstalled, which both drivers act on by starting a
// new core from the log, and that the core then answers nothing more.
func TestPersistedInstallEndsTheCore(t *testing.T) {
	s, _ := coldSingle(t, nil)
	s.n.Persisted(PersistJob{Install: &Install{From: 2, Index: s.n.Applied(), Term: s.n.Term(), Applied: s.n.Applied()}})
	_, err := s.n.Process()
	if !errors.Is(err, ErrInstalled) || FailureOf(err) != Reload {
		t.Fatalf("after a durable install: %v", err)
	}
}

// TestReplaySortsOwnWrites restarts a node whose log holds a transaction
// that sorts by value a key it wrote itself. Replay has that entry only
// from the log, not in memory, and must still read the transaction's own
// value at once.
func TestReplaySortsOwnWrites(t *testing.T) {
	disk := simfs.New()
	s := startSingle(t, disk, 1)
	r := s.do(1, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a"), SortTarget: pb.RangeRequest_VALUE}}},
	}})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	want := s.n.Hash()
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	s = startSingle(t, disk, 2)
	if s.n.Hash() != want {
		t.Fatal("the restarted node's state differs")
	}
}

// TestCompareBatches runs a VALUE compare over 30 values of 100 KiB on
// disk only, in a read-only transaction and in one that writes. Each
// read the core asks for must stay near one batch, a paused write must
// hold no more than one batch it read, and both must answer right.
func TestCompareBatches(t *testing.T) {
	const keys, size = 30, 100 << 10
	value := bytes.Repeat([]byte("v"), size)
	s, _ := coldSingle(t, nil)
	for i := range keys {
		if r := s.do(uint64(500+i), &pb.PutRequest{Key: fmt.Appendf(nil, "big%02d", i), Value: value}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	fill(t, s, 600, 20)
	compare := []*pb.Compare{{Key: []byte("big"), RangeEnd: []byte("bih"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: value}}}
	write := []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("x"), Value: []byte("1")}}}}
	for _, tc := range []struct {
		name string
		id   uint64
		txn  *pb.TxnRequest
	}{
		{"read-only", 700, &pb.TxnRequest{Compare: compare}},
		{"write", 701, &pb.TxnRequest{Compare: compare, Success: write}},
	} {
		s.hold = true
		s.n.Submit(Request{ID: tc.id, Op: tc.txn})
		reads := 0
		for got := replies(s); ; got = replies(s) {
			if r, ok := got[tc.id]; ok {
				if r.Err != nil || !r.Resp.(*pb.TxnResponse).Succeeded {
					t.Fatalf("%s: %v, %v", tc.name, r.Resp, r.Err)
				}
				break
			}
			if len(s.held) == 0 {
				t.Fatalf("%s: no reply and no read", tc.name)
			}
			held := s.held
			s.held = nil
			for _, r := range held {
				n := 0
				for _, f := range r.fills {
					n += int(f.Ref.Len)
				}
				if n > 1<<20+size {
					t.Fatalf("%s: a read of %d bytes", tc.name, n)
				}
				reads++
				s.n.ValuesRead(r, r.Run())
			}
			if w := s.n.paused; w != nil {
				n := 0
				for _, b := range w.fetched {
					n += len(b)
				}
				if n > 1<<20+size {
					t.Fatalf("%s: the paused write holds %d bytes it read", tc.name, n)
				}
			}
		}
		if reads < 3 {
			t.Fatalf("%s: %d reads, want one per batch", tc.name, reads)
		}
	}
	s.hold = false
	if kvs := s.do(800, &pb.RangeRequest{Key: []byte("x")}).Resp.(*pb.RangeResponse).Kvs; len(kvs) != 1 {
		t.Fatalf("the write's put: %v", kvs)
	}
}
