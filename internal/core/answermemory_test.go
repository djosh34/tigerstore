package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func answerRange() *pb.RangeRequest {
	return &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("f"), Limit: 5}
}

func answerTxn(write bool) *pb.TxnRequest {
	ops := []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: answerRange()}}}
	if write {
		ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("written"), Value: []byte("yes")}}})
	}
	return &pb.TxnRequest{Success: ops}
}

func TestAnswerMemoryReads(t *testing.T) {
	for _, kind := range []string{"range", "chunk", "txn"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := coldSingle(t, nil)
			s.n.cfg.MemoryBytes = 16 << 10
			s.n.cache.setLimit(0)
			s.hold = true
			for i := range 8 {
				r := Request{ID: uint64(1000 + i), Op: answerRange(), Chunk: kind == "chunk"}
				if kind == "txn" {
					r.Op = answerTxn(false)
				}
				s.n.Submit(r)
			}
			if got := s.run(); len(got) != 0 || len(s.held) == 0 || len(s.held) >= 8 || len(s.n.replyWait) == 0 || s.n.replyBytes == 0 {
				t.Fatalf("answers not bounded: replies=%d reads=%d waiting=%d bytes=%d", len(got), len(s.held), len(s.n.replyWait), s.n.replyBytes)
			}
			seen := 0
			for seen < 8 {
				got := release(t, s)
				if len(got) == 0 {
					t.Fatal("no progress after releasing reads")
				}
				for _, r := range got {
					if r.Err != nil || r.Held <= 0 {
						t.Fatalf("unreserved answer: %+v", r)
					}
					var rr *pb.RangeResponse
					if kind == "txn" {
						rr = r.Resp.(*pb.TxnResponse).Responses[0].GetResponseRange()
					} else {
						rr = r.Resp.(*pb.RangeResponse)
					}
					if len(rr.Kvs) != 5 || string(rr.Kvs[0].Value) != "va" || string(rr.Kvs[4].Value) != "ve" {
						t.Fatalf("wrong answer: %v", rr)
					}
					s.n.ReleaseReplyBytes(r.Held)
					seen++
				}
				s.n.cache.setLimit(0)
				s.hold = true
				if got := s.run(); len(got) != 0 {
					t.Fatalf("cold answers escaped: %v", got)
				}
			}
			if s.n.replyBytes != 0 {
				t.Fatalf("retained %d bytes", s.n.replyBytes)
			}
		})
	}
}

func TestAnswerMemoryTxnDropsPartialFills(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.cache.setLimit(0)
	held := s.n.cfg.MemoryBytes - s.n.kv.Bytes() - s.n.EntryBytes() - 6000
	s.n.SetReplyBytes(held)
	txn := &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: answerRange()}},
	}}
	s.n.Submit(Request{ID: 1000, Op: txn})
	if got := s.run(); len(got) != 0 || len(s.n.replyWait) != 1 || len(s.n.cold) != 0 || s.n.replyBytes != held {
		t.Fatalf("partial transaction: replies=%v waits=%d reads=%d bytes=%d", got, len(s.n.replyWait), len(s.n.cold), s.n.replyBytes)
	}
	if fills := s.n.kv.TakeFills(); len(fills) != 0 {
		t.Fatalf("refused transaction retained %d unreserved fills", len(fills))
	}
	s.n.ReleaseReplyBytes(held)
	got := s.run()
	if len(got) != 1 || got[0].Err != nil || got[0].Held == 0 {
		t.Fatalf("transaction did not resume: %+v", got)
	}
	s.n.ReleaseReplyBytes(got[0].Held)
	s.n.cfg.MemoryBytes = 6000
	s.n.cache.setLimit(0)
	s.hold = true
	s.n.Submit(Request{ID: 1001, Op: txn})
	got = s.run()
	if len(got) != 1 || status.Code(got[0].Err) != codes.ResourceExhausted || len(s.held) != 0 || s.n.replyBytes != 0 {
		t.Fatalf("oversized transaction read discarded values: replies=%v reads=%d bytes=%d", got, len(s.held), s.n.replyBytes)
	}
}

func TestAnswerMemoryCommittedTxn(t *testing.T) {
	for _, memory := range []int64{1 << 20, 1} {
		s, _ := coldSingle(t, nil)
		s.n.cfg.MemoryBytes = memory
		s.n.cache.setLimit(0)
		r := s.do(1000, answerTxn(true))
		if memory == 1 {
			if status.Code(r.Err) != codes.Unavailable || r.Held != 0 {
				t.Fatalf("oversized committed answer: %+v", r)
			}
		} else {
			if r.Err != nil || r.Held == 0 {
				t.Fatalf("committed answer not reserved: %+v", r)
			}
			s.n.ReleaseReplyBytes(r.Held)
		}
		// A local answer refusal must not suppress any committed writes.
		got, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("written"), KeysOnly: true})
		if err != nil || len(got.Kvs) != 1 || s.n.Err() != nil || s.n.replyBytes != 0 {
			t.Fatalf("apply changed by answer budget: response=%v err=%v node=%v bytes=%d", got, err, s.n.Err(), s.n.replyBytes)
		}
	}
}

func TestAnswerMemorySortedTxn(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.cache.setLimit(0)
	s.hold = true
	rangeOp := answerRange()
	rangeOp.SortTarget, rangeOp.SortOrder, rangeOp.Limit = pb.RangeRequest_VALUE, pb.RangeRequest_DESCEND, 2
	nested := &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: rangeOp}}}}
	txn := &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("missing"), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 1}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestTxn{RequestTxn: nested}}},
	}
	s.n.Submit(Request{ID: 1000, Op: txn})
	if got := s.run(); len(got) != 0 || len(s.held) == 0 || s.n.replyBytes == 0 {
		t.Fatalf("sort not reserved: replies=%d reads=%d bytes=%d", len(got), len(s.held), s.n.replyBytes)
	}
	r := release(t, s)[1000]
	if r.Err != nil || r.Held == 0 {
		t.Fatalf("sort response: %+v", r)
	}
	out := r.Resp.(*pb.TxnResponse)
	kvs := out.Responses[0].GetResponseTxn().Responses[0].GetResponseRange().Kvs
	if out.Succeeded || len(kvs) != 2 || string(kvs[0].Value) != "ve" || string(kvs[1].Value) != "vd" {
		t.Fatalf("wrong sorted transaction: %v", out)
	}
	s.n.ReleaseReplyBytes(r.Held)
	if s.n.replyBytes != 0 {
		t.Fatalf("sort retained %d bytes", s.n.replyBytes)
	}
}

func TestAnswerMemoryLiveWatch(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), PrevKv: true}))
	s.run()
	s.watch = nil
	s.put(1000, "next")
	if len(s.watch) != 1 || s.watch[0].Held == 0 || s.n.replyBytes != s.watch[0].Held {
		t.Fatalf("live event not reserved: responses=%v bytes=%d", s.watch, s.n.replyBytes)
	}
	ev := s.watch[0].Resp.Events[0]
	if string(ev.Kv.Value) != "next" || ev.PrevKv == nil || string(ev.PrevKv.Value) != "va" {
		t.Fatalf("wrong live event: %v", ev)
	}
	s.n.ReleaseReplyBytes(s.watch[0].Held)
	s.watch = nil
	// A busy watch cannot hold back a committed write or read its values.
	held := s.n.cfg.MemoryBytes
	s.n.SetReplyBytes(held)
	s.put(1001, "last")
	if len(s.watch) != 1 || !s.watch[0].Resp.Canceled || s.watch[0].Held != 0 || s.n.replyBytes != held {
		t.Fatalf("busy watch: responses=%v bytes=%d", s.watch, s.n.replyBytes)
	}
	s.n.ReleaseReplyBytes(held)
}

func TestAnswerMemoryWatchKeepsRevisionWhole(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.cfg.WindowBytes, s.n.cfg.WindowMaxBytes = 1<<20, 1<<20
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("c")}))
	s.run()
	s.watch = nil
	rev := s.n.kv.Rev() + 1
	// One event fits, but the whole two-key revision and the transaction
	// answer do not. A client must never see just the first event.
	held := s.n.cfg.MemoryBytes - s.n.kv.Bytes() - s.n.EntryBytes() - 1800
	s.n.SetReplyBytes(held)
	var ops []*pb.RequestOp
	for _, key := range []string{"a", "b"} {
		ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte("next")}}})
	}
	r := s.do(1000, &pb.TxnRequest{Success: ops})
	if status.Code(r.Err) != codes.Unavailable {
		t.Fatalf("expected a committed answer refusal, got %v", r.Err)
	}
	if len(s.watch) != 1 || !s.watch[0].Resp.Canceled || len(s.watch[0].Resp.Events) != 0 || s.watch[0].Held == 0 {
		t.Fatalf("memory cancellation sent a partial revision: %+v", s.watch)
	}
	s.n.ReleaseReplyBytes(s.watch[0].Held)
	s.n.ReleaseReplyBytes(held)
	s.watch = nil
	// Resume from the last delivered revision, not from the cancellation
	// header. Both committed changes must still be delivered together.
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("c"), StartRevision: rev}))
	s.run()
	if len(s.watch) != 2 || !s.watch[0].Resp.Created || len(s.watch[1].Resp.Events) != 2 {
		t.Fatalf("resume lost part of the revision: %+v", s.watch)
	}
	for _, ev := range s.watch[1].Resp.Events {
		if ev.Kv.ModRevision != rev || string(ev.Kv.Value) != "next" {
			t.Fatalf("wrong resumed event: %v", ev)
		}
	}
	s.n.ReleaseReplyBytes(s.watch[1].Held)
	if s.n.replyBytes != 0 {
		t.Fatalf("canceled watch retained %d bytes", s.n.replyBytes)
	}
}

func TestAnswerMemoryWatch(t *testing.T) {
	s, _ := coldSingle(t, nil)
	s.n.cfg.MemoryBytes = 16 << 10
	s.n.cache.setLimit(0)
	s.hold = true
	for id := uint64(1); id <= 8; id++ {
		s.n.Watch(id, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("f"), StartRevision: 1, PrevKv: true}))
		s.run()
	}
	canceled := 0
	for _, w := range s.watch {
		if w.Resp.Canceled {
			canceled++
		}
	}
	if canceled == 0 || canceled == 8 || s.n.replyBytes <= 0 || s.n.replyBytes > s.n.cfg.MemoryBytes {
		t.Fatalf("watch history not bounded: canceled=%d bytes=%d", canceled, s.n.replyBytes)
	}
	s.watch = nil
	release(t, s)
	for _, w := range s.watch {
		if len(w.Resp.Events) != 5 || w.Held == 0 || string(w.Resp.Events[0].Kv.Value) != "va" {
			t.Fatalf("watch answer: %+v", w)
		}
		s.n.ReleaseReplyBytes(w.Held)
	}
	if s.n.replyBytes != 0 {
		t.Fatalf("watch retained %d bytes", s.n.replyBytes)
	}
}

func TestAnswerMemoryClosedWatchRead(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.cache.setLimit(0)
	s.hold = true
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), StartRevision: 1}))
	s.run()
	held := s.n.replyBytes
	if held == 0 || len(s.held) == 0 {
		t.Fatal("watch did not reserve its cold read")
	}
	s.n.CloseWatchStream(1)
	if s.n.replyBytes != held {
		t.Fatal("close released a read still running")
	}
	s.watch = nil
	release(t, s)
	if s.n.replyBytes != 0 || len(s.watch) != 0 {
		t.Fatalf("closed watch: bytes=%d responses=%d", s.n.replyBytes, len(s.watch))
	}
}
