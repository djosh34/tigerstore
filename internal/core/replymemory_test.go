package core

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestRangeReplyMemory repeats the shape of #214: sixteen clients each
// ask for a thousand values. Hold disk reads, then hold completed replies
// as a slow transport would. Neither stage may exceed the node's budget.
func TestRangeReplyMemory(t *testing.T) {
	const memory = 12 << 20
	s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) {
		cfg.MemoryBytes = memory
		cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
	})
	value := bytes.Repeat([]byte("v"), 1024)
	for i := range 1000 {
		if r := s.do(uint64(i+1), &pb.PutRequest{Key: fmt.Appendf(nil, "k%04d", i), Value: value}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	fill(t, s, 2000, 200)
	s.n.cache.setLimit(0)
	s.hold = true
	for i := range 16 {
		s.n.Submit(Request{ID: uint64(3000 + i), Compact: true, Op: &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Limit: 1000}})
	}
	if got := s.run(); len(got) != 0 {
		t.Fatalf("cold answers escaped held reads: %d", len(got))
	}
	if len(s.held) == 0 || len(s.held) >= 16 || len(s.n.replyWait) == 0 {
		t.Fatalf("range answers were not bounded: reading=%d waiting=%d", len(s.held), len(s.n.replyWait))
	}
	checkBudget := func() {
		t.Helper()
		used := s.n.replyBytes + s.n.cache.bytes + s.n.kv.Bytes() + s.n.EntryBytes()
		if s.n.replyBytes <= 0 || used > memory {
			t.Fatalf("answer reservations=%d, total=%d, budget=%d", s.n.replyBytes, used, memory)
		}
	}
	checkBudget()
	seen := map[uint64]bool{}
	for len(seen) < 16 {
		got := release(t, s)
		if len(got) == 0 {
			t.Fatal("no answers after releasing the readers")
		}
		checkBudget()
		if extra := s.run(); len(extra) != 0 {
			t.Fatalf("%d answers passed replies still held by the transport", len(extra))
		}
		for id, r := range got {
			if seen[id] || r.Err != nil || len(r.Range.KVs) != 1000 || r.Range.Count != 1000 || r.Range.More || r.Held == 0 {
				t.Fatalf("reply %d: duplicate=%v err=%v keys=%d count=%d more=%v held=%d", id, seen[id], r.Err, len(r.Range.KVs), r.Range.Count, r.Range.More, r.Held)
			}
			for i, kv := range r.Range.KVs {
				if string(kv.Key) != fmt.Sprintf("k%04d", i) || !bytes.Equal(kv.Value, value) {
					t.Fatalf("reply %d has a wrong key or value at %d", id, i)
				}
			}
			seen[id] = true
			s.n.ReleaseReplyBytes(r.Held)
		}
		s.n.cache.setLimit(0)
		s.hold = true
		if got := s.run(); len(got) != 0 {
			t.Fatalf("cold answers escaped held reads: %d", len(got))
		}
	}
	if s.n.replyBytes != 0 || len(s.n.replyWait) != 0 {
		t.Fatalf("reservations=%d waiting=%d after every answer", s.n.replyBytes, len(s.n.replyWait))
	}
}

func TestRangeReplySortedMemory(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	s.n.cache.setLimit(0)
	s.hold = true
	s.n.Submit(Request{ID: 1000, Compact: true, Op: &pb.RangeRequest{
		Key: []byte("a"), RangeEnd: []byte("f"), Limit: 2,
		SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_DESCEND,
	}})
	if got := s.run(); len(got) != 0 || len(s.held) != 1 || s.n.replyBytes <= 0 {
		t.Fatalf("sort values were not reserved: replies=%d reads=%d bytes=%d", len(got), len(s.held), s.n.replyBytes)
	}
	got := release(t, s)
	r := got[1000]
	if r.Err != nil || len(r.Range.KVs) != 2 || string(r.Range.KVs[0].Value) != "ve" || r.Held == 0 || s.n.replyBytes != r.Held {
		t.Fatalf("sort lost its reservation or answer: err=%v keys=%v held=%d reserved=%d", r.Err, r.Range.KVs, r.Held, s.n.replyBytes)
	}
	s.n.ReleaseReplyBytes(r.Held)
	if s.n.replyBytes != 0 {
		t.Fatalf("sort retained %d bytes", s.n.replyBytes)
	}
}

func TestRangeReplyMemoryTimeoutAndOversize(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	// A transport holds all the available space. Serializable ranges
	// must time out too, even though they bypass the read-index queue.
	held := s.n.cfg.MemoryBytes - s.n.kv.Bytes() - s.n.EntryBytes()
	s.n.SetReplyBytes(held)
	s.n.Submit(Request{ID: 1000, Compact: true, Op: &pb.RangeRequest{Key: []byte("a"), Serializable: true}})
	if got := s.run(); len(got) != 0 || len(s.n.replyWait) != 1 {
		t.Fatalf("read did not wait: replies=%d waiting=%d", len(got), len(s.n.replyWait))
	}
	for range s.n.cfg.RequestTicks {
		s.n.Tick()
	}
	got := s.run()
	if len(got) != 1 || !errors.Is(got[0].Err, rpctypes.ErrGRPCTimeout) || got[0].Held != 0 {
		t.Fatalf("waiting range did not time out: %+v", got)
	}
	s.n.ReleaseReplyBytes(held)
	if s.n.replyBytes != 0 {
		t.Fatalf("timed-out range retained %d bytes", s.n.replyBytes)
	}

	// With no other answer out, a read goes even when the index and the
	// entries fill the budget.
	s.n.cfg.MemoryBytes = s.n.kv.Bytes() + s.n.EntryBytes() + 1
	s.n.Submit(Request{ID: 1001, Compact: true, Op: &pb.RangeRequest{Key: []byte("a"), Serializable: true}})
	got = s.run()
	if len(got) != 1 || got[0].Err != nil || got[0].Held == 0 || s.n.replyBytes != got[0].Held {
		t.Fatalf("range with a full index: %+v, held=%d", got, s.n.replyBytes)
	}
	s.n.ReleaseReplyBytes(got[0].Held)

	// Only an answer larger than the whole budget is refused.
	s.n.cfg.MemoryBytes = 1
	s.n.Submit(Request{ID: 1002, Compact: true, Op: &pb.RangeRequest{Key: []byte("a"), Serializable: true}})
	got = s.run()
	if len(got) != 1 || status.Code(got[0].Err) != codes.ResourceExhausted || s.n.replyBytes != 0 {
		t.Fatalf("oversized range: %+v, held=%d", got, s.n.replyBytes)
	}
	if s.n.Err() != nil {
		t.Fatal(s.n.Err())
	}
}
