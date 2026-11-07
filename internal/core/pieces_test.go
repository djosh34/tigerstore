package core

import (
	"fmt"
	"runtime"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// rest asks for the pieces of the answer to id after its first reply,
// and returns their KVs' count and the replies.
func (s *single) rest(id uint64, first Reply) (int, []Reply) {
	s.t.Helper()
	n, replies := 0, []Reply{first}
	for r := first; r.MorePieces; {
		s.n.Next(id)
		got := false
		for _, rep := range s.run() {
			if rep.ID == id {
				r, got = rep, true
			}
		}
		if !got {
			s.t.Fatalf("no piece of the answer to %d", id)
		}
		if r.Err != nil {
			s.t.Fatalf("a piece of the answer to %d: %v", id, r.Err)
		}
		n += len(r.Range.KVs)
		replies = append(replies, r)
	}
	return n, replies
}

// TestTxnRangeIsBounded reads 3 MB inside a Txn: its first reply holds
// at most a piece of the answer, as a plain Range does, and the pieces
// bring the rest (#427).
func TestTxnRangeIsBounded(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.PieceBytes = 64 << 10 })
	for i := range 2000 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%05d", i), Value: make([]byte, 1500)}})
		if i%100 == 99 {
			s.run()
		}
	}
	all := &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}
	r := s.do(5001, &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: all}}}})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	first := r.Resp.(*pb.TxnResponse).Responses[0].GetResponseRange()
	if !r.MorePieces || len(r.Rests) != 1 || len(first.Kvs) > 64<<10/1500+1 {
		t.Fatalf("a Txn's range of 3 MB answered %d keys at once, rest %v", len(first.Kvs), r.MorePieces)
	}
	if n, _ := s.rest(5001, r); n+len(first.Kvs) != 2000 || first.Count != 2000 {
		t.Fatalf("the pieces brought %d keys and the first %d, count %d, want 2000", n, len(first.Kvs), first.Count)
	}
}

// TestRangeFromCacheAllocatesOnce reads 3 MB whose values are in the
// cache, in pieces: the values come as the cache holds them, so the read
// allocates far less than the answer (#430).
func TestRangeFromCacheAllocatesOnce(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.MemoryBytes = 1 << 30; c.WindowBytes, c.WindowMaxBytes = 512, 1024 })
	for i := range 2000 {
		if r := s.do(uint64(i+1), &pb.PutRequest{Key: fmt.Appendf(nil, "k%05d", i), Value: make([]byte, 1500)}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	all := &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}
	read := func(id uint64) int {
		r := s.do(id, all)
		n, _ := s.rest(id, r)
		return n + len(r.Range.KVs)
	}
	read(9000) // warm the cache
	hits := s.n.Metrics().CacheHits
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	n := read(9001)
	runtime.ReadMemStats(&after)
	size := 2000 * (6 + 1500)
	t.Logf("answer %d keys of %d bytes, %d cache hits, %d bytes allocated", n, size, s.n.Metrics().CacheHits-hits, after.TotalAlloc-before.TotalAlloc)
	if n != 2000 {
		t.Fatalf("the answer has %d keys, want 2000", n)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > uint64(size)/2 {
		t.Fatalf("a Range of %d bytes from the cache allocated %d bytes", size, alloc)
	}
}
