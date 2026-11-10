package core

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// rest asks for the pieces of the answer to id after its first reply,
// and returns their KVs' count.
func (s *single) rest(id uint64, first Reply) int {
	s.t.Helper()
	n := 0
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
	}
	return n
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
	if n := s.rest(5001, r); n+len(first.Kvs) != 2000 || first.Count != 2000 {
		t.Fatalf("the pieces brought %d keys and the first %d, count %d, want 2000", n, len(first.Kvs), first.Count)
	}
}

// TestRangeFromCacheAllocatesOnce reads 3 MB whose values are in the
// cache, in pieces: each piece copies its values out of the cache's slabs
// into one buffer, so the read allocates its answer once, not twice
// (#430, #602).
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
		n := s.rest(id, r)
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
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > uint64(size)*3/2 {
		t.Fatalf("a Range of %d bytes from the cache allocated %d bytes", size, alloc)
	}
}

// TestAnswerKeepsCachedValue reads a value from the cache, then frees its
// slot and gives the slot to another value of its size: the answer, which
// the driver encodes later, must still hold the value it read, so a cache
// hit is copied out of the slabs (#602).
func TestAnswerKeepsCachedValue(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.MemoryBytes = 1 << 30; c.WindowBytes, c.WindowMaxBytes = 512, 1024 })
	id := uint64(0)
	put := func(key string, value []byte) {
		t.Helper()
		id++
		if r := s.do(id, &pb.PutRequest{Key: []byte(key), Value: value}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	// Small writes push an entry out of the window, and its value into
	// the cache.
	leave := func() {
		for i := range 20 {
			put(fmt.Sprintf("f%02d", i), make([]byte, 100))
		}
	}
	a, b := bytes.Repeat([]byte("a"), 1500), bytes.Repeat([]byte("b"), 1500)
	put("k", a)
	leave()
	hits := s.n.Metrics().CacheHits
	id++
	r := s.do(id, &pb.RangeRequest{Key: []byte("k")})
	if r.Err != nil || len(r.Range.KVs) != 1 || s.n.Metrics().CacheHits != hits+1 {
		t.Fatalf("the read of k: %v, %d values, %d cache hits", r.Err, len(r.Range.KVs), s.n.Metrics().CacheHits-hits)
	}
	got := r.Range.KVs[0].Value
	// Overwriting k frees its slot, and b takes it once its entry leaves.
	put("k", []byte("x"))
	put("other", b)
	leave()
	if !bytes.Equal(got, a) {
		t.Fatalf("the answer's value changed after its slot was taken again: %q...", got[:8])
	}
}
