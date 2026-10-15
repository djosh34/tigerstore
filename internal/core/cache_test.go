package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestCacheLRU checks that the least recently used value goes first once
// the cache is past its limit, and that a value it drops or that does
// not fit is gone.
func TestCacheLRU(t *testing.T) {
	c := newCache()
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: 10} }
	c.add(ref(1), make([]byte, 10))
	if len(c.items) != 0 {
		t.Fatal("a cache without a limit kept a value")
	}
	c.setLimit(2 * (10 + cachedOverhead))
	for i := uint64(1); i <= 3; i++ {
		c.add(ref(i), make([]byte, 10))
	}
	if _, ok := c.get(ref(1)); ok {
		t.Fatal("the oldest value stayed past the limit")
	}
	if _, ok := c.get(ref(2)); !ok {
		t.Fatal("value 2 is gone")
	}
	// 2 is now the newest, so 3 goes for 4.
	c.add(ref(4), make([]byte, 10))
	if _, ok := c.get(ref(3)); ok {
		t.Fatal("value 3 stayed, though 2 was used after it")
	}
	c.drop(ref(2))
	if _, ok := c.get(ref(2)); ok || c.bytes != 10+cachedOverhead {
		t.Fatalf("after a drop: value 2 there %v, %d bytes", ok, c.bytes)
	}
	c.setLimit(0)
	if len(c.items) != 0 || c.bytes != 0 || c.head.next != &c.head {
		t.Fatalf("a limit of 0 left %d values, %d bytes", len(c.items), c.bytes)
	}
	if c.hits != 1 {
		t.Fatalf("%d hits, want 1", c.hits)
	}
	// The map is made again once it holds half its most, so that the
	// memory of the values that went is freed.
	c.setLimit(1 << 20)
	for i := uint64(1); i <= 100; i++ {
		c.add(ref(i), make([]byte, 10))
	}
	for i := uint64(1); i <= 50; i++ {
		c.drop(ref(i))
	}
	if c.peak != 50 || len(c.items) != 50 {
		t.Fatalf("after 50 of 100 values went: peak %d, %d values", c.peak, len(c.items))
	}
}

// cached reports whether the newest value of key is in the cache.
func (s *single) cached(key string) bool {
	refs := s.n.kv.Refs([]byte(key))
	if len(refs) == 0 {
		return false
	}
	_, ok := s.n.cache.items[refs[len(refs)-1]]
	return ok
}

// get reads key with request id, at revision rev if not 0, and returns
// its value and whether the reply waited for a value read.
func (s *single) get(id uint64, key string, rev int64) (string, bool) {
	s.t.Helper()
	s.hold = true
	s.n.Submit(Request{ID: id, Op: &pb.RangeRequest{Key: []byte(key), Revision: rev}})
	got := replies(s)
	waited := len(got) == 0
	if waited {
		got = release(s.t, s)
	}
	s.hold = false
	r, ok := got[id]
	if !ok || r.Err != nil {
		s.t.Fatalf("read of %s: %v, %v", key, ok, r.Err)
	}
	kvs := r.Resp.(*pb.RangeResponse).Kvs
	if len(kvs) != 1 {
		s.t.Fatalf("read of %s: %d keys", key, len(kvs))
	}
	return string(kvs[0].Value), waited
}

// TestCacheFillsAsEntriesLeave checks that the newest values of the
// entries that leave memory stay in the cache, so reading them waits for
// no disk, and that an overwrite or a delete drops the value it replaces.
func TestCacheFillsAsEntriesLeave(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	for i, k := range coldKeys {
		if !s.cached(k) {
			t.Fatalf("%s is not in the cache", k)
		}
		if v, waited := s.get(uint64(500+i), k, 0); v != "v"+k || waited {
			t.Fatalf("read of %s = %q, waited %v", k, v, waited)
		}
	}
	if m := s.n.Metrics(); m.CacheHits < int64(len(coldKeys)) || m.CacheMisses != 0 || m.CacheBytes != s.n.cache.bytes || m.CacheBytes == 0 {
		t.Fatalf("metrics: %+v", m)
	}

	old := s.n.kv.Refs([]byte("a"))[0]
	rev := s.n.kv.Rev()
	before := len(s.n.cache.items)
	s.ok(600, &pb.PutRequest{Key: []byte("a"), Value: []byte("new")})
	s.ok(601, &pb.DeleteRangeRequest{Key: []byte("b")})
	// A put that keeps the value keeps it cached.
	s.ok(602, &pb.PutRequest{Key: []byte("c"), IgnoreValue: true})
	if _, ok := s.n.cache.items[old]; ok {
		t.Fatal("the overwritten value of a is still cached")
	}
	if len(s.n.cache.items) != before-2 {
		t.Fatalf("%d values cached after an overwrite and a delete", len(s.n.cache.items))
	}
	if !s.cached("c") {
		t.Fatal("a put with ignore_value dropped the value it keeps")
	}
	// The old value is on disk only.
	if v, waited := s.get(603, "a", rev); v != "va" || !waited {
		t.Fatalf("read of the old a = %q, waited %v", v, waited)
	}
	if _, ok := s.n.cache.items[old]; ok {
		t.Fatal("a read cached a value that is not the newest")
	}
}

// TestCacheFillsOnReadMiss checks that a newest value read from the log
// goes into the cache, so the next read of it waits for no disk.
func TestCacheFillsOnReadMiss(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	s.n.cache.clear()
	if v, waited := s.get(500, "d", 0); v != "vd" || !waited {
		t.Fatalf("first read of d = %q, waited %v", v, waited)
	}
	if v, waited := s.get(501, "d", 0); v != "vd" || waited {
		t.Fatalf("second read of d = %q, waited %v", v, waited)
	}
	if m := s.n.Metrics(); m.CacheMisses != 1 || m.CacheHits != 1 {
		t.Fatalf("metrics: %+v", m)
	}
}

// TestCacheShrinksAsIndexGrows checks that the cache stays within what
// the index and the entries in memory leave of MemoryBytes, and gives up
// its values as new keys grow the index.
func TestCacheShrinksAsIndexGrows(t *testing.T) {
	const memory = 8 << 10
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = memory })
	limit := s.n.cache.limit
	if limit <= 0 || len(s.n.cache.items) == 0 {
		t.Fatalf("limit %d, %d values cached", limit, len(s.n.cache.items))
	}
	for i := range 100 {
		s.ok(uint64(1000+i), &pb.PutRequest{Key: fmt.Appendf(nil, "new%03d", i), Value: make([]byte, 64)})
		want := memory - s.n.kv.Bytes() - s.n.EntryBytes()
		if c := s.n.cache; c.limit != want || c.bytes > max(want, 0) {
			t.Fatalf("after key %d: limit %d, want %d, %d bytes cached", i, c.limit, want, c.bytes)
		}
	}
	if s.n.cache.limit > 0 || len(s.n.cache.items) > 0 {
		t.Fatalf("the index outgrew the memory, yet %d values are cached", len(s.n.cache.items))
	}
}

// TestCacheOnInstall installs a snapshot on a node whose second write of
// an entry waits for a value, as TestInstallAfterPausedWrite does, with
// the cache on. Without a checkpoint, the values of the entries that
// leave memory go into the cache. With one, the state is new, the cache
// starts empty and the values come from the log.
func TestCacheOnInstall(t *testing.T) {
	for _, withCheckpoint := range []bool{false, true} {
		s, _ := coldSingle(t, func(cfg *Config) {
			cfg.MemoryBytes = 1 << 20
			cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
		})
		// Rewrite the f keys until a checkpoint is on disk.
		for id := uint64(1000); s.n.DurableCheckpoint().Index == 0 && id < 2000; id += 20 {
			fill(t, s, id, 20)
		}
		// An empty cache, so that the compare on a waits for the log.
		s.n.cache.clear()
		s.hold = true
		s.n.Submit(Request{ID: 201, Op: &pb.PutRequest{Key: []byte("x"), Value: []byte("1")}})
		s.n.Submit(Request{ID: 202, Op: &pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("va")}}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("swapped")}}}},
		}})
		s.run()
		w := s.n.paused
		if w == nil || w.write != 1 || s.cached("f19") || s.n.DurableCheckpoint().Index == 0 {
			t.Fatalf("setup: paused %+v, f19 cached %v, checkpoint at %d", w, s.cached("f19"), s.n.DurableCheckpoint().Index)
		}
		term, err := s.n.termAt(w.index)
		if err != nil {
			t.Fatal(err)
		}
		in := &Install{From: 1, Index: w.index, Term: term, Applied: w.index - 1}
		if withCheckpoint {
			in.Checkpoint = s.n.DurableCheckpoint()
		}
		if err := s.n.install(in); err != nil {
			t.Fatal(err)
		}
		release(t, s)
		if withCheckpoint && len(s.n.cache.items) != 0 {
			t.Fatalf("%d values cached after the checkpoint loaded", len(s.n.cache.items))
		}
		if !withCheckpoint && !s.cached("f19") {
			t.Fatal("f19 left memory, yet is not cached")
		}
		if v, waited := s.get(300, "f19", 0); v != string(make([]byte, 64)) || waited != withCheckpoint {
			t.Fatalf("with checkpoint %v: read of f19 = %q, waited %v", withCheckpoint, v, waited)
		}
	}
}
