package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestCacheLRU checks that the least recently used value goes first once
// the cache is past its limit, and that a value it drops or that does
// not fit is gone.
func TestCacheLRU(t *testing.T) {
	const size = 1000
	c := newCache()
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: size} }
	c.add(ref(1), make([]byte, size))
	if len(c.items) != 0 {
		t.Fatal("a cache without a limit kept a value")
	}
	// Room for two values and four map slots.
	c.setLimit(2*(size+cachedOverhead) + 4*slotOverhead)
	for i := uint64(1); i <= 3; i++ {
		c.add(ref(i), make([]byte, size))
	}
	if _, ok := c.get(ref(1)); ok {
		t.Fatal("the oldest value stayed past the limit")
	}
	if _, ok := c.get(ref(2)); !ok {
		t.Fatal("value 2 is gone")
	}
	// 2 is now the newest, so 3 goes for 4.
	c.add(ref(4), make([]byte, size))
	if _, ok := c.get(ref(3)); ok {
		t.Fatal("value 3 stayed, though 2 was used after it")
	}
	if _, ok := c.get(ref(4)); !ok {
		t.Fatal("value 4 is gone")
	}
	c.drop(ref(2))
	if _, ok := c.get(ref(2)); ok || c.bytes != size+cachedOverhead+slotOverhead {
		t.Fatalf("after a drop: value 2 there %v, %d bytes", ok, c.bytes)
	}
	c.setLimit(0)
	if len(c.items) != 0 || c.bytes != 0 || c.head.next != &c.head {
		t.Fatalf("a limit of 0 left %d values, %d bytes", len(c.items), c.bytes)
	}
	if c.hits != 2 {
		t.Fatalf("%d hits, want 2", c.hits)
	}
}

// TestCacheMapSlots checks that the slots of the map count up to the
// most values it held, until it holds half of that and is made again,
// so that the memory of the values that went is freed.
func TestCacheMapSlots(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 20)
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: 10} }
	want := func(values, slots int) {
		t.Helper()
		if len(c.items) != values || c.peak != slots || c.bytes != int64(values*(10+cachedOverhead)+slots*slotOverhead) {
			t.Fatalf("%d values, peak %d, %d bytes; want %d values and %d slots", len(c.items), c.peak, c.bytes, values, slots)
		}
	}
	for i := uint64(1); i <= 100; i++ {
		c.add(ref(i), make([]byte, 10))
	}
	want(100, 100)
	for i := uint64(1); i <= 40; i++ {
		c.drop(ref(i))
	}
	want(60, 100)
	for i := uint64(41); i <= 50; i++ {
		c.drop(ref(i))
	}
	want(50, 50)
	c.clear()
	want(0, 0)
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

// TestCacheShrinksAsOtherMemoryGrows checks that the cache stays within
// what the rest of the process leaves of MemoryBytes, as the driver
// measures it, and gives up its values at once as that grows.
func TestCacheShrinksAsOtherMemoryGrows(t *testing.T) {
	const memory = 8 << 10
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = memory })
	if s.n.cache.limit != memory || len(s.n.cache.items) == 0 {
		t.Fatalf("limit %d, %d values cached", s.n.cache.limit, len(s.n.cache.items))
	}
	for other := int64(0); other <= memory; other += 1 << 10 {
		s.n.SetOtherMemory(other)
		if c := s.n.cache; c.limit != memory-other || c.bytes > memory-other {
			t.Fatalf("with %d bytes besides the cache: limit %d, %d bytes cached", other, c.limit, c.bytes)
		}
	}
	if len(s.n.cache.items) > 0 {
		t.Fatalf("the rest of the process takes all the memory, yet %d values are cached", len(s.n.cache.items))
	}
}
