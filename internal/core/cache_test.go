package core

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestCacheLRU checks that the least recently used value goes first once
// the cache is past its limit, and that a value it drops or that does
// not fit is gone.
func TestCacheLRU(t *testing.T) {
	// A slot of the slabs holds exactly this.
	const size = 1024
	c := newCache()
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: size} }
	c.add(ref(1), make([]byte, size))
	if c.count() != 0 {
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
	settle(t, c)
	if _, ok := c.get(ref(2)); ok || c.bytes != size+cachedOverhead+slotOverhead {
		t.Fatalf("after a drop: value 2 there %v, %d bytes", ok, c.bytes)
	}
	c.setLimit(0)
	if c.count() != 0 || c.bytes != 0 || c.at(0).next != 0 {
		t.Fatalf("a limit of 0 left %d values, %d bytes", c.count(), c.bytes)
	}
	if c.hits != 2 {
		t.Fatalf("%d hits, want 2", c.hits)
	}
}

// TestCacheMapSlots checks that the slots of the map count up to the
// most values it held, until it holds half of that and is made again,
// a step at a time, so that the memory of the values that went is freed.
func TestCacheMapSlots(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 20)
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: 10} }
	want := func(values, slots int) {
		t.Helper()
		// A 10-byte value takes a slot of 16 bytes.
		if c.count() != values || c.peak+c.oldPeak != slots || c.bytes != int64(values*(16+cachedOverhead)+slots*slotOverhead) {
			t.Fatalf("%d values, peaks %d and %d, %d bytes; want %d values and %d slots", c.count(), c.peak, c.oldPeak, c.bytes, values, slots)
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
	if c.busy() {
		t.Fatal("the cache has steps to take with the map 60% full")
	}
	for i := uint64(41); i <= 50; i++ {
		c.drop(ref(i))
	}
	// The old map holds its 100 slots until every ref moved.
	if !c.busy() || c.old == nil {
		t.Fatal("the map is not made again at half its peak")
	}
	for c.busy() {
		c.step()
	}
	want(50, 50)
	for i := uint64(51); i <= 100; i++ {
		if !c.has(ref(i)) {
			t.Fatalf("value %d is gone after the map was made again", i)
		}
	}
	c.clear()
	want(0, 0)
}

// cached reports whether the newest value of key is in the cache.
func (s *single) cached(key string) bool {
	refs := s.n.kv.Refs([]byte(key))
	if len(refs) == 0 {
		return false
	}
	return s.n.cache.has(refs[len(refs)-1])
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
	kvs := rangeResp(r).Kvs
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
	deleted := s.n.kv.Refs([]byte("b"))[0]
	rev := s.n.kv.Rev()
	s.ok(600, &pb.PutRequest{Key: []byte("a"), Value: []byte("new")})
	s.ok(601, &pb.DeleteRangeRequest{Key: []byte("b")})
	// A put that keeps the value keeps it cached.
	s.ok(602, &pb.PutRequest{Key: []byte("c"), IgnoreValue: true})
	if s.n.cache.has(old) {
		t.Fatal("the overwritten value of a is still cached")
	}
	if s.n.cache.has(deleted) {
		t.Fatal("the deleted value of b is still cached")
	}
	if !s.cached("c") {
		t.Fatal("a put with ignore_value dropped the value it keeps")
	}
	// The old value is on disk only.
	if v, waited := s.get(603, "a", rev); v != "va" || !waited {
		t.Fatalf("read of the old a = %q, waited %v", v, waited)
	}
	if s.n.cache.has(old) {
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
	if s.n.cache.limit != memory || s.n.cache.count() == 0 {
		t.Fatalf("limit %d, %d values cached", s.n.cache.limit, s.n.cache.count())
	}
	for other := int64(0); other <= memory; other += 1 << 10 {
		s.n.SetOtherMemory(other)
		if c := s.n.cache; c.limit != memory-other || c.bytes > memory-other {
			t.Fatalf("with %d bytes besides the cache: limit %d, %d bytes cached", other, c.limit, c.bytes)
		}
	}
	if s.n.cache.count() > 0 {
		t.Fatalf("the rest of the process takes all the memory, yet %d values are cached", s.n.cache.count())
	}
}

// TestCacheLow checks the low mark the memory gauge relies on: a cache
// that is emptied and then filled again between two calls reports the
// empty cache, and the next span starts from its size then.
func TestCacheLow(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	full := s.n.cache.bytes
	if full == 0 {
		t.Fatal("setup: nothing cached")
	}
	s.n.CacheLow()
	s.n.cache.setLimit(0)
	s.n.cache.setLimit(1 << 20)
	s.n.cache.add(mvcc.Ref{Index: 1 << 40, Len: 1000}, make([]byte, 1000))
	if low := s.n.CacheLow(); low != 0 {
		t.Fatalf("emptied and filled again: low %d, want 0", low)
	}
	if low, now := s.n.CacheLow(), s.n.cache.bytes; low != now || now == 0 {
		t.Fatalf("next span: low %d, cache %d", low, now)
	}
}

// TestCacheRebuildKeepsOrder checks that dense entries and making the map
// again keep the values and their order of use.
func TestCacheRebuildKeepsOrder(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 20)
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: 10} }
	for i := uint64(1); i <= 10; i++ {
		c.add(ref(i), []byte{byte(i), 1, 2, 3, 4, 5, 6, 7, 8, 9})
	}
	// Used last: 2, then 4.
	c.get(ref(2))
	c.get(ref(4))
	for i := uint64(5); i <= 9; i++ {
		c.drop(ref(i))
	}
	for c.busy() {
		c.step()
	}
	if c.peak != 5 || c.n != 6 {
		t.Fatalf("after the map was made again: peak %d, %d entries", c.peak, c.n)
	}
	// Room for two values in slots of 16 bytes: 4 and 2 were used last.
	c.setLimit(2*(16+cachedOverhead) + 5*slotOverhead)
	for i := uint64(1); i <= 10; i++ {
		v, ok := c.get(ref(i))
		if ok != (i == 2 || i == 4) || ok && (len(v) != 10 || v[0] != byte(i) || v[9] != 9) {
			t.Fatalf("value %d: cached %v, %v", i, ok, v)
		}
	}
}

// TestCacheStepsAreTurns checks that the core asks for its next turn at
// once while its value cache has steps left (Working), and that its turns
// take them: a cache whose limit fell from 64 MiB to 1 MiB fits it a
// step a turn, with every value it keeps intact (#647).
func TestCacheStepsAreTurns(t *testing.T) {
	const memory = 64 << 20
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = memory })
	c := s.n.cache
	const size = 1000
	val := func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8)}, size/2) }
	ref := func(i int) mvcc.Ref { return mvcc.Ref{Index: 1<<40 + uint64(i), Len: size} }
	for i := range memory / 2 / size {
		c.add(ref(i), val(i))
	}
	settle(t, c)
	if s.n.Working() {
		t.Fatal("the core works with no steps of its cache left")
	}
	s.n.SetOtherMemory(memory - 1<<20)
	if !s.n.Working() || c.bytes <= c.limit {
		t.Fatalf("after the limit fell: working %v, %d bytes for a limit of %d", s.n.Working(), c.bytes, c.limit)
	}
	turns := 0
	for s.n.Working() {
		if _, err := s.n.Process(); err != nil {
			t.Fatal(err)
		}
		if turns++; turns > 1000 {
			t.Fatal("the core's turns do not end its cache's steps")
		}
	}
	if c.busy() || c.bytes > c.limit || c.count() == 0 {
		t.Fatalf("after %d turns: busy %v, %d bytes for a limit of %d, %d values", turns, c.busy(), c.bytes, c.limit, c.count())
	}
	for i := range memory / 2 / size {
		if v, ok := c.get(ref(i)); ok && !bytes.Equal(v, val(i)) {
			t.Fatalf("value %d changed", i)
		}
	}
	t.Logf("%d turns", turns)
}

// TestCacheLowAfterMapMoves checks the low mark once the map was made
// again: the old map's slots leave the cache's bytes, and the next
// CacheLow says so, or the memory gauge would take memory the cache no
// longer holds for its own.
func TestCacheLowAfterMapMoves(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 20)
	ref := func(i uint64) mvcc.Ref { return mvcc.Ref{Index: i, Len: 10} }
	for i := uint64(1); i <= 100; i++ {
		c.add(ref(i), make([]byte, 10))
	}
	for i := uint64(1); i <= 50; i++ {
		c.drop(ref(i))
	}
	c.low = c.bytes
	before := c.bytes
	settle(t, c)
	if c.bytes >= before {
		t.Fatalf("making the map again left %d bytes of %d", c.bytes, before)
	}
	if c.low != c.bytes {
		t.Fatalf("low mark %d after the map was made again, with %d bytes", c.low, c.bytes)
	}
}
