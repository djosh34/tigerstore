package core

import (
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
)

// cache holds the newest values of keys written or read lately, after
// their entries left memory, so that reading them again needs no disk.
// A value goes in when its entry leaves the window (fillCache) and when
// a read from the log brings it (ValuesRead), and out when its key is
// overwritten or deleted, or when it is the least recently used and the
// cache is past its limit.
//
// The limit is Config.MemoryBytes less what the rest of the process
// uses, as the driver measures it, so the cache shrinks as anything else
// grows. A value is named by its Raft position, which no later write
// changes, so the cache never hands out a stale value; dropping
// overwritten ones frees memory. The values are in slabs, whose slots are
// taken again, so a reader copies what get returns before the cache
// changes.
type cache struct {
	// bytes counts every value by its room in the slabs with
	// cachedOverhead and every slot of items up to peak with
	// slotOverhead, and low is the smallest it was since the last
	// CacheLow.
	limit, bytes, low int64
	// items names the entry of each cached value. Neither items nor
	// entries holds a pointer, so the garbage collector does not walk
	// them: with a million values cached, walking records that pointed
	// at each other and at their values took a fifth of a node's core
	// (#506). slabs holds the values (#602).
	items map[mvcc.Ref]int32
	// peak is the most values items held since it was made. A Go map
	// keeps its memory as it empties, so items is made again once it
	// holds half of that.
	peak int
	// entries[0] is the start and end of the ring of cached values, the
	// most recently used after it; free lists the entries not in use.
	entries []cached
	slabs   slabs
	free    []int32
	// hits are the values the cache returned, and misses those read from
	// the log.
	hits, misses int64
}

// cached is one value in the cache, by the entries before and after it
// in the ring, and its slot in the slabs.
type cached struct {
	ref        mvcc.Ref
	prev, next int32
	slot       slot
}

// cachedOverhead is what a cached value takes besides its slot and its
// map slot: its entry. slotOverhead estimates a slot of the map with the
// room Go keeps free in it; a million one-byte values took 83 bytes each
// in the map on Go 1.27.
const (
	cachedOverhead = 40
	slotOverhead   = 96
)

func newCache() *cache {
	c := &cache{}
	c.clear()
	return c
}

// get returns the value r names if the cache has it, and marks it used.
func (c *cache) get(r mvcc.Ref) ([]byte, bool) {
	i, ok := c.items[r]
	if !ok && planted.On("B31") {
		for ref, item := range c.items {
			if ref.Off == r.Off && ref.Len == r.Len {
				i, ok = item, true
				break
			}
		}
	}
	if !ok {
		return nil, false
	}
	c.hits++
	c.unlink(i)
	c.push(i)
	e := &c.entries[i]
	return c.slabs.bytes(e.slot, int(e.ref.Len)), true
}

// has reports whether the cache holds the value r names, and does not
// mark it used.
func (c *cache) has(r mvcc.Ref) bool {
	_, ok := c.items[r]
	return ok
}

// add copies val, the bytes r names, into the cache.
func (c *cache) add(r mvcc.Ref, val []byte) {
	if _, ok := c.items[r]; ok || c.limit <= 0 {
		return
	}
	e := cached{ref: r, slot: c.slabs.put(val)}
	var i int32
	if n := len(c.free); n > 0 {
		i, c.free = c.free[n-1], c.free[:n-1]
		c.entries[i] = e
	} else {
		i = int32(len(c.entries))
		c.entries = append(c.entries, e)
	}
	c.items[r] = i
	if len(c.items) > c.peak {
		c.peak++
		c.bytes += slotOverhead
	}
	c.push(i)
	c.bytes += c.slabs.room(e.slot, len(val)) + cachedOverhead
	c.fit()
}

// drop forgets the value r names.
func (c *cache) drop(r mvcc.Ref) {
	if i, ok := c.items[r]; ok {
		c.remove(i)
	}
}

// setLimit sets the cache's limit and drops the least recently used
// values past it.
func (c *cache) setLimit(limit int64) {
	c.limit = limit
	c.fit()
}

// clear forgets every value.
func (c *cache) clear() {
	c.items, c.peak = map[mvcc.Ref]int32{}, 0
	c.entries, c.free, c.slabs = []cached{{}}, nil, slabs{}
	c.bytes, c.low = 0, 0
}

// fit drops the least recently used values until the cache is within
// its limit.
func (c *cache) fit() {
	for c.bytes > max(c.limit, 0) {
		c.remove(c.entries[0].prev)
	}
}

func (c *cache) remove(i int32) {
	c.unlink(i)
	e := &c.entries[i]
	delete(c.items, e.ref)
	c.bytes -= c.slabs.room(e.slot, int(e.ref.Len)) + cachedOverhead
	c.slabs.free(e.slot)
	c.free = append(c.free, i)
	if len(c.items) <= c.peak/2 || c.slabs.slack() > max(c.slabs.used/2, maxSlack) {
		c.rebuild()
	}
	c.low = min(c.low, c.bytes)
}

// maxSlack is the room of freed slots that the cache keeps however few
// values it holds, so that it does not rebuild for little.
const maxSlack = 4 << 20

// rebuild makes the map, the entries and the slabs again with room for
// only the values cached, in the same order, so the memory of those that
// went is freed. The cache does so when its map holds half the values it
// held at most, or when the slabs' freed slots take more than half the
// room of those in use: values of a new size fill new chunks, and the old
// size's chunks stay as its values go (#602).
func (c *cache) rebuild() {
	c.bytes -= int64(c.peak-len(c.items)) * slotOverhead
	items := make(map[mvcc.Ref]int32, len(c.items))
	entries := make([]cached, 1, len(c.items)+1)
	var sl slabs
	for i := c.entries[0].next; i != 0; i = c.entries[i].next {
		e := &c.entries[i]
		n := int32(len(entries))
		items[e.ref] = n
		entries = append(entries, cached{ref: e.ref, prev: n - 1, next: n + 1, slot: sl.put(c.slabs.bytes(e.slot, int(e.ref.Len)))})
	}
	last := int32(len(entries) - 1)
	entries[0].next, entries[0].prev = min(1, last), last
	entries[last].next = 0
	c.items, c.peak, c.entries, c.slabs, c.free = items, len(items), entries, sl, nil
}

// push puts entry i at the front of the ring.
func (c *cache) push(i int32) {
	next := c.entries[0].next
	c.entries[i].prev, c.entries[i].next = 0, next
	c.entries[0].next, c.entries[next].prev = i, i
}

// unlink takes entry i out of the ring.
func (c *cache) unlink(i int32) {
	e := &c.entries[i]
	c.entries[e.prev].next, c.entries[e.next].prev = e.next, e.prev
	e.prev, e.next = 0, 0
}

// fillCache moves into the cache the newest values written in the entries
// up to index, which leave memory.
func (n *Node) fillCache(index uint64) {
	n.kv.Leaving(index, func(r mvcc.Ref) {
		data, ok := n.storage.entryData(r.Index)
		if ok && uint64(r.Off)+uint64(r.Len) <= uint64(len(data)) {
			n.cache.add(r, data[r.Off:r.Off+r.Len])
		}
	})
}

// SetOtherMemory tells the core how much memory the process uses besides
// the value cache, as the driver measured it. The cache may take what
// that leaves of Config.MemoryBytes, and gives back the rest at once.
func (n *Node) SetOtherMemory(bytes int64) {
	n.otherMemory = bytes
	n.fitCache()
}

// CacheLow is the smallest the value cache held since the last call,
// and the next span starts from what it holds now. A collection of the
// garbage collector in between found at least that much of it live,
// whatever it dropped and took in meanwhile.
func (n *Node) CacheLow() int64 {
	low := n.cache.low
	n.cache.low = n.cache.bytes
	return low
}

// fitCache sets the cache's limit to the memory the rest of the process
// leaves.
func (n *Node) fitCache() {
	n.cache.setLimit(n.cfg.MemoryBytes - n.otherMemory)
}
