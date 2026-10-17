package core

import (
	"bytes"
	"maps"

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
// overwritten ones frees memory.
type cache struct {
	// bytes counts every value with cachedOverhead and every slot of
	// items up to peak with slotOverhead, and low is the smallest it was
	// since the last CacheLow.
	limit, bytes, low int64
	items             map[mvcc.Ref]*cached
	// peak is the most values items held since it was made. A Go map
	// keeps its memory as it empties, so items is made again once it
	// holds half of that.
	peak int
	// head is the start and end of the ring of cached values, the most
	// recently used after it.
	head cached
	// hits are the values the cache returned, and misses those read from
	// the log.
	hits, misses int64
}

// cached is one value in the cache.
type cached struct {
	ref        mvcc.Ref
	val        []byte
	prev, next *cached
}

// cachedOverhead estimates what a cached value takes besides its bytes
// and its map slot: its record and its slice header, and the rounding up
// of a small value's allocation. slotOverhead estimates a slot of the
// map with the room Go keeps free in it. A million one-byte values took
// 148 bytes each on Go 1.27, 83 of them in the map.
const (
	cachedOverhead = 80
	slotOverhead   = 96
)

func newCache() *cache {
	c := &cache{items: map[mvcc.Ref]*cached{}}
	c.head.prev, c.head.next = &c.head, &c.head
	return c
}

// get returns the value r names if the cache has it, and marks it used.
func (c *cache) get(r mvcc.Ref) ([]byte, bool) {
	e, ok := c.items[r]
	if !ok && planted.On("B31") {
		for ref, item := range c.items {
			if ref.Off == r.Off && ref.Len == r.Len {
				e, ok = item, true
				break
			}
		}
	}
	if !ok {
		return nil, false
	}
	c.hits++
	c.unlink(e)
	c.push(e)
	return e.val, true
}

// add keeps val, the bytes r names, which nobody may change afterwards.
func (c *cache) add(r mvcc.Ref, val []byte) {
	if _, ok := c.items[r]; ok || c.limit <= 0 {
		return
	}
	e := &cached{ref: r, val: val}
	c.items[r] = e
	if len(c.items) > c.peak {
		c.peak++
		c.bytes += slotOverhead
	}
	c.push(e)
	c.bytes += int64(len(val)) + cachedOverhead
	c.fit()
}

// drop forgets the value r names.
func (c *cache) drop(r mvcc.Ref) {
	if e, ok := c.items[r]; ok {
		c.remove(e)
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
	c.items, c.peak = map[mvcc.Ref]*cached{}, 0
	c.head.prev, c.head.next = &c.head, &c.head
	c.bytes, c.low = 0, 0
}

// fit drops the least recently used values until the cache is within
// its limit.
func (c *cache) fit() {
	for c.bytes > max(c.limit, 0) {
		c.remove(c.head.prev)
	}
}

func (c *cache) remove(e *cached) {
	c.unlink(e)
	delete(c.items, e.ref)
	c.bytes -= int64(len(e.val)) + cachedOverhead
	if len(c.items) <= c.peak/2 {
		items := make(map[mvcc.Ref]*cached, len(c.items))
		maps.Copy(items, c.items)
		c.bytes -= int64(c.peak-len(items)) * slotOverhead
		c.items, c.peak = items, len(items)
	}
	c.low = min(c.low, c.bytes)
}

func (c *cache) push(e *cached) {
	e.prev, e.next = &c.head, c.head.next
	e.prev.next, e.next.prev = e, e
}

func (c *cache) unlink(e *cached) {
	e.prev.next, e.next.prev = e.next, e.prev
	e.prev, e.next = nil, nil
}

// fillCache moves into the cache the newest values written in the entries
// up to index, which leave memory.
func (n *Node) fillCache(index uint64) {
	n.kv.Leaving(index, func(r mvcc.Ref) {
		data, ok := n.storage.entryData(r.Index)
		if ok && uint64(r.Off)+uint64(r.Len) <= uint64(len(data)) {
			n.cache.add(r, bytes.Clone(data[r.Off:r.Off+r.Len]))
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
