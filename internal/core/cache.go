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
// taken again and whose values move, so a reader copies what get returns
// before the cache changes.
//
// What gives memory back takes bounded steps, one a turn of the core
// (step), so that no turn holds the node's heartbeats: dropping the values
// past a limit that fell far, compacting the slabs, and making the map
// again. Copying the whole cache at once took
// 1.8 s with 900 MB cached, and held two copies of it meanwhile (#647).
type cache struct {
	// bytes counts every value by its room in the slabs with
	// cachedOverhead and every slot of items and old up to their peaks
	// with slotOverhead, and low is the smallest it was since the last
	// CacheLow.
	limit, bytes, low int64
	// items names the entry of each cached value. Neither the maps nor
	// the entries hold a pointer, so the garbage collector does not walk
	// them: with a million values cached, walking records that pointed
	// at each other and at their values took a fifth of a node's core
	// (#506). slabs holds the values (#602).
	//
	// A Go map keeps its memory as it empties, so once items holds half
	// the values it held at most (peak), it is made again: it becomes
	// old, and each step moves the refs of the next stepRefs entries
	// from old into a new items, from entry next on. Meanwhile a value is
	// in one of the two maps.
	items, old    map[mvcc.Ref]int32
	peak, oldPeak int
	next          int32
	// The entries are in pages of entryPage, n of them in use: entry 0
	// is the start and end of the ring of cached values, the most
	// recently used after it. They stay dense: removing an entry moves
	// the last one into its place, and pages past the last in use go.
	pages [][]cached
	n     int32
	slabs slabs
	// compacting is set while the slabs' free slots take more room than
	// the cache allows, and each step moves values out of chunks it can
	// empty (slabs.compact).
	compacting bool
	// evictions is how many more values fit may drop before the next
	// step: a limit that falls far is met over a few turns.
	evictions int
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

const (
	entryBits = 12
	entryPage = 1 << entryBits
	// stepBytes is the most a step copies in the slabs, and stepRefs
	// the most refs it moves from old: a few milliseconds of work.
	stepBytes = 4 << 20
	stepRefs  = 1 << 14
	// stepEvictions is the most values fit drops between two steps.
	stepEvictions = 1 << 14
	// minRebuildSlack is the least room of free slots that makes the
	// slabs compact, however few values the cache holds, so that they
	// do not compact for little.
	minRebuildSlack = 4 << 20
)

func newCache() *cache {
	c := &cache{}
	c.clear()
	return c
}

// at returns entry i.
func (c *cache) at(i int32) *cached { return &c.pages[i>>entryBits][i&(entryPage-1)] }

// lookup returns the entry of the value r names, if the cache has it.
func (c *cache) lookup(r mvcc.Ref) (int32, bool) {
	if i, ok := c.items[r]; ok {
		return i, true
	}
	i, ok := c.old[r]
	return i, ok
}

// count is the number of values cached.
func (c *cache) count() int { return len(c.items) + len(c.old) }

// get returns the value r names if the cache has it, and marks it used.
func (c *cache) get(r mvcc.Ref) ([]byte, bool) {
	i, ok := c.lookup(r)
	if !ok && planted.On("B31") {
		for _, m := range []map[mvcc.Ref]int32{c.items, c.old} {
			for ref, item := range m {
				if ref.Off == r.Off && ref.Len == r.Len {
					i, ok = item, true
					break
				}
			}
		}
	}
	if !ok {
		return nil, false
	}
	c.hits++
	c.unlink(i)
	c.push(i)
	e := c.at(i)
	return c.slabs.bytes(e.slot, int(e.ref.Len)), true
}

// has reports whether the cache holds the value r names, and does not
// mark it used.
func (c *cache) has(r mvcc.Ref) bool {
	_, ok := c.lookup(r)
	return ok
}

// add copies val, the bytes r names, into the cache. A cache still past
// its limit, which may drop no more values before its next step, takes
// none.
func (c *cache) add(r mvcc.Ref, val []byte) {
	if _, ok := c.lookup(r); ok || c.limit <= 0 || c.evictions <= 0 && c.bytes > c.limit {
		return
	}
	i := c.n
	if int(i>>entryBits) == len(c.pages) {
		c.pages = append(c.pages, make([]cached, entryPage))
	}
	c.n++
	e := c.at(i)
	*e = cached{ref: r, slot: c.slabs.put(val, i)}
	c.put(r, i)
	c.push(i)
	c.bytes += c.slabs.room(e.slot, len(val)) + cachedOverhead
	c.fit()
}

// put names entry i for r in items.
func (c *cache) put(r mvcc.Ref, i int32) {
	c.items[r] = i
	if len(c.items) > c.peak {
		c.peak++
		c.bytes += slotOverhead
	}
}

// drop forgets the value r names.
func (c *cache) drop(r mvcc.Ref) {
	if i, ok := c.lookup(r); ok {
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
	c.items, c.old, c.peak, c.oldPeak, c.next = map[mvcc.Ref]int32{}, nil, 0, 0, 0
	c.pages, c.n = [][]cached{make([]cached, entryPage)}, 1
	c.slabs, c.compacting = slabs{}, false
	c.bytes, c.low, c.evictions = 0, 0, stepEvictions
}

// fit drops the least recently used values until the cache is within
// its limit, or until it dropped stepEvictions since the last step; the
// steps drop the rest.
func (c *cache) fit() {
	for c.bytes > max(c.limit, 0) && c.n > 1 && c.evictions > 0 {
		c.evictions--
		c.remove(c.at(0).prev)
	}
}

// remove forgets entry i and moves the last entry into its place.
func (c *cache) remove(i int32) {
	c.unlink(i)
	e := *c.at(i)
	delete(c.items, e.ref)
	delete(c.old, e.ref)
	c.bytes -= c.slabs.room(e.slot, int(e.ref.Len)) + cachedOverhead
	c.slabs.free(e.slot)
	last := c.n - 1
	if i != last {
		m := *c.at(last)
		*c.at(i) = m
		c.at(m.prev).next, c.at(m.next).prev = i, i
		c.renamed(m.ref, i)
		c.slabs.setOwner(m.slot, i)
	}
	*c.at(last) = cached{}
	c.n--
	// One empty page stays, so that a cache that grows and shrinks by a
	// few values does not make a page each time.
	for len(c.pages) > int(c.n>>entryBits)+2 {
		c.pages[len(c.pages)-1] = nil
		c.pages = c.pages[:len(c.pages)-1]
	}
	switch {
	case c.n == 1:
		// An empty cache needs neither map nor slabs.
		c.bytes -= int64(c.peak+c.oldPeak) * slotOverhead
		c.items, c.old, c.peak, c.oldPeak = map[mvcc.Ref]int32{}, nil, 0, 0
		c.slabs, c.compacting = slabs{}, false
	case c.old != nil:
		c.moved()
	case len(c.items) <= c.peak/2:
		c.old, c.oldPeak = c.items, c.peak
		c.items, c.peak, c.next = map[mvcc.Ref]int32{}, 0, 1
	}
	if c.slabs.slack() > max(c.slabs.used/2, minRebuildSlack) {
		c.compacting = true
	}
	c.low = min(c.low, c.bytes)
}

// renamed records that the value r names is in entry i now. One that is
// still in old moves to items if step has passed i.
func (c *cache) renamed(r mvcc.Ref, i int32) {
	if _, ok := c.old[r]; ok {
		if i >= c.next {
			c.old[r] = i
			return
		}
		delete(c.old, r)
	}
	c.put(r, i)
}

// busy reports whether the cache has steps to take.
func (c *cache) busy() bool {
	return c.old != nil || c.compacting || c.bytes > max(c.limit, 0) && c.n > 1
}

// step takes the cache's next bounded step: it drops values past its
// limit, up to stepEvictions since the last step, moves the refs of the
// next stepRefs entries from old to items, and compacts the slabs by up
// to stepBytes.
func (c *cache) step() {
	c.fit()
	c.evictions = stepEvictions
	if c.old != nil {
		for k := 0; k < stepRefs && c.next < c.n; k++ {
			r := c.at(c.next).ref
			if _, ok := c.old[r]; ok {
				delete(c.old, r)
				c.put(r, c.next)
			}
			c.next++
		}
		c.moved()
	}
	if c.compacting {
		more := c.slabs.compact(stepBytes, func(owner int32, to slot) { c.at(owner).slot = to })
		c.compacting = more && c.slabs.slack() > max(c.slabs.used/4, minRebuildSlack/2)
	}
}

// moved ends making the map again once every entry's ref is in items:
// old's memory goes.
func (c *cache) moved() {
	if c.next < c.n {
		return
	}
	if len(c.old) != 0 {
		panic("cache: refs left in the old map")
	}
	c.bytes -= int64(c.oldPeak) * slotOverhead
	c.old, c.oldPeak = nil, 0
}

// push puts entry i at the front of the ring.
func (c *cache) push(i int32) {
	next := c.at(0).next
	c.at(i).prev, c.at(i).next = 0, next
	c.at(0).next, c.at(next).prev = i, i
}

// unlink takes entry i out of the ring.
func (c *cache) unlink(i int32) {
	e := c.at(i)
	c.at(e.prev).next, c.at(e.next).prev = e.next, e.prev
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
