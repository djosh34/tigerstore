package core

import "math/bits"

// slabs holds the value cache's bytes. Values up to maxSlot bytes go in
// chunks of chunkBytes, each cut into slots of one size class, four
// classes to each doubling of size, so a slot wastes at most a fifth of
// itself. A million cached values are then a few thousand chunks with no
// pointers in them, which the garbage collector neither scans nor marks
// one by one; a value each, it marked a million objects every cycle
// (#602). A larger value gets an allocation of its own.
//
// A value goes in the lowest free slot of its class, so a class's last
// chunks empty first as values go. A freed slot takes the next value of
// its class, and compact moves values out of a class's last chunk into
// the free slots below it, so the bytes that bytes returned may change
// after free or compact: a reader that keeps them copies them first.
// compact moves a bounded number of bytes a call, and drops each chunk
// it empties, so the slabs give back the room of the values that went a
// step at a time, never holding two copies of the cache (#647).
type slabs struct {
	classes [numClasses]slabClass
	// used is the room of the slots that hold a value, and freed that of
	// the free slots below each class's next fresh slot.
	used, freed int64
	// big holds the values larger than maxSlot, and bigFree its unused
	// places. A big value is never moved.
	big     [][]byte
	bigFree []uint32
}

// slabClass is the chunks of one size class. Slot n is in chunk n/per at
// n%per, per being the slots a chunk holds. Slots from fresh on were
// never taken, or given back by compact. nfree counts the free slots
// below fresh, and low is the lowest chunk that may have one.
type slabClass struct {
	chunks []slabChunk
	fresh  uint32
	nfree  int
	low    int
}

// slabChunk is one chunk of a class: its bytes, the cache entry that owns
// each slot, and a bitmap of the free slots below the class's fresh, of
// which there are nfree. Neither holds a pointer.
type slabChunk struct {
	data  []byte
	owner []int32
	free  []uint64
	nfree int
}

// slot names where a value is: slot n of its class, or place n of big if
// class is bigClass.
type slot struct {
	n     uint32
	class uint8
}

const (
	// minSlot and maxSlot are the smallest and the largest class, and
	// steps the classes to each doubling.
	minSlotBits = 4
	maxSlotBits = 18
	maxSlot     = 1 << maxSlotBits
	steps       = 4
	numClasses  = (maxSlotBits-minSlotBits)*steps + 1
	bigClass    = numClasses
	chunkBytes  = 1 << 20
)

// classSizes are the slot sizes of the classes, smallest first.
var classSizes = func() (s [numClasses]int) {
	for i := range numClasses - 1 {
		base := 1 << (minSlotBits + i/steps)
		s[i] = base + base*(i%steps)/steps
	}
	s[numClasses-1] = maxSlot
	return s
}()

// classOf returns the smallest class whose slots hold n bytes, or
// bigClass.
func classOf(n int) int {
	if n <= 1<<minSlotBits {
		return 0
	}
	if n > maxSlot {
		return bigClass
	}
	b := bits.Len(uint(n-1)) - 1 // 1<<b < n <= 1<<(b+1)
	base := 1 << b
	return (b-minSlotBits)*steps + ((n-base)*steps+base-1)/base
}

// perChunk is the number of slots a chunk of class ci holds.
func perChunk(ci int) uint32 { return uint32(chunkBytes / classSizes[ci]) }

// put copies val into the lowest free slot of its class, which owner, an
// entry of the cache, owns, and returns the slot.
func (sl *slabs) put(val []byte, owner int32) slot {
	ci := classOf(len(val))
	if ci == bigClass {
		b := append([]byte(nil), val...)
		if k := len(sl.bigFree); k > 0 {
			n := sl.bigFree[k-1]
			sl.bigFree = sl.bigFree[:k-1]
			sl.big[n] = b
			return slot{n: n, class: bigClass}
		}
		sl.big = append(sl.big, b)
		return slot{n: uint32(len(sl.big) - 1), class: bigClass}
	}
	c := &sl.classes[ci]
	size := classSizes[ci]
	n, ok := sl.takeFree(ci)
	if !ok {
		per := perChunk(ci)
		n = c.fresh
		c.fresh++
		if n/per == uint32(len(c.chunks)) {
			c.chunks = append(c.chunks, slabChunk{
				data:  make([]byte, chunkBytes/size*size),
				owner: make([]int32, per),
				free:  make([]uint64, (per+63)/64),
			})
		}
	}
	sl.used += int64(size)
	sl.set(ci, n, val, owner)
	return slot{n: n, class: uint8(ci)}
}

// set copies val into slot n of class ci, which owner owns.
func (sl *slabs) set(ci int, n uint32, val []byte, owner int32) {
	size := classSizes[ci]
	per := perChunk(ci)
	ch := &sl.classes[ci].chunks[n/per]
	off := int(n%per) * size
	copy(ch.data[off:off+size], val)
	ch.owner[n%per] = owner
}

// takeFree takes the lowest free slot of class ci, if it has one.
func (sl *slabs) takeFree(ci int) (uint32, bool) {
	c := &sl.classes[ci]
	for ; c.low < len(c.chunks); c.low++ {
		ch := &c.chunks[c.low]
		if ch.nfree == 0 {
			continue
		}
		for w, word := range ch.free {
			if word != 0 {
				b := bits.TrailingZeros64(word)
				ch.free[w] &^= 1 << b
				ch.nfree--
				c.nfree--
				sl.freed -= int64(classSizes[ci])
				return uint32(c.low)*perChunk(ci) + uint32(w*64+b), true
			}
		}
	}
	return 0, false
}

// bytes returns the n bytes in s, which the next put, free or compact may
// overwrite.
func (sl *slabs) bytes(s slot, n int) []byte {
	if s.class == bigClass {
		return sl.big[s.n][:n:n]
	}
	size := classSizes[s.class]
	per := perChunk(int(s.class))
	off := int(s.n%per) * size
	return sl.classes[s.class].chunks[s.n/per].data[off : off+n : off+n]
}

// setOwner records that owner, an entry of the cache, now owns s.
func (sl *slabs) setOwner(s slot, owner int32) {
	if s.class == bigClass {
		return
	}
	per := perChunk(int(s.class))
	sl.classes[s.class].chunks[s.n/per].owner[s.n%per] = owner
}

// free gives s back.
func (sl *slabs) free(s slot) {
	if s.class == bigClass {
		sl.big[s.n] = nil
		sl.bigFree = append(sl.bigFree, s.n)
		return
	}
	c := &sl.classes[s.class]
	per := perChunk(int(s.class))
	k := int(s.n / per)
	ch := &c.chunks[k]
	i := s.n % per
	ch.free[i/64] |= 1 << (i % 64)
	ch.nfree++
	c.nfree++
	c.low = min(c.low, k)
	size := int64(classSizes[s.class])
	sl.used -= size
	sl.freed += size
}

// slack is the room of the free slots, which compact gives back as far as
// whole chunks empty.
func (sl *slabs) slack() int64 { return sl.freed }

// room is the memory a value of n bytes takes in s.
func (sl *slabs) room(s slot, n int) int64 {
	if s.class == bigClass {
		return int64(n)
	}
	return int64(classSizes[s.class])
}

// loose reports whether class ci can give back its last chunk: the free
// slots below that chunk can take every value in it.
func (sl *slabs) loose(ci int) bool {
	c := &sl.classes[ci]
	last := len(c.chunks) - 1
	if last < 1 {
		return false
	}
	ch := &c.chunks[last]
	live := int(c.fresh-uint32(last)*perChunk(ci)) - ch.nfree
	return c.nfree-ch.nfree >= live
}

// compact moves the values of loose classes' last chunks into the free
// slots below them, the class with the most free room first, and drops
// each chunk it empties. It copies at most budget bytes, and calls moved
// for every value it moves, with the value's owner and new slot. It
// reports whether any class is still loose.
func (sl *slabs) compact(budget int, moved func(owner int32, to slot)) bool {
	for {
		ci, most := -1, int64(0)
		for i := range numClasses {
			free := int64(sl.classes[i].nfree) * int64(classSizes[i])
			if free > most && sl.loose(i) {
				ci, most = i, free
			}
		}
		if ci < 0 {
			return false
		}
		if budget <= 0 {
			return true
		}
		budget = sl.empty(ci, budget, moved)
	}
}

// empty moves the values of class ci's last chunk, from its highest slot
// down, into the lowest free slots below it, and drops the chunk once it
// holds none. The class is loose. It copies at most budget bytes, at least
// one value, and returns what is left of budget.
func (sl *slabs) empty(ci, budget int, moved func(owner int32, to slot)) int {
	c := &sl.classes[ci]
	size := classSizes[ci]
	per := perChunk(ci)
	last := len(c.chunks) - 1
	start := uint32(last) * per
	ch := &c.chunks[last]
	for c.fresh > start {
		top := c.fresh - 1
		i := top - start
		if ch.free[i/64]&(1<<(i%64)) != 0 {
			ch.free[i/64] &^= 1 << (i % 64)
			ch.nfree--
			c.nfree--
			sl.freed -= int64(size)
			c.fresh--
			continue
		}
		if budget <= 0 {
			return 0
		}
		// The class is loose, so the lowest free slot is below the chunk.
		owner := ch.owner[i]
		to, _ := sl.takeFree(ci)
		off := int(i) * size
		sl.set(ci, to, ch.data[off:off+size], owner)
		c.fresh--
		budget -= size
		moved(owner, slot{n: to, class: uint8(ci)})
	}
	c.chunks[last] = slabChunk{}
	c.chunks = c.chunks[:last]
	c.low = min(c.low, last)
	return budget
}
