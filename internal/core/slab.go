package core

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
// which there are nfree, none in the words before word. Neither holds a
// pointer.
type slabChunk struct {
	data  []byte
	owner []int32
	free  []uint64
	nfree int
	word  int
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

// perChunk is the number of slots a chunk of class ci holds.
func perChunk(ci int) uint32 { return uint32(chunkBytes / classSizes[ci]) }

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
	ch.word = min(ch.word, int(i/64))
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

// anyLoose reports whether any class is loose, so that compact has work.
func (sl *slabs) anyLoose() bool {
	for ci := range numClasses {
		if sl.loose(ci) {
			return true
		}
	}
	return false
}
