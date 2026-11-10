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
// A slot that is freed takes the next value of its class, so the bytes
// that bytes returned may change after free: a reader that keeps them
// copies them first. Chunks are never given back; the cache makes all
// its slabs again when it rebuilds (cache.rebuild).
type slabs struct {
	classes [numClasses]slabClass
	// big holds the values larger than maxSlot, and bigFree its unused
	// places.
	big     [][]byte
	bigFree []uint32
}

// slabClass is the chunks of one size class. Slot n is in chunk n/per at
// n%per, per being the slots a chunk holds; used is the number of slots
// ever taken, and free lists those given back.
type slabClass struct {
	chunks [][]byte
	free   []uint32
	used   uint32
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

// put copies val into a free slot and returns it.
func (sl *slabs) put(val []byte) slot {
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
	per := uint32(chunkBytes / size)
	var n uint32
	if k := len(c.free); k > 0 {
		n, c.free = c.free[k-1], c.free[:k-1]
	} else {
		n = c.used
		c.used++
		if n/per == uint32(len(c.chunks)) {
			c.chunks = append(c.chunks, make([]byte, int(per)*size))
		}
	}
	off := int(n%per) * size
	copy(c.chunks[n/per][off:off+size], val)
	return slot{n: n, class: uint8(ci)}
}

// bytes returns the n bytes in s, which the next put after free(s) may
// overwrite.
func (sl *slabs) bytes(s slot, n int) []byte {
	if s.class == bigClass {
		return sl.big[s.n][:n:n]
	}
	size := classSizes[s.class]
	per := uint32(chunkBytes / size)
	off := int(s.n%per) * size
	return sl.classes[s.class].chunks[s.n/per][off : off+n : off+n]
}

// free gives s back.
func (sl *slabs) free(s slot) {
	if s.class == bigClass {
		sl.big[s.n] = nil
		sl.bigFree = append(sl.bigFree, s.n)
		return
	}
	c := &sl.classes[s.class]
	c.free = append(c.free, s.n)
}

// room is the memory a value of n bytes takes in s.
func (sl *slabs) room(s slot, n int) int64 {
	if s.class == bigClass {
		return int64(n)
	}
	return int64(classSizes[s.class])
}
