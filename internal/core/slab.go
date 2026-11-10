package core

import "math/bits"

// slabs keeps the cache's values in large pointer-free chunks, one set
// of chunks per size class, so that a million cached values are a few
// thousand objects without pointers for the garbage collector instead of
// a million (prototype #602).
type slabs struct {
	classes [numClasses]class
	// big holds values past the largest class, one allocation each.
	big     map[uint32][]byte
	bigNext uint32
	bigFree []uint32
}

// slot names a value's place: its class (bigClass for big) and its slot.
type slot struct {
	class uint8
	n     uint32
}

type class struct {
	chunks [][]byte
	free   []uint32
	next   uint32
}

const (
	minClassBits = 4  // 16 bytes
	maxClassBits = 18 // 256 KiB
	stepsPerTwo  = 4
	numClasses   = (maxClassBits-minClassBits)*stepsPerTwo + 1
	bigClass     = 255
	chunkBytes   = 1 << 20
)

// classSize is the size of each class.
var classSize = func() (s [numClasses]int) {
	i := 0
	for b := minClassBits; b < maxClassBits; b++ {
		base := 1 << b
		for k := 0; k < stepsPerTwo; k++ {
			s[i] = base + base*k/stepsPerTwo
			i++
		}
	}
	s[i] = 1 << maxClassBits
	return s
}()

// classOf returns the smallest class that holds n bytes, or bigClass.
func classOf(n int) int {
	if n <= 1<<minClassBits {
		return 0
	}
	if n > 1<<maxClassBits {
		return bigClass
	}
	b := bits.Len(uint(n-1)) - 1 // 2^b < n <= 2^(b+1)
	base := 1 << b
	k := ((n-base)*stepsPerTwo + base - 1) / base // ceil
	i := (b-minClassBits)*stepsPerTwo + k
	return i
}

func (sl *slabs) put(val []byte) slot {
	ci := classOf(len(val))
	if ci == bigClass {
		if sl.big == nil {
			sl.big = map[uint32][]byte{}
		}
		var n uint32
		if k := len(sl.bigFree); k > 0 {
			n, sl.bigFree = sl.bigFree[k-1], sl.bigFree[:k-1]
		} else {
			n = sl.bigNext
			sl.bigNext++
		}
		b := make([]byte, len(val))
		copy(b, val)
		sl.big[n] = b
		return slot{class: bigClass, n: n}
	}
	c := &sl.classes[ci]
	size := classSize[ci]
	per := uint32(chunkBytes / size)
	var n uint32
	if k := len(c.free); k > 0 {
		n, c.free = c.free[k-1], c.free[:k-1]
	} else {
		n = c.next
		c.next++
		if n/per >= uint32(len(c.chunks)) {
			c.chunks = append(c.chunks, make([]byte, per*uint32(size)))
		}
	}
	off := int(n%per) * size
	copy(c.chunks[n/per][off:off+size], val)
	return slot{class: uint8(ci), n: n}
}

// bytes returns the n bytes at s, which the next put may overwrite.
func (sl *slabs) bytes(s slot, n int) []byte {
	if s.class == bigClass {
		return sl.big[s.n][:n]
	}
	size := classSize[s.class]
	per := uint32(chunkBytes / size)
	off := int(s.n%per) * size
	return sl.classes[s.class].chunks[s.n/per][off : off+n : off+n]
}

func (sl *slabs) free(s slot) {
	if s.class == bigClass {
		delete(sl.big, s.n)
		sl.bigFree = append(sl.bigFree, s.n)
		return
	}
	c := &sl.classes[s.class]
	c.free = append(c.free, s.n)
}

// size is the room the n bytes at s take.
func (sl *slabs) size(s slot, n int) int {
	if s.class == bigClass {
		return n
	}
	return classSize[s.class]
}
