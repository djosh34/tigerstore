package wl

import (
	"errors"
	"math/bits"
	"unsafe"
)

// mmapSlice reserves room for n values of a pointer-free type T.
func mmapSlice[T any](n int) ([]T, error) {
	var z T
	b, err := mmapBytes(n * int(unsafe.Sizeof(z)))
	if err != nil {
		return nil, err
	}
	return unsafe.Slice((*T)(unsafe.Pointer(&b[0])), n), nil
}

// munmapSlice gives back what mmapSlice reserved.
func munmapSlice[T any](s []T) error {
	var z T
	return unixMunmap(unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(s))), len(s)*int(unsafe.Sizeof(z))))
}

// slab hands out byte ranges of a big mapping in size classes, eight per
// doubling, with a free list per class threaded through the free slots.
// Offset 0 is never handed out, so 0 means "none".
type slab struct {
	mem   []byte
	top   int
	heads [classes]uint64
	used  int64 // bytes of the slots handed out
}

const (
	minClass = 64
	classes  = 8 * 20
)

func newSlab(n int) (*slab, error) {
	mem, err := mmapBytes(n)
	if err != nil {
		return nil, err
	}
	return &slab{mem: mem, top: minClass}, nil
}

// classOf returns the class of n bytes and its slot size.
func classOf(n int) (int, int) {
	if n <= minClass {
		return 0, minClass
	}
	k := bits.Len(uint(n-1)) - 1 // 2^k < n <= 2^(k+1)
	base := 1 << k
	step := base >> 3
	j := (n - 1 - base) / step
	return (k-6)*8 + j + 1, base + (j+1)*step
}

var errSlabFull = errors.New("the store's slab is full")

func (s *slab) alloc(n int) (uint64, error) {
	c, size := classOf(n)
	if h := s.heads[c]; h != 0 {
		s.heads[c] = *(*uint64)(unsafe.Pointer(&s.mem[h]))
		s.used += int64(size)
		return h, nil
	}
	off := s.top
	if off+size > len(s.mem) {
		return 0, errSlabFull
	}
	s.top += size
	s.used += int64(size)
	return uint64(off), nil
}

func (s *slab) free(off uint64, n int) {
	if off == 0 {
		return
	}
	c, size := classOf(n)
	s.used -= int64(size)
	*(*uint64)(unsafe.Pointer(&s.mem[off])) = s.heads[c]
	s.heads[c] = off
}

func (s *slab) bytes(off uint64, n int) []byte { return s.mem[off : int(off)+n : int(off)+n] }

// slotEnd is where the slot of n bytes at off ends.
func slotEnd(off uint64, n int) int {
	_, size := classOf(n)
	return int(off) + size
}

// shrink lowers the slab's top to to, a page boundary, and gives the pages
// past it back to the system: the free lists keep only the free slots that
// end by to. Every slot handed out must end by to.
func (s *slab) shrink(to int) {
	if to >= s.top {
		return
	}
	for c := range s.heads {
		size := minClass
		if c > 0 {
			size = classSize(c)
		}
		var prev uint64
		for h := s.heads[c]; h != 0; {
			next := *(*uint64)(unsafe.Pointer(&s.mem[h]))
			if int(h)+size <= to {
				if prev == 0 {
					s.heads[c] = h
				} else {
					*(*uint64)(unsafe.Pointer(&s.mem[prev])) = h
				}
				prev = h
			}
			h = next
		}
		if prev == 0 {
			s.heads[c] = 0
		} else {
			*(*uint64)(unsafe.Pointer(&s.mem[prev])) = 0
		}
	}
	dropPages(s.mem[to:s.top])
	s.top = to
}

// classSize is the slot size of class c > 0: classOf's, the other way.
func classSize(c int) int {
	k, j := (c-1)/8+6, (c-1)%8
	base := 1 << k
	return base + (j+1)*(base>>3)
}
