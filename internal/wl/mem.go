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

var (
	errSlabFull  = errors.New("the store's slab is full")
	errArenaFull = errors.New("the store's key arena is full")
)

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

// arena is a bump allocator of bytes that are never freed: keys and
// directory paths.
type arena struct {
	mem []byte
	top int
}

func (a *arena) add(p []byte) (uint64, error) {
	if a.top+len(p) > len(a.mem) {
		return 0, errArenaFull
	}
	off := a.top
	a.top += copy(a.mem[off:], p)
	return uint64(off), nil
}

func (a *arena) get(off uint64, n uint32) []byte { return a.mem[off : off+uint64(n) : off+uint64(n)] }
