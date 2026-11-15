package wl

import (
	"fmt"
	"math/bits"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmapBytes reserves n bytes of anonymous memory outside Go's heap, which
// the collector never scans and the memory limit never counts. Pages are
// backed on first touch.
func mmapBytes(n int) []byte {
	b, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_NORESERVE)
	if err != nil {
		panic(fmt.Sprintf("mmap %d bytes: %v", n, err))
	}
	// Huge pages where the kernel gives them: the index and the slab are
	// read at random, and each miss would also miss the TLB.
	_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
	return b
}

// mmapSlice reserves room for n values of a pointer-free type T.
func mmapSlice[T any](n int) []T {
	var z T
	b := mmapBytes(n * int(unsafe.Sizeof(z)))
	return unsafe.Slice((*T)(unsafe.Pointer(&b[0])), n)
}

// slab hands out byte ranges of a big mapping in size classes, eight per
// doubling, with a free list per class threaded through the free slots.
// Offset 0 is never handed out, so 0 means "none".
type slab struct {
	mem   []byte
	top   int
	heads [classes]uint64
}

const (
	minClass = 64
	classes  = 8 * 20
)

func newSlab(n int) *slab {
	return &slab{mem: mmapBytes(n), top: minClass}
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

func (s *slab) alloc(n int) uint64 {
	c, size := classOf(n)
	if h := s.heads[c]; h != 0 {
		s.heads[c] = *(*uint64)(unsafe.Pointer(&s.mem[h]))
		return h
	}
	off := s.top
	if off+size > len(s.mem) {
		panic("slab is full")
	}
	s.top += size
	return uint64(off)
}

func (s *slab) free(off uint64, n int) {
	if off == 0 {
		return
	}
	c, _ := classOf(n)
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

func (a *arena) add(p []byte) uint64 {
	if a.top+len(p) > len(a.mem) {
		panic("arena is full")
	}
	off := a.top
	a.top += copy(a.mem[off:], p)
	return uint64(off)
}

func (a *arena) get(off uint64, n uint32) []byte { return a.mem[off : off+uint64(n) : off+uint64(n)] }
