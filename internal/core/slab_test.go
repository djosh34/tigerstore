package core

import (
	"bytes"
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// TestSlabClasses checks that every size goes to the smallest class that
// holds it, that a slot wastes at most a fifth of itself, and that a value
// past the largest class gets an allocation of its own.
func TestSlabClasses(t *testing.T) {
	for n := 0; n <= maxSlot; n++ {
		ci := classOf(n)
		if ci == bigClass || classSizes[ci] < n || ci > 0 && classSizes[ci-1] >= n {
			t.Fatalf("%d bytes go to class %d", n, ci)
		}
		if n > 1<<minSlotBits && 5*(classSizes[ci]-n) > classSizes[ci] {
			t.Fatalf("%d bytes waste %d of a %d-byte slot", n, classSizes[ci]-n, classSizes[ci])
		}
	}
	if classOf(maxSlot+1) != bigClass {
		t.Fatal("a value past the largest class has a class")
	}
}

// TestSlabsReuseSlots checks that put copies a value, that a freed slot
// takes the next value of its class, and that chunks fill before a new
// one is made.
func TestSlabsReuseSlots(t *testing.T) {
	var sl slabs
	val := func(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }
	src := val(100, 1)
	a := sl.put(src)
	src[0] = 9
	if got := sl.bytes(a, 100); !bytes.Equal(got, val(100, 1)) {
		t.Fatal("put did not copy the value")
	}
	b := sl.put(val(100, 2))
	if a.class != b.class || b.n != a.n+1 {
		t.Fatalf("two values of one class took slots %v and %v", a, b)
	}
	sl.free(a)
	c := sl.put(val(99, 3))
	if c != a || !bytes.Equal(sl.bytes(c, 99), val(99, 3)) || !bytes.Equal(sl.bytes(b, 100), val(100, 2)) {
		t.Fatalf("the freed slot %v was not taken again: %v", a, c)
	}
	size := classSizes[a.class]
	for range chunkBytes/size - 2 {
		sl.put(val(100, 4))
	}
	if n := len(sl.classes[a.class].chunks); n != 1 {
		t.Fatalf("a full chunk's worth of values took %d chunks", n)
	}
	sl.put(val(100, 5))
	if n := len(sl.classes[a.class].chunks); n != 2 {
		t.Fatalf("one more value took %d chunks, want 2", n)
	}
	big := sl.put(val(maxSlot+1, 6))
	if big.class != bigClass || sl.room(big, maxSlot+1) != maxSlot+1 || !bytes.Equal(sl.bytes(big, maxSlot+1), val(maxSlot+1, 6)) {
		t.Fatalf("a big value: %v", big)
	}
	sl.free(big)
	if again := sl.put(val(maxSlot+2, 7)); again != big || !bytes.Equal(sl.bytes(again, maxSlot+2), val(maxSlot+2, 7)) {
		t.Fatalf("the freed big place %v was not taken again: %v", big, again)
	}
}

// TestCacheRebuildFreesSlabs checks that a rebuild moves the values that
// stay into new slabs, so that the chunks the others left go.
func TestCacheRebuildFreesSlabs(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 30)
	const size = 1024
	per := chunkBytes / size
	ref := func(i int) mvcc.Ref { return mvcc.Ref{Index: uint64(i), Len: size} }
	for i := 1; i <= 4*per; i++ {
		c.add(ref(i), bytes.Repeat([]byte{byte(i)}, size))
	}
	for i := 1; i <= 4*per; i++ {
		if i%8 != 0 {
			c.drop(ref(i))
		}
	}
	if n := len(c.slabs.classes[classOf(size)].chunks); n != 1 {
		t.Fatalf("%d values of %d bytes are in %d chunks after the rebuild, want 1", len(c.items), size, n)
	}
	for i := 8; i <= 4*per; i += 8 {
		if v, ok := c.get(ref(i)); !ok || !bytes.Equal(v, bytes.Repeat([]byte{byte(i)}, size)) {
			t.Fatalf("value %d after the rebuild: cached %v", i, ok)
		}
	}
}
