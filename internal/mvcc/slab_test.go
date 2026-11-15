package mvcc

import (
	"bytes"
	"testing"
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
	a := sl.put(src, 1)
	src[0] = 9
	if got := sl.bytes(a, 100); !bytes.Equal(got, val(100, 1)) {
		t.Fatal("put did not copy the value")
	}
	b := sl.put(val(100, 2), 1)
	if a.class != b.class || b.n != a.n+1 {
		t.Fatalf("two values of one class took slots %v and %v", a, b)
	}
	sl.free(a)
	c := sl.put(val(99, 3), 1)
	if c != a || !bytes.Equal(sl.bytes(c, 99), val(99, 3)) || !bytes.Equal(sl.bytes(b, 100), val(100, 2)) {
		t.Fatalf("the freed slot %v was not taken again: %v", a, c)
	}
	size := classSizes[a.class]
	for range chunkBytes/size - 2 {
		sl.put(val(100, 4), 1)
	}
	if n := len(sl.classes[a.class].chunks); n != 1 {
		t.Fatalf("a full chunk's worth of values took %d chunks", n)
	}
	sl.put(val(100, 5), 1)
	if n := len(sl.classes[a.class].chunks); n != 2 {
		t.Fatalf("one more value took %d chunks, want 2", n)
	}
	big := sl.put(val(maxSlot+1, 6), 1)
	if big.class != bigClass || sl.room(big, maxSlot+1) != maxSlot+1 || !bytes.Equal(sl.bytes(big, maxSlot+1), val(maxSlot+1, 6)) {
		t.Fatalf("a big value: %v", big)
	}
	sl.free(big)
	if again := sl.put(val(maxSlot+2, 7), 1); again != big || !bytes.Equal(sl.bytes(again, maxSlot+2), val(maxSlot+2, 7)) {
		t.Fatalf("the freed big place %v was not taken again: %v", big, again)
	}
}

// TestSlabsCompactWithinBudget checks that one call of compact copies at
// most its budget and one slot more, that a chunk it empties goes, and
// that it tells the owner of each value it moves where the value went.
func TestSlabsCompactWithinBudget(t *testing.T) {
	var sl slabs
	const size = 4096
	per := chunkBytes / size
	where := map[int32]slot{}
	for i := range 8 * per {
		where[int32(i)] = sl.put(bytes.Repeat([]byte{byte(i)}, size), int32(i))
	}
	for i := range 8 * per {
		if i%4 != 0 {
			sl.free(where[int32(i)])
			delete(where, int32(i))
		}
	}
	before, _ := slabBytes(&sl)
	for calls := 0; ; calls++ {
		copied := 0
		more := sl.compact(64<<10, func(owner int32, to slot) {
			copied += size
			where[owner] = to
		})
		if copied > 64<<10+size {
			t.Fatalf("a call copied %d bytes, past a budget of %d", copied, 64<<10)
		}
		now, _ := slabBytes(&sl)
		if now > before {
			t.Fatalf("the chunks grew from %d to %d bytes while compacting", before, now)
		}
		if !more {
			break
		}
		if calls > 1000 {
			t.Fatal("compact does not end")
		}
	}
	if n := len(sl.classes[classOf(size)].chunks); n != 2 {
		t.Fatalf("%d values of a quarter chunk's room each are in %d chunks, want 2", len(where), n)
	}
	for owner, s := range where {
		if v := sl.bytes(s, size); v[0] != byte(owner) || v[size-1] != byte(owner) {
			t.Fatalf("value %d moved to %v holds %d", owner, s, v[0])
		}
	}
}
