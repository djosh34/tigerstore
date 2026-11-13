package core

import (
	"bytes"
	"testing"
	"time"

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

// held is the room of the chunks the slabs hold, and the number of
// classes that hold any.
func held(sl *slabs) (bytes int64, classes int64) {
	for _, cl := range sl.classes {
		if len(cl.chunks) > 0 {
			classes++
		}
		for _, ch := range cl.chunks {
			bytes += int64(len(ch.data))
		}
	}
	return bytes, classes
}

// settle takes the cache's steps until it has none left.
func settle(t *testing.T, c *cache) {
	t.Helper()
	for range 1 << 20 {
		if !c.busy() {
			return
		}
		c.step()
	}
	t.Fatal("the cache's steps do not end")
}

// TestCacheRebuildFreesSlabs checks that the steps move the values that
// stay out of the chunks they can empty, so that those chunks go, and
// that every value stays as it was.
func TestCacheRebuildFreesSlabs(t *testing.T) {
	c := newCache()
	c.setLimit(1 << 30)
	const size = 1024
	per := chunkBytes / size
	ref := func(i int) mvcc.Ref { return mvcc.Ref{Index: uint64(i), Len: size} }
	val := func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8)}, size/2) }
	for i := 1; i <= 16*per; i++ {
		c.add(ref(i), val(i))
	}
	for i := 1; i <= 16*per; i++ {
		if i%8 != 0 {
			c.drop(ref(i))
		}
	}
	if !c.compacting {
		t.Fatal("the slabs do not compact with 7 of 8 slots free")
	}
	settle(t, c)
	// 2 chunks' worth of values stay, and the steps stop once the free
	// slots take at most a quarter of the room in use or 2 MiB.
	if n := len(c.slabs.classes[classOf(size)].chunks); n > 2+(minRebuildSlack/2)/chunkBytes {
		t.Fatalf("%d values of %d bytes are in %d chunks after the steps", c.count(), size, n)
	}
	for i := 8; i <= 16*per; i += 8 {
		if v, ok := c.get(ref(i)); !ok || !bytes.Equal(v, val(i)) {
			t.Fatalf("value %d after the steps: cached %v", i, ok)
		}
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
	before, _ := held(&sl)
	for calls := 0; ; calls++ {
		copied := 0
		more := sl.compact(64<<10, func(owner int32, to slot) {
			copied += size
			where[owner] = to
		})
		if copied > 64<<10+size {
			t.Fatalf("a call copied %d bytes, past a budget of %d", copied, 64<<10)
		}
		now, _ := held(&sl)
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

// TestCacheRebuildsForNewSizes fills the cache with values of one size,
// then with values of the next class: the first class's slots empty as
// its values go, and the steps give back their chunks, so the chunks it
// holds stay near what it counts (#602).
func TestCacheRebuildsForNewSizes(t *testing.T) {
	c := newCache()
	const limit = 32 << 20
	c.setLimit(limit)
	for i := range 2 * limit / 1000 {
		c.add(mvcc.Ref{Index: uint64(i + 1), Len: 1000}, make([]byte, 1000))
		if i%64 == 0 {
			c.step()
		}
	}
	for i := range 2 * limit / 1100 {
		c.add(mvcc.Ref{Index: uint64(1<<30 + i), Len: 1100}, make([]byte, 1100))
		if i%64 == 0 {
			c.step()
		}
	}
	settle(t, c)
	// The chunks hold the slots in use, the free slots the cache allows
	// once its steps end, and the unused end of each class's last chunk.
	held, classes := held(&c.slabs)
	if bound := c.slabs.used + max(c.slabs.used/2, minRebuildSlack) + classes*chunkBytes; held > bound {
		t.Fatalf("the slots in use take %d bytes, and the chunks hold %d, more than %d", c.slabs.used, held, bound)
	}
}

// TestShrinkingCacheTakesShortSteps fills a cache with 256 MiB of values
// and shrinks it, once by its limit and once by dropping 7 of each 8
// values, as overwrites do. No call holds the core for long, and the
// slabs never hold more chunks than before: the cache gives memory back
// a step at a time, never copying itself whole (#647). Rebuilding the
// whole cache at once took 1.8 s with 900 MB cached, past the election
// timeout, and the leader lost its leadership.
func TestShrinkingCacheTakesShortSteps(t *testing.T) {
	const (
		size  = 1000
		total = 256 << 20
		count = total / size
	)
	val := func(i int) []byte { return bytes.Repeat([]byte{byte(i), byte(i >> 8), byte(i >> 16), 7}, size/4) }
	ref := func(i int) mvcc.Ref { return mvcc.Ref{Index: uint64(i + 1), Len: size} }
	for _, shrink := range []string{"limit", "drops"} {
		t.Run(shrink, func(t *testing.T) {
			c := newCache()
			c.setLimit(2 * total)
			for i := range count {
				c.add(ref(i), val(i))
			}
			before, _ := held(&c.slabs)
			var longest time.Duration
			timed := func(f func()) {
				start := time.Now()
				f()
				longest = max(longest, time.Since(start))
				if now, _ := held(&c.slabs); now > before {
					t.Fatalf("the chunks grew from %d to %d bytes", before, now)
				}
			}
			kept := func(i int) bool { return i%8 == 0 }
			if shrink == "limit" {
				// The most recently used eighth stays.
				kept = func(i int) bool { return i >= count-count/8 }
				timed(func() { c.setLimit(c.bytes / 8) })
			} else {
				for i := range count {
					if !kept(i) {
						timed(func() { c.drop(ref(i)) })
					}
				}
			}
			if !c.busy() {
				t.Fatal("the cache has no steps to take after shrinking by 7/8")
			}
			steps := 0
			for c.busy() {
				timed(c.step)
				if steps++; steps > 100000 {
					t.Fatal("the cache's steps do not end")
				}
			}
			after, classes := held(&c.slabs)
			if bound := c.slabs.used + max(c.slabs.used/4, minRebuildSlack/2) + classes*chunkBytes; after > bound {
				t.Fatalf("after %d steps the slots in use take %d bytes and the chunks hold %d, more than %d", steps, c.slabs.used, after, bound)
			}
			if c.old != nil || c.peak > 2*c.count() {
				t.Fatalf("after the steps: old map %v, peak %d for %d values", c.old != nil, c.peak, c.count())
			}
			for i := range count {
				v, ok := c.get(ref(i))
				if ok != kept(i) && shrink == "drops" || ok && !bytes.Equal(v, val(i)) {
					t.Fatalf("value %d: cached %v, intact %v", i, ok, ok && bytes.Equal(v, val(i)))
				}
			}
			t.Logf("%d steps, the longest call %v", steps, longest)
			// The race detector makes every copy many times slower; the
			// bound holds in the normal build.
			if !raceOn && longest > 50*time.Millisecond {
				t.Fatalf("a call took %v while the cache shrank", longest)
			}
		})
	}
}
