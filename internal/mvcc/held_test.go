package mvcc

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// countedHeap is a Heap that counts the values read from it, which a
// node reads from Raft entries or the log.
type countedHeap struct {
	Heap
	reads int
}

func (h *countedHeap) Read(r Ref, arena *[]byte) ([]byte, bool, error) {
	h.reads++
	return h.Heap.Read(r, arena)
}

// getValue returns the value of key as a Range reads it now.
func getValue(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	resp := get(t, s, &pb.RangeRequest{Key: []byte(key)})
	if len(resp.Kvs) != 1 {
		t.Fatalf("%s: %d keys", key, len(resp.Kvs))
	}
	return resp.Kvs[0].Value
}

// TestHeldValues checks that the store answers reads of the newest
// values from memory, that the value a write replaces stays readable from
// memory until Release, and that an older value comes from Values.
func TestHeldValues(t *testing.T) {
	h := &countedHeap{}
	s := New(h)
	s.SetValueLimit(1 << 20)
	put(t, s, "a", "1")
	put(t, s, "b", "2")
	if string(getValue(t, s, "a")) != "1" || string(getValue(t, s, "b")) != "2" || h.reads != 0 {
		t.Fatalf("reads of the newest values went to Values %d times", h.reads)
	}
	put(t, s, "a", "3")
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	// IgnoreValue keeps the value it had, held.
	put(t, s, "c", "4")
	if _, err := s.Put(&pb.PutRequest{Key: []byte("c"), IgnoreValue: true}); err != nil {
		t.Fatal(err)
	}
	at := func(key string, rev int64) []byte {
		resp := get(t, s, &pb.RangeRequest{Key: []byte(key), Revision: rev})
		if len(resp.Kvs) != 1 {
			t.Fatalf("%s at %d: %d keys", key, rev, len(resp.Kvs))
		}
		return resp.Kvs[0].Value
	}
	// The replaced values of a and b, at revisions 2 and 3.
	if string(at("a", 2)) != "1" || string(at("b", 3)) != "2" || string(getValue(t, s, "c")) != "4" || h.reads != 0 {
		t.Fatalf("reads in the turn went to Values %d times", h.reads)
	}
	held := s.ValueBytes()
	s.Release()
	if s.ValueBytes() >= held {
		t.Fatalf("Release left %d bytes of %d held", s.ValueBytes(), held)
	}
	if string(at("a", 2)) != "1" || h.reads != 1 {
		t.Fatalf("a replaced value after Release: %d reads of Values", h.reads)
	}
	if string(getValue(t, s, "a")) != "3" || h.reads != 1 {
		t.Fatalf("the newest value after Release: %d reads of Values", h.reads)
	}
}

// TestHeldValuesWithinLimit checks that the store holds no more values
// than its limit lets it, lets go of older keys' values for new ones, and
// holds a value again that Keep brings back.
func TestHeldValuesWithinLimit(t *testing.T) {
	h := &countedHeap{}
	s := New(h)
	val := func(i int) string { return fmt.Sprintf("%0100d", i) }
	// Room for about 10 values of 100 bytes in their slots of 112.
	s.SetValueLimit(10 * 112)
	for i := range 20 {
		put(t, s, fmt.Sprintf("k%02d", i), val(i))
	}
	s.Release()
	s.Step()
	if b := s.ValueBytes(); b > 10*112 {
		t.Fatalf("%d bytes held, past the limit of %d", b, 10*112)
	}
	if string(getValue(t, s, "k19")) != val(19) || h.reads != 0 {
		t.Fatal("the newest key's value is not held")
	}
	if string(getValue(t, s, "k00")) != val(0) || h.reads != 1 {
		t.Fatal("the oldest key's value is still held")
	}
	s.SetValueLimit(30 * 112)
	r := s.Refs([]byte("k00"))[0]
	s.Keep([]byte("k00"), r, []byte(val(0)))
	if string(getValue(t, s, "k00")) != val(0) || h.reads != 1 {
		t.Fatal("Keep did not hold the value read back")
	}
}

// slabBytes is the room of the chunks the slabs hold, and the number of
// classes that hold any.
func slabBytes(sl *slabs) (bytes int64, classes int64) {
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

// TestHeldValuesShrinkInShortSteps holds 256 MiB of values and shrinks
// them, once by the limit and once by deleting 7 of each 8 keys, as
// overwrites do. No call holds the core for long, and the slabs never
// hold more chunks than before: the store gives memory back a step at a
// time, never copying its values whole (#647). Copying the whole value
// cache at once took 1.8 s with 900 MB in it, past the election timeout,
// and the leader lost its leadership.
func TestHeldValuesShrinkInShortSteps(t *testing.T) {
	// Values of 16 bytes fill a chunk's slots 65,536 to the chunk, so a
	// step that moves many of them must not search the chunk's free
	// slots from its start for each.
	for _, tc := range []struct {
		shrink      string
		size, total int
	}{{"limit", 1000, 256 << 20}, {"drops", 1000, 256 << 20}, {"drops", 16, 8 << 20}} {
		size, total := tc.size, tc.total
		count := total / size
		t.Run(fmt.Sprintf("%s/%dB", tc.shrink, size), func(t *testing.T) {
			s := New(&Heap{})
			s.SetValueLimit(int64(total) * 2)
			key := func(i int) []byte { return fmt.Appendf(nil, "k%08d", i) }
			val := func(i int) []byte {
				return bytes.Repeat([]byte{byte(i), byte(i >> 8), byte(i >> 16), 7}, size/4)
			}
			for i := range count {
				if _, err := s.Put(&pb.PutRequest{Key: key(i), Value: val(i)}); err != nil {
					t.Fatal(err)
				}
			}
			s.Release()
			before, _ := slabBytes(&s.vals.slabs)
			var longest time.Duration
			timed := func(f func()) {
				start := time.Now()
				f()
				longest = max(longest, time.Since(start))
				if now, _ := slabBytes(&s.vals.slabs); now > before {
					t.Fatalf("the chunks grew from %d to %d bytes", before, now)
				}
			}
			if tc.shrink == "limit" {
				timed(func() { s.SetValueLimit(s.ValueBytes() / 8) })
			} else {
				for i := range count {
					if i%8 != 0 {
						if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: key(i)}); err != nil {
							t.Fatal(err)
						}
					}
				}
				timed(s.Release)
			}
			if !s.ValuesBusy() {
				t.Fatal("the held values have no steps to take after shrinking by 7/8")
			}
			steps := 0
			for s.ValuesBusy() {
				timed(s.Step)
				if steps++; steps > 100000 {
					t.Fatal("the steps do not end")
				}
			}
			v := &s.vals
			after, classes := slabBytes(&v.slabs)
			if bound := v.slabs.used + max(v.slabs.used/4, minSlack/2) + classes*chunkBytes; after > bound {
				t.Fatalf("after %d steps the slots in use take %d bytes and the chunks hold %d, more than %d", steps, v.slabs.used, after, bound)
			}
			for i := range count {
				k, ok := s.get(key(i))
				if !ok || !k.held {
					continue
				}
				if got := v.slabs.bytes(k.slotOf(), size); !bytes.Equal(got, val(i)) {
					t.Fatalf("value %d is not intact after the steps", i)
				}
			}
			t.Logf("%d steps, the longest call %v", steps, longest)
			// The race detector makes every copy many times slower; the
			// bound holds in the normal build.
			if !raceOn && longest > 20*time.Millisecond {
				t.Fatalf("a call took %v while the values shrank", longest)
			}
		})
	}
}
