package mvcc

import (
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestIndexPacksAndReusesSlots removes most keys in a compaction, so that
// the arena is packed and new keys take the removed keys' slots, and
// checks every key, by lookup, by a walk and by its history, against a
// model, with a key that an earlier answer holds unchanged.
func TestIndexPacksAndReusesSlots(t *testing.T) {
	s := New(&Heap{})
	model := map[string]string{}
	key := func(i int) string {
		return fmt.Sprintf("/registry/pods/ns-%03d/pod-%06d-%s", i%97, i, "padding-of-the-key")
	}
	const n = 40000
	for i := range n {
		put(t, s, key(i), strconv.Itoa(i))
		model[key(i)] = strconv.Itoa(i)
	}
	held := get(t, s, &pb.RangeRequest{Key: []byte(key(1)), RangeEnd: []byte(key(1) + "\x00")}).Kvs[0].Key
	check := func(when string) {
		t.Helper()
		for k, v := range model {
			kv := get(t, s, &pb.RangeRequest{Key: []byte(k)}).Kvs
			if len(kv) != 1 || string(kv[0].Value) != v {
				t.Fatalf("%s: %q reads %v, want %q", when, k, kv, v)
			}
		}
		all := get(t, s, &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}).Kvs
		want := slices.Sorted(maps.Keys(model))
		if len(all) != len(want) {
			t.Fatalf("%s: a walk finds %d keys, want %d", when, len(all), len(want))
		}
		for i, kv := range all {
			if string(kv.Key) != want[i] || string(kv.Value) != model[want[i]] {
				t.Fatalf("%s: key %d of the walk is %q=%q, want %q=%q", when, i, kv.Key, kv.Value, want[i], model[want[i]])
			}
		}
		if got := s.Counts().Keys; got != int64(len(model)) {
			t.Fatalf("%s: %d keys counted, want %d", when, got, len(model))
		}
		if string(held) != key(1) {
			t.Fatalf("%s: a key an answer holds became %q", when, held)
		}
	}
	for i := range n {
		if i%10 != 0 {
			del(t, s, key(i))
			delete(model, key(i))
		}
	}
	// A tombstone at the compaction's revision stays; this put leaves
	// none there.
	put(t, s, "/z", "z")
	model["/z"] = "z"
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	if removed := n - n/10; s.deadKeys != 0 || len(s.free) != removed {
		t.Fatalf("after the compaction: %d dead key bytes, %d free slots, want 0 and %d", s.deadKeys, len(s.free), removed)
	}
	check("after the compaction")
	// As many new keys as were removed take their slots.
	used := s.used
	from := s.Rev() + 1
	last := n + n - n/10
	for i := n; i < last; i++ {
		put(t, s, key(i), strconv.Itoa(i))
		model[key(i)] = strconv.Itoa(i)
	}
	if s.used != used || len(s.free) != 0 {
		t.Fatalf("%d slots made and %d left free, want the removed ones taken", s.used-used, len(s.free))
	}
	check("after the slots were taken again")
	h := s.History([]byte{0}, []byte{0}, from)
	if len(h) != last-n {
		t.Fatalf("%d changes since revision %d, want %d", len(h), from, last-n)
	}
	for i, c := range h {
		if want := key(n + i); string(c.Key()) != want {
			t.Fatalf("change %d is of %q, want %q", i, c.Key(), want)
		}
	}
	for pos := s.HistoryFrom(from); pos < s.HistoryEnd(); pos++ {
		c, want := s.Changed(pos), h[pos-s.HistoryFrom(from)]
		if c.Gone() || string(c.Key()) != string(want.Key()) || c.ModRev() != want.ModRev() {
			t.Fatalf("history position %d holds %q at %d, want %q at %d", pos, c.Key(), c.ModRev(), want.Key(), want.ModRev())
		}
	}
}

// TestNestedWalks walks a range of keys and, for each, a second range:
// the inner walk sets the probes the outer one compares with, so the
// outer walk must still stop at its own end.
func TestNestedWalks(t *testing.T) {
	s := New(&Heap{})
	for i := range 2000 {
		put(t, s, fmt.Sprintf("k%04d", i), "v")
	}
	var outer, inner int
	s.ascend([]byte("k0100"), []byte("k0200"), func(k *keyIndex) bool {
		outer++
		s.ascend([]byte("k1500"), []byte("k1510"), func(*keyIndex) bool {
			inner++
			return true
		})
		s.ascendFrom([]byte("k1990"), func(*keyIndex) bool { return true })
		return true
	})
	if outer != 100 || inner != 100*10 {
		t.Fatalf("the outer walk saw %d keys and the inner ones %d, want 100 and 1000", outer, inner)
	}
}

// TestIndexMemoryPerKey bounds the heap objects and bytes the index takes
// for a key with one version. With #552's map beside the B-tree it took
// 4.05 objects and 276 bytes a key, and past-ram's watches stopped once
// the index outgrew the node's memory (#558); with slots, 1.05 and 184.
func TestIndexMemoryPerKey(t *testing.T) {
	const n = 200000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "/registry/pods/bench-%03d-%04d/pod-%08d", i%7, i%5000, i)
	}
	val := make([]byte, 100)
	measure := func() runtime.MemStats {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m
	}
	before := measure()
	s := New(&nullValues{})
	for _, k := range keys {
		if _, err := s.Put(&pb.PutRequest{Key: k, Value: val}); err != nil {
			t.Fatal(err)
		}
	}
	after := measure()
	// The keys stay live through both measures, so only the index counts.
	runtime.KeepAlive(keys)
	objects := float64(after.HeapObjects-before.HeapObjects) / n
	bytes := float64(after.HeapAlloc-before.HeapAlloc) / n
	t.Logf("%.2f objects and %.0f bytes a key; Bytes says %d", objects, bytes, s.Bytes()/n)
	if objects > 1.5 || bytes > 220 {
		t.Fatalf("the index takes %.2f heap objects and %.0f bytes a key, want at most 1.5 and 220", objects, bytes)
	}
	runtime.KeepAlive(s)
}

// nullValues places values without keeping them, so that only the index
// takes memory.
type nullValues struct{ n uint64 }

func (v *nullValues) Place(b []byte) (Ref, error) {
	v.n++
	return Ref{Index: v.n, Len: uint32(len(b))}, nil
}

func (v *nullValues) Copies(Ref) bool { return false }

func (v *nullValues) Read(Ref, *[]byte) ([]byte, bool, error) { return nil, false, nil }

// TestRecordSize checks recBytes, what Bytes counts for a record.
func TestRecordSize(t *testing.T) {
	if got := unsafe.Sizeof(keyIndex{}); got != recBytes {
		t.Fatalf("a record takes %d bytes, recBytes says %d", got, recBytes)
	}
}
