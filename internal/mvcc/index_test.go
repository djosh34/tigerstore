package mvcc

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestIndexPacksAndReusesSlots removes most keys in two compactions, so
// that the arena is packed and the removed keys' slots are taken again,
// and checks every key, by lookup, by a walk and by its history, against
// a model, with the keys an earlier answer holds unchanged.
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
	if s.deadKeys != 0 || len(s.limbo) != n-n/10 {
		t.Fatalf("after the first compaction: %d dead key bytes, %d slots in limbo, want 0 and %d", s.deadKeys, len(s.limbo), n-n/10)
	}
	check("after the first compaction")
	// The new keys get slots of their own until the next compaction lets
	// the removed ones go again.
	used := s.used
	from := s.Rev() + 1
	for i := n; i < n+100; i++ {
		put(t, s, key(i), strconv.Itoa(i))
		model[key(i)] = strconv.Itoa(i)
	}
	if s.used != used+100 {
		t.Fatalf("%d slots made for 100 keys while the removed ones wait", s.used-used)
	}
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	// As many as were removed.
	last := n + 100 + n - n/10
	for i := n + 100; i < last; i++ {
		put(t, s, key(i), strconv.Itoa(i))
		model[key(i)] = strconv.Itoa(i)
	}
	if s.used != used+100 {
		t.Fatalf("%d slots made after the compaction, want the removed ones taken again", s.used-used-100)
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
	// The second compaction dropped the history before it, which the
	// slots taken again then wrote.
	tail := h[len(h)-int(s.HistoryEnd()-s.HistoryStart()):]
	for pos := s.HistoryStart(); pos < s.HistoryEnd(); pos++ {
		c, want := s.Changed(pos), tail[pos-s.HistoryStart()]
		if c.Gone() || string(c.Key()) != string(want.Key()) || c.ModRev() != want.ModRev() {
			t.Fatalf("history position %d holds %q at %d, want %q at %d", pos, c.Key(), c.ModRev(), want.Key(), want.ModRev())
		}
	}
}

// TestRecordSize checks recBytes, what Bytes counts for a record.
func TestRecordSize(t *testing.T) {
	if got := unsafe.Sizeof(keyIndex{}); got != recBytes {
		t.Fatalf("a record takes %d bytes, recBytes says %d", got, recBytes)
	}
}
