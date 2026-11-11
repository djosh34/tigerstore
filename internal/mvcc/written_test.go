package mvcc

import (
	"bufio"
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestLeavingAndReplaced checks that Leaving hands out the values written
// up to an index that are still their key's newest, once, and that
// Replaced hears of each value an overwrite or a delete replaces.
func TestLeavingAndReplaced(t *testing.T) {
	s := New(&Heap{})
	s.TrackWritten()
	var replaced []uint64
	s.Replaced = func(r Ref) { replaced = append(replaced, r.Index) }
	put(t, s, "a", "1") // value 1
	put(t, s, "b", "2") // value 2
	put(t, s, "c", "3") // value 3
	put(t, s, "a", "4") // value 4 replaces 1
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	// Keeps value 3: neither replaced nor written again.
	if _, err := s.Put(&pb.PutRequest{Key: []byte("c"), IgnoreValue: true}); err != nil {
		t.Fatal(err)
	}
	put(t, s, "d", "") // an empty value is never handed out
	if !slices.Equal(replaced, []uint64{1, 2}) {
		t.Fatalf("replaced %v, want [1 2]", replaced)
	}
	leaving := func(index uint64) []uint64 {
		var out []uint64
		s.Leaving(index, func(r Ref) { out = append(out, r.Index) })
		return out
	}
	if got := leaving(3); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("leaving up to 3: %v, want [3]", got)
	}
	if got := leaving(3); len(got) != 0 {
		t.Fatalf("leaving up to 3 again: %v", got)
	}
	put(t, s, "e", "5")
	if got := leaving(10); !slices.Equal(got, []uint64{4, 5}) {
		t.Fatalf("leaving up to 10: %v, want [4 5]", got)
	}
	if !s.Current([]byte("a"), s.Refs([]byte("a"))[1]) || s.Current([]byte("a"), s.Refs([]byte("a"))[0]) || s.Current([]byte("b"), s.Refs([]byte("b"))[0]) {
		t.Fatal("Current is wrong")
	}
}

// TestBytes checks that the index estimate counts the room allocated for
// records, free slots, the key table, the arena's chunks, version arrays
// and the history, through puts, deletes, two compactions and a checkpoint
// load, and that a compaction gives back the array of a key that lost most
// of its versions. Long keys that each take a chunk, and leave its tail
// unused, count with their chunks (#560).
func TestBytes(t *testing.T) {
	check := func(s *Store, keys int) int64 {
		t.Helper()
		var room int64
		s.each(func(k *keyIndex) bool {
			room += int64(cap(k.many))
			return true
		})
		if (s.keys.Len()+s.delta.Len()) != keys || s.byKey.n != keys {
			t.Fatalf("%d keys and %d in the table, want %d", (s.keys.Len() + s.delta.Len()), s.byKey.n, keys)
		}
		allocated := 4*int64(cap(s.free)) + 8*int64(cap(s.byKey.entries)) + room*64 + 4*int64(cap(s.hist.keys)) + 8*int64(cap(s.hist.starts))
		for _, c := range s.recs {
			allocated += int64(cap(c)) * recBytes
		}
		for _, c := range s.arena {
			allocated += int64(cap(c))
		}
		if got, want := s.Bytes(), allocated+int64(keys)*keySlot; got != want {
			t.Fatalf("Bytes = %d, want %d: %d allocated and %d keys", got, want, allocated, keys)
		}
		return s.Bytes()
	}
	s := New(&Heap{})
	put(t, s, "a", "1")
	put(t, s, "bb", "2")
	put(t, s, "a", "3")
	check(s, 2)
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("bb")}); err != nil {
		t.Fatal(err)
	}
	check(s, 2)
	for i := range 1000 {
		put(t, s, "hot", strconv.Itoa(i))
	}
	before := check(s, 3)
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	// The tombstone of bb is older than the compaction, so bb goes.
	if after := check(s, 2); before-after < 900*64 {
		t.Fatalf("Bytes went from %d to %d when a compaction dropped 999 versions", before, after)
	}
	put(t, s, "c", "4")
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	check(s, 3)
	s.Freeze()
	b, _, done := s.CopyFrozen(nil, 1<<20)
	if !done {
		t.Fatal("the copy is not done")
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	check(loaded, 3)
	long := New(&Heap{})
	for i := range 100 {
		key := fmt.Sprintf("%04d", i)
		put(t, long, key+strings.Repeat("x", keyChunk/5+1-len(key)), "")
	}
	if got := check(long, 100); got < 100*(keyChunk/5) {
		t.Fatalf("Bytes = %d for 100 keys of %d bytes", got, keyChunk/5+1)
	}
}
