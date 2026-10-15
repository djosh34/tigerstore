package mvcc

import (
	"bufio"
	"bytes"
	"slices"
	"strconv"
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

// TestBytes checks that the index estimate follows the keys, their bytes
// and the room their version arrays have through puts, deletes, two
// compactions and a checkpoint load, and that a compaction gives back the
// array of a key that lost most of its versions.
func TestBytes(t *testing.T) {
	check := func(s *Store, keys int) {
		t.Helper()
		var keyBytes, room int64
		s.keys.Ascend(func(k *keyIndex) bool {
			keyBytes += int64(len(k.key))
			room += int64(cap(k.vers))
			return true
		})
		if s.keys.Len() != keys {
			t.Fatalf("%d keys, want %d", s.keys.Len(), keys)
		}
		if got := s.Bytes(); got != int64(keys)*64+keyBytes+room*64 {
			t.Fatalf("Bytes = %d, want %d keys, %d key bytes, room for %d versions", got, keys, keyBytes, room)
		}
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
	check(s, 3)
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	// The tombstone of bb is older than the compaction, so bb goes.
	check(s, 2)
	if s.Bytes() > 1024 {
		t.Fatalf("Bytes = %d after the compaction left one version per key", s.Bytes())
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
}
