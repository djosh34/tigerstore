package mvcc

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

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
		if s.keys.Len() != keys || s.byKey.n != keys {
			t.Fatalf("%d keys and %d in the table, want %d", s.keys.Len(), s.byKey.n, keys)
		}
		allocated := 4*int64(cap(s.free)) + 8*int64(cap(s.byKey.entries)) + room*64 + 4*int64(cap(s.hist.keys)) + 8*int64(cap(s.hist.starts))
		for _, c := range s.recs {
			allocated += int64(cap(c)) * recBytes
		}
		for _, c := range s.arena {
			allocated += int64(cap(c))
		}
		if got, want := s.Bytes(), allocated+s.keys.bytes(); got != want {
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
