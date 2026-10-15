package mvcc

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func hashKV(t *testing.T, s *Store, rev int64) KVHash {
	t.Helper()
	h, err := s.HashKV(rev)
	if err != nil {
		t.Fatalf("HashKV(%d): %v", rev, err)
	}
	return h
}

func del(t *testing.T, s *Store, k string) {
	t.Helper()
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte(k)}); err != nil {
		t.Fatal(err)
	}
}

// history writes a, b, a again, deletes b and writes c: revisions 2 to 6.
func history(t *testing.T) *Store {
	t.Helper()
	s := New(&Heap{})
	put(t, s, "a", "1")
	put(t, s, "b", "1")
	put(t, s, "a", "2")
	del(t, s, "b")
	put(t, s, "c", "1")
	return s
}

func TestHashKVRevisions(t *testing.T) {
	s := history(t)
	h := hashKV(t, s, 0)
	if h.Revision != 6 || h.CompactRevision != -1 {
		t.Fatalf("HashKV(0) = %+v, want revision 6 and compaction revision -1, as etcd before any compaction", h)
	}
	if hashKV(t, s, 6) != h {
		t.Fatal("HashKV(6) differs from HashKV(0) at revision 6")
	}
	at4 := hashKV(t, s, 4)
	put(t, s, "a", "3")
	if hashKV(t, s, 4) != at4 {
		t.Fatal("a write after revision 4 changed HashKV(4)")
	}
	if _, err := s.HashKV(8); !errors.Is(err, rpctypes.ErrGRPCFutureRev) {
		t.Fatalf("HashKV(8) at revision 7: %v, want ErrFutureRev", err)
	}
	if err := s.Compact(4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HashKV(3); !errors.Is(err, rpctypes.ErrGRPCCompacted) {
		t.Fatalf("HashKV(3) after compacting at 4: %v, want ErrCompacted", err)
	}
	if h := hashKV(t, s, 4); h.CompactRevision != 4 || h.Revision != 4 {
		t.Fatalf("HashKV(4) after compacting at 4 = %+v", h)
	}
}

func TestHashKVSeesValues(t *testing.T) {
	a, b := history(t), history(t)
	if hashKV(t, a, 0) != hashKV(t, b, 0) {
		t.Fatal("the same writes hash differently")
	}
	put(t, a, "d", "x")
	put(t, b, "d", "y")
	if hashKV(t, a, 0).Hash == hashKV(t, b, 0).Hash {
		t.Fatal("different values hash the same")
	}
}

// etcd's rules for versions at or below the compaction revision C: the
// version a read at the hash revision sees counts, other ones do not, and
// a tombstone at C never does.
func TestHashKVCompactedVersions(t *testing.T) {
	// a=1 at 2, b=1 at 3, a=2 at 4, b deleted at 5, c=1 at 6.
	compacted := history(t)
	if err := compacted.Compact(5); err != nil {
		t.Fatal(err)
	}
	// Left: a=2 at 4, the tombstone of b at 5, c=1 at 6. With a key
	// alive at 6, only a=2 counts below or at 5.
	want := newKVHasher()
	for _, k := range []string{"a", "c"} {
		ki, _ := compacted.keys.Get(&keyIndex{key: []byte(k)})
		want.add(ki.key, &ki.vers[len(ki.vers)-1])
	}
	if got := hashKV(t, compacted, 6); got.Hash != want.sum {
		t.Fatalf("HashKV(6) after compacting at 5 = %x, want %x", got.Hash, want.sum)
	}
	// A checkpoint copy holds the compaction back; the hash must not see
	// the versions it keeps for the copy.
	frozen := history(t)
	frozen.Freeze()
	if err := frozen.Compact(5); err != nil {
		t.Fatal(err)
	}
	if hashKV(t, frozen, 6) != hashKV(t, compacted, 6) {
		t.Fatal("a compaction held back by a checkpoint copy changes HashKV")
	}
	if frozen.HashAll() != compacted.HashAll() {
		t.Fatal("a compaction held back by a checkpoint copy changes HashAll")
	}
}

// With no key alive at the hash revision, etcd counts every version at or
// below C that is still there, but not a tombstone at C.
func TestHashKVNoLiveKey(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")
	put(t, s, "a", "2")
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	del(t, s, "a")
	ki, _ := s.keys.Get(&keyIndex{key: []byte("a")})
	want := newKVHasher()
	for i := range ki.vers {
		want.add(ki.key, &ki.vers[i])
	}
	if got := hashKV(t, s, 4); got.Hash != want.sum {
		t.Fatalf("HashKV(4) = %x, want %x over a=2 and its tombstone", got.Hash, want.sum)
	}
	if err := s.Compact(4); err != nil {
		t.Fatal(err)
	}
	if got := hashKV(t, s, 4); got.Hash != newKVHasher().sum {
		t.Fatalf("HashKV(4) after compacting at the tombstone = %x, want the empty hash", got.Hash)
	}
}

// TestCompactHash checks that a compaction takes HashKV of its revision
// as it was before the compaction, as etcd does, also while a checkpoint
// copy holds the compaction back.
func TestCompactHash(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		s := history(t)
		if err := s.Compact(3); err != nil {
			t.Fatal(err)
		}
		if frozen {
			s.Freeze()
		}
		put(t, s, "c", "2")
		want := hashKV(t, s, 6)
		if err := s.Compact(6); err != nil {
			t.Fatal(err)
		}
		if got := s.CompactHash(); got != want || got.CompactRevision != 3 {
			t.Fatalf("frozen %v: compaction hash %+v, HashKV before %+v", frozen, got, want)
		}
	}
}
