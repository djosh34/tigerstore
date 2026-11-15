package wl

import (
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// storeEntries appends entries to l and s as the node does: data in the
// log first, then the entries with their offsets.
func storeEntries(t *testing.T, s *logStorage, l *memLog, ents ...*pb.Entry) {
	t.Helper()
	offs := make([]int64, len(ents))
	for i, e := range ents {
		offs[i] = l.add(string(e.GetData()))
	}
	if err := s.append(ents, offs); err != nil {
		t.Fatal(err)
	}
}

func entryData(t *testing.T, s *logStorage, lo, hi uint64) []string {
	t.Helper()
	ents, err := s.Entries(lo, hi, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for i, e := range ents {
		if e.GetIndex() != lo+uint64(i) {
			t.Fatalf("entry %d has index %d", lo+uint64(i), e.GetIndex())
		}
		out = append(out, string(e.GetData()))
	}
	return out
}

func TestStorageBuildsLetGoEntriesFromTheLog(t *testing.T) {
	l := &memLog{}
	s := newLogStorage(l)
	e1, e2, e3 := testEntry(1, 1, "one"), testEntry(2, 1, "two"), testEntry(3, 2, "three")
	storeEntries(t, s, l, e1, e2, e3)
	// The ring's bytes of entry 2 are overwritten once it is let go.
	s.letGo(e2)
	e2.Data[0] = 'X'
	if got := entryData(t, s, 1, 4); !slices.Equal(got, []string{"one", "two", "three"}) {
		t.Fatalf("entries %q", got)
	}
	if s.stats.reads != 1 || s.stats.letGo != 1 {
		t.Fatalf("%d reads and %d let go, want 1 and 1", s.stats.reads, s.stats.letGo)
	}
	if term, err := s.Term(2); err != nil || term != 1 {
		t.Fatalf("term of 2: %d, %v", term, err)
	}
}

func TestStorageReplacesAConflictingTail(t *testing.T) {
	l := &memLog{}
	s := newLogStorage(l)
	old3 := testEntry(3, 1, "old three")
	storeEntries(t, s, l, testEntry(1, 1, "one"), testEntry(2, 1, "two"), old3)
	storeEntries(t, s, l, testEntry(2, 2, "new two"))
	if last, _ := s.LastIndex(); last != 2 {
		t.Fatalf("last index %d, want 2", last)
	}
	if got := entryData(t, s, 1, 3); !slices.Equal(got, []string{"one", "new two"}) {
		t.Fatalf("entries %q", got)
	}
	if s.holds(old3) {
		t.Fatal("the storage still holds the entry it replaced")
	}
	if err := s.append([]*pb.Entry{testEntry(5, 2, "gap")}, []int64{0}); err == nil {
		t.Fatal("an append past the end was taken")
	}
}

func TestStorageCompacts(t *testing.T) {
	l := &memLog{}
	s := newLogStorage(l)
	storeEntries(t, s, l, testEntry(1, 1, "one"), testEntry(2, 1, "two"), testEntry(3, 2, "three"), testEntry(4, 2, "four"))
	s.compact(2)
	if first, _ := s.FirstIndex(); first != 3 {
		t.Fatalf("first index %d, want 3", first)
	}
	if term, err := s.Term(2); err != nil || term != 1 {
		t.Fatalf("term of the compacted 2: %d, %v", term, err)
	}
	if _, err := s.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("term of 1: %v, want ErrCompacted", err)
	}
	if _, err := s.Entries(2, 4, 1<<30); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("entries from 2: %v, want ErrCompacted", err)
	}
	if got := entryData(t, s, 3, 5); !slices.Equal(got, []string{"three", "four"}) {
		t.Fatalf("entries %q", got)
	}
	// An append that starts before the first entry keeps what is after.
	storeEntries(t, s, l, testEntry(2, 1, "two"), testEntry(3, 2, "three"), testEntry(4, 2, "four"), testEntry(5, 3, "five"))
	if got := entryData(t, s, 3, 6); !slices.Equal(got, []string{"three", "four", "five"}) {
		t.Fatalf("entries %q", got)
	}
}

func TestStorageEntriesKeepToTheirSize(t *testing.T) {
	l := &memLog{}
	s := newLogStorage(l)
	storeEntries(t, s, l, testEntry(1, 1, "aaaaaaaa"), testEntry(2, 1, "bbbbbbbb"), testEntry(3, 1, "cccccccc"))
	ents, err := s.Entries(1, 4, 1)
	if err != nil || len(ents) != 1 {
		t.Fatalf("%d entries, %v: want one past a limit below the first's size", len(ents), err)
	}
	ents, err = s.Entries(1, 4, 50)
	if err != nil || len(ents) != 2 {
		t.Fatalf("%d entries, %v: want two within 50 bytes", len(ents), err)
	}
}
