package wl

import (
	"bytes"
	"os"
	"slices"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// reopen opens the log in dir as a node does when it starts, into a new
// Raft storage.
func reopen(t *testing.T, dir string) (*wal, *logStorage) {
	t.Helper()
	s := newLogStorage(nil)
	w, err := openWAL(dir, s.restore)
	if err != nil {
		t.Fatal(err)
	}
	s.log = w
	return w, s
}

// writeSynced writes entries and a hard state, and waits until they are
// on disk.
func writeSynced(t *testing.T, w *wal, s *logStorage, hs *pb.HardState, ents ...*pb.Entry) {
	t.Helper()
	var b []byte
	if hs != nil {
		var err error
		if b, err = proto.Marshal(hs); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.write(ents, b); err != nil {
		t.Fatal(err)
	}
	if err := s.append(ents, w.offs); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
}

func allData(t *testing.T, s *logStorage) []string {
	t.Helper()
	last, _ := s.LastIndex()
	first, _ := s.FirstIndex()
	if last < first {
		return nil
	}
	return entryData(t, s, first, last+1)
}

func TestRecoveryRestoresTheLogAndGoesOn(t *testing.T) {
	dir := t.TempDir()
	w, s := reopen(t, dir)
	if last, _ := s.LastIndex(); last != 0 {
		t.Fatalf("an empty log recovers entries to %d", last)
	}
	writeSynced(t, w, s, &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, testEntry(1, 1, "one"), testEntry(2, 1, "two"), testEntry(3, 1, "three"))
	// A new leader's append replaces entry 3.
	writeSynced(t, w, s, &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(2)), Commit: new(uint64(3))}, testEntry(3, 2, "new three"))

	w2, s2 := reopen(t, dir)
	if got := allData(t, s2); !slices.Equal(got, []string{"one", "two", "new three"}) {
		t.Fatalf("recovered entries %q", got)
	}
	if term, _ := s2.Term(3); term != 2 {
		t.Fatalf("entry 3 recovered with term %d, want 2", term)
	}
	if s2.hs.GetTerm() != 2 || s2.hs.GetVote() != 2 || s2.hs.GetCommit() != 3 {
		t.Fatalf("recovered hard state %v", s2.hs)
	}
	if w2.off != w.off {
		t.Fatalf("the recovered log ends at %d, the written one at %d", w2.off, w.off)
	}
	// It goes on where it ended, and a third start sees both.
	writeSynced(t, w2, s2, nil, testEntry(4, 2, "four"))
	_, s3 := reopen(t, dir)
	if got := allData(t, s3); !slices.Equal(got, []string{"one", "two", "new three", "four"}) {
		t.Fatalf("entries after a second start %q", got)
	}
}

func TestRecoveryCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	w, s := reopen(t, dir)
	writeSynced(t, w, s, nil, testEntry(1, 1, "one"), testEntry(2, 1, "two"), testEntry(3, 1, "torn"))
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[bytes.Index(b, []byte("torn"))] ^= 1
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	w2, s2 := reopen(t, dir)
	if got := allData(t, s2); !slices.Equal(got, []string{"one", "two"}) {
		t.Fatalf("recovered entries %q, want the two before the torn one", got)
	}
	writeSynced(t, w2, s2, nil, testEntry(3, 1, "three"))
	_, s3 := reopen(t, dir)
	if got := allData(t, s3); !slices.Equal(got, []string{"one", "two", "three"}) {
		t.Fatalf("entries after the torn tail was written over %q", got)
	}
}
