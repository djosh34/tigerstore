package wl

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// reopen opens the log in dir as a node does when it starts, into a new
// Raft storage.
func reopen(t *testing.T, dir string) (*wal, *logStorage) {
	t.Helper()
	s := newLogStorage(nil)
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, s.restore)
	if err != nil {
		t.Fatal(err)
	}
	s.log = w
	t.Cleanup(w.close)
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

	w.close()
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
	w2.close()
	_, s3 := reopen(t, dir)
	if got := allData(t, s3); !slices.Equal(got, []string{"one", "two", "new three", "four"}) {
		t.Fatalf("entries after a second start %q", got)
	}
}

func TestRecoveryCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	w, s := reopen(t, dir)
	writeSynced(t, w, s, nil, testEntry(1, 1, "one"), testEntry(2, 1, "two"), testEntry(3, 1, "torn"))
	w.close()
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tear(b, int64(bytes.Index(b, []byte("torn")))-walEntHdr)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	w2, s2 := reopen(t, dir)
	if got := allData(t, s2); !slices.Equal(got, []string{"one", "two"}) {
		t.Fatalf("recovered entries %q, want the two before the torn one", got)
	}
	writeSynced(t, w2, s2, nil, testEntry(3, 1, "three"))
	w2.close()
	_, s3 := reopen(t, dir)
	if got := allData(t, s3); !slices.Equal(got, []string{"one", "two", "three"}) {
		t.Fatalf("entries after the torn tail was written over %q", got)
	}
}

// reopenFrom opens the log in dir as a node does that starts from a
// checkpoint at index, whose log goes on at from.
func reopenFrom(t *testing.T, dir string, index uint64, from int64) (*wal, *logStorage) {
	t.Helper()
	s := newLogStorage(nil)
	s.applySnapshot(&pb.SnapshotMetadata{Index: new(index), Term: new(uint64(1))}, "c")
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, from, from, s.restore)
	if err != nil {
		t.Fatal(err)
	}
	s.log = w
	t.Cleanup(w.close)
	return w, s
}

// Behind a checkpoint, the log's segments are removed, and a node starts
// from the checkpoint and the log after it.
func TestTheLogIsCutBehindACheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, s := reopen(t, dir)
	data := strings.Repeat("d", 1<<20)
	const ents = 140 // more than two segments of 64 MiB
	for i := uint64(1); i <= ents; i += 8 {
		var batch []*pb.Entry
		for j := i; j < i+8 && j <= ents; j++ {
			batch = append(batch, testEntry(j, 1, data))
		}
		writeSynced(t, w, s, nil, batch...)
	}
	from := s.offOf(100) - walEntHdr
	if from/segSize != 1 {
		t.Fatalf("entry 100 lies in segment %d", from/segSize)
	}
	if err := w.cut(from); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segPath(dir, 0)); !os.IsNotExist(err) {
		t.Fatalf("segment 0 is still there: %v", err)
	}
	if _, err := os.Stat(segPath(dir, 1)); err != nil {
		t.Fatalf("segment 1, which holds entry 100: %v", err)
	}
	p := make([]byte, 4)
	if err := w.read(p, s.offOf(100)); err != nil {
		t.Fatalf("read entry 100 after the cut: %v", err)
	}
	if err := w.read(p, s.offOf(10)); err == nil {
		t.Fatal("read entry 10 from a segment the cut removed")
	}
	end := w.off
	w.close()

	w2, s2 := reopenFrom(t, dir, 99, from)
	if first, _ := s2.FirstIndex(); first != 100 {
		t.Fatalf("the log from the checkpoint starts at %d", first)
	}
	if last, _ := s2.LastIndex(); last != ents || w2.off != end {
		t.Fatalf("the log from the checkpoint ends at entry %d, offset %d; want %d, %d", last, w2.off, ents, end)
	}
	if got := entryData(t, s2, 140, 141); got[0] != data {
		t.Fatal("entry 140 reads back wrong")
	}
	writeSynced(t, w2, s2, nil, testEntry(ents+1, 1, "after"))
	w2.close()
	_, s3 := reopenFrom(t, dir, 99, from)
	if got := entryData(t, s3, ents+1, ents+2); got[0] != "after" {
		t.Fatalf("entry %d reads %q", ents+1, got[0])
	}
}

// A received snapshot's checkpoint names the log's end when it was
// installed; a crash can lose what lay before it, not yet durable. The
// log then goes on from there.
func TestTheLogGoesOnFromACheckpointPastItsEnd(t *testing.T) {
	dir := t.TempDir()
	w, s := reopen(t, dir)
	writeSynced(t, w, s, nil, testEntry(1, 1, "one"))
	from := w.off + 3*block + 100
	w.close()
	w2, s2 := reopenFrom(t, dir, 50, from)
	if w2.off != from {
		t.Fatalf("the log goes on at %d, want %d", w2.off, from)
	}
	writeSynced(t, w2, s2, nil, testEntry(51, 2, "fifty-one"))
	w2.close()
	_, s3 := reopenFrom(t, dir, 50, from)
	if got := allData(t, s3); !slices.Equal(got, []string{"fifty-one"}) {
		t.Fatalf("entries after the checkpoint %q", got)
	}
}
