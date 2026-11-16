package wl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A value the disk damaged at rest, in the log or in a checkpoint, is
// refused wherever it is read back: by a read, by the copy into the next
// checkpoint, and by the copy sent to a follower. The store keeps the
// first such value, and the loop stops on it (#708).

// dropRecords lets go of every record the store holds, so that each read
// builds its record again from the value on disk.
func dropRecords(st *store) {
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}
}

// wantBadValue fails the test unless err is ErrBadValue.
func wantBadValue(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrBadValue) {
		t.Fatalf("%s: %v, want %v", what, err, ErrBadValue)
	}
}

func TestALiveValueDamagedInTheLogIsRefused(t *testing.T) {
	st, l := newTestStore(t)
	st.setBudget(1, 0) // every record is let go
	putValue(t, st, l, "/a", "a value")
	id, _ := st.find([]byte("/a"))
	if st.keys[id].rec != 0 {
		t.Fatal("the record is still held")
	}
	l.b[st.keys[id].voff+2] ^= 1
	rec, err := st.live(id)
	if rec != nil {
		t.Fatalf("served %q", recValue(rec))
	}
	wantBadValue(t, "a read of the live value", err)
	wantBadValue(t, "the store's first bad value", st.bad)
}

func TestAnOldValueDamagedInTheLogIsRefused(t *testing.T) {
	st, l := newTestStore(t)
	putValue(t, st, l, "/a", "old") // rev 2
	off := int64(len(l.b))
	putValue(t, st, l, "/a", "new") // rev 3
	l.b[off-1] ^= 0x40
	id, _ := st.find([]byte("/a"))
	if rec, err := st.at(id, 2); rec != nil || !errors.Is(err, ErrBadValue) {
		t.Fatalf("a read at revision 2: %q, %v; want %v", rec, err, ErrBadValue)
	}
	if rec, err := st.putAt(id, 2); rec != nil || !errors.Is(err, ErrBadValue) {
		t.Fatalf("a watch's read of revision 2: %q, %v; want %v", rec, err, ErrBadValue)
	}
	// The newest value is held, and served.
	if got, _ := valueAt(t, st, "/a", 3); got != "new" {
		t.Fatalf("revision 3 reads %q", got)
	}
}

func TestAValueDamagedInACheckpointIsRefused(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "a value in the checkpoint")
	putValue(t, st, l, "/b", "another")
	writeCkpt(t, n, 9)
	l.b = nil
	dropRecords(st)
	id, _ := st.find([]byte("/a"))
	k := &st.keys[id]
	if !inCkpt(k.voff) {
		t.Fatalf("the value lies at %#x, not in the checkpoint", k.voff)
	}
	f, err := os.OpenFile(filepath.Join(n.ckptDir, ckptName(9)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{0}
	at := k.voff&ckptOffMask + 3
	if _, err := f.ReadAt(b, at); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x10
	if _, err := f.WriteAt(b, at); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = st.live(id)
	wantBadValue(t, "a read from the checkpoint", err)
	if got, _ := valueAt(t, st, "/b", 0); got != "another" {
		t.Fatalf("/b reads %q", got)
	}
}

func TestAValueDamagedInTheLogIsNotCopiedIntoACheckpoint(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "a value the checkpoint takes in")
	id, _ := st.find([]byte("/a"))
	l.b[st.keys[id].voff] ^= 1
	takeEveryValue(t)
	n.ckptFrom = int64(len(l.b))
	if err := n.startCkpt(9, 1, 0); err != nil {
		t.Fatal(err)
	}
	var err error
	for n.ckpt != nil && err == nil {
		err = n.ckptStep(n.ckpt.sent > 0)
	}
	wantBadValue(t, "the checkpoint", err)
}

func TestAValueDamagedInTheLogIsNotSentToAFollower(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "a value the log keeps")
	writeCkptAt(t, n, 7) // the values stay in the log
	id, _ := st.find([]byte("/a"))
	if inCkpt(st.keys[id].voff) {
		t.Fatal("the checkpoint took the value in")
	}
	l.b[st.keys[id].voff+1] ^= 1
	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(7)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sent, err := os.Create(filepath.Join(t.TempDir(), "sent"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sent.Close() }()
	wantBadValue(t, "the follower's checkpoint", selfContained(f, l, sent))
}

// A start checks the values a checkpoint names in the log before where
// its scan of the log begins, and refuses one that does not check.
func TestAStartRefusesAValueDamagedInTheLogBeforeItsScan(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "a value the log keeps")
	putValue(t, st, l, "/b", "another")
	putValue(t, st, l, "/a", "the newest")
	writeCkptAt(t, n, 7) // the values stay in the log
	from := int64(len(l.b))
	load := func() *store {
		t.Helper()
		f, err := os.Open(filepath.Join(n.ckptDir, ckptName(7)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		loaded, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		loaded.log = l
		if _, err := loadCkpt(f, 1, loaded); err != nil {
			t.Fatal(err)
		}
		return loaded
	}
	if vs := load().logValues(from, nil); len(vs) != 3 {
		t.Fatalf("the checkpoint names %d values in the log, want 3", len(vs))
	}
	if err := load().checkLogValues(from); err != nil {
		t.Fatalf("a whole log: %v", err)
	}
	// The value overwritten: a read at its revision would need it.
	l.b[3] ^= 0x20
	err := load().checkLogValues(from)
	if !errors.Is(err, ErrCorruptLog) || !errors.Is(err, ErrBadValue) {
		t.Fatalf("a damaged log: %v, want %v and %v", err, ErrCorruptLog, ErrBadValue)
	}
}

// An entry Raft's storage let go and builds again from the log is read
// as its whole record, which must check: a damaged one is never handed
// to Raft to send, and the loop stops on it (storage.failed).
func TestAnEntryDamagedInTheLogIsNotBuiltAgain(t *testing.T) {
	l := &memLog{}
	s := newLogStorage(l)
	e1, e2 := testEntry(1, 1, "one"), testEntry(2, 1, "two")
	storeEntries(t, s, l, e1, e2)
	s.letGo(e2)
	l.b[len(l.b)-1] ^= 1
	ents, err := s.Entries(1, 3, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) == 2 && len(ents[1].GetData()) > 0 {
		t.Fatalf("entry 2 built again as %q", ents[1].GetData())
	}
	if !errors.Is(s.failed, ErrBadEntry) {
		t.Fatalf("the storage failed with %v, want %v", s.failed, ErrBadEntry)
	}
}
