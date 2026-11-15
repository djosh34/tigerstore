package wl

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Keys that Kubernetes deletes, such as events, must not stay forever: a
// key deleted before the compacted revision gives back its id, its bytes
// and its index entry, and the next key takes them (#602).
func TestCompactionFreesDeletedKeys(t *testing.T) {
	st, l := newTestStore(t)
	const keys = 1000
	for i := range keys {
		putValue(t, st, l, fmt.Sprintf("/events/e%04d", i), "v")
	}
	for i := range keys {
		id, _ := st.find(fmt.Appendf(nil, "/events/e%04d", i))
		st.rev++
		if _, err := st.del(id, st.rev); err != nil {
			t.Fatal(err)
		}
		st.endApply()
	}
	putValue(t, st, l, "/keep", "k")
	ids, used := st.nkeys, st.kmem.used
	st.compact(st.rev)
	st.trimSteps(1 << 20)
	if len(st.keyFree) != keys {
		t.Fatalf("%d keys freed, want %d", len(st.keyFree), keys)
	}
	if st.kmem.used >= used {
		t.Fatalf("the key bytes stayed at %d", st.kmem.used)
	}
	if _, ok := st.find([]byte("/events/e0500")); ok {
		t.Fatal("a freed key is still in the index")
	}
	if got, ok := valueAt(t, st, "/keep", st.rev); !ok || got != "k" {
		t.Fatalf("/keep is %q, %v after the others were freed", got, ok)
	}
	for i := range keys {
		putValue(t, st, l, fmt.Sprintf("/events/f%04d", i), "w")
	}
	if st.nkeys != ids {
		t.Fatalf("the store holds %d key ids after new keys took the freed ones, want %d", st.nkeys, ids)
	}
	for i := 0; i < keys; i += 99 {
		if got, ok := valueAt(t, st, fmt.Sprintf("/events/f%04d", i), st.rev); !ok || got != "w" {
			t.Fatalf("a key in a freed id reads %q, %v", got, ok)
		}
	}
}

// The index stays whole when entries leave the middle of a run.
func TestRemoveEntryKeepsRunsFindable(t *testing.T) {
	idx := make([]uint64, 16)
	// Five entries whose home is slot 3, two whose home is 5: one run.
	for i := range 5 {
		putEntry(idx, 3, uint32(i))
	}
	putEntry(idx, 5, 10)
	putEntry(idx, 5, 11)
	removeEntry(idx, 4) // id 1
	removeEntry(idx, 3) // id 0
	have := map[uint32]bool{}
	for _, home := range []uint64{3, 5} {
		for i := home; idx[i] != 0; i = (i + 1) & 15 {
			if uint64(entryHash(idx[i]))&15 == home {
				have[uint32(idx[i])-1] = true
			}
		}
	}
	for _, id := range []uint32{2, 3, 4, 10, 11} {
		if !have[id] {
			t.Fatalf("id %d cannot be found from its home after the removals: %x", id, idx)
		}
	}
	if have[0] || have[1] {
		t.Fatal("a removed id is still there")
	}
}

// ckptNode is a node with only a store and Raft's storage, which
// checkpoints write.
func ckptNode(t *testing.T) (*node, *memLog) {
	t.Helper()
	st, l := newTestStore(t)
	return &node{st: st, ckptDir: t.TempDir(), confState: &pb.ConfState{Voters: []uint64{1, 2, 3}}, storage: newLogStorage(l), auth: newAuthStore()}, l
}

// writeCkpt writes a whole checkpoint, as the loop does turn by turn,
// that takes in every value the store names in its memLog.
func writeCkpt(t *testing.T, n *node, index uint64) {
	t.Helper()
	takeEveryValue(t)
	if l, ok := n.st.log.(*memLog); ok {
		n.ckptFrom = int64(len(l.b))
	}
	writeCkptAt(t, n, index)
}

// takeEveryValue makes checkpoints cut the log anywhere and take in every
// value before the cut.
func takeEveryValue(t *testing.T) {
	t.Helper()
	share, unit := ckptCutShare, ckptCutUnit
	ckptCutShare, ckptCutUnit = 1, 1
	t.Cleanup(func() { ckptCutShare, ckptCutUnit = share, unit })
}

// writeCkptAt writes a whole checkpoint, as the loop does turn by turn.
func writeCkptAt(t *testing.T, n *node, index uint64) {
	t.Helper()
	if err := n.startCkpt(index, 1, 0); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil {
		block := n.ckpt.sent > 0
		if err := n.ckptStep(block); err != nil {
			t.Fatal(err)
		}
	}
}

// A checkpoint holds what a read at the compacted revision or later sees,
// the leases and the changes; the store then reads its values from it and
// not from the log, and a store loaded from it reads the same.
func TestACheckpointHoldsTheStoreAndItsValues(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	st.leases[7] = &lease{ttl: 60, keys: map[uint32]struct{}{}}
	putValue(t, st, l, "/a", "a1")       // 2
	putValue(t, st, l, "/b", "b1")       // 3
	putValue(t, st, l, "/gone", "early") // 4
	putValue(t, st, l, "/a", "a2")       // 5
	del := func(key string) {
		id, _ := st.find([]byte(key))
		st.rev++
		if _, err := st.del(id, st.rev); err != nil {
			t.Fatal(err)
		}
		st.endApply()
	}
	del("/gone")                   // 6
	putValue(t, st, l, "/c", "c")  // 7
	putValue(t, st, l, "/a", "a3") // 8
	del("/b")                      // 9
	id, err := st.intern([]byte("/leased"))
	if err != nil {
		t.Fatal(err)
	}
	st.rev++
	if _, err := st.put(id, []byte("l"), 7, st.rev, l.add("l")); err != nil { // 10
		t.Fatal(err)
	}
	st.endApply()
	st.compact(6)
	st.trimSteps(1 << 20)

	writeCkpt(t, n, 42)
	if n.storage.snap.GetMetadata().GetIndex() != 42 || string(n.storage.snap.GetData()) != ckptName(42) {
		t.Fatalf("Raft's storage has the snapshot %v", n.storage.snap)
	}
	// Nothing is read from the log any more.
	l.b = nil
	st.fit()
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}

	type read struct {
		key  string
		rev  int64
		want string
		live bool
	}
	reads := []read{
		{"/a", 6, "a2", true},
		{"/a", 8, "a3", true},
		{"/a", 10, "a3", true},
		{"/b", 6, "b1", true},
		{"/b", 9, "", false},
		{"/gone", 6, "", false},
		{"/c", 7, "c", true},
		{"/c", 6, "", false},
		{"/leased", 10, "l", true},
	}
	check := func(st *store, what string) {
		t.Helper()
		for _, r := range reads {
			got, live := valueAt(t, st, r.key, r.rev)
			if got != r.want || live != r.live {
				t.Errorf("%s: %s at %d: %q live %v, want %q live %v", what, r.key, r.rev, got, live, r.want, r.live)
			}
		}
	}
	check(st, "the store")

	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(42)))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := loadCkpt(f, 1, loaded)
	if err != nil {
		t.Fatal(err)
	}
	if m.index != 42 || m.rev != 10 || m.compactRev != 6 || !slices.Equal(m.cs.GetVoters(), []uint64{1, 2, 3}) {
		t.Fatalf("the header reads %+v", m)
	}
	if loaded.rev != 10 || loaded.compactRev != 6 {
		t.Fatalf("the loaded store is at %d, compacted %d", loaded.rev, loaded.compactRev)
	}
	check(loaded, "the loaded store")
	if l := loaded.leases[7]; l == nil || l.ttl != 60 || len(l.keys) != 1 {
		t.Fatalf("lease 7 loads as %+v", l)
	}
	var got []string
	for _, ch := range loaded.changes {
		got = append(got, fmt.Sprintf("%d %s %v", ch.rev, loaded.key(ch.key), ch.del))
	}
	want := []string{"6 /gone true", "7 /c false", "8 /a false", "9 /b true", "10 /leased false"}
	if !slices.Equal(got, want) {
		t.Fatalf("changes %q, want %q", got, want)
	}
	if live := scanKeys(loaded, "/", ""); !slices.Equal(live, []string{"/a", "/c", "/leased"}) {
		t.Fatalf("live keys %q", live)
	}
}

// A checkpoint whose bytes changed does not load.
func TestACorruptCheckpointDoesNotLoad(t *testing.T) {
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", "value")
	writeCkpt(t, n, 3)
	path := filepath.Join(n.ckptDir, ckptName(3))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-6] ^= 1
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadCkpt(f, 0, st); err == nil {
		t.Fatal("a corrupt checkpoint loaded")
	}
}

// The pointer keeps the node's own part of a checkpoint.
func TestThePointerReadsBackWhatWasWritten(t *testing.T) {
	dir := t.TempDir()
	if p, err := readPointer(dir); p != nil || err != nil {
		t.Fatalf("no pointer reads as %v, %v", p, err)
	}
	hs := &pb.HardState{Term: new(uint64(4)), Vote: new(uint64(2)), Commit: new(uint64(99))}
	if err := writePointer(dir, &ckptPointer{gen: 1, name: "x.ckpt", logFrom: 12345, hs: hs}); err != nil {
		t.Fatal(err)
	}
	p, err := readPointer(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.gen != 1 || p.name != "x.ckpt" || p.logFrom != 12345 || p.hs.GetTerm() != 4 || p.hs.GetVote() != 2 || p.hs.GetCommit() != 99 {
		t.Fatalf("read back %+v", p)
	}
}

// A checkpoint leaves the values of the log it does not cut where they
// are: it names them there, and a store loaded from it reads them from
// the log. The copy a follower gets holds every value, and loads with no
// log at all.
func TestACheckpointLeavesValuesInTheLogItKeeps(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/b", "b1") // 3
	putValue(t, st, l, "/a", "a2") // 4
	writeCkptAt(t, n, 7)
	if n.ckptCut != 0 {
		t.Fatalf("the log is cut at %d", n.ckptCut)
	}
	reads := map[string]string{"/a@2": "a1", "/a@4": "a2", "/b@4": "b1"}
	check := func(st *store, what string) {
		t.Helper()
		for at, want := range reads {
			key, rev, _ := strings.Cut(at, "@")
			r, _ := strconv.ParseInt(rev, 10, 64)
			if got, live := valueAt(t, st, key, r); got != want || !live {
				t.Errorf("%s: %s reads %q live %v, want %q", what, at, got, live, want)
			}
		}
	}
	check(st, "the store")
	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(7)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	loaded.log = l
	if _, err := loadCkpt(f, 1, loaded); err != nil {
		t.Fatal(err)
	}
	check(loaded, "the loaded store")

	sent, err := os.Create(filepath.Join(t.TempDir(), "sent"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sent.Close() }()
	if err := selfContained(f, l, sent, nil); err != nil {
		t.Fatal(err)
	}
	follower, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	l.b = nil
	if _, err := loadCkpt(sent, 0, follower); err != nil {
		t.Fatal(err)
	}
	check(follower, "the follower's store")
}

func TestCutPoint(t *testing.T) {
	cases := []struct {
		live           map[int64]int64
		from, prevFrom int64
		want           int64
	}{
		{nil, 0, 0, 0},
		{nil, 0, 10, 10},
		{map[int64]int64{3: 1}, 0, 10, 10},         // 1 of 10 named
		{map[int64]int64{3: 4}, 0, 10, 3},          // 4 of 10 is more than a quarter; 0 of 3 before it
		{map[int64]int64{0: 1, 5: 9}, 0, 10, 5},    // 1 of 5 before segment 5
		{map[int64]int64{0: 9}, 0, 10, 0},          // nothing to cut
		{map[int64]int64{6: 1}, 4, 10, 10},         // 1 of 6 past the cut before
		{map[int64]int64{6: 1, 12: 50}, 4, 10, 10}, // what lies past prevFrom is not cut
		{map[int64]int64{4: 2, 6: 2}, 4, 10, 4},    // 4 of 6 is too much, 2 of 2 too: no cut
		{map[int64]int64{4: 1, 9: 9}, 4, 10, 9},    // 1 of 5 before segment 9
	}
	for _, c := range cases {
		if got := cutPoint(c.live, c.from, c.prevFrom, 1); got != c.want {
			t.Errorf("cutPoint(%v, %d, %d) = %d, want %d", c.live, c.from, c.prevFrom, got, c.want)
		}
	}
}
