package wl

import (
	"errors"
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
		if _, _, err := st.del(id, st.rev, true); err != nil {
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
	n := &node{st: st, ckptDir: t.TempDir(), confState: &pb.ConfState{Voters: []uint64{1, 2, 3}}, storage: newLogStorage(l), auth: newAuthStore()}
	n.rm = newRemover(n.sys, func() {})
	t.Cleanup(n.rm.close)
	return n, l
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
		if _, _, err := st.del(id, st.rev, true); err != nil {
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
	if _, err := st.put(id, []byte("l"), 7, st.rev, l.add("l"), true); err != nil { // 10
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
	if p, err := readPointer(sysEnv{}, dir); p != nil || err != nil {
		t.Fatalf("no pointer reads as %v, %v", p, err)
	}
	hs := &pb.HardState{Term: new(uint64(4)), Vote: new(uint64(2)), Commit: new(uint64(99))}
	if err := writePointer(sysEnv{}, dir, &ckptPointer{gen: 1, name: "x.ckpt", logFrom: 12345, hs: hs}); err != nil {
		t.Fatal(err)
	}
	p, err := readPointer(sysEnv{}, dir)
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
	if err := selfContained(f, l, sent); err != nil {
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

// A checkpoint keeps whether its store compacted, which tells one at
// revision 0 from none: a store started from it refuses a second
// Compact at 0, as etcd's does, and one never compacted takes a first.
func TestACheckpointKeepsACompactionAtZero(t *testing.T) {
	for _, compacted := range []bool{true, false} {
		n, l := ckptNode(t)
		putValue(t, n.st, l, "/a", "a")
		if compacted {
			n.st.compact(0)
		}
		writeCkpt(t, n, 7)
		f, err := os.Open(filepath.Join(n.ckptDir, ckptName(7)))
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := loadCkpt(f, 1, loaded); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		n.st = loaded
		got := rejected(n.applyCompact(appendVarintField(nil, 1, 0), nil, nil))
		want := ""
		if compacted {
			want = msgCompacted
		}
		if got != want {
			t.Fatalf("compacted at 0 before the checkpoint %v: a Compact at 0 after it got %q, want %q", compacted, got, want)
		}
	}
}

// A key deleted at the compacted revision keeps that deletion in a
// checkpoint at a later revision, as its newest state there, even when a
// put makes the key again before the checkpoint's scan reaches it. The
// scan kept such a deletion only while it was the key's state in the
// store, so the checkpoint lost it, and the store started from it would
// differ from the store it was taken of (seed 33970 of TestSeeds, #712).
func TestACheckpointKeepsADeletionAtTheCompactedRevision(t *testing.T) {
	n, l := ckptNode(t)
	takeEveryValue(t)
	putValue(t, n.st, l, "/a", "a1")
	id, _ := n.st.find([]byte("/a"))
	n.st.rev++
	if _, _, err := n.st.del(id, n.st.rev, true); err != nil {
		t.Fatal(err)
	}
	n.st.endApply()
	deleted := n.st.rev
	putValue(t, n.st, l, "/b", "b1")
	n.st.compact(deleted)
	want, at := n.st.stateHash(), n.st.rev
	n.ckptFrom = int64(len(l.b))
	if err := n.startCkpt(7, 1, 0); err != nil {
		t.Fatal(err)
	}
	// The next write makes /a again before the scan.
	putValue(t, n.st, l, "/a", "a2")
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(7)))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	loaded.log = l
	if _, err := loadCkpt(f, 1, loaded); err != nil {
		t.Fatal(err)
	}
	if got := loaded.stateHash(); got != want {
		t.Fatal("the checkpoint holds another state than the store it was taken of")
	}
	if _, ok := valueAt(t, loaded, "/b", at); !ok {
		t.Fatal("the checkpoint lost /b")
	}
}

// A checkpoint makes and removes files off the loop's thread: under the
// writeback of other checkpoints, unlinking the one before held the loop
// 215 to 555 ms (#704). Its writer makes its file, and the remover
// removes the checkpoint before, so neither waits in the loop.
func TestACheckpointLeavesItsFilesToOtherGoroutines(t *testing.T) {
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", "a1")
	writeCkpt(t, n, 1)
	first := filepath.Join(n.ckptDir, n.ckptName)

	// A remover that removes nothing until it runs.
	idle := &remover{sys: n.sys, paths: make(chan string, removerQueue), done: make(chan struct{}), wake: func() {}}
	n.rm = idle
	putValue(t, n.st, l, "/a", "a2")
	if err := n.startCkpt(2, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(n.ckptDir, n.ckpt.w.tmp)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the loop made the checkpoint's file: %v", err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("the loop removed the checkpoint before: %v", err)
	}
	go idle.run()
	idle.close()
	if _, err := os.Stat(first); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the remover left the checkpoint before: %v", err)
	}
	if got, ok := valueAt(t, n.st, "/a", n.st.rev); !ok || got != "a2" {
		t.Fatalf("/a reads %q from the new checkpoint", got)
	}
}

// A turn's key scan counts the history it walks behind a hold, not only
// the states it takes in: a key put 1,000 times since a held revision
// ends the turn (the re-check of 6c8b0fcc: counting the three it took
// in, the turn went on past it).
func TestACheckpointScanCountsTheHistoryItWalks(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	bound := ckptScanStates
	ckptScanStates = 100
	t.Cleanup(func() { ckptScanStates = bound })
	putValue(t, st, l, "/a", "a1") // 2
	st.holdRead(2)
	for i := range 1000 {
		putValue(t, st, l, "/a", strconv.Itoa(i))
	}
	for k := range 3 {
		putValue(t, st, l, fmt.Sprintf("/once%d", k), "v")
	}
	st.compact(st.rev)
	st.trimSteps(1 << 20)
	if err := n.startCkpt(uint64(st.rev), 1, 0); err != nil {
		t.Fatal(err)
	}
	r := n.ckpt
	n.scanKeys(r)
	if id, _ := st.find([]byte("/a")); r.phase != ckptScanning || r.next != id+1 {
		t.Fatalf("the turn went on past /a, which walked 1,000 states: scanning %v, at key %d", r.phase == ckptScanning, r.next)
	}
	n.abortCkpt()
	st.releaseRead(2)
}

// A turn's key scan reads at most ckptScanStates states past the key it is
// on: a store that never compacts holds every key's history, and a turn of
// 4,096 such keys held the loop 220-300 ms (#704). The checkpoint, scanned
// over many turns, still holds every key.
func TestACheckpointScansAFewStatesATurn(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	bound := ckptScanStates
	ckptScanStates = 100
	t.Cleanup(func() { ckptScanStates = bound })
	for i := range 300 {
		for k := range 3 {
			putValue(t, st, l, fmt.Sprintf("/k%d", k), strconv.Itoa(i))
		}
	}
	for k := range 50 {
		putValue(t, st, l, fmt.Sprintf("/once%02d", k), "v")
	}
	if err := n.startCkpt(uint64(st.rev), 1, 0); err != nil {
		t.Fatal(err)
	}
	r := n.ckpt
	var per []uint32
	for r.phase == ckptScanning {
		before := r.next
		n.scanKeys(r)
		per = append(per, r.next-before)
	}
	// Each of the three keys of 300 states takes a turn alone; the 50 of
	// one state each go at most 100 a turn.
	if len(per) < 4 || per[0] != 1 || per[1] != 1 || per[2] != 1 {
		t.Fatalf("keys scanned a turn: %v; want the first three alone", per)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(filepath.Join(n.ckptDir, ckptName(uint64(st.rev))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadCkpt(f, 1, loaded); err != nil {
		t.Fatal(err)
	}
	if got := scanKeys(loaded, "/", ""); len(got) != 53 {
		t.Fatalf("the checkpoint holds %d keys, want 53", len(got))
	}
}

// A node loads its checkpoint within its budget: Go's memory limit comes
// down as the store's tables grow outside Go's heap during the load, and
// ends at what the budget leaves Go once they are counted. Under the
// whole GOMEMLIMIT, past-ram's member, restarted on 1.4M keys, peaked at
// 762 MiB on a 560 MiB budget, with Go's heap at 413 MB on top of the
// tables.
func TestACheckpointLoadsWithinTheBudget(t *testing.T) {
	n, l := ckptNode(t)
	const keys = 4 * loadGrowKeys
	for i := range keys {
		putValue(t, n.st, l, fmt.Sprintf("/load/%06d", i), "v")
	}
	writeCkptAt(t, n, 7)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	st.log = l
	m := &node{st: st, ckptDir: n.ckptDir, storage: newLogStorage(nil), auth: newAuthStore()}
	const budget = 256 << 20
	if _, err := m.loadWithinBudget(budget, 0); err != nil {
		t.Fatal(err)
	}
	if m.st.nkeys != keys {
		t.Fatalf("the load took %d keys, want %d", m.st.nkeys, keys)
	}
	if m.st.grew != nil {
		t.Fatal("the store's grew outlives the load")
	}
	// Once before the load, and again as its tables grew.
	if m.goLimits < 2 {
		t.Fatalf("Go's limit was set %d times over the load, want it to follow the tables", m.goLimits)
	}
	if want := max(budget-m.st.outside(), m.st.goNeed()); m.goLimit > want+budget/64 {
		t.Fatalf("Go's limit after the load is %d MiB, past the %d MiB the budget leaves it", m.goLimit>>20, want>>20)
	}
}

// A follower installs a snapshot within its budget: it lets go of its old
// store before it builds the new one, whose tables Go's limit follows as
// they grow, and ends within what the budget leaves Go. Both stores at
// once held two stores' tables, past the budget on a large store (#719).
func TestASnapshotInstallsWithinTheBudget(t *testing.T) {
	const keys = 4 * loadGrowKeys
	// The leader's checkpoint, which the follower receives.
	n, l := ckptNode(t)
	for i := range keys {
		putValue(t, n.st, l, fmt.Sprintf("/snap/%06d", i), "v")
	}
	writeCkptAt(t, n, 7)
	b, err := os.ReadFile(filepath.Join(n.ckptDir, ckptName(7)))
	if err != nil {
		t.Fatal(err)
	}
	recv := filepath.Join(t.TempDir(), "received")
	if err := os.WriteFile(recv, b, 0o600); err != nil {
		t.Fatal(err)
	}
	// The follower, its store as large and its log on disk.
	m, ml := ckptNode(t)
	for i := range keys {
		putValue(t, m.st, ml, fmt.Sprintf("/old/%06d", i), "v")
	}
	if m.wal, err = openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.wal.close)
	const budget = 256 << 20
	m.st.setBudget(budget, 0)
	m.limitGo()
	old, before := m.st, m.goLimits
	sn := &pb.Snapshot{Data: []byte(recv), Metadata: &pb.SnapshotMetadata{Index: new(uint64(9)), Term: new(uint64(1)), ConfState: m.confState}}
	if err := m.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	if m.st == old || !old.closed {
		t.Fatal("the old store outlives the install")
	}
	if m.st.nkeys != keys || m.st.grew != nil {
		t.Fatalf("the installed store holds %d keys (grew set: %v), want %d", m.st.nkeys, m.st.grew != nil, keys)
	}
	// Down once the old store went, then up again as the new one grew.
	if got := m.goLimits - before; got < 2 {
		t.Fatalf("Go's limit was set %d times over the install, want it to follow the tables", got)
	}
	if want := max(budget-m.st.outside(), m.st.goNeed()); m.goLimit > want+budget/64 {
		t.Fatalf("Go's limit after the install is %d MiB, past the %d MiB the budget leaves it", m.goLimit>>20, want>>20)
	}
}

// A checkpoint's writer checks for an abort before it names its file
// and points CURRENT at it, and abortCkpt waits for it: an install's
// pointer is never written alongside, or before, the writer's (#689's
// area-1 review, finding 1). Here the writer gets its last chunk after
// the abort: it names nothing and leaves CURRENT alone.
func TestAnAbortedCheckpointNeverPointsCurrent(t *testing.T) {
	dir := t.TempDir()
	cw := &ckptWriter{dir: dir, tmp: "w.tmp", name: ckptName(5), quit: make(chan struct{}), done: make(chan struct{})}
	close(cw.quit)
	c := &ckptChunk{buf: []byte("the last chunk"), last: true, ptr: &ckptPointer{name: ckptName(5)}}
	if err := cw.write(c); !errors.Is(err, errCkptAborted) {
		t.Fatalf("the aborted writer's last chunk gave %v, want errCkptAborted", err)
	}
	cw.closeFile()
	for _, name := range []string{ptrName, ckptName(5)} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the aborted writer made %s: %v", name, err)
		}
	}
}

// A checkpoint whose writer wrote its last chunk, aborted before the loop
// took the result, has its file closed: the writer leaves the file to
// ckptDone, which an abort never runs, and the descriptor stayed open
// (#689's re-check of area A).
func TestAnAbortedCheckpointClosesItsFile(t *testing.T) {
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/a", "v")
	if err := n.startCkpt(5, 1, 0); err != nil {
		t.Fatal(err)
	}
	r := n.ckpt
	for !r.lastOut {
		if err := n.ckptStep(false); err != nil {
			t.Fatal(err)
		}
		if n.ckpt == nil {
			t.Fatal("the checkpoint was taken before its last chunk went out")
		}
	}
	<-r.w.done
	f := r.w.f
	if f == nil {
		t.Fatal("the writer that wrote its last chunk holds no file")
	}
	n.abortCkpt()
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("the aborted checkpoint's file is still open: Stat gave %v", err)
	}
}

// A read held at a revision whose value only a replaced checkpoint holds
// reads it still, and checkpoints go on while it is held: each takes in
// the held states' values, so the one it replaces closes at once (#689's
// re-check, C4 and D1). Before, the replaced file was closed and reused,
// and the held read failed its checksum; then the checkpoint after waited
// for the hold, which a client that never reads keeps for good. A store
// loaded from each checkpoint holds the state it would without the hold.
func TestAHeldReadOutlivesTheCheckpointsItReadsFrom(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/b", "b1") // 3
	writeCkpt(t, n, 10)
	st.holdRead(2)
	putValue(t, st, l, "/a", "a2") // 4
	putValue(t, st, l, "/b", "b2") // 5
	st.compact(5)
	st.trimSteps(1 << 20)
	for index := uint64(11); index <= 14; index++ {
		gen := n.ckptGen
		writeCkpt(t, n, index)
		if n.ckptGen == gen {
			t.Fatalf("checkpoint %d was not written while a read is held", index)
		}
		if st.ckpts[gen] != nil {
			t.Fatalf("checkpoint %d left the one it replaced open", index)
		}
		f, err := os.Open(filepath.Join(n.ckptDir, ckptName(index)))
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		loaded.log = l
		if _, err := loadCkpt(f, n.ckptGen, loaded); err != nil {
			t.Fatal(err)
		}
		if loaded.stateHash() != st.stateHash() {
			t.Fatalf("checkpoint %d holds another state than its store's", index)
		}
		// Its keys hold the states from the compacted revision alone: the
		// held one's value lies in it unnamed.
		for _, key := range []string{"/a", "/b"} {
			id, _ := st.find([]byte(key))
			lid, _ := loaded.find([]byte(key))
			if got, want := len(loaded.ckptStates(lid, loaded.rev, 0, nil)), len(st.ckptStates(id, st.rev, st.compactRev, nil)); got != want {
				t.Fatalf("checkpoint %d holds %d states of %s, want %d", index, got, key, want)
			}
		}
		_ = loaded.close()
		_ = f.Close()
	}
	// Nothing is read from the log or the slab.
	l.b = nil
	st.fit()
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}
	if got, live := valueAt(t, st, "/a", 2); got != "a1" || !live {
		t.Fatalf("the held read of /a at 2 reads %q live %v", got, live)
	}
	st.releaseRead(2)
}

// cutLog is a memLog cut at cut: a read before it fails, as one of a
// segment removed.
type cutLog struct {
	l   *memLog
	cut int64
}

func (l cutLog) read(p []byte, off int64) error {
	if off < l.cut {
		return fmt.Errorf("log offset %d was cut at %d", off, l.cut)
	}
	return l.l.read(p, off)
}

// A held state's value that a checkpoint left in the log is not cut away
// by the next: that one takes it in (#689's re-check, D2). Before, it
// scanned the states from the compacted revision alone, cut the log past
// the value, and the held read failed.
func TestAHeldValueInTheLogOutlivesTheCut(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/b", "b1") // 3
	// No checkpoint before it: it leaves every value in the log.
	if err := n.startCkpt(10, 1, int64(len(l.b))); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	st.holdRead(2)
	putValue(t, st, l, "/a", "a2") // 4
	putValue(t, st, l, "/b", "b2") // 5
	st.compact(5)
	st.trimSteps(1 << 20)
	writeCkptAt(t, n, 11)
	if n.ckptCut == 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	st.log = cutLog{l, n.ckptCut}
	st.fit()
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}
	if got, live := valueAt(t, st, "/a", 2); got != "a1" || !live {
		t.Fatalf("the held read of /a at 2 reads %q live %v", got, live)
	}
}

// A watch sent in pieces holds the revision before its first event's,
// which may be the compacted one's (bigwatch.go), and may come while a
// checkpoint is written, after it scanned its keys: the checkpoint took
// in the value before the compacted revision too, while the store had it.
func TestAWatchHeldBeforeTheCompactedRevisionOutlivesTheCut(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/a", "a2") // 3
	st.compact(3)                  // not trimmed yet: /a at 2 is still there
	n.ckptFrom = int64(len(l.b))
	if err := n.startCkpt(10, 1, 0); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil && n.ckpt.phase == ckptScanning {
		if err := n.ckptStep(false); err != nil {
			t.Fatal(err)
		}
	}
	st.holdRead(2) // a prev_kv watch of /a's put at 3
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut <= 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	st.fit()
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 {
			st.slab.free(k.rec, int(k.rlen))
			k.rec, k.rlen = 0, 0
		}
	}
	if got, live := valueAt(t, st, "/a", 2); got != "a1" || !live {
		t.Fatalf("the held read of /a at 2 reads %q live %v", got, live)
	}
}

// A checkpoint takes in the states a held read sees, not the history
// between, which trimming keeps for the oldest hold: a key put 10,000
// times since the revision a read holds gives three states to take in:
// the newest, the one before the compacted revision, which a watch may
// hold (takeStates), and the held one (the re-check of 7ab6166c, area A: taking in
// every state since the hold, a checkpoint's own lists passed the
// node's budget). The held read still reads its value after the log is
// cut.
func TestACheckpointTakesInOnlyWhatHoldsSee(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	st.holdRead(2)
	for i := range 10000 {
		putValue(t, st, l, "/a", fmt.Sprintf("a%d", i+2))
	}
	st.compact(st.rev)
	st.trimSteps(1 << 20)
	id, _ := st.find([]byte("/a"))
	if got, _ := st.takeStates(id, st.rev, st.ckptRev(), nil); len(got) != 3 {
		t.Fatalf("a checkpoint takes in %d states of /a, want 3: the newest, the one before, and the held one", len(got))
	}
	writeCkpt(t, n, 10)
	// The values it took in: three of a few bytes, not 10,001.
	if fi, err := os.Stat(filepath.Join(n.ckptDir, ckptName(10))); err != nil || fi.Size() > 4<<10 {
		t.Fatalf("the checkpoint is %d bytes (%v): it took in the history between", fi.Size(), err)
	}
	st.log = cutLog{l, n.ckptCut}
	if n.ckptCut == 0 {
		t.Fatal("the checkpoint cut no log: the test shows nothing")
	}
	dropRecords(st)
	if got, live := valueAt(t, st, "/a", 2); got != "a1" || !live {
		t.Fatalf("the held read of /a at 2 reads %q live %v", got, live)
	}
}

// Reads held at several revisions keep the states each sees: a held
// reply holds each of its records' revisions (reserve.go), so a record
// newer than its oldest outlives the checkpoints too. Held at the
// oldest alone, /b at 3, overwritten since, was seen by no hold.
func TestReadsHeldAtSeveralRevisionsKeepTheStatesEachSees(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	defer st.close() //nolint:errcheck // a test's store
	takeEveryValue(t)
	putValue(t, st, l, "/a", "a1") // 2
	putValue(t, st, l, "/b", "b1") // 3
	st.holdRead(2)
	st.holdRead(3)
	putValue(t, st, l, "/b", "b2") // 4
	putValue(t, st, l, "/a", "a2") // 5
	st.compact(5)
	st.trimSteps(1 << 20)
	writeCkpt(t, n, 10)
	writeCkpt(t, n, 11)
	st.log = cutLog{l, n.ckptCut}
	dropRecords(st)
	for _, c := range []struct {
		key  string
		rev  int64
		want string
	}{{"/a", 2, "a1"}, {"/b", 3, "b1"}} {
		if got, live := valueAt(t, st, c.key, c.rev); got != c.want || !live {
			t.Fatalf("the held read of %s at %d reads %q live %v", c.key, c.rev, got, live)
		}
	}
}

// failingFile is a checkpoint file whose reads fail as a disk's do.
type failingFile struct{ name string }

func (failingFile) Read([]byte) (int, error)          { return 0, errCkptReadFails }
func (failingFile) ReadAt([]byte, int64) (int, error) { return 0, errCkptReadFails }
func (failingFile) Seek(int64, int) (int64, error)    { return 0, nil }
func (failingFile) Close() error                      { return nil }
func (f failingFile) Name() string                    { return f.name }

var errCkptReadFails = errors.New("input/output error")

// A checkpoint whose first read fails is not called corrupt: the read's
// error comes back, as a disk's (seed 32 of TestMembershipFaults took an
// EIO at a snapshot install for "no magic").
func TestACheckpointReadThatFailsIsNotCorrupt(t *testing.T) {
	st, _ := newTestStore(t)
	_, err := loadCkptChecked(failingFile{name: "c.ckpt"}, 0, st, true)
	if !errors.Is(err, errCkptReadFails) || errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("a checkpoint whose read fails loads with %v, want the read's error, not a corrupt checkpoint", err)
	}
}

// installFixture is a follower with a checkpoint at index 3 that CURRENT
// names, and the leader's snapshot at index 9 it receives.
func installFixture(t *testing.T) (*node, *pb.Snapshot) {
	t.Helper()
	n, l := ckptNode(t)
	putValue(t, n.st, l, "/leader", "v")
	writeCkptAt(t, n, 9)
	b, err := os.ReadFile(filepath.Join(n.ckptDir, ckptName(9)))
	if err != nil {
		t.Fatal(err)
	}
	m, ml := ckptNode(t)
	putValue(t, m.st, ml, "/follower", "v")
	writeCkptAt(t, m, 3)
	recv := filepath.Join(m.ckptDir, "received.tmp")
	if err := os.WriteFile(recv, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if m.wal, err = openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.wal.close)
	return m, &pb.Snapshot{Data: []byte(recv), Metadata: &pb.SnapshotMetadata{Index: new(uint64(9)), Term: new(uint64(1)), ConfState: m.confState}}
}

// An install that cannot write CURRENT leaves the checkpoint CURRENT
// names: the node starts again from it. Removed before the pointer was
// written, it left CURRENT naming a file gone, and the node could not
// start (#711's failure, by the install; #689's area-1 review, finding 2).
func TestAnInstallKeepsTheCheckpointCurrentNamesUntilItPointsOn(t *testing.T) {
	m, sn := installFixture(t)
	// CURRENT.tmp a directory: writing the pointer fails.
	if err := os.Mkdir(filepath.Join(m.ckptDir, ptrName+".tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.installSnapshot(sn); err == nil {
		t.Fatal("the install wrote CURRENT over a directory")
	}
	p, err := readPointer(sysEnv{}, m.ckptDir)
	if err != nil || p == nil {
		t.Fatalf("CURRENT after the failed install: %v, %v", p, err)
	}
	if _, err := os.Stat(filepath.Join(m.ckptDir, p.name)); err != nil {
		t.Fatalf("CURRENT names %s, which the failed install removed: %v", p.name, err)
	}
}

// An install's pointer takes the newest hard state handed to the log: an
// append still being synced, which the restart does not read again, may
// carry a vote that is sent once it is durable (#689's area-2 review).
func TestAnInstallsPointerKeepsTheNewestHardState(t *testing.T) {
	m, sn := installFixture(t)
	m.wal.durableHS = &pb.HardState{Term: new(uint64(4)), Vote: new(uint64(1)), Commit: new(uint64(2))}
	m.storage.setHardState(&pb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(uint64(2))})
	if err := m.installSnapshot(sn); err != nil {
		t.Fatal(err)
	}
	p, err := readPointer(sysEnv{}, m.ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	if p.hs.GetTerm() != 5 || p.hs.GetVote() != 3 {
		t.Fatalf("CURRENT after the install holds term %d, vote %d; want the newest, term 5 and vote 3", p.hs.GetTerm(), p.hs.GetVote())
	}
	if _, err := os.Stat(filepath.Join(m.ckptDir, ckptName(3))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the checkpoint before outlived the install: %v", err)
	}
}

// A snapshot received twice in one turn, before Raft's Ready, keeps the
// file Raft took: Raft ignores the second, at its new commit, and the
// Ready names the first. Removed as the second came in, the install
// found no file and the node stopped (#689's area-2 review).
func TestASnapshotReceivedTwiceKeepsTheOneRaftTook(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	dir := t.TempDir()
	m := func(path string) *pb.Message {
		return &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Snapshot: &pb.Snapshot{
			Data:     []byte(path),
			Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
		}}
	}
	var paths []string
	for _, name := range []string{"first.tmp", "second.tmp"} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("a checkpoint"), 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	for _, p := range paths {
		n.takeSnapshot(p, m(p))
	}
	rd := n.rn.Ready()
	named := string(rd.Snapshot.GetData())
	if named == "" {
		t.Fatal("Raft took neither snapshot")
	}
	if _, err := os.Stat(named); err != nil {
		t.Fatalf("Raft's Ready names %s, which is gone: %v", filepath.Base(named), err)
	}
}
