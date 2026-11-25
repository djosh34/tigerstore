package wl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// memLog is a log in memory: values are appended and named by offset. A
// checkpoint's writer reads it from its own goroutine while a test writes
// on (the model test, heldmodel_test.go), so its writes and reads take mu.
type memLog struct {
	mu sync.Mutex
	b  []byte
}

func (l *memLog) add(v string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	off := int64(len(l.b))
	l.b = append(l.b, v...)
	return off
}

func (l *memLog) read(p []byte, off int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(l.b)) {
		return errors.New("read past the end")
	}
	copy(p, l.b[off:])
	return nil
}

// A memLog is durable as soon as it is written: a checkpoint's writer
// reads it as it reads the log.
func (l *memLog) durableEnd() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.b))
}
func (l *memLog) readDurable(p []byte, off int64) error { return l.read(p, off) }

func newTestStore(t *testing.T) (*store, *memLog) {
	t.Helper()
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	l := &memLog{}
	st.log = l
	return st, l
}

// putValue writes key=value at the store's next revision, the value in
// the log as the apply puts it there.
func putValue(t *testing.T, st *store, l *memLog, key, value string) {
	t.Helper()
	id, err := st.intern([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	st.rev++
	if _, err := st.put(id, []byte(value), 0, st.rev, l.add(value), true); err != nil {
		t.Fatal(err)
	}
	st.endApply()
}

func valueAt(t *testing.T, st *store, key string, rev int64) (string, bool) {
	t.Helper()
	id, ok := st.find([]byte(key))
	if !ok {
		return "", false
	}
	rec, err := st.at(id, rev)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		return "", false
	}
	return string(recValue(rec)), true
}

func TestStoreReadsOldValuesFromTheLog(t *testing.T) {
	st, l := newTestStore(t)
	putValue(t, st, l, "/a", "one")   // rev 2
	putValue(t, st, l, "/a", "two")   // rev 3
	putValue(t, st, l, "/b", "other") // rev 4
	id, _ := st.find([]byte("/a"))
	st.rev++
	if _, _, err := st.del(id, st.rev, true); err != nil { // rev 5
		t.Fatal(err)
	}
	st.endApply()
	putValue(t, st, l, "/a", "three") // rev 6

	for _, c := range []struct {
		rev  int64
		want string
		live bool
	}{{1, "", false}, {2, "one", true}, {3, "two", true}, {4, "two", true}, {5, "", false}, {6, "three", true}, {0, "three", true}} {
		got, live := valueAt(t, st, "/a", c.rev)
		if got != c.want || live != c.live {
			t.Errorf("/a at %d: %q live %v, want %q live %v", c.rev, got, live, c.want, c.live)
		}
	}
}

func TestStoreBudgetLetsRecordsGoAndBuildsThemAgain(t *testing.T) {
	st, l := newTestStore(t)
	// No room for records at all: every apply lets go of what it can.
	st.setBudget(1, 0)
	for i := range 100 {
		putValue(t, st, l, fmt.Sprintf("/k/%03d", i), fmt.Sprintf("value %d", i))
	}
	if st.evictions == 0 || st.slab.used != 0 {
		t.Fatalf("the slab holds %d bytes after %d evictions, want none held", st.slab.used, st.evictions)
	}
	for i := range 100 {
		id, _ := st.find(fmt.Appendf(nil, "/k/%03d", i))
		rec, err := st.live(id)
		if err != nil {
			t.Fatal(err)
		}
		want := appendKV(nil, fmt.Appendf(nil, "/k/%03d", i), fmt.Appendf(nil, "value %d", i), int64(i+2), int64(i+2), 1, 0)
		if !bytes.Equal(rec, want) {
			t.Fatalf("key %d built again as %x, want %x", i, rec, want)
		}
	}
	if st.loads != 100 {
		t.Fatalf("%d loads, want 100", st.loads)
	}
}

// A read of more records the budget let go than the slab's limit holds,
// with no apply in between to let any go, keeps them only up to the limit
// and builds the rest for its caller (#805). A range over every key kept
// each one it read: past the budget, and under the simulator, whose slab
// is fixed, the slab filled and the node closed the client ("the store's
// slab is full", TestAgedBulk's final reads).
func TestStoreReadKeepsRecordsOnlyWithinTheLimit(t *testing.T) {
	st, l := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	st.setBudget(1, 0)
	value := strings.Repeat("v", 4000)
	for i := range 64 {
		putValue(t, st, l, fmt.Sprintf("/k/%03d", i), value)
	}
	if st.slab.used != 0 {
		t.Fatalf("the slab holds %d bytes after every apply let its records go", st.slab.used)
	}
	// Room for three records.
	_, slot := classOf(kvSize(len("/k/000"), len(value), 2, 2, 1, 0))
	st.limit = int64(3 * slot)
	top := int64(st.slab.top)
	for i := range 64 {
		key := fmt.Appendf(nil, "/k/%03d", i)
		id, _ := st.find(key)
		rec, err := st.at(id, 0)
		if err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
		if want := appendKV(nil, key, []byte(value), int64(i+2), int64(i+2), 1, 0); !bytes.Equal(rec, want) {
			t.Fatalf("key %d read as %d bytes, want %d", i, len(rec), len(want))
		}
		if st.slab.used > st.limit {
			t.Fatalf("after %d reads the slab holds %d bytes, past its limit of %d", i+1, st.slab.used, st.limit)
		}
		// Its top, whose pages stay resident, grows no further than
		// topMax either.
		if t2 := int64(st.slab.top); t2 > max(top, st.topMax()) {
			t.Fatalf("after %d reads the slab's top is %d, past %d", i+1, t2, max(top, st.topMax()))
		}
	}
	kept := 0
	for i := range st.keys[:st.nkeys] {
		if st.keys[i].rec != 0 {
			kept++
		}
	}
	if st.loads != 64 || kept == 0 || kept > 3 || st.unkept != uint64(64-kept) {
		t.Fatalf("%d loads, %d kept, %d not kept; want 64 loads, 1 to 3 kept within the limit, the rest not", st.loads, kept, st.unkept)
	}
}

// A read keeps what it loads up to topMax, the room past the limit the
// next apply takes back (fit): a key read again and again is loaded once,
// not read from the log each time, though writes left the slab at its
// limit (the review of #807).
func TestStoreReadKeepsRecordsUpToTopMax(t *testing.T) {
	st, l := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	st.setBudget(1, 0)
	value := strings.Repeat("v", 4000)
	for i := range 16 {
		putValue(t, st, l, fmt.Sprintf("/k/%03d", i), value)
	}
	_, slot := classOf(kvSize(len("/k/000"), len(value), 2, 2, 1, 0))
	// A limit of four slots, and room for four more past it.
	st.budget, st.limit = int64(32*4*slot), int64(4*slot)
	read := func(i int) {
		t.Helper()
		id, _ := st.find(fmt.Appendf(nil, "/k/%03d", i))
		if _, err := st.at(id, 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4 {
		read(i)
	}
	loads := st.loads
	for range 20 {
		read(10)
	}
	if st.loads != loads+1 {
		t.Fatalf("a key read 20 times past the limit was loaded %d times, want once", st.loads-loads)
	}
}

// A store whose slab has no room left, as the simulator's fixed one past
// what it holds, builds what a read loads for its caller instead of
// failing it (#805): the node closed the client on errSlabFull.
func TestStoreReadWithAFullSlab(t *testing.T) {
	st, l := newTestStore(t)
	t.Cleanup(func() { _ = st.close() })
	st.setBudget(1, 0)
	value := strings.Repeat("v", 4000)
	for i := range 16 {
		putValue(t, st, l, fmt.Sprintf("/k/%03d", i), value)
	}
	// No budget, and a slab of room for two records.
	_, slot := classOf(kvSize(len("/k/000"), len(value), 2, 2, 1, 0))
	small, err := newSlab(minClass + 2*slot)
	if err != nil {
		t.Fatal(err)
	}
	st.budget, st.slab = 0, small
	for i := range 16 {
		key := fmt.Appendf(nil, "/k/%03d", i)
		id, _ := st.find(key)
		rec, err := st.at(id, 0)
		if err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
		if want := appendKV(nil, key, []byte(value), int64(i+2), int64(i+2), 1, 0); !bytes.Equal(rec, want) {
			t.Fatalf("key %d read as %d bytes, want %d", i, len(rec), len(want))
		}
	}
	if st.unkept != 14 {
		t.Fatalf("%d records built and not kept, want 14 past the two the slab holds", st.unkept)
	}
}

func TestStoreWithoutABudgetKeepsEveryRecord(t *testing.T) {
	st, l := newTestStore(t)
	for i := range 100 {
		putValue(t, st, l, fmt.Sprintf("/k/%03d", i), "v")
	}
	if st.evictions != 0 {
		t.Fatalf("%d evictions without a budget", st.evictions)
	}
}

func TestCompactionKeepsWhatReadsAtItAndAfterNeed(t *testing.T) {
	st, l := newTestStore(t)
	putValue(t, st, l, "/a", "one")   // 2
	putValue(t, st, l, "/a", "two")   // 3
	putValue(t, st, l, "/b", "b")     // 4
	putValue(t, st, l, "/a", "three") // 5
	id, _ := st.find([]byte("/b"))
	st.rev++
	if _, _, err := st.del(id, st.rev, true); err != nil { // 6
		t.Fatal(err)
	}
	st.endApply()
	putValue(t, st, l, "/a", "four") // 7

	st.compact(5)
	st.trimSteps(1 << 20)
	if st.trimming {
		t.Fatal("the trim did not finish")
	}
	for _, c := range []struct {
		key  string
		rev  int64
		want string
		live bool
	}{{"/a", 5, "three", true}, {"/a", 6, "three", true}, {"/a", 7, "four", true}, {"/b", 5, "b", true}, {"/b", 6, "", false}} {
		got, live := valueAt(t, st, c.key, c.rev)
		if got != c.want || live != c.live {
			t.Errorf("%s at %d: %q live %v, want %q live %v", c.key, c.rev, got, live, c.want, c.live)
		}
	}
	if len(st.changes) != 3 || st.changes[0].rev != 5 {
		t.Fatalf("the change index after compaction at 5: %+v", st.changes)
	}
	// The freed slots take the next history.
	high := st.nhist
	putValue(t, st, l, "/a", "five")
	putValue(t, st, l, "/a", "six")
	if st.nhist != high {
		t.Fatalf("history grew from %d to %d slots with freed ones to take", high, st.nhist)
	}
}

func TestCompactionTrimsAFewKeysAtATime(t *testing.T) {
	st, l := newTestStore(t)
	for i := range 2 * trimStep {
		putValue(t, st, l, fmt.Sprintf("/k/%05d", i), "v")
	}
	st.compact(st.rev)
	putValue(t, st, l, "/x", "v") // one apply
	if !st.trimming || st.trimKey != trimStep {
		t.Fatalf("after one apply the trim is at key %d (trimming %v), want %d", st.trimKey, st.trimming, trimStep)
	}
}

// The key index doubles as keys come: a k8s bench seed made more than the
// 4,194,304 keys that its old fixed size held, and every node stopped on
// errTooManyKeys (#602).
func TestTheStoreTakesKeysPastItsFirstIndex(t *testing.T) {
	st, _ := newTestStore(t)
	const keys = 1<<22 + 1000
	key := make([]byte, 0, 16)
	for i := range keys {
		key = fmt.Appendf(key[:0], "/k/%08d", i)
		id, err := st.intern(key)
		if err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
		if id != uint32(i) {
			t.Fatalf("key %d got id %d", i, id)
		}
	}
	for i := 0; i < keys; i += 997 {
		key = fmt.Appendf(key[:0], "/k/%08d", i)
		if id, ok := st.find(key); !ok || id != uint32(i) {
			t.Fatalf("key %d found as %d, %v", i, id, ok)
		}
	}
	if _, ok := st.find([]byte("/k/none")); ok {
		t.Fatal("found a key never added")
	}
}

// Once the limit falls well below the slab's top, as the tables grow with
// the keys, the store lets go of the records past the limit's page and
// gives the slab's pages past it back: a freed slot stays mapped and
// resident, and the slab kept the most it ever held, so a node of
// past-ram's 560 MiB budget reached 658 MB and its cgroup killed it.
// Every value still reads back, built again from the log, and later puts
// take slots that overlap none kept.
func TestTheSlabGivesBackWhatItsLimitNoLongerAllows(t *testing.T) {
	st, l := newTestStore(t)
	value := func(i int) string { return fmt.Sprintf("%04d", i) + strings.Repeat("v", 1000) }
	for i := range 2000 {
		putValue(t, st, l, fmt.Sprintf("/s/%04d", i), value(i))
	}
	top := st.slab.top
	st.budget, st.limit = 32<<20, int64(top/4)
	st.shrinkSlab()
	if st.slab.top >= top || st.slab.top > (int(st.limit)+pageSize-1)&^(pageSize-1) || st.shrinks != 1 || st.evictions == 0 {
		t.Fatalf("the slab's top went from %d to %d with a limit of %d: %d shrinks, %d evictions", top, st.slab.top, st.limit, st.shrinks, st.evictions)
	}
	for i := range st.keys[:st.nkeys] {
		if k := &st.keys[i]; k.rec != 0 && slotEnd(k.rec, int(k.rlen)) > st.slab.top {
			t.Fatalf("key %d's record ends at %d, past the slab's top %d", i, slotEnd(k.rec, int(k.rlen)), st.slab.top)
		}
	}
	st.budget = 0
	for i := 2000; i < 2500; i++ {
		putValue(t, st, l, fmt.Sprintf("/s/%04d", i), value(i))
	}
	for i := range 2500 {
		id, _ := st.find(fmt.Appendf(nil, "/s/%04d", i))
		rec, err := st.live(id)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(rec, []byte(value(i))) {
			t.Fatalf("key %d reads back as %q", i, rec[:min(len(rec), 40)])
		}
	}
}

// classSize is classOf's slot size of a class, the other way round; and a
// shrunk slab keeps on its free lists only the slots that end by its new
// top, handing out the rest of what it gives anew from that top.
func TestASlabShrinksToItsFreeSlotsBelowTheNewTop(t *testing.T) {
	for n := 1; n < 1<<22; n += n/7 + 1 {
		if c, size := classOf(n); c > 0 && classSize(c) != size {
			t.Fatalf("class %d of %d bytes: classSize %d, classOf %d", c, n, classSize(c), size)
		}
	}
	s, err := newSlab(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	var offs []uint64
	for range 400 {
		off, err := s.alloc(1000)
		if err != nil {
			t.Fatal(err)
		}
		offs = append(offs, off)
	}
	// Free every other slot; then shrink to the page past the 100th, the
	// live slots that end past it let go by their owners first.
	live := map[uint64]bool{}
	for i, off := range offs {
		if i%2 == 1 {
			s.free(off, 1000)
		} else {
			live[off] = true
		}
	}
	to := (slotEnd(offs[100], 1000) + pageSize - 1) &^ (pageSize - 1)
	for off := range live {
		if slotEnd(off, 1000) > to {
			s.free(off, 1000)
			delete(live, off)
		}
	}
	s.shrink(to)
	if s.top != to {
		t.Fatalf("the top is %d, want %d", s.top, to)
	}
	seen := map[uint64]bool{}
	for {
		off, err := s.alloc(1000)
		if err != nil {
			break
		}
		if seen[off] {
			t.Fatalf("slot %d handed out twice", off)
		}
		seen[off] = true
		if int(off) < to && slotEnd(off, 1000) > to {
			t.Fatalf("slot %d straddles the new top %d", off, to)
		}
		for l := range live {
			if off < uint64(slotEnd(l, 1000)) && l < uint64(slotEnd(off, 1000)) {
				t.Fatalf("slot %d overlaps live slot %d", off, l)
			}
		}
	}
}

// Writes are refused before the key slab, which holds the directories'
// paths too, the key table or the history's slots fill, whatever the quota, even with none:
// a write that fills one fails in the apply on every node, and again at
// each start. Reads and deletes go on. History slots compaction freed are
// room again.
func TestWritesAreRefusedBeforeTheArenasFill(t *testing.T) {
	msg, err := proto.Marshal(&pb.PutRequest{Key: []byte("/k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	for _, arena := range []string{"keys", "key table", "history"} {
		st, _ := newTestStore(t)
		n := &node{st: st, quota: -1}
		if !n.quotaLeft(kPut, msg) {
			t.Fatalf("%s: a put refused on an empty store with no quota", arena)
		}
		switch arena {
		case "keys":
			st.kmem.top = len(st.kmem.mem)/8*7 + 1
		case "key table":
			st.nkeys = maxKeys/8*7 + 1
		case "history":
			st.nhist = maxHist/8*7 + 1
		}
		for _, kind := range []uint8{kPut, kTxn, kLeaseGrant} {
			if n.quotaLeft(kind, msg) {
				t.Errorf("%s past seven eighths: a request of kind %d let in", arena, kind)
			}
		}
		if !n.quotaLeft(kDel, nil) || !n.quotaLeft(kRange, nil) {
			t.Errorf("%s past seven eighths: a delete or a range refused", arena)
		}
		if arena == "history" {
			st.nfree = 2 // slots compaction freed
			if !n.quotaLeft(kPut, msg) {
				t.Error("a put refused with history slots free again")
			}
		}
	}
}

// A delete of a range, or a lease's revoke, reads no value back when
// nothing asks for the values before (no prevKV, no watcher that wants
// them): with every record let go of, deleting a range of keys read each
// value back into the slab and held it to the apply's end, past the
// budget on every node. A delete that asks for them reads them.
func TestDeletesReadNoValueNothingAsksFor(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	st.setBudget(1, 0) // no room for records: each is let go of
	put := func(prefix string, lease int64) {
		for i := range 100 {
			key := fmt.Sprintf("%s%03d", prefix, i)
			id, err := st.intern([]byte(key))
			if err != nil {
				t.Fatal(err)
			}
			st.rev++
			if _, err := st.put(id, []byte("v"), lease, st.rev, l.add("v"), true); err != nil {
				t.Fatal(err)
			}
			st.endApply()
		}
	}
	del := func(prefix string, prevKV bool) ([][]byte, int64) {
		st.rev++
		kvs, deleted, err := n.deleteRange(&delReq{key: []byte(prefix), end: prefixEnd([]byte(prefix)), prevKV: prevKV}, st.rev, nil)
		if err != nil {
			t.Fatal(err)
		}
		st.endApply()
		return kvs, deleted
	}
	put("/a/", 0)
	loads := st.loads
	if kvs, deleted := del("/a/", false); deleted != 100 || len(kvs) != 0 || st.loads != loads {
		t.Fatalf("a delete of 100 keys without prevKV: %d deleted, %d records, %d loads", deleted, len(kvs), st.loads-loads)
	}
	put("/b/", 0)
	loads = st.loads
	if kvs, deleted := del("/b/", true); deleted != 100 || len(kvs) != 100 || st.loads-loads != 100 {
		t.Fatalf("a delete of 100 keys with prevKV: %d deleted, %d records, %d loads", deleted, len(kvs), st.loads-loads)
	}
	st.leases[7] = &lease{ttl: 60, keys: map[uint32]struct{}{}}
	put("/c/", 7)
	loads = st.loads
	req, err := proto.Marshal(&pb.LeaseRevokeRequest{ID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.applyLeaseRevoke(req, nil, nil); err != nil {
		t.Fatal(err)
	}
	st.endApply()
	if st.loads != loads || st.liveKeys != 0 {
		t.Fatalf("a revoke of a lease of 100 keys: %d loads, %d keys left", st.loads-loads, st.liveKeys)
	}
}

// A read that must not wait for the disk, of a file on tmpfs, which
// takes no RWF_NOWAIT, goes off the loop: it is not an error. Taken as
// one, every range of a value a node on tmpfs let go answered Internal.
func TestANoWaitReadOnTmpfsGoesOffTheLoop(t *testing.T) {
	dir, err := os.MkdirTemp("/dev/shm", "wl-nowait-")
	if err != nil {
		t.Skipf("no tmpfs at /dev/shm: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f, err := os.Create(filepath.Join(dir, "ckpt"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.Write([]byte("a value on tmpfs")); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 5)
	if err := readFileNoWait(f, p, 2); err != nil && !errors.Is(err, errAway) {
		t.Fatalf("a read of a file on tmpfs that must not wait: %v, want it read or errAway", err)
	}
}

// A put over a key whose record the budget let go of reads no value
// nothing asks for: it read each one back from the disk into the slab, on
// every node, for a value it then replaced. A put that asks for the value
// before (prevKV) reads it.
func TestPutsReadNoValueNothingAsksFor(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	st.setBudget(1, 0) // no room for records: each is let go of
	put := func(prevKV bool) [][]byte {
		var prevs [][]byte
		for i := range 100 {
			st.rev++
			v := []byte("v") // the request, whose value lies at its start in the log
			prev, err := n.putKey(&putReq{key: fmt.Appendf(nil, "/p/%03d", i), value: v, prevKV: prevKV}, v, l.add("v"), st.rev)
			if err != nil {
				t.Fatal(err)
			}
			st.endApply()
			if prev != nil {
				prevs = append(prevs, prev)
			}
		}
		return prevs
	}
	put(false)
	loads := st.loads
	if prevs := put(false); len(prevs) != 0 || st.loads != loads {
		t.Fatalf("100 puts over let-go records without prevKV: %d values before, %d loads", len(prevs), st.loads-loads)
	}
	loads = st.loads
	if prevs := put(true); len(prevs) != 100 || st.loads-loads < 100 {
		t.Fatalf("100 puts with prevKV: %d values before, %d loads", len(prevs), st.loads-loads)
	}
}

// A range of keys only reads no value: with every record let go of, it
// read each value back from the disk to strip it after. It answers each
// key with its revisions, at the head and in the history. A range that
// sorts by value still reads them.
func TestKeysOnlyRangesReadNoValue(t *testing.T) {
	n, l := ckptNode(t)
	st := n.st
	st.setBudget(1, 0) // no room for records: each is let go of
	for round := range 2 {
		for i := range 100 {
			v := []byte(fmt.Sprintf("value %d", round))
			st.rev++
			if _, err := n.putKey(&putReq{key: fmt.Appendf(nil, "/k/%03d", i), value: v}, v, l.add(string(v)), st.rev); err != nil {
				t.Fatal(err)
			}
			st.endApply()
		}
	}
	for _, rev := range []int64{0, 50} {
		loads := st.loads
		r := &rangeReq{key: []byte("/k/"), end: prefixEnd([]byte("/k/")), keysOnly: true, rev: rev}
		kvs, count, _, err := n.rangeKVs(r, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if st.loads != loads {
			t.Fatalf("a keys-only range at %d read %d values", rev, st.loads-loads)
		}
		want := int64(100)
		if rev == 50 {
			want = 49 // revisions 2 to 50
		}
		if count != want || int64(len(kvs)) != want {
			t.Fatalf("a keys-only range at %d: %d keys, count %d, want %d", rev, len(kvs), count, want)
		}
		for _, kv := range kvs {
			if len(recValue(kv)) != 0 || recVersion(kv) == 0 {
				t.Fatalf("a keys-only range at %d answered %q", rev, kv)
			}
		}
	}
	loads := st.loads
	r := &rangeReq{key: []byte("/k/"), end: prefixEnd([]byte("/k/")), keysOnly: true, sortTarget: sortByValue, sortOrder: sortAscend}
	if _, _, _, err := n.rangeKVs(r, nil, 0); err != nil || st.loads-loads != 100 {
		t.Fatalf("a keys-only range sorted by value: %d values read, %v", st.loads-loads, err)
	}
}
