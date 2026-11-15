package wl

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// memLog is a log in memory: values are appended and named by offset.
type memLog struct{ b []byte }

func (l *memLog) add(v string) int64 {
	off := int64(len(l.b))
	l.b = append(l.b, v...)
	return off
}

func (l *memLog) read(p []byte, off int64) error {
	if off < 0 || off+int64(len(p)) > int64(len(l.b)) {
		return errors.New("read past the end")
	}
	copy(p, l.b[off:])
	return nil
}

// A memLog is durable as soon as it is written: a checkpoint's writer
// reads it as it reads the log.
func (l *memLog) durableEnd() int64                     { return int64(len(l.b)) }
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
	if _, err := st.put(id, []byte(value), 0, st.rev, l.add(value)); err != nil {
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
	if _, err := st.del(id, st.rev); err != nil { // rev 5
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
	if _, err := st.del(id, st.rev); err != nil { // 6
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
