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
