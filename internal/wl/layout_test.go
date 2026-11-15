package wl

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Key ids differ between nodes once freed ids are reused: a node that
// freed and reused ids, and one started from its checkpoint, hold the same
// keys under other ids. Nothing a client, the log or a checkpoint sees
// may follow the ids: replies, the order of a multi-key delete and of a
// lease's revoke, and the change index watches catch up from.

// applier applies requests to a node's store as the apply does, with
// each request's bytes in the node's log.
type applier struct {
	t *testing.T
	n *node
	l *memLog
}

func (a applier) put(key, value string, lease int64) {
	a.t.Helper()
	req := appendBytesField(nil, 1, []byte(key))
	req = appendBytesField(req, 2, []byte(value))
	if lease != 0 {
		req = appendVarintField(req, 3, uint64(lease))
	}
	if err := a.n.applyPut(req, a.l.add(string(req)), nil, nil); err != nil {
		a.t.Fatal(err)
	}
	a.n.st.endApply()
}

// del deletes [key, end) and returns the records it deleted, in order.
func (a applier) del(key, end string) []string {
	a.t.Helper()
	st := a.n.st
	rev := st.rev + 1
	kvs, deleted, err := a.n.deleteRange(&delReq{key: []byte(key), end: []byte(end), prevKV: true}, rev, nil)
	if err != nil {
		a.t.Fatal(err)
	}
	if deleted > 0 {
		st.rev = rev
	}
	var out []string
	for _, kv := range kvs {
		out = append(out, string(kv))
	}
	st.endApply()
	return out
}

func (a applier) grant(id, ttl int64) {
	a.t.Helper()
	req := appendVarintField(appendVarintField(nil, 1, uint64(ttl)), 2, uint64(id))
	if err := a.n.applyLeaseGrant(req, nil, nil); err != nil {
		a.t.Fatal(err)
	}
}

func (a applier) revoke(id int64) {
	a.t.Helper()
	if err := a.n.applyLeaseRevoke(appendVarintField(nil, 1, uint64(id)), nil, nil); err != nil {
		a.t.Fatal(err)
	}
	a.n.st.endApply()
}

func (a applier) compact() {
	a.t.Helper()
	if err := a.n.applyCompact(appendVarintField(nil, 1, uint64(a.n.st.rev)), nil, nil); err != nil {
		a.t.Fatal(err)
	}
	a.n.st.trimSteps(1 << 20)
}

// rng returns a range's records and count.
func (a applier) rng(key, end string, limit, rev int64) string {
	a.t.Helper()
	kvs, count, _, err := a.n.rangeKVs(&rangeReq{key: []byte(key), end: []byte(end), limit: limit, rev: rev}, nil)
	if err != nil {
		a.t.Fatal(err)
	}
	return fmt.Sprintf("%d %q", count, kvs)
}

// changes lists the change index from revision from on, by key name.
func (a applier) changes(from int64) []string {
	st := a.n.st
	var out []string
	for _, ch := range st.changes[searchChanges(st.changes, from):] {
		out = append(out, fmt.Sprintf("%d %s %v", ch.rev, st.key(ch.key), ch.del))
	}
	return out
}

func TestNodesWithOtherKeyIDsAnswerAlike(t *testing.T) {
	na, la := ckptNode(t)
	a := applier{t, na, la}
	for i := range 60 {
		a.put(fmt.Sprintf("/x/k%02d", i), "v", 0)
	}
	a.del("/x/k00", "/x/k30")
	a.put("/x/k59", "again", 0)
	a.compact()
	if len(na.st.keyFree) != 30 {
		t.Fatalf("%d ids freed, want 30", len(na.st.keyFree))
	}

	// b starts from a's checkpoint: the same keys, numbered anew.
	writeCkpt(t, na, 1)
	nb, lb := ckptNode(t)
	f, err := os.Open(filepath.Join(na.ckptDir, ckptName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadCkpt(f, 1, nb.st); err != nil {
		t.Fatal(err)
	}
	nb.ckptGen = 1
	b := applier{t, nb, lb}

	from := na.st.rev + 1
	for _, x := range []applier{a, b} {
		x.grant(7, 600)
		// On a, these take the freed ids, the last freed first.
		for i := range 10 {
			x.put(fmt.Sprintf("/x/l%d", i), "leased", 7)
		}
		for i := range 30 {
			x.put(fmt.Sprintf("/x/n%02d", i), "new", 0)
		}
	}
	var ida, idb []uint32
	for i := range 10 {
		x, _ := na.st.find(fmt.Appendf(nil, "/x/l%d", i))
		y, _ := nb.st.find(fmt.Appendf(nil, "/x/l%d", i))
		ida, idb = append(ida, x), append(idb, y)
	}
	if slices.Equal(ida, idb) {
		t.Fatalf("the two nodes number the leased keys alike (%v): the test needs other layouts", ida)
	}
	past := na.st.rev

	same := func(what, x, y string) {
		t.Helper()
		if x != y {
			t.Fatalf("%s differs:\n a %s\n b %s", what, x, y)
		}
	}
	for _, r := range []struct {
		key, end   string
		limit, rev int64
	}{{"/x/", "/x0", 0, 0}, {"/x/", "/x0", 5, 0}, {"/x/k", "/x/m", 3, 0}, {"/x/", "/x0", 0, from}} {
		same(fmt.Sprintf("range %+v", r), a.rng(r.key, r.end, r.limit, r.rev), b.rng(r.key, r.end, r.limit, r.rev))
	}
	same("a multi-key delete", fmt.Sprint(a.del("/x/k40", "/x/k50")), fmt.Sprint(b.del("/x/k40", "/x/k50")))
	same("a multi-key delete of new keys", fmt.Sprint(a.del("/x/n00", "/x/n10")), fmt.Sprint(b.del("/x/n00", "/x/n10")))
	a.revoke(7)
	b.revoke(7)
	same("the changes", fmt.Sprint(a.changes(from)), fmt.Sprint(b.changes(from)))
	// A range at a revision before the deletes still sees the deleted keys.
	same("a range in the past", a.rng("/x/", "/x0", 0, past), b.rng("/x/", "/x0", 0, past))
	if got := a.rng("/x/l", "/x/m", 0, past); !bytes.Contains([]byte(got), []byte("/x/l9")) {
		t.Fatalf("a range before the revoke misses the revoked keys: %s", got)
	}

	// And their checkpoints hold the same leases and keys.
	writeCkpt(t, na, 2)
	writeCkpt(t, nb, 2)
	for _, x := range []*node{na, nb} {
		f, err := os.Open(filepath.Join(x.ckptDir, ckptName(2)))
		if err != nil {
			t.Fatal(err)
		}
		st, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := loadCkpt(f, 0, st); err != nil {
			t.Fatal(err)
		}
		x.st = st
	}
	same("the loaded checkpoints' ranges", a.rng("/x/", "/x0", 0, 0), b.rng("/x/", "/x0", 0, 0))
	same("the loaded checkpoints' changes", fmt.Sprint(a.changes(from)), fmt.Sprint(b.changes(from)))
}
