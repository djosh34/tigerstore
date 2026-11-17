package wl

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

type seen struct {
	key   string
	rev   int64
	del   bool
	value string
	prev  string
}

func historyOf(t *testing.T, n *node, x watcher, from int64) []seen {
	t.Helper()
	x.prefix = len(x.end) > 0 && slices.Equal(x.end, prefixEnd(x.key))
	var out []seen
	x.syncRev = from
	b := syncBudget{changes: 1 << 30, events: 1 << 30, bytes: 1 << 62}
	_, err := n.catchUp(&x, &b, func(evs []event) bool {
		for _, ev := range evs {
			s := seen{key: string(n.st.key(ev.key)), rev: ev.rev, del: ev.del}
			if ev.kv != nil {
				s.value = string(recValue(ev.kv))
			}
			if ev.prev != nil {
				s.prev = string(recValue(ev.prev))
			}
			out = append(out, s)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWatchHistoryReplaysChangesInRevisionOrder(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	putValue(t, n.st, l, "/a/1", "one")   // 2
	putValue(t, n.st, l, "/b/1", "other") // 3
	putValue(t, n.st, l, "/a/2", "two")   // 4
	putValue(t, n.st, l, "/a/1", "uno")   // 5
	id, _ := n.st.find([]byte("/a/2"))
	n.st.rev++
	if _, _, err := n.st.del(id, n.st.rev, true); err != nil { // 6
		t.Fatal(err)
	}
	n.st.endApply()

	got := historyOf(t, n, watcher{key: []byte("/a/"), end: prefixEnd([]byte("/a/")), prevKV: true}, 3)
	want := []seen{
		{key: "/a/2", rev: 4, value: "two"},
		{key: "/a/1", rev: 5, value: "uno", prev: "one"},
		{key: "/a/2", rev: 6, del: true, prev: "two"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("history from 3:\n got %+v\nwant %+v", got, want)
	}
	got = historyOf(t, n, watcher{key: []byte("/a/1"), noPut: true}, 1)
	if len(got) != 0 {
		t.Fatalf("a watch of deletes only of /a/1 got %+v", got)
	}
}

// A Txn that puts a key and then deletes it: a watch that catches up
// gets the put with the value it wrote, as etcd's sub-revisions have it,
// then the delete (difffuzz).
func TestWatchHistoryHasAPutThatTheSameTxnDeleted(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	putValue(t, n.st, l, "/a/1", "one") // 2
	id, _ := n.st.find([]byte("/a/1"))
	fresh, err := n.st.intern([]byte("/a/2"))
	if err != nil {
		t.Fatal(err)
	}
	n.st.rev++ // 3: put both, delete both
	for _, k := range []uint32{id, fresh} {
		if _, err := n.st.put(k, []byte("uno"), 0, n.st.rev, l.add("uno"), true); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []uint32{id, fresh} {
		if _, _, err := n.st.del(k, n.st.rev, true); err != nil {
			t.Fatal(err)
		}
	}
	n.st.endApply()
	got := historyOf(t, n, watcher{key: []byte("/a/"), end: prefixEnd([]byte("/a/")), prevKV: true}, 3)
	want := []seen{
		{key: "/a/1", rev: 3, value: "uno", prev: "one"},
		{key: "/a/2", rev: 3, value: "uno"},
		{key: "/a/1", rev: 3, del: true, prev: "one"},
		{key: "/a/2", rev: 3, del: true},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("history from 3:\n got %+v\nwant %+v", got, want)
	}
}

func TestWatchCatchUpKeepsToItsBudgetAndEndsAtARevision(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	for i := range 10 {
		putValue(t, n.st, l, fmt.Sprintf("/k/%02d", i), "v") // revs 2..11
	}
	// One revision with three changes.
	n.st.rev++
	for _, k := range []string{"/k/a", "/k/b", "/k/c"} {
		id, err := n.st.intern([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := n.st.put(id, nil, 0, n.st.rev, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	n.st.endApply()
	x := watcher{key: []byte("/k/"), end: prefixEnd([]byte("/k/")), syncRev: 1}
	var got []int64
	emit := func(evs []event) bool {
		for _, ev := range evs {
			got = append(got, ev.rev)
		}
		return true
	}
	// A budget of ten takes revisions 2 to 11 and stops before 12.
	b := syncBudget{changes: 10, events: 1 << 10, bytes: 1 << 62}
	if done, err := n.catchUp(&x, &b, emit); err != nil || done {
		t.Fatalf("a catch-up past its budget: done %v, %v", done, err)
	}
	if len(got) != 10 || x.syncRev != 12 {
		t.Fatalf("the first turn sent revisions %v and goes on from %d, want 2 to 11 and 12", got, x.syncRev)
	}
	// A budget of one finishes revision 12's three changes.
	b = syncBudget{changes: 1, events: 1 << 10, bytes: 1 << 62}
	if done, err := n.catchUp(&x, &b, emit); err != nil || !done {
		t.Fatalf("the second turn: done %v, %v", done, err)
	}
	if len(got) != 13 || got[12] != 12 {
		t.Fatalf("revisions %v, want each change once and all of 12", got)
	}
}

func TestAWatchCatchingUpGetsNoLiveEvents(t *testing.T) {
	var w watches
	i := w.add(watcher{key: []byte("/a")})
	w.ws[i].syncRev = 5
	w.push(i, 0, false, 9, nil, nil, 0)
	if len(w.events) != 0 {
		t.Fatal("a watcher catching up got a live event")
	}
}

// As etcd's, a progress request waits until every watch of its stream
// caught up: a progress at the head before older events would tell a
// client, as kube-apiserver's watch cache, it has seen them all.
func TestAProgressWaitsForTheStreamsWatchesToCatchUp(t *testing.T) {
	c, s := flowClient(t)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	n := c.node
	n.st = st
	// The stream's own watch, as a create makes it (watchRequest).
	n.w.ws = []watcher{{active: true, syncRev: 5, sock: c.idx, gen: c.gen, sid: s.id}}
	s.wids = []int32{0}
	s.progressDue = true
	n.progressIfSynced(c, s)
	if !s.progressDue {
		t.Fatal("a progress was answered while a watch caught up")
	}
	n.w.ws[0].syncRev = 0
	n.progressIfSynced(c, s)
	if s.progressDue {
		t.Fatal("a progress still waits after the watch caught up")
	}
}

// A progress notification of watch 0 leaves the ID out, as proto3 does
// and as its size counts it. It wrote the ID anyway, one field past the
// message's length, and the loop then spun for good opening empty frames
// (CI on acd57bf6: a node restarted under watches with progress_notify).
func TestAProgressOfWatchZeroIsAsLongAsItSays(t *testing.T) {
	for _, id := range []int64{0, 7, -1} {
		c, s := flowClient(t)
		st, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		c.node.st = st
		c.node.writeProgress(c, s, id)
		if c.broken {
			t.Fatalf("watch %d: the progress came out another length than it said", id)
		}
	}
}

// A watch from a revision the store has not reached gets no progress
// notification before it, periodic or asked for, as etcd's: clientv3
// resumes a watch on another node from the revision after its last
// event, and a node still replaying its log sent it a progress at
// revision 1 after events at 4486 (chaos-env seed 6241418673, CI on
// cf2192c9). An asked-for progress waits until the store reaches the
// start, and the tick asks again.
func TestNoProgressComesBeforeAWatchsStartRevision(t *testing.T) {
	c, s := flowClient(t)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	n := c.node
	n.st = st
	c.idx = len(n.socks)
	n.socks = append(n.socks, c)
	n.gens = append(n.gens, 0)
	n.w.ws = []watcher{{active: true, sock: c.idx, sid: s.id, minRev: 10, progressNotify: true}}
	s.wids = []int32{0}
	n.progressEvery, n.now = time.Second, time.Now()
	st.rev = 1
	win := s.win

	n.notifyProgress()
	s.progressDue = true
	n.progressIfSynced(c, s)
	n.retryProgress()
	if s.win != win || !s.progressDue {
		t.Fatal("a progress went out at revision 1 to a watch from revision 10")
	}

	st.rev = 10
	n.retryProgress()
	if s.progressDue || s.win == win {
		t.Fatal("the asked-for progress still waits once the store reached the watch's start")
	}
	win = s.win
	n.progressAt = time.Time{}
	n.notifyProgress()
	if s.win == win {
		t.Fatal("no periodic progress once the store reached the watch's start")
	}
}

// A delete of a key that compaction took out of the tree, while a prefix
// watch is open, deletes nothing and records nothing: the key names no
// directory, and looking its watchers up there panicked every node that
// applied it (96ed3a18, every simulator seed).
func TestADeleteOfAKeyOutOfTheTreeFindsNoWatcher(t *testing.T) {
	n := leaseNode(t)
	l := n.st.log.(*memLog)
	putValue(t, n.st, l, "/a/1", "one")
	id, _ := n.st.find([]byte("/a/1"))
	n.st.rev++
	if _, _, err := n.st.del(id, n.st.rev, false); err != nil {
		t.Fatal(err)
	}
	n.st.endApply()
	n.st.compact(n.st.rev)
	n.st.trimSteps(1 << 20)
	if k := &n.st.keys[id]; k.klen == 0 || k.inTree {
		t.Fatalf("the key deleted at the compacted revision: kept %v, in the tree %v; want kept, out of the tree", k.klen != 0, k.inTree)
	}
	n.w.add(watcher{key: []byte("/a/"), end: prefixEnd([]byte("/a/")), prefix: true, prevKV: true, active: true})
	n.st.rev++
	_, deleted, err := n.deleteRange(&delReq{key: []byte("/a/1")}, n.st.rev, nil)
	if err != nil || deleted != 0 {
		t.Fatalf("deleted %d, %v; want 0 deleted", deleted, err)
	}
}
