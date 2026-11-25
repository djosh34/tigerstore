package wl

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// walCounts is a node's log counts: its syncs, its idle marks' syncs, the
// syncs for hard states alone, the commits it skipped the sync of, and the
// syncs that took those once commitHold passed with no other.
type walCounts struct{ all, marks, stateOnly, skipped, flushes uint64 }

func (a walCounts) minus(b walCounts) walCounts {
	return walCounts{a.all - b.all, a.marks - b.marks, a.stateOnly - b.stateOnly, a.skipped - b.skipped, a.flushes - b.flushes}
}

// walSyncs is node l's log counts.
func walSyncs(t *testing.T, l *loop) walCounts {
	t.Helper()
	got := make(chan walCounts, 1)
	l.post(func() error {
		s := l.n.wal.stats
		got <- walCounts{s.syncs, s.marks, s.stateOnly, s.skipped, s.flushes}
		return nil
	})
	select {
	case c := <-got:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("a node's loop runs nothing posted to it in 5 s")
	}
	return walCounts{}
}

// TestSyncsPerWrite counts each node's log syncs per write, as the
// apiserver's compactor writes: puts apart, and pairs back to back (#802).
// Every append synced, the commit's too: each write took three syncs on
// every node (its entry, the commit, the ring's idle mark), and the next
// write's sync waited behind the commit's, so the apiserver's TestCompact,
// whose compaction of two writes must end within 100 ms, failed on CI's
// disks. Now:
//   - the leader's commit owes no response, and takes no sync: each write
//     skips one, but for one that comes while its own entries' sync runs,
//     which owes their ack and is held;
//   - a follower's commit owes its ack, and is held until the next
//     append's sync, or commitHold: at most one sync of its own a write;
//   - each write's entry takes about one sync on every node.
//
// The idle mark is apart: it syncs once the log goes idle, off the path
// of a write.
func TestSyncsPerWrite(t *testing.T) {
	loopOf := watchLoops(t)
	was := segSize
	segSize = 1 << 20
	t.Cleanup(func() { segSize = was })
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.logRing = 16 * segSize })
	c := tc.clientOf(t, tc.endpoints[:3]...)
	ctx := context.Background()
	measure := func(name string, writes int, do func(i int) time.Duration) {
		time.Sleep(time.Second)
		lead := slices.Index(tc.endpoints[:3], tc.leaderEndpoint(t, c))
		var before [3]walCounts
		for i := range 3 {
			before[i] = walSyncs(t, loopOf(tc.ids[i]))
		}
		var worst, total time.Duration
		for i := range writes {
			d := do(i)
			total += d
			worst = max(worst, d)
		}
		time.Sleep(time.Second)
		if l := slices.Index(tc.endpoints[:3], tc.leaderEndpoint(t, c)); l != lead {
			t.Fatalf("%s: the leader moved from node %d to node %d", name, lead+1, l+1)
		}
		w := uint64(writes)
		for i := range 3 {
			d := walSyncs(t, loopOf(tc.ids[i])).minus(before[i])
			role := "follower"
			if i == lead {
				role = "leader"
			}
			t.Logf("%s, node %d (%s): %.2f syncs per write (%.2f idle marks, %.2f for hard states alone, %.2f taking skipped commits), %.2f commits skipped per write; writes took %v on average, %v at most",
				name, i+1, role, float64(d.all)/float64(w), float64(d.marks)/float64(w), float64(d.stateOnly)/float64(w), float64(d.flushes)/float64(w), float64(d.skipped)/float64(w), total/time.Duration(writes), worst)
			entries := d.all - d.marks - d.stateOnly - d.flushes
			if d.flushes > d.skipped {
				t.Errorf("%s, node %d: %d syncs taking skipped commits, for %d skipped", name, i+1, d.flushes, d.skipped)
			}
			if entries > w+w/10 {
				t.Errorf("%s, node %d: %d syncs for entries in %d writes, want about one a write", name, i+1, entries, w)
			}
			if i == lead {
				// A leader's commit that comes while its own entries' sync
				// runs owes Raft the ack of those entries, and is held, not
				// skipped: more often the slower its syncs. Before, each of
				// its writes took a sync for the commit alone.
				if d.stateOnly*4 > w {
					t.Errorf("%s, the leader: %d syncs for hard states alone in %d writes, want a quarter at most", name, d.stateOnly, w)
				}
				if d.skipped*4 < 3*w {
					t.Errorf("%s, the leader: %d commits skipped in %d writes, want three quarters at least", name, d.skipped, w)
				}
				continue
			}
			if d.stateOnly > w {
				t.Errorf("%s, node %d: %d syncs for hard states alone in %d writes, want one a write at most", name, i+1, d.stateOnly, w)
			}
		}
	}
	put := func(key string) time.Duration {
		start := time.Now()
		if _, err := c.Put(ctx, key, "v"); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	measure("puts 150 ms apart", 10, func(i int) time.Duration {
		d := put(fmt.Sprintf("/sparse/%d", i))
		time.Sleep(150 * time.Millisecond)
		return d
	})
	measure("pairs back to back, 150 ms apart", 20, func(i int) time.Duration {
		d := put(fmt.Sprintf("/pair/%d", i))
		if i%2 == 1 {
			time.Sleep(150 * time.Millisecond)
		}
		return d
	})
}

// TestAHeldCommitSyncsWithinItsBound is a follower's commit held with no
// append after it (walJob.held, #821's ruling): its sync does not start
// while the hold lasts, the loop is set to wake at its end, and once
// commitHold passed it starts, though no append came. An append that
// carries entries starts the sync at once, the held commit with it.
func TestAHeldCommitSyncsWithinItsBound(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, 8*(16<<10), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	hold := func() time.Time {
		hs, err := proto.Marshal(&pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.write(nil, hs); err != nil {
			t.Fatal(err)
		}
		at := time.Now()
		w.pending = append(w.pending, walJob{end: w.off, at: at, held: true})
		return at
	}
	// Right after a large sync, whose gap to the next is loadGap, longer
	// than the hold: the hold's end comes first (#821's re-review).
	w.lastKick, w.lastBytes = time.Now(), loadBytes
	if w.gap() <= commitHold {
		t.Fatalf("the gap after a large sync is %v, not past the hold's %v: the test tests nothing", w.gap(), commitHold)
	}
	at := hold()
	w.kick()
	if w.inflight || !w.deferred {
		t.Fatalf("a commit held started its sync at once: in flight %v, deferred %v", w.inflight, w.deferred)
	}
	if until := w.deferUntil(); until.After(at.Add(commitHold)) || until.Before(at) {
		t.Fatalf("the loop wakes for the held commit %v after its hold began, want within %v", until.Sub(at), commitHold)
	}
	// The hold over, with no append after it, the sync starts.
	w.pending[0].at = at.Add(-commitHold - time.Millisecond)
	w.kick()
	if !w.inflight {
		t.Fatal("a commit held past commitHold did not start its sync")
	}
	if w.deferred {
		t.Fatal("the sync of a commit held past commitHold is still deferred")
	}
}

// TestASkippedCommitSyncsWithinItsBound is a leader's commit whose sync was
// skipped (appendNow): written, owing no response, with no job pending.
// No sync takes it while commitHold lasts, the loop wakes at its end, and
// then one does, though no append came: a commit stays unsynced that long
// at most, past which a clean stop keeps it (#821's re-review).
func TestASkippedCommitSyncsWithinItsBound(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, 8*(16<<10), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	hs, err := proto.Marshal(&pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.write(nil, hs); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	w.skippedAt = at
	w.kick()
	if w.inflight || !w.deferred {
		t.Fatalf("a skipped commit's sync started at once: in flight %v, deferred %v", w.inflight, w.deferred)
	}
	if until := w.deferUntil(); until.After(at.Add(commitHold)) || until.Before(at) {
		t.Fatalf("the loop wakes for the skipped commit %v after it, want within %v", until.Sub(at), commitHold)
	}
	w.skippedAt = at.Add(-commitHold - time.Millisecond)
	w.kick()
	if !w.inflight || w.want.Load() != w.off {
		t.Fatalf("a commit skipped past commitHold was not synced: in flight %v, the sync up to %d of %d", w.inflight, w.want.Load(), w.off)
	}
	if !w.skippedAt.IsZero() || w.stats.flushes != 1 {
		t.Fatalf("after its sync: skipped at %v, flushes %d; want none left, 1", w.skippedAt, w.stats.flushes)
	}
}

// TestAnAppendSyncsAHeldCommitAtOnce is the hold's other end: an append
// with entries after a held commit starts the sync at once, which covers
// both.
func TestAnAppendSyncsAHeldCommitAtOnce(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, 8*(16<<10), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	hs, err := proto.Marshal(&pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.write(nil, hs); err != nil {
		t.Fatal(err)
	}
	w.pending = append(w.pending, walJob{end: w.off, at: time.Now(), held: true})
	w.kick()
	if w.inflight {
		t.Fatal("a commit held started its sync at once")
	}
	ents := []*pb.Entry{{Term: new(uint64(1)), Index: new(uint64(2)), Data: []byte("x")}}
	if err := w.write(ents, hs); err != nil {
		t.Fatal(err)
	}
	w.pending = append(w.pending, walJob{end: w.off, at: time.Now(), ents: true})
	w.kick()
	if !w.inflight || w.want.Load() != w.off {
		t.Fatalf("an append after a held commit did not start the sync of both: in flight %v, the sync up to %d of %d", w.inflight, w.want.Load(), w.off)
	}
}
