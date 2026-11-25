package wl

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// walSyncs is node l's log sync counts: all, idle marks, and those for
// hard states alone.
func walSyncs(t *testing.T, l *loop) (all, marks, stateOnly uint64) {
	t.Helper()
	done := make(chan struct{})
	l.post(func() error {
		s := l.n.wal.stats
		all, marks, stateOnly = s.syncs, s.marks, s.stateOnly
		close(done)
		return nil
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a node's loop runs nothing posted to it in 5 s")
	}
	return all, marks, stateOnly
}

// TestSyncsPerWrite counts each node's log syncs per write, as the
// apiserver's compactor writes: puts apart, and pairs back to back (#802).
// Each write takes one sync for its entry on every node, as etcd's, and
// none for the hard state alone when only the commit moved (commitOnly):
// that took a sync of its own on every node, which the next write's sync
// waited behind, three syncs to a write in all, and the apiserver's
// TestCompact, whose compaction of two writes must end within 100 ms,
// failed on CI's disks. The ring's idle mark (markIdle) is apart: it
// syncs once the log goes idle, off the path of a write.
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
		var before [3][3]uint64
		for i := range 3 {
			before[i][0], before[i][1], before[i][2] = walSyncs(t, loopOf(tc.ids[i]))
		}
		var worst, total time.Duration
		for i := range writes {
			d := do(i)
			total += d
			worst = max(worst, d)
		}
		time.Sleep(time.Second)
		for i := range 3 {
			a, m, s := walSyncs(t, loopOf(tc.ids[i]))
			a, m, s = a-before[i][0], m-before[i][1], s-before[i][2]
			t.Logf("%s, node %d: %.2f syncs per write (%.2f idle marks, %.2f for hard states alone); writes took %v on average, %v at most",
				name, i+1, float64(a)/float64(writes), float64(m)/float64(writes), float64(s)/float64(writes), total/time.Duration(writes), worst)
			// A hard state alone still syncs when its term or vote moved,
			// or an append before it waits for a sync: rarely, here.
			if s*10 > uint64(writes) {
				t.Errorf("%s, node %d: %d syncs for hard states alone in %d writes", name, i+1, s, writes)
			}
			if a-m > uint64(writes)+uint64(writes)/10 {
				t.Errorf("%s, node %d: %d syncs but idle marks in %d writes, want about one a write", name, i+1, a-m, writes)
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
