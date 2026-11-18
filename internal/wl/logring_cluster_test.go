package wl

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ringSeen is what a node's loop showed of its log's ring.
type ringSeen struct {
	off, segs, free int64
	files           int64
}

func seeRing(t *testing.T, l *loop, dir string) ringSeen {
	t.Helper()
	got := make(chan ringSeen, 1)
	l.post(func() error {
		w := l.n.wal
		r := ringSeen{off: w.off, free: w.free.Load()}
		if w.ring != nil {
			r.segs = w.ring.segs
		}
		got <- r
		return nil
	})
	r := <-got
	r.files = countRingFiles(t, dir)
	return r
}

// A three-node cluster whose logs have small rings, of 16 segments of
// 64 KiB, wraps them many times under writers and a compaction a second,
// and keeps every write: each key's last value reads back on every node,
// each node's ring keeps its files, and no write is refused but by the
// ring's backpressure while a checkpoint frees its tail (#602's ruling,
// condition 2). Its throughput is reported, for information.
func TestALogRingWrapsUnderLoadWithCompaction(t *testing.T) {
	was := segSize
	segSize = 64 << 10
	t.Cleanup(func() { segSize = was })
	const segs = 16
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.logRing = segs * segSize })
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const writers, keys = 8, 64
	value := func(w, k, n int) string {
		return fmt.Sprintf("%d/%d/%d/", w, k, n) + strings.Repeat("v", 900)
	}
	var mu sync.Mutex
	last := map[string]string{}
	var puts, busy atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				k := n % keys
				key := fmt.Sprintf("/ring/%d/%02d", w, k)
				v := value(w, k, n)
				if _, err := c.Put(ctx, key, v); err != nil {
					if errors.Is(err, rpctypes.ErrTooManyRequests) {
						// The ring's backpressure while a checkpoint frees
						// its tail: the Put took no effect; try it again.
						busy.Add(1)
						time.Sleep(10 * time.Millisecond)
						n--
						continue
					}
					t.Errorf("writer %d: %v", w, err)
					return
				}
				puts.Add(1)
				mu.Lock()
				last[key] = v
				mu.Unlock()
			}
		})
	}
	start := time.Now()
	var laps int64
	for time.Since(start) < 20*time.Second && laps < 6 {
		time.Sleep(time.Second)
		r, err := c.Get(ctx, "/ring/0/00")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Compact(ctx, r.Header.Revision); err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
			t.Fatal(err)
		}
		laps = seeRing(t, loopOf(1), tc.walDir(0)).off / (segs * segSize)
	}
	close(stop)
	wg.Wait()
	took := time.Since(start)
	for i := range 3 {
		r := seeRing(t, loopOf(tc.ids[i]), tc.walDir(i))
		if r.segs != segs || r.files != segs {
			t.Fatalf("node %d: a ring of %d segments and %d files, formatted with %d", i+1, r.segs, r.files, segs)
		}
		t.Logf("node %d: the log at %d, %d laps of the ring, %d spares", i+1, r.off, r.off/(segs*segSize), r.free)
		if r.off/(segs*segSize) < 3 {
			t.Fatalf("node %d wrapped its ring %d times; the test needs it to wrap", i+1, r.off/(segs*segSize))
		}
	}
	for i, ep := range tc.endpoints {
		ci := tc.clientOf(t, ep)
		got, err := ci.Get(ctx, "/ring/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Kvs) != len(last) {
			t.Fatalf("node %d holds %d keys, %d were written", i+1, len(got.Kvs), len(last))
		}
		for _, kv := range got.Kvs {
			if last[string(kv.Key)] != string(kv.Value) {
				t.Fatalf("node %d: %s holds %.20q, its last write was %.20q", i+1, kv.Key, kv.Value, last[string(kv.Key)])
			}
		}
	}
	t.Logf("%d Puts of about 1 KiB in %v: %.0f a second, for information; %d refused while a checkpoint freed the ring's tail",
		puts.Load(), took.Round(time.Millisecond), float64(puts.Load())/took.Seconds(), busy.Load())
}

// A full ring with nothing to reclaim raises NOSPACE, as etcd's full disk
// does, and neither grows past what it was formatted with nor stops the
// node (#602's ruling, condition 1). Node 1's ring is put in the state
// the log since its newest checkpoint's start leaves it in once it fills
// it: no spare left, no checkpoint running, and none due, the log not
// grown since the newest one started. A put to it is refused with NOSPACE
// and the alarm goes up; while it is up, writes are refused and reads go
// on, and the ring keeps its files; once its spares are back and the alarm
// disarmed, puts are taken again.
func TestAFullLogRingRaisesNOSPACE(t *testing.T) {
	was := segSize
	segSize = 64 << 10
	t.Cleanup(func() { segSize = was })
	const segs = 8
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.logRing = segs * segSize })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := c.Put(ctx, "/full/before", "x"); err != nil {
		t.Fatal(err)
	}
	l := loopOf(tc.ids[0])
	var took int64
	for {
		got := make(chan int64, 1)
		l.post(func() error {
			n := l.n
			if n.ckpt != nil {
				got <- -1
				return nil
			}
			k := n.wal.free.Load()
			n.wal.free.Add(-k)
			n.ckptFrom = n.wal.off
			got <- k
			return nil
		})
		if took = <-got; took >= 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := c.Put(ctx, "/full/k", "x"); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a put to a full ring: %v", err)
	}
	for {
		alarms, err := c.AlarmList(ctx)
		if err == nil && len(alarms.Alarms) == 1 && alarms.Alarms[0].Alarm == pb.AlarmType_NOSPACE && alarms.Alarms[0].MemberID == tc.ids[0] {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("alarms after the full ring: %v %v", alarms, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := c.Txn(ctx).Then(clientv3.OpPut("/full/k", "x")).Commit(); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a Txn that puts while NOSPACE is up: %v", err)
	}
	if r, err := c.Get(ctx, "/full/before"); err != nil || len(r.Kvs) != 1 {
		t.Fatalf("a read while NOSPACE is up: %v %v", r, err)
	}
	if n := countRingFiles(t, tc.walDir(0)); n != segs {
		t.Fatalf("the full ring holds %d files, formatted with %d", n, segs)
	}
	l.post(func() error { l.n.wal.free.Add(took); return nil })
	if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[0], Alarm: pb.AlarmType_NOSPACE}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, "/full/k", "after"); err != nil {
		t.Fatalf("a put once the ring has its spares back and the alarm is disarmed: %v", err)
	}
	if r := seeRing(t, l, tc.walDir(0)); r.segs != segs || r.files != segs {
		t.Fatalf("a ring of %d segments and %d files, formatted with %d", r.segs, r.files, segs)
	}
}

// walDir is node i's log directory.
func (tc *testCluster) walDir(i int) string { return filepath.Join(tc.dataDir(i), "wal") }
