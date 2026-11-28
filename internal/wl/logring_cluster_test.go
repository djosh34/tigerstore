package wl

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
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
		r := ringSeen{off: w.off}
		if w.ring != nil {
			r.segs, r.free = w.ring.segs, w.ringFree()
		}
		got <- r
		return nil
	})
	r := <-got
	r.files = countRingFiles(t, dir)
	// A running node's maker renames spares to segments meanwhile, and a
	// directory read during a rename may miss the file: read again, for a
	// second at most, until the ring's number shows. A file lost for good
	// stays missing.
	for deadline := time.Now().Add(time.Second); r.files != r.segs && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
		r.files = countRingFiles(t, dir)
	}
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
		t.Logf("node %d: the log at %d, %d laps of the ring, %d segments free", i+1, r.off, r.off/(segs*segSize), r.free)
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
// node (#602's ruling, condition 1). The ring fills under real writes and
// real checkpoints; ringHoldCut keeps the log's cut where it is, as a log
// that no checkpoint can free. Writes are refused as too many requests
// while a checkpoint that could free the tail runs or is due; once none
// can, a put gets NOSPACE and the alarm goes up. While it is up, writes
// are refused and reads go on, and every ring keeps its files; once the
// cut may go on and the alarm is disarmed, puts are taken again.
func TestAFullLogRingRaisesNOSPACE(t *testing.T) {
	t.Run("cut released before disarm", func(t *testing.T) { aFullLogRingRaisesNOSPACE(t, false) })
	t.Run("cut released after disarm", func(t *testing.T) { aFullLogRingRaisesNOSPACE(t, true) })
}

func aFullLogRingRaisesNOSPACE(t *testing.T, lateCut bool) {
	was := segSize
	segSize = 64 << 10
	ringHoldCut.Store(true)
	t.Cleanup(func() { waitStrays(t); segSize = was; ringHoldCut.Store(false) })
	const segs = 8
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.logRing = segs * segSize })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := c.Put(ctx, "/full/before", "x"); err != nil {
		t.Fatal(err)
	}
	value := strings.Repeat("v", 900)
	var err error
	puts, busy := 0, 0
	for {
		if _, err = c.Put(ctx, fmt.Sprintf("/full/%04d", puts%256), value); err == nil {
			puts++
			continue
		}
		if errors.Is(err, rpctypes.ErrTooManyRequests) {
			busy++
			time.Sleep(5 * time.Millisecond)
			continue
		}
		break
	}
	if !errors.Is(err, rpctypes.ErrNoSpace) {
		var states []string
		for i := range 3 {
			states = append(states, ringState(loopOf(tc.ids[i])))
		}
		t.Fatalf("after %d puts and %d refused as busy: %v; the rings: %s", puts, busy, err, strings.Join(states, "; "))
	}
	t.Logf("%d puts of about 1 KiB filled a ring of %d segments of %d bytes; %d were refused while a checkpoint could free it", puts, segs, segSize, busy)
	for {
		alarms, err := c.AlarmList(ctx)
		if err == nil && slices.ContainsFunc(alarms.Alarms, func(a *pb.AlarmMember) bool { return a.Alarm == pb.AlarmType_NOSPACE }) {
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
	for i := range 3 {
		if n := countRingFiles(t, tc.walDir(i)); n != segs {
			t.Fatalf("node %d: the full ring holds %d files, formatted with %d", i+1, n, segs)
		}
	}
	disarm := func() {
		alarms, err := c.AlarmList(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range alarms.Alarms {
			if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: a.MemberID, Alarm: a.Alarm}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if lateCut {
		// Make the recovery window deterministic: the disarm has applied
		// and its checkpoints have ended, but the tail is still held. A
		// disarm clears an alarm, not the ring's shortage. Releasing the
		// hook below does not itself call cutLog; without an allowed write
		// to drive an apply, the next put raises NOSPACE again (#920).
		disarm()
		for {
			settled, short := false, false
			l := loopOf(tc.ids[0])
			if !onLoop(l, func() {
				n := l.n
				settled, short = n.ckpt == nil && !n.ringDue(), n.wal.ringShort()
			}) {
				t.Fatal("the loop stopped before the held cut was released")
			}
			if settled {
				if !short {
					t.Fatal("the held ring is not short; the recovery window was not exercised")
				}
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("checkpoints with the cut held: %s", ringState(l))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	// Allow the cut, then drive an apply through every member with a
	// delete of a missing key, which NOSPACE permits. Besides calling
	// cutLog, each apply orders any earlier alarm proposal from that
	// member before the alarm list below. Wait for actual ring room, not
	// just the hook's release, before disarming: a still-short ring may
	// legitimately refuse puts with NOSPACE and raise its alarm again.
	ringHoldCut.Store(false)
	for i, ep := range tc.endpoints {
		ci := tc.rawClient(t, ep)
		for {
			_, err := ci.Delete(ctx, "/full/missing")
			if err == nil {
				break
			}
			if !errors.Is(err, rpctypes.ErrTooManyRequests) || ctx.Err() != nil {
				t.Fatalf("node %d: an allowed delete to drive the cut: %v", i+1, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	for i := range 3 {
		for seeRing(t, loopOf(tc.ids[i]), tc.walDir(i)).free < ringReserve(segs) {
			if ctx.Err() != nil {
				t.Fatalf("the released cut did not free the ring: %s", ringState(loopOf(tc.ids[i])))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	disarm()
	for {
		_, err := c.Put(ctx, "/full/k", "after")
		if err == nil {
			break
		}
		if !errors.Is(err, rpctypes.ErrTooManyRequests) || ctx.Err() != nil {
			t.Fatalf("a put once the cut may go on and the alarm is disarmed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := range 3 {
		if r := seeRing(t, loopOf(tc.ids[i]), tc.walDir(i)); r.segs != segs || r.files != segs {
			t.Fatalf("node %d: a ring of %d segments and %d files, formatted with %d", i+1, r.segs, r.files, segs)
		}
	}
}

// ringState describes a node's ring and checkpoints, for a failure.
func ringState(l *loop) string {
	got := make(chan string, 1)
	l.post(func() error {
		n := l.n
		snap := uint64(0)
		if n.storage.snap != nil {
			snap = n.storage.snap.GetMetadata().GetIndex()
		}
		got <- fmt.Sprintf("node %d: %d segments free, short %v, low %v, due %v, checkpoint running %v, applied %d, newest checkpoint at %d from %d cut %d, log %d to %d",
			n.id, n.wal.ringFree(), n.wal.ringShort(), n.wal.ringLow(), n.ringDue(), n.ckpt != nil, n.applied, snap, n.ckptFrom, n.ckptCut, n.wal.rbase*segSize, n.wal.off)
		return nil
	})
	return <-got
}

// walDir is node i's log directory.
func (tc *testCluster) walDir(i int) string { return filepath.Join(tc.dataDir(i), "wal") }

// A ring short under writers frees its tail when a checkpoint ends, not at
// the next entry applied: while every put is refused, nothing applies, and
// a ring that waited for an apply stayed short for good (#751's review).
// The first checkpoint is held (ckptHold) until eight writers on a ring of
// the fewest segments a sized ring holds are refused as too many requests,
// then let go: the writers go on, never refused with NOSPACE, the ring
// wraps three times, and every key's last value reads back.
func TestAShortLogRingFreesWhenACheckpointEnds(t *testing.T) {
	was := segSize
	segSize = 64 << 10
	t.Cleanup(func() { segSize = was })
	const segs = ringMinSegs
	ckptHold.Store(true)
	t.Cleanup(func() { ckptHold.Store(false) })
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.logRing = segs * segSize })
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var mu sync.Mutex
	last := map[string]string{}
	var busy atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("/short/%d/%02d", w, n%32)
				v := fmt.Sprintf("%d/", n) + strings.Repeat("v", 2000)
				if _, err := c.Put(ctx, key, v); err != nil {
					if errors.Is(err, rpctypes.ErrTooManyRequests) {
						busy.Add(1)
						n--
						continue
					}
					t.Errorf("writer %d: %v", w, err)
					return
				}
				mu.Lock()
				last[key] = v
				mu.Unlock()
			}
		})
	}
	for busy.Load() < 50 && ctx.Err() == nil && !t.Failed() {
		time.Sleep(10 * time.Millisecond)
	}
	ckptHold.Store(false)
	held := busy.Load()
	for seeRing(t, loopOf(tc.ids[0]), tc.walDir(0)).off < 3*segs*segSize && ctx.Err() == nil && !t.Failed() {
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatalf("the ring did not wrap three times in time, %d puts refused as busy", busy.Load())
	}
	got, err := c.Get(ctx, "/short/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range got.Kvs {
		if last[string(kv.Key)] != string(kv.Value) {
			t.Fatalf("%s holds %.12q, its last write was %.12q", kv.Key, kv.Value, last[string(kv.Key)])
		}
	}
	if len(got.Kvs) != len(last) {
		t.Fatalf("%d keys, %d written", len(got.Kvs), len(last))
	}
	t.Logf("%d puts refused as busy while the checkpoint was held, %d in all", held, busy.Load())
}
