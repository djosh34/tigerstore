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
	settled         bool
	state           string
}

// observeRing bounds both a predicate's observation and its diagnostic by
// ctx and, at most, 5 s: a stopped loop silently drops posted callbacks.
// The buffered reply also lets a late callback finish after cancellation.
func observeRing(ctx context.Context, l *loop) (ringSeen, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return ringSeen{}, err
	}
	got := make(chan ringSeen, 1)
	l.post(func() error {
		n := l.n
		w := n.wal
		r := ringSeen{off: w.off, settled: n.ckpt == nil && !n.ringDue(), state: fmt.Sprintf("node %d: no log ring", n.id)}
		if w.ring != nil {
			r.segs, r.free = w.ring.segs, w.ringFree()
			snap := uint64(0)
			if n.storage.snap != nil {
				snap = n.storage.snap.GetMetadata().GetIndex()
			}
			r.state = fmt.Sprintf("node %d: %d segments free, short %v, low %v, due %v, checkpoint running %v, applied %d, newest checkpoint at %d from %d cut %d, log %d to %d",
				n.id, r.free, w.ringShort(), w.ringLow(), n.ringDue(), n.ckpt != nil, n.applied, snap, n.ckptFrom, n.ckptCut, w.rbase*segSize, w.off)
		}
		got <- r
		return nil
	})
	select {
	case r := <-got:
		return r, nil
	case <-ctx.Done():
		return ringSeen{}, ctx.Err()
	}
}

func seeRing(t *testing.T, l *loop, dir string) ringSeen {
	t.Helper()
	r, err := observeRing(context.Background(), l)
	if err != nil {
		t.Fatalf("the loop did not report its ring: %v", err)
	}
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
	t.Cleanup(func() { segSize = was; ringHoldCut.Store(false) })
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
	var noSpaceMember uint64
	for {
		alarms, err := c.AlarmList(ctx)
		if err == nil {
			if i := slices.IndexFunc(alarms.Alarms, func(a *pb.AlarmMember) bool { return a.Alarm == pb.AlarmType_NOSPACE }); i >= 0 {
				noSpaceMember = alarms.Alarms[i].MemberID
				break
			}
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
		// Exercise a related recovery window, not #920's CI schedule:
		// the disarm has applied and the NOSPACE member's checkpoints have
		// ended, but its tail is still held. A disarm clears an alarm, not
		// the ring's shortage. Releasing the hook below does not itself
		// call cutLog; without an allowed write to drive an apply, a put
		// to that member raises NOSPACE again.
		disarm()
		for {
			// The filling client's member may be a follower whose own ring
			// is not short: the leader refused its forwarded put instead.
			r, err := observeRing(ctx, loopOf(noSpaceMember))
			if err != nil {
				t.Fatalf("NOSPACE member %d: the loop did not report checkpoints with the cut held: %v", noSpaceMember, err)
			}
			if r.settled {
				if r.free >= ringReserve(r.segs) {
					t.Fatalf("NOSPACE member %d: the held ring is not short; the recovery window was not exercised: %s", noSpaceMember, r.state)
				}
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("checkpoints with the cut held: %s", r.state)
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
		for {
			r, err := observeRing(ctx, loopOf(tc.ids[i]))
			if err != nil {
				t.Fatalf("node %d: the loop did not report room after the released cut: %v", tc.ids[i], err)
			}
			if r.free >= ringReserve(segs) {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("the released cut did not free the ring: %s", r.state)
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
func ringState(l *loop) string { return ringStateContext(context.Background(), l) }

func ringStateContext(ctx context.Context, l *loop) string {
	r, err := observeRing(ctx, l)
	if err != nil {
		return fmt.Sprintf("the loop did not report its ring: %v", err)
	}
	return r.state
}

func TestRingObservationHonorsItsContext(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("already canceled %v", canceled), func(t *testing.T) {
			// No node or goroutine is needed: post drops callbacks after stop.
			l := &loop{stopped: true}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			want := context.DeadlineExceeded
			if canceled {
				cancel()
				want = context.Canceled
			}
			if _, err := observeRing(ctx, l); !errors.Is(err, want) {
				t.Fatalf("observing a stopped loop: %v, want %v", err, want)
			}
			if got := ringStateContext(ctx, l); !strings.Contains(got, want.Error()) {
				t.Fatalf("diagnosing a stopped loop: %s, want %v", got, want)
			}
		})
	}
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
