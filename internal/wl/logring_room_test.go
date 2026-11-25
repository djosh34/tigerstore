package wl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// roomRun is #814's cluster: a ring of 512 KiB (32 segments of 16 KiB),
// each of whose puts of 128 KiB is 8 segments of it, while the leader
// keeps 4 entries for a follower that is down. One follower is stopped;
// the test writes through the leader or the other follower, the live one.
type roomRun struct {
	tc      *testCluster
	c       *clientv3.Client
	loopOf  func(id uint64) *loop
	stopped int
	live    int
	lead    int
	// hold, set, holds the live follower's checkpoints, when the test
	// asked for it, so that its ring fills.
	hold atomic.Bool
}

// roomOpts are a roomRun's choices. throughLeader writes through the
// leader, else through the live follower; old makes the live follower act
// as a release before the refusal frame; holdLive lets the test hold its
// checkpoints; allUp stops no follower. bigFollowers gives the followers
// rings of four times the leader's, so that the leader's is the one full;
// smallLive gives every node but the live follower a ring of twice its,
// so that only the live follower's is full: a ring of one size for every
// node, its cut a checkpoint behind, may leave no room for the next entry
// on another, which then stalls (#815).
type roomOpts struct {
	throughLeader, old, holdLive, allUp, bigFollowers, smallLive bool
}

// roomCluster starts a roomRun. With bigFollowers or smallLive, node 1
// leads, node 2 is the live follower, and node 3 is the one stopped.
func roomCluster(t *testing.T, o roomOpts) *roomRun {
	throughLeader, old, holdLive := o.throughLeader, o.old, o.holdLive
	r := &roomRun{loopOf: watchLoops(t)}
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { waitStrays(t); segSize = was })
	const segs = 32
	oldName, heldName := "", ""
	r.tc = newTestClusterWith(t, func(tc *testCluster) {
		tc.logRing = segs * segSize
		tc.config = func(c *Config) {
			c.CatchupEntries = 4
			c.CheckpointLog = 128 << 10
			c.oldPeerWire = c.Name == oldName
			if c.Name == heldName {
				c.holdCkpt = &r.hold
			}
			if o.bigFollowers && c.Name != "n1" {
				c.LogRing = 4 * segs * segSize
			}
			if o.smallLive && c.Name != "n2" {
				c.LogRing = 2 * segs * segSize
			}
		}
	})
	tc := r.tc
	all := tc.clientOf(t, tc.endpoints[:3]...)
	sized := o.bigFollowers || o.smallLive
	if sized {
		// Node 1 leads.
		if at := tc.leaderEndpoint(t, all); at != tc.endpoints[0] {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := tc.clientOf(t, at).MoveLeader(ctx, tc.ids[0])
			cancel()
			if err != nil {
				t.Fatalf("move the leadership to node 1: %v", err)
			}
		}
		for deadline := time.Now().Add(30 * time.Second); tc.leaderEndpoint(t, all) != tc.endpoints[0]; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("node 1 does not lead 30 s after it was asked to")
			}
		}
	}
	lead := slices.Index(tc.endpoints[:3], tc.leaderEndpoint(t, all))
	r.lead = lead
	if sized {
		r.live = 1
	} else {
		r.live = slices.Index(tc.endpoints[:3], tc.followerEndpoint(t, all))
	}
	r.stopped = 3 - lead - r.live
	writer := lead
	if !throughLeader {
		writer = r.live
	}
	if old || holdLive {
		if old {
			oldName = fmt.Sprintf("n%d", r.live+1)
		}
		if holdLive {
			heldName = fmt.Sprintf("n%d", r.live+1)
		}
		before := r.loopOf(tc.ids[r.live])
		tc.stopNode(t, r.live)
		tc.startNode(t, r.live)
		// The test reads the follower's new loop, not the one that stopped.
		for deadline := time.Now().Add(30 * time.Second); r.loopOf(tc.ids[r.live]) == before; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the follower's loop did not start again in 30 s")
			}
		}
	}
	if !o.allUp {
		tc.stopNode(t, r.stopped)
	}
	t.Cleanup(func() {
		r.hold.Store(false)
		if !t.Failed() {
			return
		}
		for i := range 3 {
			t.Log(roomState(r.loopOf(tc.ids[i])))
		}
	})
	r.c = tc.clientOf(t, tc.endpoints[writer])
	// The writer is up, and knows the leader.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if _, err := r.c.Put(ctx, "/room/up", "x"); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("node %d takes no put after its start: %v", writer+1, ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
	return r
}

// liveDropped is how many appends the live follower dropped for want of
// room in its ring.
func (r *roomRun) liveDropped() int { return dropped(r.loopOf(r.tc.ids[r.live])) }

// liveFull reports whether the live follower's ring is the full one: it
// dropped appends, or has no room for one more put of 128 KiB.
func (r *roomRun) liveFull() bool { return ringFull(r.loopOf(r.tc.ids[r.live])) }

// putsUntil puts 128 KiB values through the writer, up to 64, until done
// reports true: each is taken, refused at once as too many requests or
// with NOSPACE, or, with timeouts and only once the live follower's ring
// is full, times out after requestTimeout, as the full ring is then the
// follower's (#815). No other write comes between them. Without
// timeouts, a put that times out fails the test. It returns the counts
// and the slowest refusal.
func (r *roomRun) putsUntil(t *testing.T, timeouts bool, done func(ok, busy, nospace, timedOut int) bool) (ok, busy, nospace, timedOut int, slowest time.Duration) {
	t.Helper()
	value := strings.Repeat("v", 128<<10)
	for i := 0; i < 64 && !done(ok, busy, nospace, timedOut); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
		start := time.Now()
		_, err := r.c.Put(ctx, fmt.Sprintf("/room/%02d", i), value)
		took := time.Since(start)
		cancel()
		switch {
		case err == nil:
			ok++
			continue
		case errors.Is(err, rpctypes.ErrTooManyRequests):
			busy++
		case errors.Is(err, rpctypes.ErrNoSpace):
			nospace++
		case timeouts && errors.Is(err, rpctypes.ErrTimeout) && r.liveFull():
			timedOut++
			continue
		default:
			t.Fatalf("put %d after %d taken, %d refused as busy, %d with NOSPACE and %d timed out, the live follower having dropped %d appends: %v", i, ok, busy, nospace, timedOut, r.liveDropped(), err)
		}
		slowest = max(slowest, took)
		time.Sleep(50 * time.Millisecond)
	}
	return ok, busy, nospace, timedOut, slowest
}

// nudgesBounded fails the test if the leader proposed more empty entries
// for its ring's sake (nudgeRing) than one per checkpoint it made, and one
// more: a refused write must not turn into a loop of proposals.
func (r *roomRun) nudgesBounded(t *testing.T, when string) {
	t.Helper()
	r.nudgesBoundedOn(t, r.lead, when)
}

// nudgesBoundedOn is nudgesBounded of node i.
func (r *roomRun) nudgesBoundedOn(t *testing.T, i int, when string) {
	t.Helper()
	l := r.loopOf(r.tc.ids[i])
	var nudges, ckpts int
	if !onLoop(l, func() { nudges, ckpts = l.n.stats.ringNudges, int(l.n.stats.ckpts) }) {
		t.Fatalf("%s: node %d's loop runs nothing posted to it in 5 s", when, i+1)
	}
	t.Logf("%s: node %d proposed %d empty entries and made %d checkpoints", when, i+1, nudges, ckpts)
	if nudges > ckpts+1 {
		t.Fatalf("%s: node %d proposed %d empty entries for its ring, past one per checkpoint (%d)", when, i+1, nudges, ckpts)
	}
}

// takenWithin puts value through c until one is taken, and fails the test
// if none is within 30 s.
func takenWithin(t *testing.T, c *clientv3.Client, key, value string, what string) time.Duration {
	const d = 30 * time.Second
	t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := c.Put(pctx, key, value)
		pcancel()
		if err == nil {
			return time.Since(start)
		}
		if ctx.Err() != nil {
			t.Fatalf("%s: no put taken in %v: %v", what, d, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// followerCatchesUp waits, within d, for a put through the live follower
// to be taken, with three puts of 128 KiB through the leader between
// tries: a follower whose ring is full, its cut a checkpoint behind,
// catches up through the leader's snapshot, which the leader sends once
// its log let go of the entries the follower lacks, as the cluster takes
// writes (#815). The follower's client is raw: clientOf's own first put
// would wait for the catch-up before any write drives it.
func (r *roomRun) followerCatchesUp(t *testing.T, d time.Duration) time.Duration {
	t.Helper()
	cf := r.tc.rawClient(t, r.tc.endpoints[r.live])
	value := strings.Repeat("v", 128<<10)
	start := time.Now()
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cf.Put(ctx, "/room/through-full", "x")
		cancel()
		if err == nil {
			return time.Since(start)
		}
		if time.Since(start) > d {
			t.Fatalf("no put through the follower whose ring was full taken in %v: %v", d, err)
		}
		for k := range 3 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
			_, err := r.c.Put(ctx, fmt.Sprintf("/room/meanwhile/%d/%d", i, k), value)
			cancel()
			if err != nil && !errors.Is(err, rpctypes.ErrTooManyRequests) {
				t.Fatalf("a put of 128 KiB through the leader while the follower catches up: %v", err)
			}
		}
	}
}

// TestAPutThatCannotFitTheRingIsRefusedNotHeld is #814: a put of 128 KiB
// that the leader's ring has no room for, as it keeps 4 entries for a
// follower that is down, must be refused at once, as too many requests,
// never left waiting until its client gives up. Through a follower, the
// leader drops the proposal it has no room for and tells the follower,
// which refuses the put. The followers' rings are four times the
// leader's, so that the leader's is the one full. The room comes back without the
// follower that is down, and with no other write: the leader proposes an
// empty entry, whose apply lets a checkpoint be made, and its cut lets go
// of what the leader kept for it, so a put of 128 KiB is taken again
// within 30 s. Once the follower is back, the
// cluster takes writes as before. Before the fix, a put waited for good,
// from the third.
func TestAPutThatCannotFitTheRingIsRefusedNotHeld(t *testing.T) {
	for _, throughLeader := range []bool{true, false} {
		name := "through a follower"
		if throughLeader {
			name = "through the leader"
		}
		t.Run(name, func(t *testing.T) {
			r := roomCluster(t, roomOpts{throughLeader: throughLeader, bigFollowers: true})
			ok, busy, nospace, _, slowest := r.putsUntil(t, false, func(int, int, int, int) bool { return false })
			t.Logf("64 puts of 128 KiB with a follower stopped: %d taken, %d refused as busy, %d with NOSPACE; the slowest refusal took %v", ok, busy, nospace, slowest)
			if busy+nospace == 0 {
				t.Fatal("no put refused: the ring had room for them all, and the test tests nothing")
			}
			if slowest > time.Second {
				t.Fatalf("a refusal took %v: not at once", slowest)
			}
			r.nudgesBounded(t, "after 64 puts")
			// Without the follower, and with no other write: the leader's
			// empty entry applies, a checkpoint is due past it, and its cut
			// gives the ring's room back.
			value := strings.Repeat("v", 128<<10)
			since := time.Now()
			deadline := since.Add(30 * time.Second)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := r.c.Put(ctx, "/room/big-again", value)
				cancel()
				if err == nil {
					break
				}
				if !errors.Is(err, rpctypes.ErrTooManyRequests) || time.Now().After(deadline) {
					t.Fatalf("a put of 128 KiB, with the follower stopped, not taken again in 30 s: %v", err)
				}
				time.Sleep(200 * time.Millisecond)
			}
			t.Logf("a put of 128 KiB taken again %v after the last refusal", time.Since(since).Round(time.Millisecond))
			r.nudgesBounded(t, "once a put was taken again")
			// The test proves the leader's path alone: the follower, of the
			// larger ring, drops nothing (the follower tests cover drops).
			if d := r.liveDropped(); d != 0 {
				t.Fatalf("the live follower, of the larger ring, dropped %d appends", d)
			}
			// The follower back, the leader can let what it kept go.
			r.tc.startNode(t, r.stopped)
			took := takenWithin(t, r.c, "/room/after", value, "after the stopped follower came back")
			t.Logf("a put of 128 KiB taken %v after the stopped follower came back", took.Round(time.Millisecond))
		})
	}
}

// TestAFullRingWithAFollowerStoppedNeverHangs runs #814's cluster with
// rings of one size, where the live follower's ring, its cut a
// checkpoint behind the leader's, may be the full one instead of the
// leader's. Each put is taken, refused at once, or, once the follower
// dropped appends it had no room for, times out after requestTimeout, as
// the cluster has no other quorum (#815); none waits until its client
// gives up. Once the stopped follower returns, a put of 128 KiB is taken
// within 30 s.
func TestAFullRingWithAFollowerStoppedNeverHangs(t *testing.T) {
	r := roomCluster(t, roomOpts{throughLeader: true})
	ok, busy, nospace, timedOut, slowest := r.putsUntil(t, true, func(int, int, int, int) bool { return false })
	t.Logf("64 puts of 128 KiB with a follower stopped: %d taken, %d refused as busy, %d with NOSPACE, %d timed out, the live follower dropping %d appends; the slowest refusal took %v", ok, busy, nospace, timedOut, r.liveDropped(), slowest)
	if busy+nospace+timedOut == 0 {
		t.Fatal("no put refused or timed out: the rings had room for them all, and the test tests nothing")
	}
	if slowest > time.Second {
		t.Fatalf("a refusal took %v: not at once", slowest)
	}
	r.tc.startNode(t, r.stopped)
	took := takenWithin(t, r.c, "/room/after", strings.Repeat("v", 128<<10), "after the stopped follower came back")
	t.Logf("a put of 128 KiB taken %v after the stopped follower came back", took.Round(time.Millisecond))
}

// TestAFullFollowerWithTheOtherStoppedWaitsForIt holds the checkpoints of
// the live follower, with the other stopped and every other ring twice
// its, so that the live follower's ring is the one full: it drops the appends it has no room for, without blocking its loop,
// and the puts through the leader time out after requestTimeout, as the
// cluster has no other quorum (#815). Once the stopped node returns and
// the checkpoints go on, puts are taken again within 30 s, and the full
// follower catches up, through a snapshot or its own cut, within 30 s.
func TestAFullFollowerWithTheOtherStoppedWaitsForIt(t *testing.T) {
	r := roomCluster(t, roomOpts{throughLeader: true, holdLive: true, smallLive: true})
	r.hold.Store(true)
	ok, busy, nospace, timedOut, _ := r.putsUntil(t, true, func(_, _, _, timedOut int) bool { return timedOut > 0 })
	t.Logf("puts of 128 KiB with the live follower's checkpoints held: %d taken, %d refused as busy, %d with NOSPACE, %d timed out; the live follower dropped %d appends", ok, busy, nospace, timedOut, r.liveDropped())
	if timedOut == 0 {
		t.Fatal("no put timed out: the live follower's ring never filled, and the test tests nothing")
	}
	// The follower's loop goes on: it answers a read of what it applied.
	if !onLoop(r.loopOf(r.tc.ids[r.live]), func() {}) {
		t.Fatal("the full follower's loop runs nothing posted to it in 5 s")
	}
	r.hold.Store(false)
	r.tc.startNode(t, r.stopped)
	value := strings.Repeat("v", 128<<10)
	took := takenWithin(t, r.c, "/room/after", value, "through the leader, after the stopped node came back")
	t.Logf("a put of 128 KiB taken %v after the stopped node came back", took.Round(time.Millisecond))
	took = r.followerCatchesUp(t, 30*time.Second)
	t.Logf("a put through the follower that was full taken %v later: %s", took.Round(time.Millisecond), roomState(r.loopOf(r.tc.ids[r.live])))
}

// TestAFollowerRefusingBigPutsNudgesBoundedly writes 64 puts of 128 KiB
// through a follower whose own ring, of 512 KiB, is the small one, all
// three nodes up, the others' rings of 1 MiB. It refuses those its ring
// has no room for at once, and once it applied everything, with its
// newest checkpoint there, it proposes an empty entry, through the
// leader, so that a checkpoint can free its ring: at most one per
// checkpoint it made, and one more, over the run, and a put of 128 KiB
// through it is taken again within 30 s.
func TestAFollowerRefusingBigPutsNudgesBoundedly(t *testing.T) {
	r := roomCluster(t, roomOpts{smallLive: true, allUp: true})
	ok, busy, nospace, timedOut, slowest := r.putsUntil(t, true, func(int, int, int, int) bool { return false })
	t.Logf("64 puts of 128 KiB through the follower of the small ring: %d taken, %d refused as busy, %d with NOSPACE, %d timed out; the slowest refusal took %v", ok, busy, nospace, timedOut, slowest)
	if busy+nospace == 0 {
		t.Fatal("no put refused: the follower's ring had room for them all, and the test tests nothing")
	}
	if slowest > time.Second {
		t.Fatalf("a refusal took %v: not at once", slowest)
	}
	r.nudgesBoundedOn(t, r.live, "after 64 puts")
	took := takenWithin(t, r.c, "/room/again", strings.Repeat("v", 128<<10), "through the follower of the small ring")
	t.Logf("a put of 128 KiB through it taken %v later", took.Round(time.Millisecond))
	r.nudgesBoundedOn(t, r.live, "once a put was taken again")
}

// TestConcurrentRefusalsProposeBoundedEmptyEntries sends rounds of 32
// puts of 128 KiB at once through the leader, whose ring alone is full,
// so that many are refused in one turn: the leader proposes at most one
// empty entry at a time (nudgeRing), so no more than one per checkpoint
// it made, and one more, however many it refused before its Ready.
func TestConcurrentRefusalsProposeBoundedEmptyEntries(t *testing.T) {
	r := roomCluster(t, roomOpts{throughLeader: true, bigFollowers: true})
	r.putsUntil(t, false, func(_, busy, _, _ int) bool { return busy > 0 })
	value := strings.Repeat("v", 128<<10)
	var refused atomic.Int64
	for round := range 8 {
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
				defer cancel()
				_, err := r.c.Put(ctx, fmt.Sprintf("/room/burst/%d/%02d", round, i), value)
				switch {
				case err == nil:
				case errors.Is(err, rpctypes.ErrTooManyRequests):
					refused.Add(1)
				default:
					t.Errorf("a put of 128 KiB in round %d: %v", round, err)
				}
			})
		}
		wg.Wait()
	}
	t.Logf("8 rounds of 32 puts of 128 KiB at once: %d refused", refused.Load())
	if refused.Load() == 0 {
		t.Fatal("no put refused: the test tests nothing")
	}
	r.nudgesBounded(t, "after the bursts")
}

// TestAnOldFollowerWaitsAsBefore writes through a follower that acts as a
// release before the refusal frame (#814's rolling upgrade): it marks no
// proposal, so the leader drops the one it has no room for and tells it
// nothing, and its put times out after requestTimeout, as before the
// frame. The leader's log goes on: the follower's small puts are taken.
func TestAnOldFollowerWaitsAsBefore(t *testing.T) {
	c := roomCluster(t, roomOpts{old: true}).c
	value := strings.Repeat("v", 128<<10)
	var ok, busy, nospace int
	for i := range 64 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
		_, err := c.Put(ctx, fmt.Sprintf("/room/%02d", i), value)
		cancel()
		switch {
		case err == nil:
			ok++
			continue
		case errors.Is(err, rpctypes.ErrTooManyRequests):
			busy++
			time.Sleep(50 * time.Millisecond)
			continue
		case errors.Is(err, rpctypes.ErrNoSpace):
			nospace++
			continue
		case errors.Is(err, rpctypes.ErrTimeout):
		default:
			t.Fatalf("put %d after %d taken, %d refused as busy and %d with NOSPACE: %v", i, ok, busy, nospace, err)
		}
		t.Logf("put %d timed out, after %d taken, %d refused as busy and %d with NOSPACE", i, ok, busy, nospace)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		_, err = c.Put(ctx, "/room/small", "x")
		cancel()
		if err != nil {
			t.Fatalf("a small put through the old follower, after its put timed out: %v", err)
		}
		return
	}
	t.Fatalf("no put timed out through the old follower: %d taken, %d refused as busy and %d with NOSPACE", ok, busy, nospace)
}

// roomState describes a node's ring for a failure: ringState, and what
// the room check counts.
func roomState(l *loop) string {
	got := make(chan string, 1)
	ok := onLoop(l, func() {
		n := l.n
		got <- fmt.Sprintf("node %d: lead %d, room %d, pledged %d, wanted %d, stable %d; dropped %d, refusals sent %d, got %d; appends dropped %d; empty entries %d, checkpoints %d",
			n.id, n.lead, n.wal.ringRoom(), n.ringPledged, n.wal.wanted, n.wal.stable, n.stats.propsDropped, n.stats.refusalsSent, n.stats.refusalsGot, n.stats.appendsDropped, n.stats.ringNudges, n.stats.ckpts)
	})
	if !ok {
		return "a loop that stopped"
	}
	return ringState(l) + "; " + <-got
}

// TestAFollowerWhoseRingIsFullCatchesUp is #814's follower, with all three
// nodes up: its checkpoints are held while the cluster takes puts of 128
// KiB through the leader and the other follower, of rings of 1 MiB, so its
// log fills its ring, of 512 KiB, and stays full once they go on: its cut
// reaches only the start of the checkpoint before its newest, which holds
// the log written meanwhile. It drops the appends its ring has no room for
// rather than write them, so that the leader's snapshot, once the leader's
// log lets go of the entries it lacks, comes through, and a put through it
// is taken again within 30 s. Before, the appends waited for room behind
// the first, the snapshot too, and the follower took no put again.
func TestAFollowerWhoseRingIsFullCatchesUp(t *testing.T) {
	r := roomCluster(t, roomOpts{throughLeader: true, holdLive: true, smallLive: true, allUp: true})
	r.hold.Store(true)
	value := strings.Repeat("v", 128<<10)
	put := func(key string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for {
			_, err := r.c.Put(ctx, key, value)
			if err == nil {
				return
			}
			if !errors.Is(err, rpctypes.ErrTooManyRequests) || ctx.Err() != nil {
				t.Fatalf("a put of 128 KiB through the leader: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// The follower's log fills while its checkpoint is held, and it drops
	// the appends it has no room for.
	for i := 0; !r.liveFull(); i++ {
		if i == 64 {
			t.Fatalf("the follower's ring is not full after 64 puts: %s", roomState(r.loopOf(r.tc.ids[r.live])))
		}
		put(fmt.Sprintf("/full/%02d", i))
	}
	for i := range 8 {
		put(fmt.Sprintf("/full/more/%02d", i))
	}
	t.Logf("the follower's ring is full: %s", roomState(r.loopOf(r.tc.ids[r.live])))
	r.hold.Store(false)
	// The cluster goes on, and the leader's log lets the entries go.
	for i := range 16 {
		put(fmt.Sprintf("/full/after/%02d", i))
	}
	took := r.followerCatchesUp(t, 30*time.Second)
	t.Logf("a put through the follower whose ring was full taken %v later", took.Round(time.Millisecond))
	t.Log(roomState(r.loopOf(r.tc.ids[r.live])))
}

// ringFull reports whether a node's ring is full: it dropped appends, or
// has no room for one more put of 128 KiB.
func ringFull(l *loop) bool {
	got := make(chan bool, 1)
	if !onLoop(l, func() { got <- l.n.stats.appendsDropped > 0 || l.n.wal.ringRoom() < 128<<10+walEntHdr }) {
		return false
	}
	return <-got
}

// dropped is how many appends a node dropped for want of room in its ring.
func dropped(l *loop) int {
	got := make(chan int, 1)
	if !onLoop(l, func() { got <- l.n.stats.appendsDropped }) {
		return 0
	}
	return <-got
}

// onLoop runs fn on loop l, and reports whether it ran within 5 s: a
// loop that stopped drops what is posted to it.
func onLoop(l *loop, fn func()) bool {
	done := make(chan struct{})
	l.post(func() error {
		fn()
		close(done)
		return nil
	})
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// TestAFullLogRingRaisesNOSPACEThroughAFollower is TestAFullLogRingRaisesNOSPACE
// with its puts through a follower, which forwards them to the leader: a
// ring whose cut is held fills, and once it is short with no checkpoint to
// free it, the leader refuses the forwarded puts, as its own clients' (the
// reserve is for deletes), and raises NOSPACE, whose alarm, once it
// applies, makes the follower refuse puts with NOSPACE itself, a follower
// with no ring of its own too. Taken into the reserve, they let a delete
// wait on a segment for good; refused as busy only, they were refused so
// for good (#818's review and re-check).
func TestAFullLogRingRaisesNOSPACEThroughAFollower(t *testing.T) {
	for _, followers := range []struct {
		name string
		ring int64
	}{{"followers' rings four times the leader's", 4}, {"followers with no ring", -1}} {
		t.Run(followers.name, func(t *testing.T) { aFullLogRingRaisesNOSPACEThroughAFollower(t, followers.ring) })
	}
}

func aFullLogRingRaisesNOSPACEThroughAFollower(t *testing.T, followerRing int64) {
	was := segSize
	segSize = 64 << 10
	ringHoldCut.Store(true)
	t.Cleanup(func() { waitStrays(t); segSize = was; ringHoldCut.Store(false) })
	const segs = 8
	loopOf := watchLoops(t)
	var fid atomic.Uint64
	seen := watchAdmissions(t, &fid, "/mixed/")
	// The followers' rings are four times the leader's, node 1's, so that
	// only the leader's is short: the follower's own refusals do not stand
	// in for the leader's.
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.logRing = segs * segSize
		tc.config = func(c *Config) {
			if c.Name != "n1" {
				c.LogRing = followerRing * segs * segSize
				if followerRing < 0 {
					c.LogRing = -1
				}
			}
		}
	})
	all := tc.clientOf(t, tc.endpoints[:3]...)
	if at := tc.leaderEndpoint(t, all); at != tc.endpoints[0] {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := tc.clientOf(t, at).MoveLeader(ctx, tc.ids[0])
		cancel()
		if err != nil {
			t.Fatalf("move the leadership to node 1: %v", err)
		}
	}
	for deadline := time.Now().Add(30 * time.Second); tc.leaderEndpoint(t, all) != tc.endpoints[0]; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("node 1 does not lead 30 s after it was asked to")
		}
	}
	c := tc.clientOf(t, tc.endpoints[1])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
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
		t.Fatalf("after %d puts through a follower and %d refused as busy: %v, want NOSPACE; the leader's ring: %s", puts, busy, err, ringState(loopOf(tc.ids[0])))
	}
	t.Logf("%d puts of about 1 KiB through a follower filled a ring of %d segments of %d bytes; %d were refused as busy", puts, segs, segSize, busy)
	for {
		alarms, err := c.AlarmList(ctx)
		if err == nil && slices.ContainsFunc(alarms.Alarms, func(a *etcdpb.AlarmMember) bool { return a.Alarm == etcdpb.AlarmType_NOSPACE }) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("alarms after the full ring: %v %v", alarms, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// While NOSPACE is up, what frees a full cluster goes on, as etcd's
	// capped applier lets it: a delete and a compaction through the
	// follower are taken. A put is refused with NOSPACE.
	if _, err := c.Delete(ctx, "/full/0000"); err != nil {
		t.Fatalf("a delete while NOSPACE is up: %v", err)
	}
	rev := int64(0)
	if r, err := c.Get(ctx, "/full/0001"); err != nil {
		t.Fatalf("a read while NOSPACE is up: %v", err)
	} else {
		rev = r.Header.Revision
	}
	if _, err := c.Compact(ctx, rev); err != nil {
		t.Fatalf("a compaction while NOSPACE is up: %v", err)
	}
	if _, err := c.Put(ctx, "/full/k", "x"); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a put while NOSPACE is up: %v, want NOSPACE", err)
	}
	// A put and a delete through the follower in one turn: the leader,
	// whose ring is short, drops a batch that puts whole, so the delete
	// must not be in the put's batch (alarmApart, #827's review). The
	// follower's loop is held while both come; testAdmit shows whether it
	// took them in one turn, and a try that did not tests nothing.
	fid.Store(tc.ids[1])
	mixed := false
	for attempt := 0; attempt < 5 && !mixed; attempt++ {
		pk, dk := fmt.Sprintf("/mixed/put%d", attempt), fmt.Sprintf("/mixed/del%d", attempt)
		release := holdLoop(t, loopOf(tc.ids[1]))
		perr, derr := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := c.Put(ctx, pk, "x")
			perr <- err
		}()
		go func() {
			_, err := c.Delete(ctx, dk)
			derr <- err
		}()
		time.Sleep(200 * time.Millisecond)
		release()
		pe, de := <-perr, <-derr
		got, ok := admitted(seen, pk, dk)
		if !ok {
			t.Fatalf("the follower did not take both the put and the delete: %v %v", pe, de)
		}
		if got[pk].turn != got[dk].turn {
			t.Logf("attempt %d: the follower took the put and the delete in turns %d and %d, which tests nothing: again", attempt, got[pk].turn, got[dk].turn)
			continue
		}
		mixed = true
		if de != nil {
			t.Fatalf("a delete through the follower in the turn of a put while NOSPACE is up: %v, want it taken", de)
		}
		if !errors.Is(pe, rpctypes.ErrNoSpace) {
			t.Fatalf("the put in the turn of a delete while NOSPACE is up: %v, want NOSPACE", pe)
		}
	}
	if !mixed {
		t.Fatal("in 5 tries the follower never took a put and a delete in one turn")
	}
	// The cut may go on, the alarm is disarmed, and a put is taken again.
	ringHoldCut.Store(false)
	alarms, err := c.AlarmList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range alarms.Alarms {
		if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: a.MemberID, Alarm: a.Alarm}); err != nil {
			t.Fatalf("disarm %v: %v", a, err)
		}
	}
	for {
		_, err := c.Put(ctx, "/full/k", "after")
		if err == nil {
			break
		}
		if ctx.Err() != nil || !errors.Is(err, rpctypes.ErrTooManyRequests) && !errors.Is(err, rpctypes.ErrNoSpace) {
			t.Fatalf("a put once the cut may go on and the alarm is disarmed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitStrays waits, at most 30 s, for the goroutines nodes started that may
// outlive them (strays) to end: the test's cluster stopped, its cleanups
// run in turn, and one that sets segSize back would race with them.
func waitStrays(t testing.TB) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); strays.Load() != 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Errorf("%d goroutines of stopped nodes still run 30 s after the test", strays.Load())
			return
		}
	}
}
