package wl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

// admission is what a node held as it took a write from a client
// (testAdmit): the write's key, the node's alarm, and its turn.
type admission struct {
	key     string
	noSpace bool
	turn    uint64
}

// watchAdmissions records the writes node id takes whose keys start with
// prefix, until the test ends.
func watchAdmissions(t *testing.T, id *atomic.Uint64, prefix string) <-chan admission {
	seen := make(chan admission, 64)
	hook := func(n *node, kind uint8, msg []byte) {
		if n.id != id.Load() {
			return
		}
		var key string
		switch kind {
		case kPut:
			var r putReq
			if parsePut(msg, &r) != nil {
				return
			}
			key = string(r.key)
		case kDel:
			var r delReq
			if parseDel(msg, &r) != nil {
				return
			}
			key = string(r.key)
		default:
			return
		}
		if strings.HasPrefix(key, prefix) {
			select {
			case seen <- admission{key: key, noSpace: n.noSpace, turn: n.stats.turns}:
			default:
			}
		}
	}
	testAdmit.Store(&hook)
	t.Cleanup(func() { testAdmit.Store(nil) })
	return seen
}

// admitted waits for the admissions of keys, and reports whether each
// came within 10 s.
func admitted(seen <-chan admission, keys ...string) (map[string]admission, bool) {
	got := map[string]admission{}
	deadline := time.After(10 * time.Second)
	for len(got) < len(keys) {
		select {
		case a := <-seen:
			for _, k := range keys {
				if a.key == k {
					got[k] = a
				}
			}
		case <-deadline:
			return got, false
		}
	}
	return got, true
}

// holdLoop holds loop l until the release it returns is called, which the
// test's end calls too: a hold whose start comes late, after holdLoop gave
// up on it, finds it released and holds nothing.
func holdLoop(t *testing.T, l *loop) (release func()) {
	ch := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	held := make(chan struct{})
	go onLoop(l, func() {
		close(held)
		<-ch
	})
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("the loop was not held within 5 s")
	}
	return release
}

// pledgesApart checks, on follower l's loop while NOSPACE is up in its
// view and its own ring has room, that a put and then a delete go in
// batches apart, each with the ring room of its own write pledged
// (alarmApart before the pledge, #827's re-check): the leader drops the
// put's batch, and the room given back with it must not be the delete's,
// which is still owed.
func pledgesApart(t *testing.T, l *loop) {
	t.Helper()
	put, err := proto.Marshal(&etcdpb.PutRequest{Key: []byte("/pledge/put"), Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	del, err := proto.Marshal(&etcdpb.DeleteRangeRequest{Key: []byte("/pledge/del")})
	if err != nil {
		t.Fatal(err)
	}
	var noSpace, putRefused, delRefused bool
	var putBatch, open int64
	if !onLoop(l, func() {
		n := l.n
		noSpace = n.noSpace
		if !noSpace {
			return
		}
		n.proposeBatch(n.now)
		if putRefused = n.ringRefuses(nil, &stream{kind: kPut}, put); putRefused {
			return
		}
		n.addOp(noHandle, kPut, put)
		seq := n.fwdSeq
		if delRefused = n.ringRefuses(nil, &stream{kind: kDel}, del); delRefused {
			return
		}
		n.addOp(noHandle, kDel, del)
		putBatch, open = n.fwd[seq&(fwdSlots-1)].pledged, n.batchPledge
		n.proposeBatch(n.now)
	}) {
		t.Fatal("the follower's loop did not run the check")
	}
	switch {
	case !noSpace:
		t.Fatal("NOSPACE is not up in the follower's view")
	case putRefused || delRefused:
		t.Fatalf("the follower's ring refused the put (%v) or the delete (%v)", putRefused, delRefused)
	}
	putNeed, delNeed := int64(len(put))+walEntHdr+ringRecordSlack, int64(len(del))+walEntHdr+ringRecordSlack
	if putBatch != putNeed || open != delNeed {
		t.Fatalf("pledged: the put's batch %d, the delete's %d; want %d and %d, each its own write's", putBatch, open, putNeed, delNeed)
	}
}

// TestAWriteDroppedByTheLeaderGetsTheErrorOfTheAlarmsStillUp is #827's
// re-check: a forwarded batch keeps each write's alarmFlags as it came,
// and refused answers each with the error the alarms up at the refusal
// give it, CORRUPT first, not the error of the alarms up as it came.
func TestAWriteDroppedByTheLeaderGetsTheErrorOfTheAlarmsStillUp(t *testing.T) {
	put, err := proto.Marshal(&etcdpb.PutRequest{Key: []byte("/k"), Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	del, err := proto.Marshal(&etcdpb.DeleteRangeRequest{Key: []byte("/k")})
	if err != nil {
		t.Fatal(err)
	}
	pf, df := alarmFlags(kPut, put), alarmFlags(kDel, del)
	n := &node{}
	for _, c := range []struct {
		name             string
		corrupt, noSpace bool
		put, del         string
	}{
		{"both up", true, true, msgCorrupt, msgCorrupt},
		{"CORRUPT disarmed since", false, true, msgNoSpace, ""},
		{"NOSPACE disarmed since", true, false, msgCorrupt, msgCorrupt},
		{"none up", false, false, "", ""},
	} {
		n.corrupt, n.noSpace = c.corrupt, c.noSpace
		for _, w := range []struct {
			what string
			f    uint8
			want string
		}{{"a put", pf, c.put}, {"a delete", df, c.del}} {
			got := ""
			if e := n.alarmErr(w.f); e != nil {
				got = e.Error()
			}
			if !strings.Contains(got, w.want) || (w.want == "") != (got == "") {
				t.Errorf("%s: %s gets %q, want %q", c.name, w.what, got, w.want)
			}
		}
	}
}

// TestAPutAfterADisarmIsTakenByAFollowerThatLags is #825: a put sent once
// AlarmDisarm answered comes after the disarm in the log, so etcd's capped
// applier never refuses it. A follower that had not applied the disarm
// yet refused it before proposing it, by its own view of the alarm (#818's
// early gate). Here the follower's loop is held while the leader applies
// the disarm, and the put reaches the follower before its loop goes on:
// the put must be taken. The follower records its alarm as it takes the
// put (testAdmit); a put that came after it applied the disarm tests
// nothing, and the test runs again.
func TestAPutAfterADisarmIsTakenByAFollowerThatLags(t *testing.T) {
	loopOf := watchLoops(t)
	var fid atomic.Uint64
	seen := watchAdmissions(t, &fid, "/lag/after")
	tc := newTestCluster(t)
	all := tc.clientOf(t, tc.endpoints...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for attempt := range 5 {
		lead := tc.leaderEndpoint(t, all)
		li, f := -1, -1
		for i, ep := range tc.endpoints {
			switch {
			case ep == lead:
				li = i
			case f < 0:
				f = i
			}
		}
		if li < 0 || f < 0 {
			t.Fatalf("no leader among %v: %s", tc.endpoints, lead)
		}
		fid.Store(tc.ids[f])
		lc, fc := tc.clientOf(t, lead), tc.clientOf(t, tc.endpoints[f])
		nospace := &etcdpb.AlarmRequest{Action: etcdpb.AlarmRequest_ACTIVATE, MemberID: tc.ids[li], Alarm: etcdpb.AlarmType_NOSPACE}
		if _, err := etcdpb.NewMaintenanceClient(lc.ActiveConnection()).Alarm(ctx, nospace); err != nil {
			t.Fatalf("raise NOSPACE: %v", err)
		}
		// The follower applied the alarm once it refuses a put with it.
		for {
			_, err := fc.Put(ctx, "/lag/before", "v")
			if errors.Is(err, rpctypes.ErrNoSpace) {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("the follower never refused a put while NOSPACE was up: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		release := holdLoop(t, loopOf(tc.ids[f]))
		// The leader and the other follower apply the disarm; the held
		// one cannot yet.
		if _, err := lc.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[li], Alarm: etcdpb.AlarmType_NOSPACE}); err != nil {
			release()
			t.Fatalf("disarm NOSPACE: %v", err)
		}
		key := fmt.Sprintf("/lag/after%d", attempt)
		put := make(chan error, 1)
		go func() {
			_, err := fc.Put(ctx, key, "v")
			put <- err
		}()
		// The put's request reaches the follower's socket while its loop
		// is held, so its next turn reads it before it applies the
		// disarm, which testAdmit then shows.
		time.Sleep(200 * time.Millisecond)
		release()
		err := <-put
		got, ok := admitted(seen, key)
		if !ok {
			t.Fatalf("the follower never took the put %s: %v", key, err)
		}
		if !got[key].noSpace {
			t.Logf("attempt %d: the put came to the follower after it applied the disarm, which tests nothing: again", attempt)
			continue
		}
		if err != nil {
			t.Fatalf("a put through a follower sent after AlarmDisarm answered, which the follower took with NOSPACE still up in its view: %v", err)
		}
		return
	}
	t.Fatal("in 5 attempts no put came to the follower before it applied the disarm")
}
