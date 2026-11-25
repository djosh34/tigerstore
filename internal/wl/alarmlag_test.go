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
