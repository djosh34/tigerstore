package wl

import (
	"context"
	"errors"
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAPutAfterADisarmIsTakenByAFollowerThatLags is #825: a put sent once
// AlarmDisarm answered comes after the disarm in the log, so etcd's capped
// applier never refuses it. A follower that had not applied the disarm
// yet refused it before proposing it, by its own view of the alarm (#818's
// early gate). Here the follower's loop is held while the leader applies
// the disarm, and the put reaches the follower before its loop goes on:
// the put must be taken.
func TestAPutAfterADisarmIsTakenByAFollowerThatLags(t *testing.T) {
	loopOf := watchLoops(t)
	tc := newTestCluster(t)
	all := tc.clientOf(t, tc.endpoints...)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
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
	held, release := make(chan struct{}), make(chan struct{})
	ran := make(chan bool, 1)
	go func() {
		ran <- onLoop(loopOf(tc.ids[f]), func() {
			close(held)
			<-release
		})
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the follower's loop was not held within 5 s")
	}
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	// The leader and the other follower apply the disarm; the held one
	// cannot yet.
	if _, err := lc.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[li], Alarm: etcdpb.AlarmType_NOSPACE}); err != nil {
		t.Fatalf("disarm NOSPACE: %v", err)
	}
	put := make(chan error, 1)
	go func() {
		_, err := fc.Put(ctx, "/lag/after", "v")
		put <- err
	}()
	// The put's request reaches the follower's socket while its loop is
	// held, so its next turn reads it before it applies the disarm.
	time.Sleep(200 * time.Millisecond)
	close(release)
	released = true
	if !<-ran {
		t.Fatal("the follower's loop did not run the hold")
	}
	if err := <-put; err != nil {
		t.Fatalf("a put through a follower sent after AlarmDisarm answered: %v", err)
	}
}
