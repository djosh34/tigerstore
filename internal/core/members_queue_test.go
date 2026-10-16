package core

import (
	"errors"
	"fmt"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// #210: a follower applies and answers a configuration change while the
// leader's disk is held. The next MemberUpdate must wait, not disappear
// into the empty entry Raft uses to reject overlapping changes.
func TestMemberUpdateAfterFollowerReply(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(fmt.Sprintf("exists=%v", exists), func(t *testing.T) {
			c := newMemCluster(t, func(cfg *Config) {})
			for range 100 {
				for _, n := range c.nodes {
					n.Tick()
				}
				c.step()
			}
			leader, follower := c.nodes[0], c.nodes[1]
			term := leader.term
			if !leader.capable() || !follower.capable() {
				t.Fatal("cluster did not activate membership entries")
			}
			var replies []Reply
			c.onReply = func(node int, r Reply) {
				if node != 1 {
					t.Fatalf("reply on node %d, want follower 2", node+1)
				}
				replies = append(replies, r)
			}
			c.holdDisk[0] = true
			follower.Submit(Request{ID: 1, Op: &pb.MemberUpdateRequest{ID: 3, PeerURLs: []string{"https://first:1"}}})
			for range 100 {
				c.step()
				if len(replies) > 0 {
					break
				}
			}
			if len(replies) != 1 || replies[0].ID != 1 || replies[0].Err != nil {
				t.Fatalf("first update: %+v", replies)
			}
			if leader.applied >= follower.applied {
				t.Fatalf("did not reach follower-ahead order: leader %d, follower %d", leader.applied, follower.applied)
			}
			id := uint64(1003)
			want := rpctypes.ErrGRPCMemberNotFound
			if exists {
				id, want = 3, nil
			}
			follower.Submit(Request{ID: 2, Op: &pb.MemberUpdateRequest{ID: id, PeerURLs: []string{"https://second:1"}}})
			for range 20 {
				c.step()
			}
			c.holdDisk[0] = false
			for range 100 {
				c.step()
			}
			if len(replies) != 2 {
				t.Fatalf("second MemberUpdate disappeared: %d replies, want 2", len(replies))
			}
			if replies[1].ID != 2 || !errors.Is(replies[1].Err, want) {
				t.Fatalf("second update: %+v, want error %v", replies[1], want)
			}
			if !leader.isLeader || leader.term != term {
				t.Fatalf("leader changed: leader=%v, term=%d, want %d", leader.isLeader, leader.term, term)
			}
			for _, n := range c.nodes {
				url := "https://first:1"
				if exists {
					url = "https://second:1"
				}
				if got := n.state.members[3].PeerURLs; len(got) != 1 || got[0] != url {
					t.Fatalf("node %d: peer URLs %v, want %s", n.cfg.ID, got, url)
				}
			}
		})
	}
}

func TestMemberUpdatesQueuedOnLeader(t *testing.T) {
	s := startSingle(t, simfs.New(), 1).capable()
	for id := uint64(1); id <= 3; id++ {
		s.n.Submit(Request{ID: id, Op: &pb.MemberUpdateRequest{ID: 1, PeerURLs: []string{fmt.Sprintf("https://one:%d", id)}}})
	}
	var replies []Reply
	for range 20 {
		replies = append(replies, s.run()...)
	}
	if len(replies) != 3 {
		t.Fatalf("%d replies to queued changes, want 3", len(replies))
	}
	for i, r := range replies {
		if r.Err != nil || r.ID != uint64(i+1) {
			t.Fatalf("reply %d: %+v", i, r)
		}
	}
}

// A member change Raft drops for its uncommitted-size limit still moves
// Raft's pending index, so the retry becomes an empty entry. The change
// must stay queued and go out once the log drains, and the next must too.
func TestMemberUpdateAfterUncommittedLimit(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) {
		cfg.UnappliedBytes, cfg.MaxBatchBytes = 1<<10, 1<<10
	})
	for range 100 {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	leader := c.nodes[0]
	if !leader.isLeader || !leader.capable() {
		t.Fatal("node 1 is not a capable leader")
	}
	var replies []Reply
	c.onReply = func(node int, r Reply) {
		if node == 0 {
			replies = append(replies, r)
		}
	}
	// A first entry may pass the limit; nothing commits while the acks
	// are held, so the change after it is over the limit.
	c.holdAcks = true
	leader.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 4<<10)}})
	leader.Submit(Request{ID: 2, Op: &pb.MemberUpdateRequest{ID: 3, PeerURLs: []string{"https://first:1"}}})
	for range 20 {
		c.step()
	}
	c.holdAcks = false
	leader.Submit(Request{ID: 3, Op: &pb.MemberUpdateRequest{ID: 3, PeerURLs: []string{"https://second:1"}}})
	for range 100 {
		c.step()
	}
	if len(replies) != 3 {
		t.Fatalf("%d replies, want 3: %+v", len(replies), replies)
	}
	for i, r := range replies {
		if r.ID != uint64(i+1) || r.Err != nil {
			t.Fatalf("reply %d: %+v", i, r)
		}
	}
	for _, n := range c.nodes {
		if got := n.state.members[3].PeerURLs; len(got) != 1 || got[0] != "https://second:1" {
			t.Fatalf("node %d: peer URLs %v", n.cfg.ID, got)
		}
	}
}
