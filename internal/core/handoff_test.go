package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestStalledLeaderHandsOver is #156's case: with node 3 down, the
// leader's disk holds a write past StallTicks election timeouts. The
// leader must hand leadership to node 2 and keep running, not stop and
// leave no quorum, and once its disk is back the two must elect a leader
// that takes writes. Which one leads then is Raft's: node 2 cannot get
// the vote of node 1 while node 1's disk is held, and once that disk is
// back node 1 may win the next election (#255: 18 of 3,000 runs). The
// first write may then time out, as a client sees in a leader change.
func TestStalledLeaderHandsOver(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.onReply = func(i int, r Reply) {
		switch {
		case r.Err == nil:
			c.replies++
		case r.ID != 1 || !errors.Is(r.Err, rpctypes.ErrGRPCTimeout):
			t.Fatalf("node %d answered %d with %v; %s", i+1, r.ID, r.Err, c.describe())
		}
	}
	c.down[2] = true
	c.holdDisk[0] = true
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	election := c.nodes[0].cfg.ElectionTicks
	for range (StallTicks + 1) * election {
		c.nodes[0].Tick()
		c.nodes[1].Tick()
		c.step()
	}
	if len(c.handOffs) == 0 || c.handOffs[0] != 2 {
		t.Fatalf("the stalled leader handed over to %v, want node 2; %s", c.handOffs, c.describe())
	}
	c.holdDisk[0] = false
	lead := -1
	for range 10 * election {
		c.nodes[0].Tick()
		c.nodes[1].Tick()
		c.step()
		if lead = c.leader(); lead >= 0 {
			break
		}
	}
	if lead < 0 {
		t.Fatalf("no leader within 10 election timeouts of the disk's return; %s", c.describe())
	}
	before := c.replies
	c.nodes[lead].Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("w")}})
	for range election {
		c.step()
	}
	if c.replies <= before {
		t.Fatalf("node %d leads but took no write; %s", lead+1, c.describe())
	}
}
