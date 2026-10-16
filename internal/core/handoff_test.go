package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestStalledLeaderHandsOver is #156's case: with node 3 down, the
// leader's disk holds a write past StallTicks election timeouts. The
// leader must hand leadership to node 2 and keep running, not stop and
// leave no quorum, and once its disk is back node 2 must lead and take
// writes.
func TestStalledLeaderHandsOver(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
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
		t.Fatalf("the stalled leader handed over to %v, want node 2", c.handOffs)
	}
	c.holdDisk[0] = false
	for range 3 * election {
		c.nodes[0].Tick()
		c.nodes[1].Tick()
		c.step()
		if c.nodes[1].isLeader {
			break
		}
	}
	if !c.nodes[1].isLeader {
		t.Fatal("node 2 did not take over")
	}
	before := c.replies
	c.nodes[1].Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("w")}})
	for range election {
		c.step()
	}
	if c.replies <= before {
		t.Fatal("node 2 took no write")
	}
}
