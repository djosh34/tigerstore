package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
)

// TestDiag88StuckLeaderSilent holds the leader's own disk indefinitely.
// Two healthy followers remain. After the local append limit it must
// send nothing, never hand off or exit, and let the followers elect.
func TestDiag88StuckLeaderSilent(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.onReply = func(_ int, r Reply) {
		// The first write need not complete through the held local disk.
		// An unacknowledged request timing out is not a stopped node.
		if r.ID == 1 && errors.Is(r.Err, rpctypes.ErrGRPCTimeout) {
			return
		}
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		c.replies++
	}
	n := c.nodes[0]
	start := n.now
	limit := int64(StallTicks * n.cfg.ElectionTicks)
	var late int
	c.onSend = func(m *raftpb.Message) {
		if m.GetFrom() == 1 && n.now-start >= limit {
			late++
		}
	}
	c.holdDisk[0] = true
	n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("held"), Value: []byte("v")}})
	c.step()
	// Election draws can tie for several rounds. Hold the disk throughout,
	// check silence for at least six timeouts, and allow thirty to elect.
	var ticks int
	for ticks = 0; ticks < 30*n.cfg.ElectionTicks; ticks++ {
		for _, node := range c.nodes {
			node.Tick()
		}
		c.step()
		if ticks >= 6*n.cfg.ElectionTicks && c.nodes[1].Leader() != 0 && c.nodes[1].Leader() != 1 && c.nodes[1].Leader() == c.nodes[2].Leader() {
			break
		}
	}
	if late != 0 || len(c.handOffs) != 0 {
		t.Fatalf("held disk sent %d messages past %d ticks, handoffs=%v; %s", late, limit, c.handOffs, c.describe())
	}
	if c.nodes[1].Leader() == 1 || c.nodes[1].Leader() == 0 || c.nodes[1].Leader() != c.nodes[2].Leader() {
		t.Fatalf("healthy pair did not elect while the old disk stayed held: %s", c.describe())
	}
	t.Logf("disk still held after %d ticks; messages after limit=%d; %s", ticks+1, late, c.describe())
	// Resume the old disk and prove another write commits with all nodes alive.
	c.holdDisk[0] = false
	for range 4 * n.cfg.ElectionTicks {
		for _, node := range c.nodes {
			node.Tick()
		}
		c.step()
	}
	c.onSend = nil
	leader := c.leader()
	if leader < 0 {
		t.Fatal(c.describe())
	}
	before := c.replies
	c.nodes[leader].Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("after"), Value: []byte("v")}})
	for range 20 {
		c.step()
	}
	if c.replies <= before {
		t.Fatal("no write after the disk returned")
	}
}
