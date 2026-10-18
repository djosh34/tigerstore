package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestDiag88WarnOnly(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.CommitStallElections = 1 })
	c.holdDisk = [3]bool{true, true, true}
	n := c.nodes[0]
	n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("pending"), Value: []byte("v")}})
	c.step()
	// Heartbeats still pass before the separate local-disk silence limit.
	for range n.cfg.ElectionTicks + 2 {
		for _, node := range c.nodes {
			node.Tick()
		}
		c.step()
	}
	m := n.Metrics()
	if m.CommitWarnings != 1 || m.MaxCommitWait < int64(n.cfg.ElectionTicks) {
		t.Fatalf("slow pending entry missing from warning metrics: warnings=%d wait=%d", m.CommitWarnings, m.MaxCommitWait)
	}
	if len(c.handOffs) != 0 {
		t.Fatal(c.handOffs)
	}
	t.Logf("uncommitted entry: warning count=%d, maximum wait=%d ticks; still alive", m.CommitWarnings, m.MaxCommitWait)
	c.holdDisk = [3]bool{}
	for range 20 {
		c.step()
	}
	if c.replies == 0 {
		t.Fatal("pending write did not finish after release")
	}
}
