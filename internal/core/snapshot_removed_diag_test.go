package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

// A serve can reach the core after its peer was removed. That peer is
// absent from WithProgress, so it must not leave a permanent snapWait pin.
func TestDiag379RemovedSnapshotPin(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	n := c.nodes[0]
	for range 10 {
		c.step()
	}
	ctx, err := encodeConfContext(0, 0, 0, confRemove, &pb.Member{ID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.rn.ProposeConfChange(&raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(3)), Context: ctx}); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		for _, peer := range c.nodes {
			peer.Tick()
		}
		c.step()
	}
	if n.state.members[3] != nil {
		t.Fatalf("member 3 was not removed: %s", c.describe())
	}
	n.SnapshotStarted(3)
	if at, ok := n.snapWait[3]; !ok || at != 0 {
		t.Fatalf("late serve did not start a pin: %v", n.snapWait)
	}
	n.snapshotProgress()
	if len(n.snapWait) != 0 {
		t.Fatalf("removed peer prevents checkpoint removal forever: %v", n.snapWait)
	}
}
