package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/wal"
)

// A removed leader must send its commit update before closing its links.
// Otherwise its followers can remain in the old voting configuration.
func TestRemovedLeaderDrainsCommit(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) {})
	leader := c.nodes[0]
	for range 20 {
		c.step()
	}
	ctx, err := encodeConfContext(0, 0, 0, confRemove, &pb.Member{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Persist the removal locally, but hold its follower acknowledgments.
	c.holdAcks = true
	if err := leader.rn.ProposeConfChange(&raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(1)), Context: ctx}); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		c.step()
	}
	if leader.state.removed[1] || len(c.msgs) == 0 {
		t.Fatal("removal did not wait for the held acknowledgments")
	}
	// A later append is still in flight when those acknowledgments
	// commit the removal. Its self-ack arrives after Raft removed us.
	c.holdDisk[0] = true
	if err := leader.rn.Propose(nil); err != nil {
		t.Fatal(err)
	}
	c.holdAcks = false
	for range 100 {
		c.step()
		if leader.state.removed[1] {
			break
		}
	}
	if !leader.state.removed[1] {
		t.Fatal("leader did not apply its removal")
	}
	removal := leader.Applied()
	if len(c.jobs[0]) == 0 {
		t.Fatal("no leader disk completion was held across removal")
	}
	// No ticks pass while the queued commit update and disk completions
	// drain. An immediate ErrRemoved would make the driver drop them.
	c.holdDisk[0] = false
	for range 100 {
		c.step()
	}
	// The followers take nothing from the leader after they applied its
	// removal, as etcd's Process refuses a removed member's messages.
	for _, n := range c.nodes[1:] {
		if !n.state.removed[1] || n.Applied() < removal {
			t.Fatalf("node %d did not learn the removal: applied %d, members %v", n.cfg.ID, n.Applied(), n.Members())
		}
	}

	leader.Submit(Request{ID: 10, Op: &pb.PutRequest{Key: []byte("late"), Value: []byte("value")}})
	eff, err := leader.Process()
	if err != nil || len(eff.Replies) != 1 || !errors.Is(eff.Replies[0].Err, rpctypes.ErrGRPCStopped) {
		t.Fatalf("request during removal: replies %v, error %v", eff.Replies, err)
	}
	if err := leader.Step(&raftpb.Message{Type: raftpb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1))}); !errors.Is(err, ErrRemoved) {
		t.Fatalf("forwarded proposal during removal: %v", err)
	}
	for i := int64(1); i < leader.cfg.ticksOf(1); i++ {
		leader.Tick()
		c.step()
	}
	leader.Tick()
	if _, err := leader.Process(); !errors.Is(err, ErrRemoved) {
		t.Fatalf("removed leader did not stop after the drain interval: %v", err)
	}
	c.down[0] = true
	// Two voters that time out a tick or two apart both pass pre-vote and
	// split the vote, so a few rounds can pass without a leader.
	for range 1000 {
		for _, n := range c.nodes[1:] {
			n.Tick()
		}
		c.step()
		if c.nodes[1].isLeader || c.nodes[2].isLeader {
			return
		}
	}
	t.Fatal("the remaining members did not elect a leader")
}

// The drain interval is only for a live node. A restart must not bring
// back a node whose durable commit already includes its removal.
func TestRemovedMemberRefusesRecovery(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	s.run()
	ctx, err := encodeConfContext(0, 0, 0, confRemove, &pb.Member{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(&raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(1)), Context: ctx})
	if err != nil {
		t.Fatal(err)
	}
	index := s.n.Applied() + 1
	s.log.Append(&wal.HardState{Term: 1, Commit: index}, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(index), Type: raftpb.EntryConfChange.Enum(), Data: data}})
	if err := errors.Join(s.log.Sync(), s.log.Close()); err != nil {
		t.Fatal(err)
	}
	w, rec, err := wal.Open(s.disk.Durable())
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(DefaultConfig(1, []uint64{1, 2, 3}), rec, w)
	if closeErr := w.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, ErrRemoved) {
		t.Fatalf("recovering a removed member: %v", err)
	}
}

// TestRemovedPeerReleasesSnapshotWait removes member 3, after which the
// start of a fetch it had begun reaches the leader's core. Raft no longer
// tracks the peer, and its wait must still end, or the leader keeps every
// checkpoint from then on: a membership seed filled its disk (#379,
// TestMembershipFaults seed 19462). A short test, as that seed's schedule
// moves with any change of the simulator.
func TestRemovedPeerReleasesSnapshotWait(t *testing.T) {
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
		t.Fatalf("the late start of the fetch set no wait: %v", n.snapWait)
	}
	n.snapshotProgress()
	if len(n.snapWait) != 0 {
		t.Fatalf("the removed peer still waits, which keeps every checkpoint: %v", n.snapWait)
	}
}
