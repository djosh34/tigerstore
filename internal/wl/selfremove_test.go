package wl

import (
	"encoding/binary"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func removalApply(t *testing.T, id uint64) *pb.Message {
	t.Helper()
	cc := &pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(id), Context: ccContext(0, 0, &member{id: id})}
	data, err := proto.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Message{Entries: []*pb.Entry{{Index: new(uint64(10)), Term: new(uint64(2)), Type: pb.EntryConfChange.Enum(), Data: data}}}
}

func TestRemovingLeaderWaitsForSurvivingCommitKnowledge(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	n.applied = 8
	m := removalApply(t, n.id)
	m.Entries = append([]*pb.Entry{{Index: new(uint64(9)), Term: new(uint64(2))}}, m.Entries...)
	if err := n.storageApply(m); err != nil {
		t.Fatalf("the leader exited before its quorum barrier: %v", err)
	}
	if n.selfRemoval.apply != m || n.selfRemoval.index != 10 || n.selfRemoval.offset != 1 || n.applied != 9 || n.memberAt(n.id) < 0 {
		t.Fatal("the barrier copied, applied, or lost its already-owned prefix")
	}
	// Later apply work cannot pass the guaranteed removal, and is not
	// acknowledged or put into a new queue while the barrier is outstanding.
	later := &pb.Message{Entries: []*pb.Entry{{Index: new(uint64(11))}}}
	if err := n.storageApply(later); err != nil || n.applied != 9 || n.selfRemoval.apply != m {
		t.Fatal("later work passed or replaced the removal prefix")
	}
	seq := n.selfRemoval.seq
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], seq+1)
	n.readState(ctx[:], 10)
	if n.selfRemoval.ready {
		t.Fatal("an unrelated ReadIndex permitted exit")
	}
	binary.BigEndian.PutUint64(ctx[:], seq)
	n.readState(ctx[:], 9)
	if n.selfRemoval.ready {
		t.Fatal("a quorum index short of the removal permitted exit")
	}
	n.now = n.now.Add(6 * tickEvery)
	n.retrySelfRemovalRead()
	if n.selfRemoval.seq == seq {
		t.Fatal("a lost quorum request was not retried")
	}
	n.readState(ctx[:], 10)
	if n.selfRemoval.ready {
		t.Fatal("an obsolete retry context permitted exit")
	}
	binary.BigEndian.PutUint64(ctx[:], n.selfRemoval.seq)
	n.readState(ctx[:], 10)
	if !n.selfRemoval.ready {
		t.Fatal("the quorum's committed removal was not recognized")
	}
	if err := n.resumeSelfRemoval(); !errors.Is(err, ErrRemoved) {
		t.Fatalf("the confirmed removal did not leave normally: %v", err)
	}
	if n.selfRemoval.apply != nil || n.memberAt(n.id) >= 0 || !n.removed[n.id] {
		t.Fatal("the confirmed removal leaked control ownership or kept membership")
	}
}

func TestSelfRemovalBarrierDoesNotChangeOtherMembershipWork(t *testing.T) {
	for _, mode := range []string{"follower", "other-member", "single-voter"} {
		t.Run(mode, func(t *testing.T) {
			n := gateNode(t)
			n.rn, n.lead = newTestRawNode(t), n.id
			id := n.id
			switch mode {
			case "follower":
				n.lead = 2
			case "other-member":
				id = 3
			case "single-voter":
				n.members = n.members[:1]
			}
			m := removalApply(t, id)
			var cc pb.ConfChange
			if err := proto.Unmarshal(m.Entries[0].GetData(), &cc); err != nil {
				t.Fatal(err)
			}
			if n.deferSelfRemoval(m, 0, &cc) || n.selfRemoval.apply != nil {
				t.Fatal("unrelated membership work acquired a retirement barrier")
			}
		})
	}
}
