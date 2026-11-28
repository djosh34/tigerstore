package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"time"

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
	return &pb.Message{Entries: []*pb.Entry{
		{Index: new(uint64(9)), Term: new(uint64(2))},
		{Index: new(uint64(10)), Term: new(uint64(2)), Type: pb.EntryConfChange.Enum(), Data: data},
	}}
}

func TestRemovingLeaderDefersApply(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	n.applied = 8
	if err := n.storageApply(removalApply(t, n.id)); err != nil {
		t.Fatalf("the leader exited at its removal's apply, with no quorum barrier: %v", err)
	}
}

func TestRemovingLeaderWaitsForSurvivingCommitKnowledge(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	n.applied = 8
	m := removalApply(t, n.id)
	if err := n.storageApply(m); err != nil {
		t.Fatal(err)
	}
	if n.selfRemoval.apply != m || n.selfRemoval.index != 10 || n.selfRemoval.offset != 1 || n.applied != 9 || n.memberAt(n.id) < 0 {
		t.Fatal("the barrier copied, applied, or lost its already-owned prefix")
	}
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
	// Time alone must never substitute for old-quorum knowledge.
	n.now = n.now.Add(time.Hour)
	if err := n.resumeSelfRemoval(); err != nil || n.selfRemovalDrained() || n.removed[n.id] {
		t.Fatal("elapsed time bypassed the quorum barrier")
	}
	binary.BigEndian.PutUint64(ctx[:], n.selfRemoval.seq)
	n.lead = 2
	n.readState(ctx[:], 10)
	if n.selfRemoval.ready {
		t.Fatal("a forwarded ReadIndex after losing leadership permitted exit")
	}
	n.lead = n.id
	n.selfRemoval.term++
	n.readState(ctx[:], 10)
	if n.selfRemoval.ready {
		t.Fatal("a ReadIndex in another term permitted exit")
	}
	n.selfRemoval.term--
	n.readState(ctx[:], 10)
	if !n.selfRemoval.ready {
		t.Fatal("the quorum's committed removal was not recognized")
	}
	if err := n.resumeSelfRemoval(); err != nil {
		t.Fatalf("confirmed removal skipped the output drain: %v", err)
	}
	if n.selfRemoval.apply != nil || n.memberAt(n.id) >= 0 || !n.removed[n.id] || n.selfRemoval.drainBy.IsZero() {
		t.Fatal("confirmed removal leaked ownership or kept membership")
	}
	if err := n.storageApply(later); err != nil || n.applied != 9 {
		t.Fatal("later work passed the removal during output drain")
	}
	// A socket blocked on output delays exit, but only for ten heartbeats.
	c := new(client)
	c.wantOut = true
	n.socks = []any{c}
	if n.selfRemovalDrained() {
		t.Fatal("blocked output was not given a drain turn")
	}
	n.now = n.now.Add(10 * tickEvery)
	if !n.selfRemovalDrained() {
		t.Fatal("a blocked client extended the drain deadline")
	}
}

func TestSelfRemovalNoticesNeedDistinctOldVoters(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	// Four old voters and a learner: the leader needs two other voters.
	n.members = append(n.members, member{id: 4}, member{id: 5, learner: true})
	if err := n.storageApply(removalApply(t, n.id)); err != nil {
		t.Fatal(err)
	}
	n.selfRemovalNotice(5)
	n.selfRemovalNotice(99)
	n.selfRemovalNotice(2)
	n.selfRemovalNotice(2) // The second lane is not another vote.
	if n.selfRemoval.ready {
		t.Fatal("a learner, outsider or duplicate made an old quorum")
	}
	n.selfRemovalNotice(3)
	if !n.selfRemoval.ready {
		t.Fatal("three of four old voters did not confirm removal")
	}
	// A notice is transport input, not a Raft step: ready must resume the
	// apply even when RawNode has no new Ready to hand out.
	for n.rn.HasReady() {
		_ = n.rn.Ready()
	}
	if err := n.ready(); err != nil || !n.removed[n.id] || n.selfRemoval.drainBy.IsZero() {
		t.Fatalf("transport confirmation did not resume removal: %v", err)
	}
}

func TestSelfRemovalBarrierDoesNotChangeOtherMembershipWork(t *testing.T) {
	for _, mode := range []string{"follower", "other-member", "single-voter", "update", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			n := gateNode(t)
			n.rn, n.lead = newTestRawNode(t), n.id
			m := removalApply(t, n.id)
			var cc pb.ConfChange
			if err := proto.Unmarshal(m.Entries[1].GetData(), &cc); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "follower":
				n.lead = 2
			case "other-member":
				cc.NodeId = new(uint64(3))
			case "single-voter":
				n.members = n.members[:1]
			case "update":
				cc.Type = pb.ConfChangeUpdateNode.Enum()
			case "corrupt":
				cc.Context = []byte("corrupt")
			}
			if n.deferSelfRemoval(m, 1, &cc) || n.selfRemoval.apply != nil {
				t.Fatal("unrelated membership work acquired a removal barrier")
			}
		})
	}
}

func (tc *testCluster) removalLeader(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for i, ep := range tc.endpoints {
			if tc.cancels[i] == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			s, err := tc.rawClient(t, ep).Status(ctx, ep)
			cancel()
			if err == nil && s.Leader != 0 {
				if k := slices.Index(tc.ids, s.Leader); k >= 0 && tc.cancels[k] != nil {
					return k
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader")
	return -1
}

// Two old voters leave a lone survivor; four with one down leave only two
// live survivors. Neither can elect under the old configuration. Both need
// the removing leader to publish its commit before closing its sockets.
func TestRemovingLeaderSurvivors(t *testing.T) {
	for _, size := range []int{2, 4} {
		name := "two-voters"
		if size == 4 {
			name = "four-voters-one-down"
		}
		t.Run(name, func(t *testing.T) {
			tc := newTestClusterOf(t, size, nil)
			waitMembers(t, tc.client(t), size)
			// etcd's readyToRemove requires peers connected for five seconds.
			time.Sleep(6 * time.Second)
			lead := tc.removalLeader(t)
			down := -1
			if size == 4 {
				down = (lead + 1) % size
				tc.stopNode(t, down)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, removeErr := tc.rawClient(t, tc.endpoints[lead]).MemberRemove(ctx, tc.ids[lead])
			cancel()
			// Check liveness even if the removal reply was lost, so the
			// original failure diagnoses the survivors' stuck configuration.
			if removeErr != nil {
				t.Errorf("MemberRemove client lost its reply: %v", removeErr)
			}
			select {
			case err := <-tc.stopped[lead]:
				if !errors.Is(err, ErrRemoved) {
					t.Fatalf("removed leader stopped with %v", err)
				}
				tc.cancels[lead]()
				tc.cancels[lead] = nil
			case <-time.After(20 * time.Second):
				t.Fatal("removed leader did not stop")
			}
			for i := range size {
				if i == lead || i == down {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				r, err := tc.rawClient(t, tc.endpoints[i]).MemberList(ctx)
				cancel()
				if err != nil {
					t.Fatalf("survivor %d serves no linearizable MemberList within 20 s: %v", i+1, err)
				}
				if len(r.Members) != size-1 {
					t.Fatalf("survivor has %d members, want %d", len(r.Members), size-1)
				}
			}
		})
	}
}
