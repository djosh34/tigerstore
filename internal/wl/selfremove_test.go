package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
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
	n.readState(ctx[:], 10)
	if n.selfRemoval.ready {
		t.Fatal("a ReadIndex alone permitted exit without old-voter commit knowledge")
	}
	n.removalNotice(2)
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

// removingRaftLeader commits the actual removal with voters 3 (and 4),
// while voter 2 has acknowledged no appends. The synchronous storage adapter
// keeps the schedule explicit; the production barrier runs unchanged.
func removingRaftLeader(t *testing.T, n *node, voters int) {
	t.Helper()
	ids := []uint64{1, 2, 3}
	if voters == 4 {
		ids = append(ids, 4)
	}
	ms := raft.NewMemoryStorage()
	if err := ms.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(8)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: ids}}}); err != nil {
		t.Fatal(err)
	}
	cfg := raftConfig(n.id, ms, sizeFor(0), voters)
	cfg.AsyncStorageWrites, cfg.PreVote = false, false
	rn, err := raft.NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.rn, n.lead, n.applied = rn, n.id, 9
	take := func() raft.Ready {
		rd := rn.Ready()
		if err := ms.Append(rd.Entries); err != nil {
			t.Fatal(err)
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := ms.SetHardState(rd.HardState); err != nil {
				t.Fatal(err)
			}
		}
		rn.Advance(rd)
		return rd
	}
	step := func(id uint64, kind pb.MessageType, index uint64) {
		if err := rn.Step(&pb.Message{From: new(id), To: new(n.id), Term: new(rn.BasicStatus().GetTerm()), Type: kind.Enum(), Index: new(index)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	_ = take()
	for _, id := range ids[2:] {
		step(id, pb.MsgVoteResp, 0)
	}
	_ = take() // persist the leader's no-op at 9
	for _, id := range ids[2:] {
		step(id, pb.MsgAppResp, 9)
	}
	_ = take()
	cc := &pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(n.id), Context: ccContext(0, 0, &member{id: n.id})}
	if err := rn.ProposeConfChange(cc); err != nil {
		t.Fatal(err)
	}
	_ = take() // persist removal at 10; withhold all commit publications
	for _, id := range ids[2:] {
		step(id, pb.MsgAppResp, 10)
	}
	rd := take()
	if rn.BasicStatus().GetCommit() != 10 || len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].GetType() != pb.EntryConfChange {
		t.Fatal("removal was not committed under the old configuration")
	}
	if err := n.storageApply(&pb.Message{Entries: rd.CommittedEntries}); err != nil {
		t.Fatal(err)
	}
	rd = take() // ReadIndex's heartbeat round
	lagging := false
	for _, m := range rd.Messages {
		if m.GetType() != pb.MsgHeartbeat {
			continue
		}
		if m.GetTo() == 2 {
			lagging = m.GetCommit() < 10
		}
		// A quorum answers: voter 2, plus voter 3 in the four-voter case.
		if m.GetTo() == 2 || voters == 4 && m.GetTo() == 3 {
			if err := rn.Step(&pb.Message{From: m.To, To: new(n.id), Term: m.Term, Type: pb.MsgHeartbeatResp.Enum(), Context: m.Context}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !lagging {
		t.Fatal("test did not include a heartbeat below the removal commit")
	}
	rd = take()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Index < 10 {
		t.Fatal("lagging voter did not participate in a successful ReadIndex")
	}
	for _, rs := range rd.ReadStates {
		n.readState(rs.RequestCtx, rs.Index)
	}
}

// A lagging voter can answer a ReadIndex heartbeat without knowing the
// removal's commit (Raft caps the heartbeat at that voter's match index).
// Only an old quorum's explicit application notices establish commit knowledge.
func TestSelfRemovalOldQuorumWithLaggingVoter(t *testing.T) {
	for _, voters := range []int{3, 4} {
		t.Run(strconv.Itoa(voters), func(t *testing.T) {
			n := gateNode(t)
			n.st, _ = newTestStore(t)
			t.Cleanup(func() { _ = n.st.close() })
			if voters == 4 {
				n.members = append(n.members, member{id: 4})
			}
			removingRaftLeader(t, n, voters)
			if n.selfRemoval.ready {
				t.Fatal("ReadIndex counted a lagging respondent as commit knowledge")
			}
			// Voter 2 lags. Even with a successful read, fewer than an old
			// quorum's notices must leave the leader's removal unapplied.
			if voters == 4 {
				n.removalNotice(3)
				n.removalNotice(3) // distinct voters, not distinct lanes
			}
			if err := n.resumeSelfRemoval(); err != nil || n.removed[n.id] || n.selfRemoval.ready {
				t.Fatalf("less than an old quorum permitted removal: %v", err)
			}
			// The lagging voter catches up and applies the removal. In
			// three voters this is 2/3; in four it is 3/4, including self.
			n.removalNotice(2)
			if err := n.resumeSelfRemoval(); err != nil || !n.removed[n.id] || n.selfRemoval.drainBy.IsZero() {
				t.Fatalf("old quorum did not release removal to drain: %v", err)
			}
		})
	}
}

func TestRemovalNoticeBeforeOwnApply(t *testing.T) {
	for _, leader := range []bool{true, false} {
		name := "follower"
		if leader {
			name = "leader"
		}
		t.Run(name, func(t *testing.T) {
			n := gateNode(t)
			n.rn, n.lead = newTestRawNode(t), uint64(2)
			if leader {
				n.lead = n.id
			}
			n.removalNotice(2)
			if leader {
				if n.stopErr != nil || n.selfRemoval.ready {
					t.Fatal("transport notice stopped a leader before its own removal apply")
				}
			} else if !errors.Is(n.stopErr, ErrRemoved) {
				t.Fatal("removed follower did not stop promptly")
			}
		})
	}
}

func TestSelfRemovalNoticeOrdering(t *testing.T) {
	for _, early := range []bool{true, false} {
		name := "after-local-apply"
		if early {
			name = "before-local-apply"
		}
		t.Run(name, func(t *testing.T) {
			n := gateNode(t)
			n.st, _ = newTestStore(t)
			t.Cleanup(func() { _ = n.st.close() })
			n.rn, n.lead = newTestRawNode(t), n.id
			if early {
				n.removalNotice(2)
				if n.stopErr != nil || n.selfRemoval.ready || n.removed[n.id] {
					t.Fatal("early notice stopped the leader before local apply")
				}
			}
			if err := n.storageApply(removalApply(t, n.id)); err != nil {
				t.Fatal(err)
			}
			if !early {
				if n.selfRemoval.ready {
					t.Fatal("local apply alone established an old quorum")
				}
				n.removalNotice(2)
			}
			// In the early ordering voter 2 cannot reconnect. No repeat
			// notice is available: its retained evidence must suffice.
			if err := n.resumeSelfRemoval(); err != nil || !n.removed[n.id] || n.selfRemoval.drainBy.IsZero() {
				t.Fatalf("notice plus self did not release the three-voter barrier: %v", err)
			}
		})
	}
}

func TestSelfRemovalEarlyNoticesNeedDistinctOldVoters(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	n.members = append(n.members, member{id: 4}, member{id: 5, learner: true}, member{id: 6})
	for _, id := range []uint64{2, 2, 5, 6, 99} {
		n.removalNotice(id)
	}
	// Voter 6 left in a preceding configuration change. Its early notice
	// is not evidence from the old configuration at our own removal.
	n.members = n.members[:len(n.members)-1]
	if err := n.storageApply(removalApply(t, n.id)); err != nil {
		t.Fatal(err)
	}
	if err := n.resumeSelfRemoval(); err != nil || n.removed[n.id] || n.selfRemoval.ready {
		t.Fatalf("duplicate, learner, former voter or outsider made an early quorum: %v", err)
	}
	n.removalNotice(3)
	if err := n.resumeSelfRemoval(); err != nil || !n.removed[n.id] {
		t.Fatalf("third old voter did not release removal: %v", err)
	}
}

func TestSelfRemovalKeepsParsedChange(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	m := removalApply(t, n.id)
	if err := n.storageApply(m); err != nil {
		t.Fatal(err)
	}
	// Model the released entry buffer being reused, including its context.
	for i := range m.Entries[1].Data {
		m.Entries[1].Data[i] = 0xff
	}
	n.removalNotice(2)
	if err := n.resumeSelfRemoval(); err != nil || !n.removed[n.id] {
		t.Fatalf("removal re-read the released entry instead of its parsed change: %v", err)
	}
}

func TestSelfRemovalDrainsFlowControlledMemberReply(t *testing.T) {
	n := gateNode(t)
	n.st, _ = newTestStore(t)
	t.Cleanup(func() { _ = n.st.close() })
	n.rn, n.lead = newTestRawNode(t), n.id
	c, s := flowClient(t)
	c.node, c.idx, c.gen, s.kind = n, 0, 1, kMemberRemove
	n.socks, n.gens = []any{c}, []uint16{1}
	c.flow.conn, s.win = 0, 0
	n.memberReqs = map[uint64]memberReq{1: {h: mkHandle(c, s.id), kind: kMemberRemove}}
	m := removalApply(t, n.id)
	cc := &pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(n.id), Context: ccContext(n.id, 1, &member{id: n.id})}
	var err error
	m.Entries[1].Data, err = proto.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.storageApply(m); err != nil {
		t.Fatal(err)
	}
	n.removalNotice(2)
	if err := n.resumeSelfRemoval(); err != nil {
		t.Fatal(err)
	}
	if len(c.flow.held) == 0 || len(n.memberReqs) != 0 {
		t.Fatal("MemberRemove reply was not queued behind the closed window")
	}
	// Model flush completing all writable socket output. Only HTTP/2 flow
	// control remains, which must count independently of sock.wantOut.
	c.seal()
	c.out, c.ow, c.wantOut, n.dirty = c.out[:0], 0, false, nil
	if n.selfRemovalDrained() {
		t.Fatal("closed HTTP/2 window lost the MemberRemove reply")
	}
	c.windowUpdate(s.id, 1<<20)
	if n.selfRemovalDrained() {
		t.Fatal("stream window bypassed the still-closed connection window")
	}
	c.windowUpdate(0, 1<<20)
	if len(c.flow.held) != 0 {
		t.Fatal("reply stayed held after both windows opened")
	}
	if n.selfRemovalDrained() {
		t.Fatal("reply bytes queued on the socket were not counted")
	}
	c.seal()
	c.out, c.ow, c.wantOut, n.dirty = c.out[:0], 0, false, nil
	if !n.selfRemovalDrained() {
		t.Fatal("fully flushed reply did not finish the drain")
	}
}

func TestSelfRemovalDrainDoesNotCheckpoint(t *testing.T) {
	n, _ := ckptNode(t)
	n.selfRemoval.drainBy = time.Now().Add(10 * tickEvery)
	// No storage is accessible during this drain: even a periodic start
	// must return before inspecting it, let alone beginning a checkpoint.
	n.storage = nil
	if err := n.maybeCkpt(); err != nil || n.ckpt != nil {
		t.Fatalf("drain started a checkpoint: %v", err)
	}
	// A checkpoint already in progress must not be stepped either.
	done := make(chan *ckptChunk, 1)
	done <- &ckptChunk{err: errors.New("checkpoint stepped during drain")}
	n.ckpt = &ckptRun{sent: 1, w: &ckptWriter{out: done}}
	if err := n.ckptStep(false); err != nil || n.ckpt.sent != 1 || len(done) != 1 {
		t.Fatalf("drain advanced a checkpoint: %v", err)
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
			// Observe on the loop thread, at the actual fenced apply, not
			// from an earlier Status response that can race a new election.
			appliedAsLeader := make(chan bool, 1)
			observe := func(n *node) {
				if n.id == tc.ids[lead] {
					appliedAsLeader <- n.leading()
				}
			}
			testSelfRemovalApply.Store(&observe)
			t.Cleanup(func() { testSelfRemovalApply.Store(nil) })
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
			select {
			case leading := <-appliedAsLeader:
				if !leading {
					t.Fatal("removed node was not still leader at its removal's apply")
				}
			default:
				t.Fatal("removed node never applied its own fenced removal")
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
