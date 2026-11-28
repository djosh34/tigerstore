package wl

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestASurvivorOfARemovalRecoversItsCommit is #821's review: of two voters
// with no ring, the follower is removed, MemberRemove answers and the
// follower stops, and the survivor starts again cleanly. Its log holds the
// removal; it must also hold the commit that covers it, so that its replay
// applies the removal and it elects itself alone. A node rebuilds what it
// applied from the log up to the durable commit, so a commit that moved
// over a change of members is synced, never skipped (mustSyncCommit). With
// the commit skipped, the survivor replayed the two voters' members and
// waited for the removed one's vote for good.
func TestASurvivorOfARemovalRecoversItsCommit(t *testing.T) {
	tc := newTestClusterOf(t, 2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	all := tc.clientOf(t, tc.endpoints...)
	// The leader survives; the follower is removed.
	lead := -1
	for deadline := time.Now().Add(30 * time.Second); lead < 0; time.Sleep(50 * time.Millisecond) {
		for i, ep := range tc.endpoints {
			if s, err := all.Status(ctx, ep); err == nil && s.Leader == s.Header.MemberId {
				lead = i
			}
		}
		if lead < 0 && time.Now().After(deadline) {
			t.Fatal("no leader in 30 s")
		}
	}
	gone := 1 - lead
	c := tc.clientOf(t, tc.endpoints[lead])
	if _, err := c.Put(ctx, "/before", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MemberRemove(ctx, tc.ids[gone]); err != nil {
		t.Fatalf("remove the follower: %v", err)
	}
	select {
	case err := <-tc.stopped[gone]:
		if !errors.Is(err, ErrRemoved) {
			t.Fatalf("the removed node stopped with %v", err)
		}
		tc.cancels[gone]()
		tc.cancels[gone] = nil
	case <-ctx.Done():
		t.Fatal("the removed node did not stop")
	}
	// The survivor starts again, cleanly, and leads alone.
	tc.stopNode(t, lead)
	tc.startNode(t, lead)
	c = tc.rawClient(t, tc.endpoints[lead])
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := c.Put(pctx, "/after", "x")
		pcancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the survivor of the removal took no put in 30 s after its restart: %v", err)
		}
	}
	r, err := c.MemberList(ctx, clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Members) != 1 || r.Members[0].ID != tc.ids[lead] {
		t.Fatalf("after the restart the members are %v, want the survivor alone", r.Members)
	}
}

// TestAProtectedCommitIsNeitherSkippedNorHeld checks commitMove, which
// decides both whether a commit's sync is skipped (owing no response) and
// whether it is held (owing some): a commit that moves over a change of
// members, an auth op, or an entry the node recovered at its start is
// neither, so its sync starts at once, as every append's did before
// (#821's ruling). The entry's apply waits for that sync
// (waitsForDurable, #823).
func TestAProtectedCommitIsNeitherSkippedNorHeld(t *testing.T) {
	durable := &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(1)), Commit: new(uint64(5))}
	moveTo := func(commit uint64) (*pb.Message, *pb.HardState) {
		st := &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(1)), Commit: new(commit)}
		return &pb.Message{Type: pb.MsgStorageAppend.Enum(), Term: st.Term, Vote: st.Vote, Commit: st.Commit}, st
	}
	auth := make([]byte, batchHdr, batchHdr+opHdr+1)
	copy(auth[17:], magic)
	auth = append(auth, kAuthOp, 1, 0, 0, 0, 'x')
	put := make([]byte, batchHdr, batchHdr+opHdr+1)
	copy(put[17:], magic)
	put = append(put, kPut, 1, 0, 0, 0, 'x')
	for _, c := range []struct {
		name      string
		ents      []*pb.Entry
		recovered uint64
		move      bool
	}{
		{"over a put", []*pb.Entry{{Index: new(uint64(6)), Type: pb.EntryNormal.Enum(), Data: put}}, 0, true},
		{"over a change of members", []*pb.Entry{{Index: new(uint64(6)), Type: pb.EntryConfChange.Enum()}}, 0, false},
		{"over a change of members, v2", []*pb.Entry{{Index: new(uint64(7)), Type: pb.EntryConfChangeV2.Enum()}}, 0, false},
		{"over an auth op", []*pb.Entry{{Index: new(uint64(6)), Type: pb.EntryNormal.Enum(), Data: auth}}, 0, false},
		{"over an entry recovered at the start", nil, 6, false},
		{"over a change of members past it", []*pb.Entry{{Index: new(uint64(9)), Type: pb.EntryConfChange.Enum()}}, 0, true},
	} {
		n := &node{wal: &wal{durableHS: durable}, recoveredLast: c.recovered}
		n.noteMustSync(c.ents)
		m, st := moveTo(7)
		if got := n.commitMove(m, st); got != c.move {
			t.Errorf("a commit from 5 to 7 %s: commitMove %v, want %v", c.name, got, c.move)
		}
	}
}
