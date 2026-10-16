package core

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// raftChange runs a configuration change on a copy of raft's tracker for
// configuration cs and returns raft's error, on which a node would panic
// (raft's applyConfChange), and the configuration after it.
func raftChange(t *testing.T, cs *raftpb.ConfState, cc *raftpb.ConfChange) (*raftpb.ConfState, error) {
	t.Helper()
	trk := tracker.MakeProgressTracker(1, 0)
	cfg, prs, err := confchange.Restore(confchange.Changer{Tracker: trk, LastIndex: 10}, cs)
	if err != nil {
		t.Fatalf("raft does not take the configuration %v: %v", cs, err)
	}
	trk.Config, trk.Progress = cfg, prs
	cfg, prs, err = confchange.Changer{Tracker: trk, LastIndex: 10}.Simple(cc.AsV2().Changes...)
	if err != nil {
		return nil, err
	}
	trk.Config, trk.Progress = cfg, prs
	return trk.ConfState(), nil
}

// TestMembershipChangesMatchRaft is a component test of the membership
// checks: random member adds, learner adds, removals, promotions and
// updates on random clusters. Every change the checks let through must be
// one raft takes, since raft panics on any error, and the members after it
// must be raft's configuration. A change the checks refuse goes to raft as
// a change of nobody, as in etcd, which raft must take too.
func TestMembershipChangesMatchRaft(t *testing.T) {
	rng := rand.New(rand.NewPCG(148, 1))
	ops := []byte{confAdd, confAdd, confRemove, confRemove, confPromote, confUpdate}
	refused, raftRefuses := 0, 0
	for run := range 300 {
		st := NewState(2, &mvcc.Heap{})
		maxLearners := 1 + rng.IntN(2)
		for id := uint64(1); id <= uint64(1+rng.IntN(3)); id++ {
			st.members[id] = &pb.Member{ID: id, PeerURLs: []string{fmt.Sprintf("https://n%d", id)}}
		}
		next := uint64(len(st.members) + 1)
		for step := range 40 {
			op := ops[rng.IntN(len(ops))]
			m := &pb.Member{ID: 1 + rng.Uint64N(next+1), PeerURLs: []string{fmt.Sprintf("https://n%d", rng.IntN(int(next)+2))}}
			if op == confAdd && rng.IntN(2) == 0 {
				m.ID = next
			}
			if op == confAdd {
				m.IsLearner = rng.IntN(2) == 0
			}
			typ := raftpb.ConfChangeAddNode
			switch {
			case op == confAdd && m.IsLearner:
				typ = raftpb.ConfChangeAddLearnerNode
			case op == confRemove:
				typ = raftpb.ConfChangeRemoveNode
			case op == confUpdate:
				typ = raftpb.ConfChangeUpdateNode
			}
			before := st.confState()
			// What raft does with the change as asked.
			_, raw := raftChange(t, before, &raftpb.ConfChange{Type: typ.Enum(), NodeId: new(m.ID)})
			err := st.checkConfChange(op, m, maxLearners)
			if raw != nil {
				raftRefuses++
				if err == nil {
					t.Fatalf("run %d step %d: %v of %d on %v passes the checks, but raft refuses it: %v", run, step, typ, m.ID, before, raw)
				}
			}
			id := m.ID
			if err != nil {
				refused++
				id = 0
			}
			after, rerr := raftChange(t, before, &raftpb.ConfChange{Type: typ.Enum(), NodeId: new(id)})
			if rerr != nil {
				t.Fatalf("run %d step %d: raft refuses %v of %d on %v after the checks (%v): %v", run, step, typ, id, before, err, rerr)
			}
			if err == nil {
				st.applyMembers(op, m, 1)
				if op == confAdd {
					next = max(next, m.ID+1)
				}
			}
			got := st.confState()
			if !slices.Equal(got.Voters, after.Voters) || !slices.Equal(got.Learners, after.Learners) {
				t.Fatalf("run %d step %d: members %v, raft's configuration %v", run, step, got, after)
			}
		}
	}
	if refused == 0 || raftRefuses == 0 {
		t.Fatalf("%d changes refused, %d that raft refuses; both must happen", refused, raftRefuses)
	}
	t.Logf("%d changes refused by the checks, %d of them ones raft would refuse", refused, raftRefuses)
}
