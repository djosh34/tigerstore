package wl

import (
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// A lone voter restarted with entries in its log past the commit its hard
// state kept, entries it committed, applied and answered before it
// crashed, serves no linearizable read before it applied its new term's
// first entry. Raft answers its ReadIndex at once with the commit it knows
// (raft's IsSingleton), here 3, while 4 and 5 hold writes already
// answered: served at 3, a read missed them (TestOneVoterMembership, seed
// 4 under -race, main's ci run 45424099030).
func TestALoneVoterServesNoReadBeforeItsTermsFirstEntry(t *testing.T) {
	ms := raft.NewMemoryStorage()
	if err := ms.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(1)), Term: new(uint64(7)), ConfState: &pb.ConfState{Voters: []uint64{1}},
	}}); err != nil {
		t.Fatal(err)
	}
	var ents []*pb.Entry
	for i := uint64(2); i <= 5; i++ {
		ents = append(ents, &pb.Entry{Index: new(i), Term: new(uint64(7))})
	}
	if err := ms.Append(ents); err != nil {
		t.Fatal(err)
	}
	if err := ms.SetHardState(&pb.HardState{Term: new(uint64(7)), Vote: new(uint64(1)), Commit: new(uint64(3))}); err != nil {
		t.Fatal(err)
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: ms, Applied: 3,
		AsyncStorageWrites: true, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &node{id: 1, rn: rn, applied: 3}
	// stored keeps what an append hands the storage, and steps its
	// responses: the vote, then the self-ack of the term's first entry.
	var held []*pb.Message
	stored := func(m *pb.Message) {
		if m.Term != nil {
			if err := ms.SetHardState(&pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}); err != nil {
				t.Fatal(err)
			}
		}
		if err := ms.Append(m.GetEntries()); err != nil {
			t.Fatal(err)
		}
		for _, r := range m.GetResponses() {
			if err := rn.Step(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	// ready takes a Ready as the loop does, up to the appends: the leader's
	// first entry of the term is noted (confAppended) and held unstored
	// while hold is set.
	ready := func(hold bool) {
		for rn.HasReady() {
			rd := rn.Ready()
			if rd.SoftState != nil {
				n.lead = rd.Lead
			}
			for _, rs := range rd.ReadStates {
				n.readState(rs.RequestCtx, rs.Index)
			}
			for _, m := range rd.Messages {
				if m.GetTo() != raft.LocalAppendThread {
					continue
				}
				n.confAppended(m.GetEntries())
				if hold && len(m.GetEntries()) > 0 {
					held = append(held, m)
					continue
				}
				stored(m)
			}
		}
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	ready(true)
	if !n.leading() || n.confHold.leadFirst != 6 || n.confHold.leadTerm != 8 {
		t.Fatalf("lead %d, first entry %d of term %d; want node 1 leading, its term 8's first entry 6", n.lead, n.confHold.leadFirst, n.confHold.leadTerm)
	}
	served := false
	n.reads = []pendingRead{{done: func() { served = true }}}
	n.askReads(0)
	ready(true)
	if len(n.readBatches) != 1 || !n.readBatches[0].known {
		t.Fatalf("read batches %v: want the read's index known", n.readBatches)
	}
	idx := n.readBatches[0].index
	if idx != 3 {
		t.Logf("Raft answered the lone voter's ReadIndex with %d, no longer the stale 3 this test was written for", idx)
	}
	n.serveReads()
	if served {
		t.Fatalf("a read served at applied %d with read index %d, before the term's first entry %d applied", n.applied, idx, n.confHold.leadFirst)
	}
	// The term's first entry is stored, commits with 4 and 5, and is
	// applied: the read goes.
	for _, m := range held {
		stored(m)
	}
	ready(false)
	if c := rn.BasicStatus().GetCommit(); c != 6 {
		t.Fatalf("commit %d after the first entry was stored, want 6", c)
	}
	n.applied = 6
	n.serveReads()
	if !served {
		t.Fatal("the read was not served once the term's first entry applied")
	}
}
