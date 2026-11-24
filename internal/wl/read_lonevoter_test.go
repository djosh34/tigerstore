package wl

import (
	"encoding/binary"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// loneVoter is a node of one voter, and a learner 2, restarted with
// entries 4 and 5 in its log past the commit 3 its hard state kept:
// entries it committed, applied and answered before it crashed. It is
// elected in term 8, its first entry of the term, 6, not stored yet.
type loneVoter struct {
	t  *testing.T
	ms *raft.MemoryStorage
	n  *node
	// held are the appends not stored yet; sent the messages to the
	// learner.
	held, sent []*pb.Message
}

func newLoneVoter(t *testing.T) *loneVoter {
	t.Helper()
	ms := raft.NewMemoryStorage()
	if err := ms.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(1)), Term: new(uint64(7)), ConfState: &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}},
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
	// The node's own storage holds what Raft's does: the commit's term,
	// which tells when Raft's answer could be short (readIndexUnsafe),
	// comes from it.
	st := newLogStorage(nil)
	st.snapIndex, st.snapTerm = 1, 7
	for _, e := range ents {
		st.ents = append(st.ents, storedEnt{ent: e, term: e.GetTerm(), typ: e.GetType()})
	}
	n := &node{id: 1, rn: rn, applied: 3, storage: st, members: []member{{id: 1}, {id: 2, learner: true}}}
	lv := &loneVoter{t: t, ms: ms, n: n}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	lv.ready(true)
	if h := lv.n.confHold; !lv.n.leading() || h.leadFirst != 6 || h.leadTerm != 8 {
		t.Fatalf("lead %d, first entry %d of term %d; want node 1 leading, its term 8's first entry 6", lv.n.lead, h.leadFirst, h.leadTerm)
	}
	return lv
}

// store keeps what an append hands the storage, and steps its responses:
// the vote, then the self-ack of the term's first entry.
func (lv *loneVoter) store(m *pb.Message) {
	if m.Term != nil {
		if err := lv.ms.SetHardState(&pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}); err != nil {
			lv.t.Fatal(err)
		}
	}
	if err := lv.ms.Append(m.GetEntries()); err != nil {
		lv.t.Fatal(err)
	}
	for _, e := range m.GetEntries() {
		lv.n.storage.ents = append(lv.n.storage.ents, storedEnt{ent: e, term: e.GetTerm(), typ: e.GetType()})
	}
	for _, r := range m.GetResponses() {
		if err := lv.n.rn.Step(r); err != nil {
			lv.t.Fatal(err)
		}
	}
}

// ready takes the Readys as the loop does, up to the appends: the
// leader's first entry of the term is noted (confAppended), and the
// appends with entries are held unstored while hold is set.
func (lv *loneVoter) ready(hold bool) {
	n := lv.n
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		if rd.SoftState != nil {
			n.lead = rd.Lead
		}
		for _, rs := range rd.ReadStates {
			n.readState(rs.RequestCtx, rs.Index)
		}
		for _, m := range rd.Messages {
			switch m.GetTo() {
			case raft.LocalAppendThread:
				n.confAppended(m.GetEntries())
				if hold && len(m.GetEntries()) > 0 {
					lv.held = append(lv.held, m)
					continue
				}
				lv.store(m)
			case 2:
				lv.sent = append(lv.sent, m)
			}
		}
	}
}

// commitFirst stores the term's first entry, which commits 4 to 6, and
// has the node apply them.
func (lv *loneVoter) commitFirst() {
	for _, m := range lv.held {
		lv.store(m)
	}
	lv.held = nil
	lv.ready(false)
	if c := lv.n.rn.BasicStatus().GetCommit(); c != 6 {
		lv.t.Fatalf("commit %d after the first entry was stored, want 6", c)
	}
	lv.n.applied = 6
}

// A lone voter serves no linearizable read before its new term's first
// entry commits: Raft would answer its ReadIndex at once with the commit
// it knows (raft's IsSingleton), here 3, while 4 and 5 hold writes
// already answered. Served at 3, a read missed them (TestOneVoterMembership,
// seed 4 under -race, main's ci run 45424099030). The node asks Raft
// nothing until then, and the read is served at 6.
func TestALoneVoterServesNoReadBeforeItsTermsFirstEntry(t *testing.T) {
	lv := newLoneVoter(t)
	n := lv.n
	served := false
	n.reads = []pendingRead{{done: func() { served = true }}}
	n.askReads(0)
	lv.ready(true)
	n.serveReads()
	if served {
		t.Fatalf("a read served at applied %d, before the term's first entry %d committed", n.applied, n.confHold.leadFirst)
	}
	if len(n.readBatches) != 1 || n.readBatches[0].known || !n.readBatches[0].reask {
		t.Fatalf("read batches %+v: want the read's ask held, no index known", n.readBatches)
	}
	lv.commitFirst()
	n.serveReads()
	lv.ready(false)
	if len(n.readBatches) != 1 || !n.readBatches[0].known || n.readBatches[0].index < 6 {
		t.Fatalf("read batches %+v: want the read's index known, at least 6", n.readBatches)
	}
	n.serveReads()
	if !served {
		t.Fatal("the read was not served once the term's first entry applied")
	}
}

// A lone voter answers no learner's ReadIndex short either: Raft answers a
// forwarded one at once too, and the learner, which applies what the
// leader sends, served its linearizable MemberList at 3. The request comes
// between the node's vote, which made Raft the leader, and the Ready that
// tells the loop: Raft's own state decides (readIndexUnsafe). It is
// dropped, and the learner's next ask is answered at 6.
func TestALoneVoterAnswersNoLearnersReadIndexShort(t *testing.T) {
	lv := newLoneVoter(t)
	n := lv.n
	ask := func() {
		ctx := binary.BigEndian.AppendUint64(nil, 77)
		// A learner's Raft sends it with no term.
		n.step(&pb.Message{Type: pb.MsgReadIndex.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Data: ctx}}})
	}
	// As the loop's view lags Raft's within a turn.
	n.lead = 0
	ask()
	n.lead = 1
	lv.ready(true)
	for _, m := range lv.sent {
		if m.GetType() == pb.MsgReadIndexResp {
			t.Fatalf("the learner's ReadIndex answered with %d, before the term's first entry %d committed", m.GetIndex(), n.confHold.leadFirst)
		}
	}
	lv.commitFirst()
	ask()
	lv.ready(false)
	var got []uint64
	for _, m := range lv.sent {
		if m.GetType() == pb.MsgReadIndexResp {
			got = append(got, m.GetIndex())
		}
	}
	if len(got) != 1 || got[0] < 6 {
		t.Fatalf("the learner's ReadIndex answered with %v, want once, at 6 or later", got)
	}
}

// A lone voter decides no lease's expiry before it applied its new term's
// first entry: 4 and 5 may revoke a lease it applied up to 3, or grant one
// again under the same ID, and a revoke decided on 3 went into the log
// after them. Once the entry is applied, the expired lease is revoked.
func TestALoneVoterExpiresNoLeaseBeforeItsTermsFirstEntry(t *testing.T) {
	lv := newLoneVoter(t)
	n := lv.n
	// The ring and the forward table a revoke's batch would take.
	r, err := newRing(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	n.ring, n.fwd = r, make([]fwdBatch, fwdSlots)
	n.now = time.Unix(1000, 0)
	n.leases.deadline = map[int64]time.Time{9: n.now.Add(-time.Second)}
	n.leases.revoking = map[int64]time.Time{}
	n.leases.confirmed = n.now
	n.expireLeases()
	if _, ok := n.leases.revoking[9]; ok {
		t.Fatalf("lease 9 revoked at applied %d, before the term's first entry %d applied", n.applied, n.confHold.leadFirst)
	}
	lv.commitFirst()
	n.leases.confirmed = n.now
	n.expireLeases()
	if _, ok := n.leases.revoking[9]; !ok {
		t.Fatal("lease 9 not revoked once the term's first entry applied")
	}
}
