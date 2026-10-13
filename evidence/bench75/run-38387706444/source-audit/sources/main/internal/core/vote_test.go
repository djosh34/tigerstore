package core

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

// TestVoteRequestsWaitForTheTerm makes node 1 of three a candidate, with
// node 2 granting every pre-vote: no vote request may leave before an
// append with its term is durable. Otherwise the candidate could win,
// lose the term in a failed write, and win the same term again with a
// different log.
func TestVoteRequestsWaitForTheTerm(t *testing.T) {
	n, err := New(DefaultConfig(1, []uint64{1, 2, 3}), &wal.Recovered{})
	if err != nil {
		t.Fatal(err)
	}
	var durableTerm uint64
	votes := 0
	for range 200 {
		n.Tick()
		eff, err := n.Process()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range eff.Send {
			if m.GetType() == raftpb.MessageType_MsgVote {
				if m.GetTerm() > durableTerm {
					t.Fatalf("a vote request for term %d left with term %d durable", m.GetTerm(), durableTerm)
				}
				votes++
			}
			if m.GetType() == raftpb.MessageType_MsgPreVote && m.GetTo() == 2 {
				resp := &raftpb.Message{Type: raftpb.MessageType_MsgPreVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(m.GetTerm())}
				if err := n.Step(resp); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Persist after looking at what went out, as a slow disk would.
		for _, j := range eff.Persist {
			if j.HardState != nil {
				durableTerm = max(durableTerm, j.HardState.Term)
			}
			n.Persisted(j)
		}
		if votes > 0 {
			return
		}
	}
	t.Fatal("node 1 never asked for votes")
}
