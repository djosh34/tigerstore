package wl

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A candidate asks for votes only once its new term and its vote for
// itself are durable: the requests go with the responses of the append
// that writes its hard state, never before it (TestWLFaultSeeds seed
// 19159, where a leader that lost its term led the same term again).
func TestAVoteRequestWaitsForTheCampaignsTermToBeDurable(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	if err := n.rn.Step(&pb.Message{Type: pb.MsgTimeoutNow.Enum(), From: new(uint64(2)), To: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	if err := n.ready(); err != nil {
		t.Fatal(err)
	}
	if n.stats.peerMsgs != 0 {
		t.Fatalf("%d messages went to the peers before the campaign's term was durable", n.stats.peerMsgs)
	}
	votes := 0
	for _, m := range n.wal.waiting {
		if m.GetVote() != 1 || m.GetTerm() != 1 {
			t.Fatalf("an append holds term %d and vote %d, want the campaign's term 1 and vote 1", m.GetTerm(), m.GetVote())
		}
		for _, r := range m.GetResponses() {
			if r.GetType() == pb.MsgVote {
				votes++
			}
		}
	}
	if votes != 2 {
		t.Fatalf("%d vote requests wait for the append of the hard state, want 2", votes)
	}
}
