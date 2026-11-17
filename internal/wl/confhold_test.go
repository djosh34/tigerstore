package wl

import (
	"encoding/binary"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// confEntry is a membership change at index.
func confEntry(index uint64) *pb.Entry {
	return &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryConfChange.Enum()}
}

// A leader that lost its lead, whose change at 100 a new leader then
// replaced, and that leads again at 97: it holds changes back only for
// its own first entry. With the old change's index kept, it waited for an
// index 100 the log of an idle cluster never reached.
func TestANewTermsLeaderForgetsItsReplacedChange(t *testing.T) {
	n := leaseNode(t)
	n.lead = n.id
	n.confAppended([]*pb.Entry{confEntry(100)})
	n.lead = 2
	n.leaderChanged()
	n.lead = n.id
	n.leaderChanged()
	n.confAppended([]*pb.Entry{{Index: new(uint64(97)), Term: new(uint64(3))}})
	n.applied = 97
	if n.confBlocked() {
		t.Fatalf("the new term's leader holds changes back for index %d, which its log no longer has", n.confHold.lastConf)
	}
}

// A change Raft hands out while the stage has no room is noted at once:
// its append waits, and the next change is held until it applies. Noted
// only once the append went into the stage, the next change went to Raft,
// which dropped it.
func TestAChangeThatWaitsForTheStageHoldsTheNext(t *testing.T) {
	n, _ := testNode(t)
	n.lead = n.id
	n.wal.waiting = []*pb.Message{{}} // an append waits already
	if err := n.storageAppend(&pb.Message{Type: pb.MsgStorageAppend.Enum(), Entries: []*pb.Entry{confEntry(10)}}); err != nil {
		t.Fatal(err)
	}
	if n.confHold.lastConf != 10 {
		t.Fatalf("the change at 10 that waits for the stage is not noted: lastConf %d", n.confHold.lastConf)
	}
	n.confHold.leadFirst, n.applied = 5, 9
	if !n.confBlocked() {
		t.Fatal("a change goes to Raft while the one at 10 is not applied")
	}
}

// A follower's proposal of a change held behind a named append is held
// again, when it is retried, while the leader's Raft would drop it.
// Stepped as any message, it went past the hold.
func TestARetriedProposalOfAChangeIsHeld(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	n.lead = n.id // its first entry as the leader not appended yet
	n.now = time.Now()
	f := make([]byte, msgHdr)
	f[0] = byte(pb.MsgProp)
	binary.LittleEndian.PutUint64(f[2:], 2)
	binary.LittleEndian.PutUint32(f[58:], 1)
	var eh [entryHdr]byte
	eh[16] = byte(pb.EntryConfChange)
	f = append(f, eh[:]...)
	f = binary.LittleEndian.AppendUint32(f, 0)
	p := &peerIn{from: 2}
	n.socks = []any{p}
	p.held = []heldMsg{{f: f, at: n.now}}
	n.retryHeld()
	if len(n.confHold.props) != 1 {
		t.Fatalf("%d proposals held after the retry, want the change held", len(n.confHold.props))
	}
}

// A cluster of one voter may add its second, as etcd's
// isReadyToAddVotingMember allows, so that a restored cluster grows.
func TestASingleVoterAddsItsSecond(t *testing.T) {
	n, _ := testNode(t)
	n.members = []member{{id: 1, name: "n1"}}
	if err := n.readyForVoters(1); err != nil {
		t.Fatalf("a cluster of one voter may not add its second: %v", err)
	}
	n.members = append(n.members, member{id: 2, name: "n2"})
	if err := n.readyForVoters(1); err == nil {
		t.Fatal("two voters, one of them not reached, may add a third")
	}
}
