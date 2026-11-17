package wl

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	pb "go.etcd.io/raft/v3/raftpb"
)

// heartbeatFrame is a peer frame of a heartbeat from node from at term.
func heartbeatFrame(from, term uint64) []byte {
	f := make([]byte, msgHdr)
	f[0] = byte(pb.MsgHeartbeat)
	binary.LittleEndian.PutUint64(f[2:], from)
	binary.LittleEndian.PutUint64(f[10:], term)
	return binary.LittleEndian.AppendUint32(f, 0) // no context
}

// A follower whose disk stalled let go of the batches its leader then
// named: every held append whose batch is past its wait goes in one pass,
// so the heartbeats behind them are stepped and it keeps its leader
// (#706). An append still within its wait keeps what came after it.
func TestHeldAppendsPastTheirWaitAllGoAtOnce(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	n.carryUntil = map[uint64]time.Time{}
	n.now = time.Now()
	old := n.now.Add(-time.Second)
	p := &peerIn{from: 2}
	n.socks = []any{p}
	for i := range uint64(5) {
		p.held = append(p.held,
			heldMsg{f: namedAppend(3, 1<<32|i), at: old},
			heldMsg{f: heartbeatFrame(2, 2+i), at: old})
	}
	p.held = append(p.held,
		heldMsg{f: []byte{frameCarry}, at: old},
		heldMsg{f: namedAppend(3, 1<<32|9), at: n.now},
		heldMsg{f: heartbeatFrame(2, 9), at: n.now})
	n.holding = true
	n.retryHeld()
	if st := n.rn.Status(); st.GetTerm() != 6 || st.Lead != 2 {
		t.Fatalf("after the retry: term %d, leader %d; want the last old heartbeat's term 6 from leader 2", st.GetTerm(), st.Lead)
	}
	if len(p.held) != 2 || !n.holding {
		t.Fatalf("%d messages still held, holding %v; want the young append and the heartbeat behind it", len(p.held), n.holding)
	}
	if !time.Now().Before(n.carryUntil[2]) {
		t.Fatal("a held carry request was not taken")
	}
}

// A batch Raft drops on a node that knows no leader never applies: its
// writes are answered etcd's "no leader", which a client takes as not
// done, not "leader changed", which may have been. On seeds 1 and 2 of
// TestFormedClusterAnswers, writes sent while the started cluster elected
// its leader got "leader changed", with no fault in the run.
func TestADroppedBatchWithNoLeaderAnswersNoLeader(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	if err := n.rn.Propose([]byte("batch")); err == nil {
		t.Fatal("a node with no leader took a proposal")
	}
	if got, want := n.droppedMsg(), rpctypes.ErrGRPCNoLeader.Error(); !strings.HasSuffix(want, got) {
		t.Fatalf("with no leader the writes are answered %q, want %q", got, want)
	}
	n.lead = 2
	if got := n.droppedMsg(); got != "etcdserver: leader changed" {
		t.Fatalf("with a leader the writes are answered %q", got)
	}
}

// peerSays puts frames, each with its length, in p's input, as its
// connection read them.
func peerSays(p *peerIn, frames ...[]byte) {
	var in []byte
	for _, f := range frames {
		in = binary.LittleEndian.AppendUint32(in, uint32(len(f)))
		in = append(in, f...)
	}
	p.pt, p.pr, p.pw = in, 0, len(in)
}

// A peer's frame whose counts and lengths do not fit it closes the
// connection: it neither panics the node nor makes it allocate what the
// counts say. Unchecked, the first took 4 Gi entries' slots and the
// second sliced past its frame.
func TestAPeerFrameThatDoesNotFitClosesTheConnection(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	n.now = time.Now()
	manyEntries := heartbeatFrame(2, 1)
	binary.LittleEndian.PutUint32(manyEntries[58:], 1<<32-1)
	longContext := heartbeatFrame(2, 1)
	binary.LittleEndian.PutUint32(longContext[msgHdr:], 1<<20)
	badType := namedAppend(3, 1)
	badType[msgHdr+16] = 0x7f
	for name, f := range map[string][]byte{
		"an append of 2^32-1 entries in 66 bytes": manyEntries,
		"a context past its frame":                longContext,
		"an entry of no type Raft has":            badType,
	} {
		p := &peerIn{from: 2}
		peerSays(p, f)
		if err := n.peerFrames(p); !errors.Is(err, errRecord) {
			t.Errorf("%s: %v, want errRecord", name, err)
		}
	}
}

// A peer's message that names another member as its sender closes the
// connection, as does any message of a member removed since it
// connected: etcd's peer handler drops both. Unchecked, a heartbeat of a
// later term naming node 3 moved this node's term and leader.
func TestAPeerSpeaksOnlyForItself(t *testing.T) {
	n, _ := testNode(t)
	n.rn = newTestRawNode(t)
	n.now = time.Now()
	n.removed = map[uint64]bool{}
	p := &peerIn{from: 2}
	peerSays(p, heartbeatFrame(3, 100))
	if err := n.peerFrames(p); !errors.Is(err, errPeerFrom) {
		t.Fatalf("a heartbeat naming node 3 on node 2's connection: %v, want errPeerFrom", err)
	}
	if st := n.rn.Status(); st.GetTerm() == 100 || st.Lead == 3 {
		t.Fatalf("term %d, leader %d after node 2 spoke for node 3", st.GetTerm(), st.Lead)
	}
	n.removed[2] = true
	peerSays(p, heartbeatFrame(2, 100))
	if err := n.peerFrames(p); !errors.Is(err, errPeerFrom) {
		t.Fatalf("a heartbeat of removed node 2: %v, want errPeerFrom", err)
	}
	if st := n.rn.Status(); st.GetTerm() == 100 {
		t.Fatalf("term %d after a heartbeat of a removed member", st.GetTerm())
	}
}
