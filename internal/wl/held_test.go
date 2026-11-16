package wl

import (
	"encoding/binary"
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
