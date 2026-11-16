package wl

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// sentShell is a shell that records the followers it is asked to send a
// snapshot to.
type sentShell struct {
	shell
	sent []uint64
}

func (s *sentShell) sendSnapshot(m *pb.Message, f file) {
	s.sent = append(s.sent, m.GetTo())
	_ = f.Close()
}

func (*sentShell) logf(string, ...any) {}

// A leader sends a snapshot over a connection of its own, whatever its peer
// connection to the follower is. One dropped where that connection was
// down was never reported to Raft, which then waited for it for good and
// never sent the follower anything again: on seed 24040 of TestSeeds node 2
// stayed 1,300 entries behind to the end.
func TestASnapshotIsSentWhateverThePeerConnection(t *testing.T) {
	for name, p := range map[string]*peerOut{"no connection": nil, "a dead one": {dead: true}, "a live one": {}} {
		t.Run(name, func(t *testing.T) {
			n, _ := testNode(t)
			n.ckptDir = t.TempDir()
			if err := os.WriteFile(filepath.Join(n.ckptDir, "ckpt"), []byte("checkpoint"), 0o600); err != nil {
				t.Fatal(err)
			}
			n.peersOut = map[uint64]*peerOut{}
			if p != nil {
				n.peersOut[2] = p
			}
			sh := &sentShell{}
			n.shell = sh
			snap := &pb.Snapshot{Data: []byte("ckpt"), Metadata: &pb.SnapshotMetadata{Index: new(uint64(9)), Term: new(uint64(1))}}
			n.sendPeer(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Snapshot: snap})
			if !slices.Equal(sh.sent, []uint64{2}) {
				t.Fatalf("snapshots sent to %v, want one to 2", sh.sent)
			}
		})
	}
}

// A leader may send a follower the same snapshot again, as node 2 sent
// node 1 the one at index 405 four times on seed 914212814 of
// TestMembershipFaults. Each reception had the same file, so the second
// removed the file it had just received as the one before, and the
// install that followed found no file and stopped the node (#713).
func TestASnapshotReceivedAgainKeepsItsFile(t *testing.T) {
	n, _ := testNode(t)
	n.ckptDir = t.TempDir()
	l := &loop{n: n}
	first := l.recvPath(2, 405)
	if err := os.WriteFile(first, []byte("checkpoint"), 0o600); err != nil {
		t.Fatal(err)
	}
	n.keepRecv(first)
	again := l.recvPath(2, 405)
	if err := os.WriteFile(again, []byte("checkpoint"), 0o600); err != nil {
		t.Fatal(err)
	}
	n.keepRecv(again)
	if _, err := os.Stat(again); err != nil {
		t.Fatalf("the snapshot received again lost its file: %v", err)
	}
	if _, err := os.Stat(first); first != again && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the snapshot received before kept its file: %v", err)
	}
}
