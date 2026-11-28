package wl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// installClientShell runs the real copy goroutine and accepts the install's
// membership notification without starting network connections.
type installClientShell struct {
	shell
	l *loop
}

func (*installClientShell) wakeLoop()           {}
func (*installClientShell) logf(string, ...any) {}
func (*installClientShell) membersChanged()     {}
func (s *installClientShell) prepareClientSnap(h handle, src file) {
	s.l.prepareClientSnap(h, src)
}

// installWhileClientWaits starts a client checkpoint at index 4, then
// installs index 9 before it finishes. The incoming checkpoint is made
// self-contained, as a peer's real copy would be.
func installWhileClientWaits(t *testing.T, gone bool, beforeInstall func(*stream)) (*node, *client, *stream, *loop) {
	t.Helper()
	n, received := installFixture(t)
	t.Cleanup(func() { _ = n.st.close() })
	incoming, err := n.sys.openFile(string(received.GetData()), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := selfContained(t.Context(), incoming, n.st.log.(durableLog), &b); err != nil {
		_ = incoming.Close()
		t.Fatal(err)
	}
	_ = incoming.Close()
	if err := os.WriteFile(string(received.GetData()), b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	n.storage.applySnapshot(n.storage.snap.GetMetadata(), n.ckptName)
	entry := testEntry(4, 1, "")
	if err := n.wal.write([]*pb.Entry{entry}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, n.wal)
	if err := n.storage.append([]*pb.Entry{entry}, n.wal.offs); err != nil {
		t.Fatal(err)
	}
	n.applied, n.ckptLog = 4, 1<<30
	n.ring, n.rx, n.side = newTestRing(t), newTestRing(t), newTestRing(t)
	n.metrics = &nodeMetrics{}
	c := partClient(t, n, 0)
	c.plain = true
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(1, kSnapshot)
	h := mkHandle(c, s.id)
	l := copyLoop(t, n)
	n.shell = &installClientShell{l: l}
	n.snapshotRequest(c, s)
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt == nil || n.ckpt.meta.index != 4 || len(n.ckpt.snapWaiters) != 1 || n.ckpt.snapWaiters[0] != h || len(n.snapWaiters) != 0 {
		t.Fatal("the checkpoint did not take in the client's Snapshot")
	}
	if beforeInstall != nil {
		beforeInstall(s)
	}
	if gone {
		c.endStream(s)
	}
	if err := n.installSnapshot(received); err != nil {
		t.Fatal(err)
	}
	if n.ckpt != nil || n.applied != 9 {
		t.Fatal("the incoming snapshot did not abort the checkpoint and install")
	}
	return n, c, s, l
}

// An install supersedes a checkpoint, not its live client calls. A live
// waiter receives the installed state in full; a disconnected one starts
// no further checkpoint or copy work.
func TestAnInstallDuringACheckpointStillAnswersItsSnapshotClient(t *testing.T) {
	for _, gone := range []bool{false, true} {
		name := "live client"
		if gone {
			name = "disconnected client"
		}
		t.Run(name, func(t *testing.T) {
			n, c, _, l := installWhileClientWaits(t, gone, nil)
			if gone {
				if len(n.snapWaiters) != 0 || n.storage.wantCkpt {
					t.Fatal("the install retained checkpoint demand for a disconnected waiter")
				}
			} else if len(n.snapWaiters) != 1 {
				t.Fatal("the install lost the checkpoint's live Snapshot waiter")
			}
			if err := n.maybeCkpt(); err != nil {
				t.Fatal(err)
			}
			l.goes.Wait()
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
			if gone {
				if n.ckpt != nil || len(n.snapWaiters) != 0 || len(n.clientSnaps) != 0 || n.storage.wantCkpt {
					n.abortCkpt()
					t.Fatal("the install scheduled work for the disconnected Snapshot client")
				}
				return
			}
			if len(n.clientSnaps) != 1 || len(n.snapWaiters) != 0 || n.storage.wantCkpt {
				t.Fatal("the live Snapshot did not resume after the install")
			}
			cs := n.clientSnaps[0]
			defer func() { _ = cs.f.Close() }()
			if index := binary.BigEndian.Uint64(cs.head[len(clientSnapMagic)+4+8:]); index != 9 {
				t.Fatalf("the Snapshot still describes the aborted checkpoint at %d", index)
			}
			payload, err := io.ReadAll(cs.f)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := newStore()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = loaded.close() }()
			if _, err := loadCkpt(cs.f, 0, loaded); err != nil {
				t.Fatal(err)
			}
			if got, ok := valueAt(t, loaded, "/leader", loaded.rev); !ok || got != "v" {
				t.Fatal("the resumed Snapshot lost the installed value")
			}
			if _, ok := loaded.find([]byte("/follower")); ok {
				t.Fatal("the resumed Snapshot kept the aborted store's key")
			}
			if _, err := cs.f.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			whole := append(append([]byte(nil), cs.head...), payload...)
			whole = append(whole, make([]byte, clientSnapPad(int64(len(whole))))...)
			sum := sha256.Sum256(whole)
			for range 4 {
				n.sendClientSnaps()
			}
			if c.get(1) != nil || len(n.clientSnaps) != 0 || cs.sent != cs.size || len(c.out) <= int(cs.size) || !bytes.Equal(cs.sum.Sum(nil), sum[:]) {
				t.Fatal("the Snapshot client did not receive the complete installed state and its digest")
			}
		})
	}
}
