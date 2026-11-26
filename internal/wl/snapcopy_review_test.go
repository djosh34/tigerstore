package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// heldCopyShell holds the opened source before its copy goroutine starts,
// so each real checkpoint/copy/requeue turn can lose its log to a cut.
type heldCopyShell struct {
	shell
	l       *loop
	pending []heldCopy
}

type heldCopy struct {
	h   handle
	src file
}

func (*heldCopyShell) wakeLoop()           {}
func (*heldCopyShell) logf(string, ...any) {}
func (s *heldCopyShell) prepareClientSnap(h handle, src file) {
	s.pending = append(s.pending, heldCopy{h: h, src: src})
}

func (s *heldCopyShell) release(t *testing.T) {
	t.Helper()
	if len(s.pending) != 1 {
		t.Fatalf("held %d copies, want one", len(s.pending))
	}
	p := s.pending[0]
	s.pending = nil
	s.l.prepareClientSnap(p.h, p.src)
}

// stageCopyValue advances the store without checkpointing it: maybeCkpt
// must take the waiting client through the real scheduling path.
func stageCopyValue(t *testing.T, n *node) {
	t.Helper()
	index := n.storage.lastIndex() + 1
	value := strings.Repeat("v", int(2*segSize))
	copyAppend(t, n, testEntry(index, 2, value), testEntry(index+1, 2, ""))
	id, err := n.st.intern([]byte("/kept"))
	if err != nil {
		t.Fatal(err)
	}
	n.st.rev++
	if _, err := n.st.put(id, []byte(value), 0, n.st.rev, n.wal.offs[0], true); err != nil {
		t.Fatal(err)
	}
	n.st.endApply()
	n.applied = index + 1
}

// A single bit changes an otherwise live value's offset into a retired
// segment, without any cut during the copy. The checkpoint's failed CRC
// must win over the apparent cut, on both copy completion paths.
func TestASnapshotOffsetFlipIsCheckpointDamageNotACut(t *testing.T) {
	for _, path := range []string{"copy", "client", "peer"} {
		t.Run(path, func(t *testing.T) {
			n := copyRingNode(t)
			copyAppend(t, n, testEntry(1, 2, strings.Repeat("p", int(2*segSize))), testEntry(2, 2, "kept"), testEntry(3, 2, ""))
			off := n.wal.offs[1]
			id, err := n.st.intern([]byte("/kept"))
			if err != nil {
				t.Fatal(err)
			}
			n.st.rev++
			if _, err := n.st.put(id, []byte("kept"), 0, n.st.rev, off, true); err != nil {
				t.Fatal(err)
			}
			n.st.endApply()
			copyCheckpoint(t, n, 3)
			n.wal.cut(2 * segSize)
			if n.wal.rbase != 2 || off/segSize != 2 {
				t.Fatal("the fixture needs a live value just past two retired segments")
			}
			var message *pb.Message
			if path == "peer" {
				startCopyLeader(t, n)
				message = forceCopySnapshot(t, n)
			}
			name := filepath.Join(n.ckptDir, n.ckptName)
			b, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			encoded := binary.AppendUvarint(nil, uint64(off))
			at := bytes.LastIndex(b, encoded)
			if at < 0 || len(encoded) != 3 || encoded[2] != 2 {
				t.Fatal("the fixture did not encode an offset with one high bit to flip")
			}
			b[at+2] ^= 2
			if changed, _ := binary.Uvarint(b[at:]); int64(changed)/segSize >= 2 {
				t.Fatal("the one-bit offset flip did not name a retired segment")
			}
			if err := os.WriteFile(name, b, 0o600); err != nil {
				t.Fatal(err)
			}
			src, err := n.sys.openFile(name, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = src.Close() }()
			l := copyLoop(n)
			switch path {
			case "copy":
				err = selfContained(src, n.wal, &bytes.Buffer{})
			case "client":
				_, _, h := snapshotClient(t, n)
				l.prepareClientSnap(h, src)
				waitStrays(t)
				err = l.runPosts()
			case "peer":
				l.sendSnapshot(message, src)
				waitStrays(t)
				err = l.runPosts()
			}
			if !errors.Is(err, ErrCorruptCheckpoint) {
				t.Fatalf("checkpoint offset damage was not fatal checkpoint corruption: %v", err)
			}
			if n.wal.rbase != 2 || len(n.snapWaiters) != 0 || n.storage.wantCkpt || len(n.clientSnaps) != 0 {
				t.Fatal("checkpoint damage caused a cut, retry, or successful copy")
			}
			if path == "peer" && n.rn.Status().Progress[2].State != tracker.StateSnapshot {
				t.Fatal("checkpoint damage was reported to Raft as an ordinary failed send")
			}
		})
	}
}

func TestAPeerSnapshotCheckpointDamageStopsBeforeReportingFailure(t *testing.T) {
	for _, damage := range []string{"crc", "truncated", "magic"} {
		t.Run(damage, func(t *testing.T) {
			n := copyRingNode(t)
			src := liveCopy(t, n)
			startCopyLeader(t, n)
			message := forceCopySnapshot(t, n)
			b, err := os.ReadFile(src.Name())
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "crc":
				b[len(b)-1] ^= 1
			case "truncated":
				b = b[:len(b)-2]
			case "magic":
				b[0] ^= 1
			}
			if err := os.WriteFile(src.Name(), b, 0o600); err != nil {
				t.Fatal(err)
			}
			l := copyLoop(n)
			l.sendSnapshot(message, src)
			waitStrays(t)
			if err := l.runPosts(); !errors.Is(err, ErrCorruptCheckpoint) {
				t.Fatalf("the peer's checkpoint damage did not stop the node: %v", err)
			}
			if p := n.rn.Status().Progress[2]; p.State != tracker.StateSnapshot || p.PendingSnapshot != n.applied {
				t.Fatalf("the damaged send reported failure instead of stopping: %+v", p)
			}
		})
	}
}

func TestADisconnectedRequeuedSnapshotStartsNoCheckpoint(t *testing.T) {
	for _, raftDemand := range []bool{false, true} {
		name := "client only"
		if raftDemand {
			name = "independent Raft demand"
		}
		t.Run(name, func(t *testing.T) {
			n := copyRingNode(t)
			c, s, h := snapshotClient(t, n)
			src := retiredCopy(t, n)
			l := copyLoop(n)
			l.prepareClientSnap(h, src)
			waitStrays(t)
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
			if len(n.snapWaiters) != 1 {
				t.Fatal("the live cut copy was not requeued")
			}
			c.endStream(s)
			index := n.storage.lastIndex() + 1
			copyAppend(t, n, testEntry(index, 2, ""))
			n.applied = index
			if n.ringDue() || n.wal.off-n.ckptFrom >= n.ckptEvery() {
				t.Fatal("a ring or periodic checkpoint would mask the dead client's demand")
			}
			if raftDemand {
				n.storage.confIndex = index
				if _, err := n.storage.Snapshot(); err == nil {
					t.Fatal("Raft did not ask for a new checkpoint")
				}
			}
			if err := n.maybeCkpt(); err != nil {
				t.Fatal(err)
			}
			if len(n.snapWaiters) != 0 || n.ckpt != nil && len(n.ckpt.snapWaiters) != 0 {
				n.abortCkpt()
				t.Fatal("the dead retry remained attached to checkpoint work")
			}
			if !raftDemand && (n.ckpt != nil || n.storage.wantCkpt) {
				n.abortCkpt()
				t.Fatal("a disconnect after requeue triggered a checkpoint")
			}
			if raftDemand {
				if n.ckpt == nil {
					t.Fatal("pruning a dead client cleared Raft's independent demand")
				}
				for n.ckpt != nil {
					if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestASnapshotStreamAcceptsOnlyOneRequestWithoutResettingItsBudget(t *testing.T) {
	n := copyRingNode(t)
	c, s, h := snapshotClient(t, n)
	if s.snapRetries != 0 {
		t.Fatal("a new stream inherited a Snapshot retry budget")
	}
	if err := n.request(c, s, nil); err != nil {
		t.Fatal(err)
	}
	if len(n.snapWaiters) != 1 || n.snapWaiters[0] != h {
		t.Fatal("the first Snapshot request was not accepted")
	}
	s.snapRetries = 2
	if err := n.request(c, s, nil); err != nil {
		t.Fatal(err)
	}
	if s.snapRetries != 2 || c.get(1) != nil || len(n.snapWaiters) != 1 || !bytes.Contains(c.out, []byte("only one request per stream")) {
		t.Fatal("a second Snapshot message reset its budget or queued another request instead of being refused")
	}
	// The same slot can serve a later stream, whose budget and unary
	// admission must start over at creation rather than on each message.
	next := c.newStream(1+2*streamSlots, kSnapshot)
	if next.snapRetries != 0 {
		t.Fatal("the reused stream slot inherited the retry budget")
	}
	if err := n.request(c, next, nil); err != nil {
		t.Fatal(err)
	}
	if c.get(next.id) == nil || len(n.snapWaiters) != 2 {
		t.Fatal("a new stream inherited the previous Snapshot's admission state")
	}
}

func TestAnInstallDoesNotSpendASnapshotCutRetry(t *testing.T) {
	n, _, s, l := installWhileClientWaits(t, false, func(s *stream) { s.snapRetries = 2 })
	if s.snapRetries != 2 || len(n.snapWaiters) != 1 {
		t.Fatal("the install spent a cut retry or lost the waiter")
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	waitStrays(t)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if s.snapRetries != 2 || len(n.clientSnaps) != 1 {
		t.Fatal("the resumed copy spent a cut retry")
	}
	_ = n.clientSnaps[0].f.Close()
}

func TestAQuietSnapshotHeaderUsesTheCheckpointRevision(t *testing.T) {
	n := copyRingNode(t)
	c, s, _ := snapshotClient(t, n)
	_ = retiredCopy(t, n)
	revision := n.st.rev
	// The copy must not synthesize its revision from mutable store
	// metadata, even if it normally equals the checkpoint's today.
	n.st.rev += 7
	l := copyLoop(n)
	n.shell = &copyShell{l: l}
	n.snapshotRequest(c, s)
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	waitStrays(t)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if len(n.clientSnaps) != 1 {
		t.Fatal("the quiet store did not prepare its newest snapshot")
	}
	cs := n.clientSnaps[0]
	defer func() { _ = cs.f.Close() }()
	if got := int64(binary.BigEndian.Uint64(cs.head[len(cs.head)-8:])); got != revision {
		t.Fatalf("the copy's header says revision %d, want its checkpoint's %d", got, revision)
	}
}
