package wl

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// At the newest checkpoint's own index, clients must join the forced
// checkpoint that frees the low ring, not keep copying its uncut source.
// The room retry gate still delays that forced checkpoint until it may
// try again; it must not divert the clients to the quiet-store path.
func TestALowRingForcesItsCutBeforeAnsweringSnapshotWaiters(t *testing.T) {
	n, value, from := lowRingCheckpoint(t)
	c, first, h := snapshotClient(t, n)
	c.flow.conn = 1 << 30
	streams := []*stream{first}
	handles := []handle{h}
	for _, sid := range []uint32{3, 5} {
		s := c.newStream(sid, kSnapshot)
		streams = append(streams, s)
		handles = append(handles, mkHandle(c, sid))
	}
	for _, s := range streams {
		n.snapshotRequest(c, s)
	}
	l := copyLoop(t, n)
	sh := &heldCopyShell{l: l}
	n.shell = sh
	t.Cleanup(func() {
		n.abortCkpt()
		for _, p := range sh.pending {
			_ = p.src.Close()
		}
		l.goes.Wait()
		for _, cs := range n.clientSnaps {
			_ = cs.f.Close()
		}
	})
	before := n.ckptName
	n.ckptRoomAt = n.now.Add(ckptRoomRetry)
	if !n.ringDue() {
		t.Fatal("the fixture needs a forced cut at the newest's own index")
	}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt != nil || len(sh.pending) != 0 || !slices.Equal(n.snapWaiters, handles) {
		t.Fatal("the room retry gate was bypassed by a checkpoint or a quiet-store copy")
	}
	n.now = n.ckptRoomAt
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt == nil || !n.ckpt.force || n.ckpt.meta.index != 14 || n.ckpt.logFrom != from || !slices.Equal(n.ckpt.snapWaiters, handles) || len(n.snapWaiters) != 0 || len(sh.pending) != 0 {
		t.Fatal("the Snapshot waiters did not join the forced checkpoint before any copy")
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
	if n.ckptName == before || n.wal.rbase != from/segSize || n.wal.ringLow() || n.wal.ringRoom() < n.wal.wanted || n.ringDue() {
		t.Fatal("the same-index checkpoint did not free the ring before its clients copied")
	}
	if len(sh.pending) != len(handles) {
		t.Fatalf("the forced checkpoint prepared %d copies for %d waiters", len(sh.pending), len(handles))
	}
	for _, p := range sh.pending {
		if filepath.Base(p.src.Name()) != n.ckptName {
			t.Fatal("a waiting client was given the old, uncut checkpoint")
		}
		l.prepareClientSnap(p.h, p.src)
	}
	sh.pending = nil
	l.goes.Wait()
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if len(n.clientSnaps) != len(handles) {
		t.Fatal("the forced checkpoint's clients did not all receive a copy")
	}
	copies := append([]*clientSnap(nil), n.clientSnaps...)
	for _, cs := range copies {
		if !bytes.Equal(cs.head, clientSnapHead(n.clusterID, ckptMeta{index: 14, term: 2, rev: n.st.rev})) {
			t.Fatal("a forced-checkpoint copy has the wrong header")
		}
		loaded, err := newStore()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = loaded.close() }()
		if _, err := loadCkpt(cs.f, 0, loaded); err != nil {
			t.Fatal(err)
		}
		if got, ok := valueAt(t, loaded, "/kept", loaded.rev); !ok || got != value {
			t.Fatal("the forced cut lost a waiting client's value")
		}
		if _, err := cs.f.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		n.sendClientSnaps()
	}
	if len(n.clientSnaps) != 0 || len(n.snapWaiters) != 0 {
		t.Fatal("the forced checkpoint left a waiting Snapshot unfinished")
	}
	for i, s := range streams {
		if c.get(s.id) != nil || s.snapRetries != 0 || copies[i].sent != copies[i].size {
			t.Fatal("a waiting client did not finish once, without spending a cut retry")
		}
	}
	b := clientSnapshotBytes(t, c.out)
	if len(b) != int(copies[0].size) || len(b)%512 != sha256.Size {
		t.Fatal("the forced checkpoint's snapshot was not sent in full")
	}
	if sum := sha256.Sum256(b[:len(b)-sha256.Size]); !bytes.Equal(sum[:], b[len(b)-sha256.Size:]) {
		t.Fatal("the forced checkpoint's client snapshot has a bad digest")
	}
}

// The room check counts metadata and value spans, not WAL bytes. Once it
// admits a checkpoint, the writer can meet a truncated live WAL segment;
// that error must remain fatal damage, never an ENOSPC drop or retry.
func TestACheckpointWithRoomTreatsAShortLiveLogAsDamage(t *testing.T) {
	n, _, _ := lowRingCheckpoint(t)
	t.Cleanup(n.abortCkpt)
	if err := os.Truncate(segPath(n.wal.dir, 0), walEntHdr); err != nil {
		t.Fatal(err)
	}
	before := n.ckptName
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	if n.ckpt == nil || !n.ckpt.force {
		t.Fatal("the low ring did not start a forced checkpoint")
	}
	var failed error
	for n.ckpt != nil && failed == nil {
		failed = n.ckptStep(n.ckpt.sent > 0)
	}
	if !errors.Is(failed, ErrCorruptLog) || isNoSpace(failed) {
		t.Fatalf("the checkpoint's short durable read was not fatal log damage: %v", failed)
	}
	if n.ckpt == nil || n.ckptName != before || n.wal.rbase != 0 || n.noSpace || n.ckptShort || !n.ckptRoomAt.IsZero() {
		t.Fatal("the damaged source was published, cut, or reclassified as a room retry")
	}
}

// #846 drops an ENOSPC checkpoint through abortCkpt. #862 now makes that
// abort own client requeueing, so the drop must not append them again or
// turn a dead client into an independent Raft checkpoint request.
func TestANoSpaceCheckpointRequeuesLiveClientsOnceAndKeepsRaftDemand(t *testing.T) {
	for _, gone := range []bool{false, true} {
		for _, raftWanted := range []bool{false, true} {
			name := "live client"
			if gone {
				name = "dead client"
			}
			if raftWanted {
				name += "/with Raft demand"
			}
			t.Run(name, func(t *testing.T) {
				n := copyRingNode(t)
				c, s, h := snapshotClient(t, n)
				_ = retiredCopy(t, n)
				index := n.storage.lastIndex() + 1
				copyAppend(t, n, testEntry(index, 2, ""))
				n.id, n.applied = 1, index
				l := copyLoop(t, n)
				n.shell = &copyShell{l: l}
				t.Cleanup(func() {
					n.abortCkpt()
					l.goes.Wait()
					for _, cs := range n.clientSnaps {
						_ = cs.f.Close()
					}
				})
				n.snapshotRequest(c, s)
				if raftWanted {
					n.storage.confIndex = index
					if _, err := n.storage.Snapshot(); err == nil {
						t.Fatal("Raft did not ask for a new checkpoint")
					}
				}
				if err := n.maybeCkpt(); err != nil {
					t.Fatal(err)
				}
				if n.ckpt == nil || !slices.Equal(n.ckpt.snapWaiters, []handle{h}) {
					t.Fatal("the checkpoint did not take in the live client")
				}
				if gone {
					c.endStream(s)
				}
				n.dropCkpt(syscall.ENOSPC, 0)
				want := []handle(nil)
				if !gone {
					want = []handle{h}
				}
				if !slices.Equal(n.snapWaiters, want) || n.storage.wantCkpt != raftWanted {
					t.Fatalf("the drop duplicated a client or lost independent demand: waiters %v, wantCkpt %v", n.snapWaiters, n.storage.wantCkpt)
				}
				if err := n.maybeCkpt(); err != nil {
					t.Fatal(err)
				}
				if n.ckpt != nil {
					t.Fatal("the dropped checkpoint bypassed its room retry gate")
				}
				n.now = n.ckptRoomAt
				if err := n.maybeCkpt(); err != nil {
					t.Fatal(err)
				}
				if gone && !raftWanted {
					if n.ckpt != nil || len(n.snapWaiters) != 0 {
						t.Fatal("a dead client alone started another checkpoint after ENOSPC")
					}
					return
				}
				if n.ckpt == nil || !slices.Equal(n.ckpt.snapWaiters, want) {
					t.Fatal("the room retry lost a live client or independent Raft demand")
				}
				for n.ckpt != nil {
					if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
						t.Fatal(err)
					}
				}
				l.goes.Wait()
				if err := l.runPosts(); err != nil {
					t.Fatal(err)
				}
				if n.storage.wantCkpt || len(n.clientSnaps) != len(want) {
					t.Fatal("the successful retry did not answer exactly its live clients")
				}
				for range 4 {
					n.sendClientSnaps()
				}
				if len(n.clientSnaps) != 0 || !gone && c.get(1) != nil || s.snapRetries != 0 {
					t.Fatal("the room retry did not finish its client without spending a cut retry")
				}
			})
		}
	}
}
