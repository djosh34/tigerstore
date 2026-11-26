package wl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/proto"
)

// copyShell runs copies without a socket loop. The test waits for the
// goroutine and takes back its post just as the loop would.
type copyShell struct {
	shell
	l *loop
}

func (*copyShell) wakeLoop()           {}
func (*copyShell) logf(string, ...any) {}
func (s *copyShell) prepareClientSnap(h handle, src file, head []byte) {
	s.l.prepareClientSnap(h, src, head)
}

func copyLoop(n *node) *loop {
	return &loop{n: n, hoFD: -1, ctx: context.Background(), logger: log.New(io.Discard, "", 0)}
}

func snapshotClient(t *testing.T, n *node) (*client, *stream, handle) {
	t.Helper()
	n.ring, n.rx, n.side = newTestRing(t), newTestRing(t), newTestRing(t)
	n.metrics = &nodeMetrics{}
	c := partClient(t, n, 0)
	c.plain = true
	n.socks, n.gens = []any{c}, []uint16{1}
	s := c.newStream(1, kSnapshot)
	return c, s, mkHandle(c, s.id)
}

func copyRingNode(t *testing.T) *node {
	t.Helper()
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { waitStrays(t); segSize = was })
	n, _ := ckptNode(t)
	w, err := openWALRing(sysEnv{}, t.TempDir(), 256<<10, 8*segSize, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	n.wal, n.st.log, n.storage.log = w, w, w
	n.ckptLog = 1 << 30
	return n
}

func copyCheckpoint(t *testing.T, n *node, index uint64) {
	t.Helper()
	n.applied = index
	term, err := n.storage.Term(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.startCkpt(index, term, n.storage.offOf(index)-walEntHdr); err != nil {
		t.Fatal(err)
	}
	for n.ckpt != nil {
		if err := n.ckptStep(n.ckpt.sent > 0); err != nil {
			t.Fatal(err)
		}
	}
}

func copyAppend(t *testing.T, n *node, entries ...*pb.Entry) {
	t.Helper()
	if err := n.wal.write(entries, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, n.wal)
	if err := n.storage.append(entries, n.wal.offs); err != nil {
		t.Fatal(err)
	}
}

// liveCopy opens a checkpoint that still names its values in the log.
func liveCopy(t *testing.T, n *node) file {
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
	copyCheckpoint(t, n, index+1)
	src, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if err := selfContained(src, n.wal, &bytes.Buffer{}); err != nil {
		t.Fatalf("copy before the cut: %v", err)
	}
	return src
}

// retireCopy takes the newest checkpoint's values in with an advancing
// checkpoint, then cuts their log segments, on main as after #815's fix.
func retireCopy(t *testing.T, n *node) {
	t.Helper()
	base := n.wal.rbase
	index := n.storage.lastIndex() + 1
	if n.rn == nil {
		copyAppend(t, n, testEntry(index, 2, ""))
	} else {
		// Node 3 acknowledges the leader's new empty entry, so the
		// advancing checkpoint is of committed, applied state even
		// while node 2 waits for its older snapshot's copy.
		if err := n.rn.Propose(nil); err != nil {
			t.Fatal(err)
		}
		_ = copyReady(t, n)
		index = n.storage.lastIndex()
		if err := n.rn.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(n.rn.Status().GetTerm()), Index: new(index)}); err != nil {
			t.Fatal(err)
		}
		_ = copyReady(t, n)
		if n.rn.Status().GetCommit() != index {
			t.Fatal("the advancing checkpoint's entry did not commit")
		}
	}
	// The pending append asks for more than the ring has, forcing the
	// advancing checkpoint to take in the old checkpoint's log values.
	n.wal.wanted = n.wal.ringRoom() + 1
	copyCheckpoint(t, n, index)
	n.wal.wanted = 0
	if n.wal.rbase <= base {
		t.Fatal("the advancing checkpoint retired no segment")
	}
}

func retiredCopy(t *testing.T, n *node) file {
	t.Helper()
	src := liveCopy(t, n)
	retireCopy(t, n)
	return src
}

// A client receives a complete copy of the newest checkpoint after its
// older copy loses its log, even with no further applies after the cut.
func TestAClientSnapshotRetriesACopyWhoseLogWasCut(t *testing.T) {
	n := copyRingNode(t)
	c, _, h := snapshotClient(t, n)
	src := retiredCopy(t, n)
	l := copyLoop(n)
	l.prepareClientSnap(h, src, nil)
	waitStrays(t)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if len(n.snapWaiters) != 1 || n.snapWaiters[0] != h || !n.storage.wantCkpt || len(n.clientSnaps) != 0 {
		t.Fatal("the cut copy did not queue a fresh checkpoint for its client")
	}
	n.shell = &copyShell{l: l}
	if err := n.maybeCkpt(); err != nil {
		t.Fatal(err)
	}
	waitStrays(t)
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if len(n.snapWaiters) != 0 || n.storage.wantCkpt || len(n.clientSnaps) != 1 {
		t.Fatal("the retry did not use the newest checkpoint of the quiet store")
	}
	cs := n.clientSnaps[0]
	if !bytes.Equal(cs.head, clientSnapHead(n.clusterID, ckptMeta{index: n.applied, term: 2, rev: n.st.rev})) {
		t.Fatal("the retry kept the old snapshot's header")
	}
	loaded, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loaded.close() }()
	if _, err := loadCkpt(cs.f, 0, loaded); err != nil {
		t.Fatal(err)
	}
	if got, ok := valueAt(t, loaded, "/kept", loaded.rev); !ok || got != strings.Repeat("v", int(2*segSize)) || loaded.rev != n.st.rev {
		t.Fatal("the retried snapshot lost the cut value or its revision")
	}
	if _, err := cs.f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		n.sendClientSnaps()
	}
	if c.get(1) != nil || len(n.clientSnaps) != 0 || cs.sent != cs.size || len(c.out) <= int(cs.size) {
		t.Fatal("the complete retried snapshot was not sent")
	}
	if _, err := cs.f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("the sent copy stayed open: %v", err)
	}
	b := clientSnapshotBytes(t, c.out)
	if len(b) != int(cs.size) || len(b)%512 != sha256.Size {
		t.Fatalf("the client received an incomplete snapshot of %d bytes", len(b))
	}
	if sum := sha256.Sum256(b[:len(b)-sha256.Size]); !bytes.Equal(sum[:], b[len(b)-sha256.Size:]) {
		t.Fatal("the retried snapshot's digest does not match")
	}
}

// clientSnapshotBytes joins the SnapshotResponse blobs across both HTTP/2
// frames and gRPC messages, as the client does.
func clientSnapshotBytes(t *testing.T, out []byte) []byte {
	t.Helper()
	fr := http2.NewFramer(nil, bytes.NewReader(out))
	var messages, snapshot []byte
	for {
		f, err := fr.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if d, ok := f.(*http2.DataFrame); ok && d.StreamID == 1 {
			messages = append(messages, d.Data()...)
		}
	}
	for len(messages) > 0 {
		if len(messages) < 5 || messages[0] != 0 {
			t.Fatal("an incomplete or compressed SnapshotResponse")
		}
		size := int(binary.BigEndian.Uint32(messages[1:5]))
		if size > len(messages)-5 {
			t.Fatal("an incomplete SnapshotResponse")
		}
		var response etcdpb.SnapshotResponse
		if err := proto.Unmarshal(messages[5:5+size], &response); err != nil {
			t.Fatal(err)
		}
		snapshot = append(snapshot, response.GetBlob()...)
		messages = messages[5+size:]
	}
	return snapshot
}

// Two checkpoint files at the same index can name the same uncut WAL
// byte. Replacing a checkpoint's name is no evidence that its log was
// cut: both client and peer copies must surface the shared damage.
func TestSnapshotCopiesDistinguishSharedDamageFromACut(t *testing.T) {
	for _, peer := range []bool{false, true} {
		for _, old := range []bool{false, true} {
			name := "client/current"
			if peer {
				name = "peer/current"
			}
			if old {
				name = strings.ReplaceAll(name, "current", "replaced")
			}
			t.Run(name, func(t *testing.T) {
				n, ml := ckptNode(t)
				n.wal = &wal{}
				putValue(t, n.st, ml, "/shared", "value")
				writeCkptAt(t, n, 14)
				current, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = current.Close() }()
				// Keep the first file independently of main's same-index
				// rename-over. The two files still share the WAL byte.
				held, err := n.sys.createTemp(n.ckptDir, "held-*.ckpt")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = held.Close() }()
				if _, err := io.Copy(held, current); err != nil {
					t.Fatal(err)
				}
				writeCkptAt(t, n, 14)
				src := current
				if old {
					src = held
				}
				ml.b[0] ^= 1
				copyErr := selfContained(src, ml, &bytes.Buffer{})
				if !errors.Is(copyErr, ErrBadValue) || n.wal.rbase != 0 || n.ckptCut != 0 {
					t.Fatalf("not shared WAL damage without a cut: %v", copyErr)
				}
				l := copyLoop(n)
				if peer {
					// sendSelfContained reads the same durableLog as the
					// client. A memLog is installed through a WAL descriptor
					// below so the real sender's completion is exercised.
					installCopyBytes(t, n, ml.b)
					l.sendSnapshot(&pb.Message{To: new(uint64(2))}, src)
				} else {
					_, _, h := snapshotClient(t, n)
					installCopyBytes(t, n, ml.b)
					l.prepareClientSnap(h, src, nil)
				}
				waitStrays(t)
				if err := l.runPosts(); !errors.Is(err, ErrBadValue) {
					t.Fatalf("shared damage was hidden: %v", err)
				}
				if len(n.snapWaiters) != 0 || n.storage.wantCkpt || len(n.clientSnaps) != 0 {
					t.Fatal("shared damage queued a retry or a successful copy")
				}
			})
		}
	}
	t.Run("retired segment is a cut, not damage", func(t *testing.T) {
		n := copyRingNode(t)
		src := retiredCopy(t, n)
		err := selfContained(src, n.wal, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "the log was cut") || errors.Is(err, ErrBadValue) {
			t.Fatalf("a retired segment was not distinguished from shared damage: %v", err)
		}
	})
}

// installCopyBytes gives the real copy goroutines a durable WAL holding
// the memLog fixture's bytes, including its one damaged byte.
func installCopyBytes(t *testing.T, n *node, b []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bytes")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := n.sys.open(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sysClose(fd) })
	n.wal = &wal{rfds: []int{fd}}
}

func TestASnapshotCutRetryHasALimit(t *testing.T) {
	n := copyRingNode(t)
	c, s, h := snapshotClient(t, n)
	l := copyLoop(n)
	for attempt := 1; attempt <= 4; attempt++ {
		src := retiredCopy(t, n)
		n.snapWaiters, n.storage.wantCkpt = nil, false
		l.prepareClientSnap(h, src, nil)
		waitStrays(t)
		if err := l.runPosts(); err != nil {
			t.Fatal(err)
		}
		if attempt < 4 {
			if c.get(s.id) == nil || len(n.snapWaiters) != 1 || n.snapWaiters[0] != h || !n.storage.wantCkpt {
				t.Fatalf("attempt %d did not retry the live stream", attempt)
			}
			continue
		}
		if c.get(s.id) != nil || len(n.snapWaiters) != 0 || n.storage.wantCkpt || !bytes.Contains(c.out, []byte("retry limit")) {
			t.Fatal("four consecutive real cuts did not end with a clear retry-limit error")
		}
	}
}

func TestASnapshotCutRetryEndsWithItsClient(t *testing.T) {
	for _, gone := range []string{"stream", "client", "generation"} {
		t.Run(gone, func(t *testing.T) {
			n := copyRingNode(t)
			c, s, h := snapshotClient(t, n)
			src := retiredCopy(t, n)
			l := copyLoop(n)
			l.prepareClientSnap(h, src, nil)
			waitStrays(t)
			switch gone {
			case "stream":
				c.endStream(s)
			case "client":
				c.dead = true
			case "generation":
				n.gens[0]++
			}
			if err := l.runPosts(); err != nil {
				t.Fatal(err)
			}
			if len(n.snapWaiters) != 0 || n.storage.wantCkpt {
				t.Fatal("a disconnected snapshot queued another checkpoint")
			}
			// A waiter disconnected before its checkpoint completed is
			// dropped too, without even starting another copy.
			n.shell = &copyShell{l: l}
			n.startClientSnaps([]handle{h}, n.ckptName, ckptMeta{})
			waitStrays(t)
			l.mu.Lock()
			posts := len(l.posts)
			l.mu.Unlock()
			if posts != 0 {
				_ = l.runPosts()
				t.Fatal("a disconnected waiter started a copy")
			}
		})
	}
}

// An open descriptor is not a segment identity check. In either window
// around pread, retirement must win even over a successful read.
func TestALogReadRejectsASegmentRetiredDuringPread(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "after pread"
		if before {
			name = "before pread"
		}
		t.Run(name, func(t *testing.T) {
			n := copyRingNode(t)
			w := n.wal
			copyAppend(t, n, testEntry(1, 2, strings.Repeat("v", int(2*segSize))))
			off := w.offs[0]
			reused, err := os.OpenFile(segPath(w.dir, 0), os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reused.Close() }()
			retire := func() {
				w.cut(segSize)
				if _, err := reused.WriteAt([]byte("a"), off); err != nil {
					t.Fatal(err)
				}
			}
			p := make([]byte, 1)
			err = w.readSegments(p, off, func(fd int, p []byte, at int64) (int, error) {
				if before {
					retire()
				}
				n, err := sysPread(fd, p, at)
				if !before {
					retire()
				}
				return n, err
			})
			want := byte('v')
			if before {
				want = 'a'
			}
			if p[0] != want {
				t.Fatalf("the controlled pread read %q, want %q", p, want)
			}
			if err == nil || !strings.Contains(err.Error(), "the log was cut") {
				t.Fatalf("the read accepted a retired segment's bytes %q: %v", p, err)
			}
		})
	}
}

// A real copy failure reports SnapshotFailure, so Raft leaves snapshot
// state and probes again; the next rejection gets its newest checkpoint.
func TestAPeerSnapshotRetriesACopyWithARetiredSegment(t *testing.T) {
	n := copyRingNode(t)
	src := liveCopy(t, n)
	startCopyLeader(t, n)
	old := forceCopySnapshot(t, n)
	if old.GetSnapshot().GetMetadata().GetIndex() != n.applied {
		t.Fatal("Raft did not send its checkpoint")
	}
	retireCopy(t, n)
	copyErr := selfContained(src, n.wal, &bytes.Buffer{})
	if copyErr == nil || !strings.Contains(copyErr.Error(), "the log was cut") || errors.Is(copyErr, ErrBadValue) {
		t.Fatalf("the peer's retired segment was not classified as a cut: %v", copyErr)
	}
	l := copyLoop(n)
	l.sendSnapshot(old, src)
	waitStrays(t)
	if err := l.runPosts(); err != nil {
		t.Fatalf("the cut copy stopped the node: %v", err)
	}
	if p := n.rn.Status().Progress[2]; p.State != tracker.StateProbe || p.PendingSnapshot != 0 {
		t.Fatalf("the failed copy did not return the follower to probe: %+v", p)
	}
	// The cut's advancing checkpoint replaced the one sent before.
	newest := forceCopySnapshot(t, n)
	if newest.GetSnapshot().GetMetadata().GetIndex() != n.applied || string(newest.GetSnapshot().GetData()) != n.ckptName {
		t.Fatal("Raft retried with the old snapshot")
	}
	f, err := n.sys.openFile(filepath.Join(n.ckptDir, n.ckptName), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var copied bytes.Buffer
	if err := selfContained(f, n.wal, &copied); err != nil {
		t.Fatal(err)
	}
	// The follower accepts the metadata of the newly copied snapshot.
	storage := raft.NewMemoryStorage()
	cfg := raftConfig(2, storage, sizeFor(0), 3)
	cfg.AsyncStorageWrites = false
	follower, err := raft.NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Step(newest); err != nil {
		t.Fatal(err)
	}
	received := follower.Ready().Snapshot
	if got := received.GetMetadata().GetIndex(); got != n.applied {
		t.Fatalf("the follower received index %d, want %d", got, n.applied)
	}
	fn, _ := ckptNode(t)
	fn.wal = &wal{}
	path := filepath.Join(fn.ckptDir, "received")
	if err := os.WriteFile(path, copied.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	received.Data = []byte(path)
	if err := fn.installSnapshot(received); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fn.st.close() })
	if got, ok := valueAt(t, fn.st, "/kept", fn.st.rev); !ok || got != strings.Repeat("v", int(2*segSize)) || fn.applied != n.applied {
		t.Fatal("the follower did not install the newest snapshot and its value")
	}
}

func startCopyLeader(t *testing.T, n *node) {
	t.Helper()
	n.storage.compact(n.applied)
	n.storage.cs = n.confState
	n.storage.hs = &pb.HardState{Term: new(uint64(2)), Commit: new(n.applied)}
	cfg := raftConfig(1, n.storage, sizeFor(0), 3)
	cfg.Applied = n.applied
	cfg.AsyncStorageWrites = false
	var err error
	n.rn, err = raft.NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	_ = copyReady(t, n)
	for _, typ := range []pb.MessageType{pb.MsgPreVoteResp, pb.MsgVoteResp} {
		term := n.rn.Status().GetTerm() + 1
		if typ == pb.MsgVoteResp {
			term = n.rn.Status().GetTerm()
		}
		if err := n.rn.Step(&pb.Message{Type: typ.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(term)}); err != nil {
			t.Fatal(err)
		}
		_ = copyReady(t, n)
	}
	if n.rn.Status().RaftState != raft.StateLeader {
		t.Fatalf("node 1 did not lead: %s", n.rn.Status())
	}
}

// copyReady persists synchronous Raft's writes before acknowledging them,
// including the self-votes that make it a candidate and then a leader.
func copyReady(t *testing.T, n *node) []*pb.Message {
	t.Helper()
	rd := n.rn.Ready()
	if len(rd.Entries) > 0 {
		copyAppend(t, n, rd.Entries...)
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		n.storage.hs = rd.HardState
	}
	n.rn.Advance(rd)
	return rd.Messages
}

func forceCopySnapshot(t *testing.T, n *node) *pb.Message {
	t.Helper()
	term := n.rn.Status().GetTerm()
	if err := n.rn.Step(&pb.Message{Type: pb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(term)}); err != nil {
		t.Fatal(err)
	}
	if err := n.rn.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(term), Index: new(n.rn.Status().Progress[2].Next - 1), Reject: new(true), RejectHint: new(uint64(0))}); err != nil {
		t.Fatal(err)
	}
	for _, m := range copyReady(t, n) {
		if m.GetType() == pb.MsgSnap && m.GetTo() == 2 {
			return m
		}
	}
	t.Fatal("Raft sent no snapshot to its lagging follower")
	return nil
}
