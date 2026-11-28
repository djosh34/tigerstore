package wl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"golang.org/x/sys/unix"
)

// A valid checkpoint can name a live WAL segment whose durable bytes
// were truncated. That is damage, not a cut or an ordinary failed send.
func TestASnapshotCopyTreatsATruncatedLiveLogAsDamage(t *testing.T) {
	for _, path := range []string{"copy", "client", "peer"} {
		for _, partial := range []bool{false, true} {
			name := path + "/empty read"
			if partial {
				name = path + "/partial read then EOF"
			}
			t.Run(name, func(t *testing.T) {
				n := copyRingNode(t)
				src := liveCopy(t, n)
				var message *pb.Message
				if path == "peer" {
					startCopyLeader(t, n)
					message = forceCopySnapshot(t, n)
				}
				id, _ := n.st.find([]byte("/kept"))
				off := n.st.keys[id].voff
				if inCkpt(off) || off/segSize != 0 || n.wal.rbase != 0 {
					t.Fatal("the fixture needs a valid checkpoint naming live segment 0")
				}
				end := off
				if partial {
					end += 32
				}
				if err := os.Truncate(segPath(n.wal.dir, 0), end); err != nil {
					t.Fatal(err)
				}
				l := copyLoop(t, n)
				var err error
				switch path {
				case "copy":
					err = selfContained(t.Context(), src, n.wal, io.Discard)
				case "client":
					c, s, _ := snapshotClient(t, n)
					n.shell = &copyShell{l: l}
					n.snapshotRequest(c, s)
					if err := n.maybeCkpt(); err != nil {
						t.Fatal(err)
					}
					l.goes.Wait()
					err = l.runPosts()
				case "peer":
					l.sendSnapshot(message, src)
					l.goes.Wait()
					err = l.runPosts()
				}
				if err == nil || !snapshotDamage(err) || errors.Is(err, errLogCut) {
					t.Fatalf("a truncated live log was not fatal copy damage: %v", err)
				}
				if n.wal.rbase != 0 || len(n.snapWaiters) != 0 || len(n.clientSnaps) != 0 || n.storage.wantCkpt {
					t.Fatal("the truncated live segment caused a cut, retry, or successful copy")
				}
				if path == "peer" && n.rn.Status().Progress[2].State != tracker.StateSnapshot {
					t.Fatal("a short durable value was reported to Raft as an ordinary failed send")
				}
			})
		}
	}
	t.Run("transport failure remains retryable", func(t *testing.T) {
		n := copyRingNode(t)
		src := liveCopy(t, n)
		startCopyLeader(t, n)
		message := forceCopySnapshot(t, n)
		// The copy is sound, but this fixture has no destination address.
		// A failed transport setup must still report SnapshotFailure.
		l := copyLoop(t, n)
		l.sendSnapshot(message, src)
		l.goes.Wait()
		if err := l.runPosts(); err != nil {
			t.Fatalf("an ordinary transport failure stopped the node: %v", err)
		}
		if p := n.rn.Status().Progress[2]; p.State != tracker.StateProbe || p.PendingSnapshot != 0 {
			t.Fatalf("the transport failure did not return the follower to probe: %+v", p)
		}
	})
}

// EOF in a live segment is damage; the same read after retirement is a
// cut instead. An actual I/O failure must stay an I/O failure.
func TestALogReadSeparatesPrematureEOFFromIOAndRetirement(t *testing.T) {
	for _, which := range []string{"zero read", "EOF", "unexpected EOF", "I/O error", "retired during EOF"} {
		t.Run(which, func(t *testing.T) {
			n := copyRingNode(t)
			copyAppend(t, n, testEntry(1, 2, strings.Repeat("v", int(2*segSize))))
			err := n.wal.readSegments(make([]byte, 1), n.wal.offs[0], func(_ int, _ []byte, _ int64) (int, error) {
				switch which {
				case "EOF":
					return 0, io.EOF
				case "unexpected EOF":
					return 0, io.ErrUnexpectedEOF
				case "I/O error":
					return 0, unix.EIO
				case "retired during EOF":
					n.wal.cut(segSize)
					return 0, io.EOF
				default:
					return 0, nil
				}
			})
			switch which {
			case "I/O error":
				if !errors.Is(err, unix.EIO) || snapshotDamage(err) || errors.Is(err, errLogCut) {
					t.Fatalf("ordinary I/O was reclassified: %v", err)
				}
			case "retired during EOF":
				if !errors.Is(err, errLogCut) || snapshotDamage(err) {
					t.Fatalf("retirement was reclassified as damage: %v", err)
				}
			default:
				if !snapshotDamage(err) || errors.Is(err, errLogCut) {
					t.Fatalf("premature EOF was not damage: %v", err)
				}
			}
		})
	}
}

// A corrupted external-value length must be rejected before allocation
// or a value read, even when it decodes as a valid uint64 varint.
func TestACheckpointCopyBoundsADamagedValueLengthBeforeAllocation(t *testing.T) {
	for _, length := range []uint64{maxRequestBytes + 1, ^uint64(0)} {
		name := "past request limit"
		if length == ^uint64(0) {
			name = "past machine int"
		}
		t.Run(name, func(t *testing.T) {
			b := checkpointWithDamagedValueLength(t, length)
			read := false
			err := copyCkpt(bytes.NewReader(b), func([]byte, bool, int64) error {
				read = true
				return io.ErrUnexpectedEOF
			}, io.Discard, nil)
			if !errors.Is(err, ErrCorruptCheckpoint) || read {
				t.Fatalf("the damaged length was not rejected before allocation/value read: read %v, error %v", read, err)
			}
		})
	}
}

func checkpointWithDamagedValueLength(t *testing.T, length uint64) []byte {
	t.Helper()
	n, ml := ckptNode(t)
	n.wal = &wal{}
	putValue(t, n.st, ml, "/kept", "v")
	writeCkptAt(t, n, 14)
	b, err := os.ReadFile(filepath.Join(n.ckptDir, n.ckptName))
	if err != nil {
		t.Fatal(err)
	}
	cr := &ckptReader{r: bufio.NewReader(bytes.NewReader(b))}
	uv := func() uint64 {
		v, err := cr.uvarint()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	skip := func(n uint64) {
		if err := cr.skip(n); err != nil {
			t.Fatal(err)
		}
	}
	skip(uint64(len(ckptMagic)))
	for range 5 {
		uv()
	}
	skip(uv()) // ConfState
	if _, err := cr.readMembers(); err != nil {
		t.Fatal(err)
	}
	for count := uv(); count > 0; count-- {
		uv()
		uv()
	}
	skip(uv()) // embedded value bytes
	kind, err := cr.ReadByte()
	if err != nil || kind != ckptKey {
		t.Fatalf("no key record in the fixture: %v", err)
	}
	uv()       // key ID
	skip(uv()) // key bytes
	if uv() != 1 || uv()&ckptInLog == 0 {
		t.Fatal("the fixture needs one external value")
	}
	for range 4 { // create, mod, version, lease
		uv()
	}
	at := int(cr.off)
	if uv() != 1 {
		t.Fatal("the fixture's value length is not one")
	}
	damaged := append([]byte(nil), b[:at]...)
	damaged = binary.AppendUvarint(damaged, length)
	return append(damaged, b[int(cr.off):]...)
}

// The scheduler and worker need only the checkpoint source, not a
// prebuilt header. Its verified bytes remain the metadata authority even
// if the store's revision has since changed.
func TestAClientSnapshotCopyNeedsOnlyItsCheckpointSource(t *testing.T) {
	n := copyRingNode(t)
	_, _, h := snapshotClient(t, n)
	src := liveCopy(t, n)
	l := copyLoop(t, n)
	// Keep the contract assertions runnable against the old signatures
	// too, so the regression fails before rather than needing a shim.
	if _, ok := any(n.startClientSnaps).(func([]handle, string)); !ok {
		t.Fatal("the scheduler still constructs and passes a redundant header")
	}
	prepare, ok := any(l.prepareClientSnap).(func(handle, file))
	if !ok {
		t.Fatal("the copy worker still accepts a redundant header")
	}
	revision := n.st.rev
	n.st.rev += 7
	prepare(h, src)
	l.goes.Wait()
	if err := l.runPosts(); err != nil {
		t.Fatal(err)
	}
	if len(n.clientSnaps) != 1 {
		t.Fatal("the source-only worker did not prepare its copy")
	}
	cs := n.clientSnaps[0]
	defer func() { _ = cs.f.Close() }()
	if !bytes.Equal(cs.head, clientSnapHead(n.clusterID, ckptMeta{index: n.applied, term: 2, rev: revision})) {
		t.Fatal("the source-only copy did not derive its header from its checkpoint")
	}
}

// A client keeps its request side open, receives the first response DATA,
// then sends a second request. Its error must be valid gRPC trailers,
// rather than a trailing :status pseudo-header and an HTTP/2 error.
func TestALateSecondSnapshotRequestGetsInvalidArgumentTrailers(t *testing.T) {
	tc := newTestClusterOf(t, 1, nil)
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, key := range []string{"/snap/a", "/snap/b", "/snap/c", "/snap/d"} {
		if _, err := c.Put(ctx, key, strings.Repeat("v", 512<<10)); err != nil {
			t.Fatal(err)
		}
	}
	conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(clientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	// Hold the first response message in flight until the second request
	// is processed. The test, unlike a transport, sends no automatic
	// WINDOW_UPDATE that could let the server finish the whole Snapshot.
	if err := fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 32 << 10}); err != nil {
		t.Fatal(err)
	}
	h2Call(t, fr, 1, "/etcdserverpb.Maintenance/Snapshot")
	request := make([]byte, 5) // an empty gRPC SnapshotRequest
	if err := fr.WriteData(1, false, request); err != nil {
		t.Fatal(err)
	}
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	initial, second := false, false
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("the streaming client saw an HTTP/2 error, not InvalidArgument: %v", err)
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				if err := fr.WriteSettingsAck(); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.DataFrame:
			if f.StreamID != 1 || second {
				continue
			}
			if !initial || f.StreamEnded() {
				t.Fatal("the first Snapshot response was not still streaming")
			}
			if err := fr.WriteData(1, true, request); err != nil {
				t.Fatal(err)
			}
			second = true
			for _, sid := range []uint32{1, 0} {
				if err := fr.WriteWindowUpdate(sid, 4<<20); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.MetaHeadersFrame:
			if f.StreamID != 1 {
				continue
			}
			if !f.StreamEnded() {
				if initial || f.PseudoValue("status") != "200" {
					t.Fatal("invalid initial Snapshot response headers")
				}
				initial = true
				continue
			}
			if !second {
				t.Fatal("the server finished before the second request was sent")
			}
			status := ""
			for _, field := range f.Fields {
				if strings.HasPrefix(field.Name, ":") {
					t.Fatalf("the streaming error's trailers contain forbidden pseudo-header %q", field.Name)
				}
				if field.Name == "grpc-status" {
					status = field.Value
				}
			}
			if status != "3" || !strings.Contains(grpcMessage(f), "only one request per stream") {
				t.Fatalf("the streaming caller got status %q, message %q, want InvalidArgument", status, grpcMessage(f))
			}
			return
		case *http2.RSTStreamFrame:
			t.Fatalf("the streaming call was reset instead of getting InvalidArgument: %v", f.ErrCode)
		case *http2.GoAwayFrame:
			t.Fatalf("the streaming connection closed instead of getting InvalidArgument: %v", f.ErrCode)
		}
	}
}
