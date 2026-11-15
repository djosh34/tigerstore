package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// Raft snapshots. A follower that lags further than the leader's storage
// keeps gets the leader's newest checkpoint: Raft's MsgSnap names it, and
// a goroutine streams the file over a peer connection of its own, whose
// hello has snapMark set. The follower's accept goroutine writes it to a
// file, checks its CRC, answers, and hands the loop the MsgSnap, which
// now names that file. Raft hands it back in an append, and the loop
// installs it: the store is loaded from it, and the log and storage start
// after it.

// shell is what the node asks of the goroutines around its loop.
type shell interface {
	// wakeLoop wakes the loop; any goroutine may call it.
	wakeLoop()
	// membersChanged dials the members the list now holds.
	membersChanged()
	// prepareClientSnap writes a client's snapshot of the checkpoint src
	// with head before it, and has the loop send it to h.
	prepareClientSnap(h handle, src *os.File, head []byte)
	// sendSnapshot sends the checkpoint f to the follower m is for, with
	// every value in it, and reports to Raft how it went.
	sendSnapshot(m *pb.Message, f *os.File)
	// logf logs what the node does seldom, such as a checkpoint.
	logf(format string, args ...any)
}

// logf logs through the shell, if the node has one.
func (n *node) logf(format string, args ...any) {
	if n.shell != nil {
		n.shell.logf(format, args...)
	}
}

const (
	snapMark    = uint64(1) << 63
	snapFrame   = 1 << 20
	snapTimeout = 30 * time.Second
)

var errSnapRefused = errors.New("the follower did not take the snapshot")

// sendSnapshot starts sending the checkpoint m names to its follower.
func (n *node) sendSnapshot(m *pb.Message) {
	fail := func() { n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFailure) }
	if n.shell == nil {
		fail()
		return
	}
	f, err := os.Open(filepath.Join(n.ckptDir, string(m.GetSnapshot().GetData())))
	if err != nil {
		fail()
		return
	}
	n.stats.snapsSent++
	n.logf("sending the snapshot at index %d to %d", m.GetSnapshot().GetMetadata().GetIndex(), m.GetTo())
	n.shell.sendSnapshot(m, f)
}

// takeSnapshot steps a snapshot received into the file at path.
func (n *node) takeSnapshot(path string, m *pb.Message) {
	if prev := n.recvSnap; prev != "" && !n.snapWaits(prev) {
		_ = os.Remove(prev)
	}
	n.recvSnap = path
	_ = n.rn.Step(m)
}

// snapWaits reports whether an append that waits for room in the log
// carries the snapshot at path.
func (n *node) snapWaits(path string) bool {
	for _, m := range n.wal.waiting {
		if string(m.GetSnapshot().GetData()) == path {
			return true
		}
	}
	return false
}

// installSnapshot makes the node's state the snapshot's: the received
// checkpoint becomes the newest, the store is loaded from it, and Raft's
// storage and the log start after it.
func (n *node) installSnapshot(sn *pb.Snapshot) error {
	meta := sn.GetMetadata()
	path := string(sn.GetData())
	if path == n.recvSnap {
		n.recvSnap = ""
	}
	n.abortCkpt()
	name := ckptName(meta.GetIndex())
	final := filepath.Join(n.ckptDir, name)
	if err := os.Rename(path, final); err != nil {
		return err
	}
	f, err := os.Open(final)
	if err != nil {
		return err
	}
	old := n.st
	st, err := newStore()
	if err != nil {
		_ = f.Close()
		return err
	}
	st.log = old.log
	st.goLive = old.goLive
	st.setBudget(old.budget, old.mapped)
	gen := 1 - n.ckptGen
	m, err := loadCkpt(f, gen, st)
	if err != nil {
		_ = st.close()
		return fmt.Errorf("install a snapshot: %w", err)
	}
	n.st = st
	if err := old.close(); err != nil {
		return err
	}
	// A snapshot still being received stays.
	if err := removeCkpts(n.ckptDir, name, false); err != nil {
		return err
	}
	n.ckptGen, n.ckptName = gen, name
	n.storage.applySnapshot(meta, name)
	n.applied = meta.GetIndex()
	n.confState = meta.GetConfState()
	n.members, n.removed = m.members.members, m.members.removed
	if n.shell != nil {
		n.shell.membersChanged()
	}
	n.ckptFrom, n.ckptCut = n.wal.off, n.wal.off
	if err := writePointer(n.ckptDir, &ckptPointer{gen: gen, name: name, logFrom: n.wal.off, cut: n.wal.off, hs: n.wal.durableHS}); err != nil {
		return err
	}
	if err := n.wal.cut(n.wal.off); err != nil {
		return err
	}
	// The watches go on from where they were: they catch up from the
	// changes the checkpoint holds, or learn that those are compacted.
	for i := range n.w.ws {
		if x := &n.w.ws[i]; x.active && x.syncRev == 0 {
			x.syncRev = old.rev + 1
			n.w.unsynced = append(n.w.unsynced, int32(i))
		}
	}
	n.stats.snapsInstalled++
	n.logf("installed a snapshot at index %d, revision %d, %d keys", meta.GetIndex(), st.rev, st.nkeys)
	return nil
}

// ---- the stream

// snapHeader is what the stream says before the checkpoint's bytes.
func snapHeader(m *pb.Message, size int64) ([]byte, error) {
	meta := m.GetSnapshot().GetMetadata()
	cs, err := proto.Marshal(meta.GetConfState())
	if err != nil {
		return nil, err
	}
	var b []byte
	for _, v := range []uint64{m.GetFrom(), m.GetTerm(), meta.GetIndex(), meta.GetTerm(), uint64(size), uint64(len(cs))} {
		b = binary.AppendUvarint(b, v)
	}
	return append(b, cs...), nil
}

// sendSnapshot streams f to the follower m is for, in a goroutine, and
// then reports to Raft how it went.
func (l *loop) sendSnapshot(m *pb.Message, f *os.File) {
	to := m.GetTo()
	var mem member
	for _, x := range l.n.members {
		if x.id == to {
			mem = x
		}
	}
	go func() {
		err := l.sendSelfContained(mem, m, f)
		_ = f.Close()
		status := raft.SnapshotFinish
		if err != nil {
			status = raft.SnapshotFailure
			l.logger.Printf("send a snapshot to %d: %v", to, err)
		}
		l.post(func() error {
			l.n.rn.ReportSnapshot(to, status)
			return nil
		})
	}()
}

// sendSelfContained sends the follower the checkpoint in f with every
// value in it, written to a file of its own first.
func (l *loop) sendSelfContained(mem member, m *pb.Message, f *os.File) error {
	tmp, err := os.CreateTemp(l.n.ckptDir, "send-*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if err := selfContained(f, l.n.wal, tmp, nil); err != nil {
		return err
	}
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return l.streamSnapshot(mem, m, tmp, size)
}

func (l *loop) streamSnapshot(mem member, m *pb.Message, f *os.File, size int64) error {
	addr := mem.peerAddr()
	if addr == "" {
		return fmt.Errorf("no member %d", m.GetTo())
	}
	hdr, err := snapHeader(m, size)
	if err != nil {
		return err
	}
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	cfg := l.peerTLS.Clone()
	cfg.ServerName, _, _ = net.SplitHostPort(addr)
	h, err := handshake(nc, cfg, kindPeerOut)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(h.fd) }()
	if err := blockingFD(h.fd, snapTimeout); err != nil {
		return err
	}
	s := newSock(h, &counters{})
	s.put(peerHelloBytes(l.n.id|snapMark, l.n.clusterID))
	putFrame(s, hdr)
	buf := make([]byte, snapFrame)
	for {
		k, err := f.Read(buf)
		if k > 0 {
			putFrame(s, buf[:k])
			if err := flushAll(s); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	putFrame(s, nil)
	if err := flushAll(s); err != nil {
		return err
	}
	ack, err := readFrame(s)
	if err != nil {
		return err
	}
	if len(ack) != 1 || ack[0] != 1 {
		return errSnapRefused
	}
	return nil
}

// putFrame writes p with its 4-byte length.
func putFrame(s *sock, p []byte) {
	var h [4]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(p)))
	s.put(h[:])
	s.put(p)
}

var errSnapTimeout = errors.New("a snapshot stream timed out")

// flushAll writes everything s holds, on a blocking socket.
func flushAll(s *sock) error {
	ok, err := s.flush()
	if err != nil {
		return err
	}
	if !ok {
		return errSnapTimeout
	}
	return nil
}

// readFrame reads one length-prefixed frame from a blocking socket; it
// is valid until the next read.
func readFrame(s *sock) ([]byte, error) {
	for {
		if err := s.open(); err != nil {
			return nil, err
		}
		if avail := s.pw - s.pr; avail >= 4 {
			l := int(binary.LittleEndian.Uint32(s.pt[s.pr:]))
			if l > 2*snapFrame {
				return nil, errRecord
			}
			if avail >= 4+l {
				f := s.pt[s.pr+4 : s.pr+4+l]
				s.pr += 4 + l
				return f, nil
			}
		}
		got, err := s.readSock()
		if err != nil {
			return nil, err
		}
		if !got && s.iw-s.ir < recHdr {
			return nil, errSnapTimeout
		}
	}
}

// blockingFD makes fd blocking, its reads and writes timing out after d,
// or never for 0.
func blockingFD(fd int, d time.Duration) error {
	if err := unix.SetNonblock(fd, false); err != nil {
		return err
	}
	tv := unix.NsecToTimeval(d.Nanoseconds())
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		return err
	}
	return unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &tv)
}

// peerHello reads an incoming peer connection's hello, in its accept
// goroutine: a snapshot stream is received there, and any other
// connection goes to the loop.
func (l *loop) peerHello(h *handoff) {
	if err := blockingFD(h.fd, snapTimeout); err != nil {
		_ = unix.Close(h.fd)
		return
	}
	s := newSock(h, &counters{})
	for s.pw-s.pr < 16 {
		if err := s.open(); err != nil {
			_ = unix.Close(h.fd)
			return
		}
		if s.pw-s.pr >= 16 {
			break
		}
		if got, err := s.readSock(); err != nil || !got {
			_ = unix.Close(h.fd)
			return
		}
	}
	hello, cluster := binary.LittleEndian.Uint64(s.pt[s.pr:]), binary.LittleEndian.Uint64(s.pt[s.pr+8:])
	s.pr += 16
	if hello == membersAsk {
		if err := l.answerMembers(s); err != nil {
			l.logger.Printf("answer a node that asks for the members: %v", err)
		}
		_ = unix.Close(h.fd)
		return
	}
	if cluster != l.n.clusterID {
		l.logger.Printf("refused a peer of cluster %x as %d: this is cluster %x", cluster, hello&^snapMark, l.n.clusterID)
		_ = unix.Close(h.fd)
		return
	}
	if hello&snapMark != 0 {
		if err := l.receiveSnapshot(s, hello&^snapMark); err != nil {
			l.logger.Printf("receive a snapshot from %d: %v", hello&^snapMark, err)
		}
		_ = unix.Close(h.fd)
		return
	}
	if err := blockingFD(h.fd, 0); err != nil {
		_ = unix.Close(h.fd)
		return
	}
	if err := unix.SetNonblock(h.fd, true); err != nil {
		_ = unix.Close(h.fd)
		return
	}
	h.pre, h.peer = s, hello
	l.handOver(h)
}

// receiveSnapshot writes the checkpoint a leader streams into a file,
// checks it, answers, and hands the loop its MsgSnap.
func (l *loop) receiveSnapshot(s *sock, from uint64) error {
	hdr, err := readFrame(s)
	if err != nil {
		return err
	}
	var v [6]uint64
	for i := range v {
		x, k := binary.Uvarint(hdr)
		if k <= 0 {
			return errRecord
		}
		v[i], hdr = x, hdr[k:]
	}
	if uint64(len(hdr)) != v[5] || v[4] < 4 {
		return errRecord
	}
	cs := &pb.ConfState{}
	if err := proto.Unmarshal(hdr, cs); err != nil {
		return err
	}
	term, index, snapTerm, size := v[1], v[2], v[3], int64(v[4])
	path := filepath.Join(l.n.ckptDir, fmt.Sprintf("recv-%d-%016x.tmp", from, index))
	if err := receiveFile(s, path, size); err != nil {
		_ = os.Remove(path)
		return err
	}
	s.put([]byte{1, 0, 0, 0, 1})
	if err := flushAll(s); err != nil {
		_ = os.Remove(path)
		return err
	}
	m := &pb.Message{
		Type: pb.MsgSnap.Enum(), From: new(from), To: new(l.n.id), Term: new(term),
		Snapshot: &pb.Snapshot{
			Data:     []byte(path),
			Metadata: &pb.SnapshotMetadata{Index: new(index), Term: new(snapTerm), ConfState: cs},
		},
	}
	l.post(func() error {
		l.n.takeSnapshot(path, m)
		return nil
	})
	return nil
}

// receiveFile writes the stream's frames to path, up to an empty one, and
// checks that they are size bytes whose last four are the CRC of the rest.
func receiveFile(s *sock, path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var got int64
	var crc uint32
	var tail []byte
	for {
		p, err := readFrame(s)
		if err != nil {
			return err
		}
		if len(p) == 0 {
			break
		}
		if got+int64(len(p)) > size {
			return fmt.Errorf("a snapshot longer than its %d bytes", size)
		}
		body := min(int64(len(p)), max(0, size-4-got))
		crc = crc32.Update(crc, castagnoli, p[:body])
		tail = append(tail, p[body:]...)
		if _, err := f.Write(p); err != nil {
			return err
		}
		got += int64(len(p))
	}
	if got != size || len(tail) != 4 || binary.LittleEndian.Uint32(tail) != crc {
		return fmt.Errorf("%w: a snapshot received", errCkptCorrupt)
	}
	return f.Sync()
}
