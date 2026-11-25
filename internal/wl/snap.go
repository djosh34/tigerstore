package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/djosh34/tigerstore/internal/planted"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
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
	prepareClientSnap(h handle, src file, head []byte)
	// sendSnapshot sends the checkpoint f to the follower m is for, with
	// every value in it, and reports to Raft how it went.
	sendSnapshot(m *pb.Message, f file)
	// logf logs what the node does seldom, such as a checkpoint.
	logf(format string, args ...any)
	// offLoop runs work off the loop, such as bcrypt's, and what it
	// returns on the loop.
	offLoop(work func() func() error)
	// sideCall starts the side call of h with its request, and returns
	// what ends it.
	sideCall(h handle, m *sideMethod, req []byte) context.CancelFunc
	// forwardPromote calls the MemberPromote req on the leader, at urls,
	// as a side call of h, and returns what ends it.
	forwardPromote(h handle, urls []string, req []byte) context.CancelFunc
}

// logf logs through the shell, if the node has one.
func (n *node) logf(format string, args ...any) {
	if n.shell != nil {
		n.shell.logf(format, args...)
	}
}

const (
	snapMark    = uint64(1) << 63
	beatMark    = uint64(1) << 62
	snapFrame   = 1 << 20
	snapTimeout = 30 * time.Second
)

var (
	errSnapRefused = errors.New("the follower did not take the snapshot")
	errSnapCut     = errors.New("the simulator cut the snapshot stream")
)

// sendSnapshot starts sending the checkpoint m names to its follower.
func (n *node) sendSnapshot(m *pb.Message) {
	fail := func() { n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFailure) }
	if n.shell == nil {
		fail()
		return
	}
	f, err := n.sys.openFile(filepath.Join(n.ckptDir, string(m.GetSnapshot().GetData())), os.O_RDONLY, 0)
	if err != nil {
		fail()
		return
	}
	n.stats.snapsSent++
	n.logf("sending the snapshot at index %d to %d", m.GetSnapshot().GetMetadata().GetIndex(), m.GetTo())
	n.shell.sendSnapshot(m, f)
}

// takeSnapshot steps a snapshot received into the file at path. Raft
// ignores one at or below its commit, as a second reception of the
// snapshot it took this turn: that file goes, and the one Raft's next
// Ready names stays (#689's area-2 review).
func (n *node) takeSnapshot(path string, m *pb.Message) {
	if m.GetSnapshot().GetMetadata().GetIndex() <= n.rn.Status().GetCommit() {
		_ = n.sys.remove(path)
	} else {
		n.keepRecv(path)
	}
	_ = n.rn.Step(m)
}

// keepRecv makes path the snapshot received last, and removes the one
// before unless an append that waits for room in the log carries it.
func (n *node) keepRecv(path string) {
	if prev := n.recvSnap; prev != "" && !n.snapWaits(prev) {
		_ = n.sys.remove(prev)
	}
	n.recvSnap = path
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
	n.simInstalling(meta.GetIndex())
	path := string(sn.GetData())
	if path == n.recvSnap {
		n.recvSnap = ""
	}
	n.abortCkpt()
	name := ckptName(meta.GetIndex())
	final := filepath.Join(n.ckptDir, name)
	if err := n.sys.rename(path, final); err != nil {
		return err
	}
	f, err := n.sys.openFile(final, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	old := n.st
	oldRev := old.rev
	// Planted: B27 goes on with the store it had before the install, as
	// if it held what the snapshot does.
	keepOld := planted.On("B27")
	if !keepOld {
		// The old store goes before the new one is built: both at once
		// held two stores' tables, past the budget on a large store
		// (#719). A load that fails stops the node, which starts again
		// from its data dir: CURRENT still names the checkpoint before.
		if err := old.close(); err != nil {
			_ = f.Close()
			return err
		}
	}
	st, err := newStore()
	if err != nil {
		_ = f.Close()
		return err
	}
	st.log = old.log
	st.goLive = old.goLive
	st.setBudget(old.budget, old.mapped)
	if !keepOld {
		// Go's limit follows the new store's tables as they grow, as at
		// startup (loadWithinBudget).
		n.st = st
		st.grew = n.limitGo
		n.limitGo()
	}
	gen := 1 - n.ckptGen
	// Planted: B22 installs the snapshot without its CRC.
	m, err := loadCkptChecked(f, gen, st, !planted.On("B22"))
	st.grew = nil
	if err != nil {
		if keepOld {
			_ = st.close()
		}
		return fmt.Errorf("install a snapshot: %w", err)
	}
	if keepOld {
		_ = st.close()
		st = old
	}
	n.st = st
	n.simInstalled(meta.GetIndex())
	n.ckptGen, n.ckptName = gen, name
	n.storage.applySnapshot(meta, name)
	n.applied = meta.GetIndex()
	n.confState = meta.GetConfState()
	if err := n.takeMembers(&m.members); err != nil {
		return err
	}
	if n.shell != nil {
		n.shell.membersChanged()
	}
	n.ckptFrom, n.ckptCut = n.wal.off, n.wal.off
	// The pointer takes the newest hard state handed to the log, not the
	// newest durable one: an append still being synced, below logFrom,
	// may carry a vote that is sent once it is durable, and a restart
	// reads the log only from logFrom (#689's area-2 review). Its commit
	// is below the snapshot's index, which Raft installs only past it.
	if err := writePointer(n.sys, n.ckptDir, &ckptPointer{gen: gen, name: name, logFrom: n.wal.off, cut: n.wal.off, hs: n.storage.hs}); err != nil {
		return err
	}
	// The checkpoints before go only once CURRENT names the new one: a
	// stop or an error before would leave CURRENT naming a file removed
	// (#711's failure, by the install). A snapshot still being received
	// stays.
	if err := removeCkpts(n.sys, n.ckptDir, name, false); err != nil {
		return err
	}
	n.removeAll(n.wal.cut(n.wal.off))
	// The watches go on from where they were: they catch up from the
	// changes the checkpoint holds, or learn that those are compacted. A
	// watch from a revision still to come needs nothing before it, even
	// where the old store was behind it (seed 10155 of TestWLFaultSeeds).
	for i := range n.w.ws {
		if x := &n.w.ws[i]; x.active && x.syncRev == 0 {
			x.syncRev = max(oldRev+1, x.minRev)
			n.w.unsynced = append(n.w.unsynced, int32(i))
		}
	}
	n.stats.snapsInstalled++
	n.logf("installed a snapshot at index %d, revision %d, %d keys", meta.GetIndex(), st.rev, st.nkeys)
	n.simInstallDone(meta.GetIndex())
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
func (l *loop) sendSnapshot(m *pb.Message, f file) {
	to := m.GetTo()
	var mem member
	for _, x := range l.n.members {
		if x.id == to {
			mem = x
		}
	}
	strays.Add(1)
	go func() {
		defer strays.Add(-1)
		err := l.sendSelfContained(mem, m, f)
		_ = f.Close()
		status := raft.SnapshotFinish
		if err != nil {
			status = raft.SnapshotFailure
			l.logger.Printf("send a snapshot to %d: %v", to, err)
		}
		l.post(func() error {
			if errors.Is(err, ErrBadValue) {
				// The disk damaged a value the snapshot holds: the node
				// stops instead of serving it.
				return err
			}
			l.n.rn.ReportSnapshot(to, status)
			return nil
		})
	}()
}

// sendSelfContained sends the follower the checkpoint in f with every
// value in it, written to a file of its own first.
func (l *loop) sendSelfContained(mem member, m *pb.Message, f file) error {
	tmp, err := l.n.sys.createTemp(l.n.ckptDir, "send-*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = l.n.sys.remove(tmp.Name())
	}()
	if err := selfContained(f, l.n.wal, tmp); err != nil {
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

func (l *loop) streamSnapshot(mem member, m *pb.Message, f file, size int64) error {
	addr := mem.peerAddr()
	if addr == "" {
		return fmt.Errorf("no member %d", m.GetTo())
	}
	hdr, err := snapHeader(m, size)
	if err != nil {
		return err
	}
	nc, err := l.n.sys.dial(l.ctx, addr, 5*time.Second)
	if err != nil {
		return err
	}
	cfg := l.peerTLS.Clone()
	cfg.ServerName, _, _ = net.SplitHostPort(addr)
	h, err := handshake(l.n.sys, nc, cfg, kindPeerOut)
	if err != nil {
		return err
	}
	defer func() { _ = sysClose(h.fd) }()
	if err := l.n.sys.blocking(h.fd, snapTimeout); err != nil {
		return err
	}
	s := newSock(h, &counters{})
	s.put(peerHelloBytes(l.n.id|snapMark, l.n.clusterID, ""))
	putFrame(s, hdr)
	buf := make([]byte, snapFrame)
	for first := true; ; {
		k, err := f.Read(buf)
		if k > 0 {
			keep := l.n.simSnapshotChunk(m.GetTo(), first, buf[:k])
			first = false
			if keep < k {
				// The simulator cuts the stream here (FetchCrash).
				var h [4]byte
				binary.LittleEndian.PutUint32(h[:], uint32(k))
				s.put(h[:])
				s.put(buf[:keep])
				if err := flushAll(s); err != nil {
					return err
				}
				l.n.simSnapshotCut(m.GetTo())
				return errSnapCut
			}
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

// peerHello reads an incoming peer connection's hello, in its accept
// goroutine: a snapshot stream is received there, and any other
// connection goes to the loop.
func (l *loop) peerHello(h *handoff) {
	if err := l.n.sys.blocking(h.fd, snapTimeout); err != nil {
		_ = sysClose(h.fd)
		return
	}
	s := newSock(h, &counters{})
	// The hello's ID, cluster and length of the peer's address, then the
	// address.
	need := helloHdr
	for s.pw-s.pr < need {
		if err := s.open(); err != nil {
			_ = sysClose(h.fd)
			return
		}
		if s.pw-s.pr >= helloHdr {
			need = helloHdr + int(binary.LittleEndian.Uint16(s.pt[s.pr+16:]))
		}
		if s.pw-s.pr >= need {
			break
		}
		if got, err := s.readSock(); err != nil || !got {
			_ = sysClose(h.fd)
			return
		}
	}
	hello, cluster := binary.LittleEndian.Uint64(s.pt[s.pr:]), binary.LittleEndian.Uint64(s.pt[s.pr+8:])
	h.addr = string(s.pt[s.pr+helloHdr : s.pr+need])
	s.pr += need
	if hello == membersAsk {
		if err := l.answerMembers(s); err != nil {
			l.logger.Printf("answer a node that asks for the members: %v", err)
		}
		_ = sysClose(h.fd)
		return
	}
	if cluster != l.n.clusterID {
		l.logger.Printf("refused a peer of cluster %x as %d: this is cluster %x", cluster, hello&^snapMark, l.n.clusterID)
		_ = sysClose(h.fd)
		return
	}
	if hello&snapMark != 0 {
		if err := l.receiveSnapshot(s, hello&^snapMark); err != nil {
			l.logger.Printf("receive a snapshot from %d: %v", hello&^snapMark, err)
		}
		_ = sysClose(h.fd)
		return
	}
	if err := l.n.sys.blocking(h.fd, 0); err != nil {
		_ = sysClose(h.fd)
		return
	}
	if err := l.n.sys.nonblocking(h.fd); err != nil {
		_ = sysClose(h.fd)
		return
	}
	h.pre, h.peer, h.beat = s, hello&^beatMark, hello&beatMark != 0
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
	path := l.recvPath(from, index)
	if err := receiveFile(l.n.sys, s, path, size); err != nil {
		_ = l.n.sys.remove(path)
		return err
	}
	s.put([]byte{1, 0, 0, 0, 1})
	if err := flushAll(s); err != nil {
		_ = l.n.sys.remove(path)
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

// recvPath is the file of a snapshot at index received from a peer. Each
// reception has its own: a leader may send the same snapshot again, while
// the file of the one before is still the node's to install or to remove
// (takeSnapshot, #713).
func (l *loop) recvPath(from, index uint64) string {
	return filepath.Join(l.n.ckptDir, fmt.Sprintf("recv-%d-%016x-%d.tmp", from, index, l.recvSeq.Add(1)))
}

// receiveFile writes the stream's frames to path, up to an empty one, and
// checks that they are size bytes whose last four are the CRC of the rest.
func receiveFile(sys sysEnv, s *sock, path string, size int64) error {
	f, err := sys.openFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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
	if got != size || len(tail) != 4 || binary.LittleEndian.Uint32(tail) != crc && !planted.On("B22") {
		return fmt.Errorf("%w: a snapshot received", ErrCorruptCheckpoint)
	}
	return f.Sync()
}
