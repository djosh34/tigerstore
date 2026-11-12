package wl

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"runtime"
	"sync/atomic"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// Raft runs inline in the loop: etcd/raft's RawNode with asynchronous
// storage writes. An append is one pwritev of the entries straight from
// the log ring; one helper thread runs fdatasync and wakes the loop
// through an eventfd; the append's responses are stepped once it is
// durable.

// wal is the log file and its fsync helper.
type wal struct {
	fd       int
	off      int64
	iov      [][]byte
	hdrs     []byte
	pending  []walJob
	inflight bool
	want     atomic.Int64
	done     atomic.Int64
	kickFD   int // the helper waits on it
	wakeFD   int // the loop waits on it
	idx      []idxOff
}

type walJob struct {
	end   int64
	resps []*pb.Message
}

type idxOff struct {
	index uint64
	off   int64
}

const idxSlots = 1 << 16

func openWAL(path string) (*wal, error) {
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, err
	}
	kick, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		return nil, err
	}
	wake, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, err
	}
	w := &wal{fd: fd, kickFD: kick, wakeFD: wake, idx: make([]idxOff, idxSlots)}
	go w.helper()
	return w, nil
}

func (w *wal) helper() {
	runtime.LockOSThread()
	var b [8]byte
	for {
		if _, err := unix.Read(w.kickFD, b[:]); err != nil {
			if err == unix.EINTR {
				continue
			}
			panic(err)
		}
		target := w.want.Load()
		for {
			err := unix.Fdatasync(w.fd)
			if err == nil {
				break
			}
			if err != unix.EINTR {
				panic(err)
			}
		}
		w.done.Store(target)
		binary.LittleEndian.PutUint64(b[:], 1)
		_, _ = unix.Write(w.wakeFD, b[:])
	}
}

// kick starts an fsync if none runs and appends wait.
func (w *wal) kick() {
	if w.inflight || len(w.pending) == 0 {
		return
	}
	stats.fsyncs++
	w.want.Store(w.off)
	w.inflight = true
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = unix.Write(w.kickFD, b[:])
}

// offOf returns where entry index's data lies in the WAL.
func (w *wal) offOf(index uint64) int64 {
	e := w.idx[index&(idxSlots-1)]
	if e.index != index {
		return -1
	}
	return e.off
}

const walEntHdr = 4 + 8 + 8 + 1

// write appends entries and a hard state to the file, without sync.
func (w *wal) write(ents []*pb.Entry, hs []byte) {
	w.iov = w.iov[:0]
	need := len(ents)*walEntHdr + len(hs) + 8
	if cap(w.hdrs) < need {
		w.hdrs = make([]byte, 0, 2*need)
	}
	h := w.hdrs[:need]
	at := 0
	off := w.off
	for _, e := range ents {
		d := e.GetData()
		b := h[at : at+walEntHdr]
		binary.LittleEndian.PutUint32(b, uint32(len(d)))
		binary.LittleEndian.PutUint64(b[4:], e.GetIndex())
		binary.LittleEndian.PutUint64(b[12:], e.GetTerm())
		b[20] = byte(e.GetType())
		at += walEntHdr
		w.iov = append(w.iov, b)
		off += walEntHdr
		w.idx[e.GetIndex()&(idxSlots-1)] = idxOff{index: e.GetIndex(), off: off}
		if len(d) > 0 {
			w.iov = append(w.iov, d)
			off += int64(len(d))
		}
	}
	if len(hs) > 0 {
		b := h[at : at+8+len(hs)]
		binary.LittleEndian.PutUint32(b, uint32(len(hs)))
		binary.LittleEndian.PutUint32(b[4:], 0xffffffff)
		copy(b[8:], hs)
		w.iov = append(w.iov, b)
		off += int64(len(b))
	}
	for len(w.iov) > 0 {
		k := min(len(w.iov), 1024)
		stats.walWrites++
		n, err := unix.Pwritev(w.fd, w.iov[:k], w.off)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			panic(err)
		}
		w.off += int64(n)
		stats.walBytes += uint64(n)
		// A short write leaves the rest of the vectors.
		for n > 0 {
			if n >= len(w.iov[0]) {
				n -= len(w.iov[0])
				w.iov = w.iov[1:]
			} else {
				w.iov[0] = w.iov[0][n:]
				n = 0
			}
		}
	}
	if w.off != off {
		panic("wal offset")
	}
}

func raftConfig(id uint64, st raft.Storage) *raft.Config {
	return &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   st,
		AsyncStorageWrites:        true,
		MaxSizePerMsg:             8 << 20,
		MaxInflightMsgs:           1024,
		MaxUncommittedEntriesSize: 1 << 30,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
}

// ready handles everything Raft has to do now.
func (n *node) ready() {
	for i := 0; i < 64 && n.rn.HasReady(); i++ {
		rd := n.rn.Ready()
		if rd.SoftState != nil {
			n.lead = rd.SoftState.Lead
			if rd.SoftState.RaftState == raft.StateLeader {
				isLeader.Store(1)
			} else {
				isLeader.Store(0)
			}
		}
		for _, rs := range rd.ReadStates {
			n.readState(rs.RequestCtx, rs.Index)
		}
		for _, m := range rd.Messages {
			switch m.GetTo() {
			case raft.LocalAppendThread:
				n.storageAppend(m)
			case raft.LocalApplyThread:
				n.storageApply(m)
			default:
				n.sendPeer(m)
			}
		}
		if len(rd.ReadStates) > 0 {
			n.serveReads()
		}
	}
}

func (n *node) storageAppend(m *pb.Message) {
	ents := m.GetEntries()
	if len(ents) > 0 {
		if err := n.storage.Append(ents); err != nil {
			panic(err)
		}
	}
	var hs []byte
	if m.Term != nil {
		n.term = m.GetTerm()
		st := &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
		if err := n.storage.SetHardState(st); err != nil {
			panic(err)
		}
		n.commit = m.GetCommit()
		var err error
		hs, err = proto.Marshal(st)
		if err != nil {
			panic(err)
		}
	}
	if len(ents) > 0 || len(hs) > 0 {
		n.wal.write(ents, hs)
	}
	n.wal.pending = append(n.wal.pending, walJob{end: n.wal.off, resps: m.Responses})
	if !n.wal.inflight && n.wal.done.Load() >= n.wal.off {
		n.synced()
		return
	}
	n.wal.kick()
}

// synced delivers the responses of the appends that are durable now.
func (n *node) synced() {
	w := n.wal
	done := w.done.Load()
	k := 0
	for ; k < len(w.pending) && w.pending[k].end <= done; k++ {
		for _, r := range w.pending[k].resps {
			if r.GetTo() == n.id {
				_ = n.rn.Step(r)
			} else {
				n.sendPeer(r)
			}
		}
		w.pending[k] = walJob{}
	}
	w.pending = append(w.pending[:0], w.pending[k:]...)
	w.kick()
}

// fsyncDone is the helper's wake-up.
func (n *node) fsyncDone() {
	var b [8]byte
	_, _ = unix.Read(n.wal.wakeFD, b[:])
	n.wal.inflight = false
	n.synced()
}

func (n *node) storageApply(m *pb.Message) {
	for _, e := range m.GetEntries() {
		idx := e.GetIndex()
		switch e.GetType() {
		case pb.EntryConfChange:
			var cc pb.ConfChange
			if err := proto.Unmarshal(e.GetData(), &cc); err == nil {
				n.rn.ApplyConfChange(&cc)
			}
		case pb.EntryConfChangeV2:
			var cc pb.ConfChangeV2
			if err := proto.Unmarshal(e.GetData(), &cc); err == nil {
				n.rn.ApplyConfChange(&cc)
			}
		case pb.EntryNormal:
			if d := e.GetData(); len(d) > 0 {
				n.applyBatch(d, n.wal.offOf(idx))
			}
		}
		n.applied = idx
	}
	n.flushEvents()
	n.st.endApply()
	n.serveReads()
	for _, r := range m.Responses {
		_ = n.rn.Step(r)
	}
	// Keep a window of entries for followers that lag, drop the rest.
	if first, _ := n.storage.FirstIndex(); n.applied > first+20000 {
		_ = n.storage.Compact(n.applied - 10000)
	}
}

var isLeader atomic.Int64
var peerSentBytes atomic.Uint64

// ---- peer messages

// msgHdr: type, flags, from, term, logTerm, index, commit, rejectHint,
// vote, entry count.
const msgHdr = 1 + 1 + 8*7 + 4

// sendPeer writes m into the send buffer of its peer.
func (n *node) sendPeer(m *pb.Message) {
	stats.peerMsgs++
	p := n.peersOut[m.GetTo()]
	if p == nil || p.dead {
		return
	}
	if m.GetType() == pb.MsgSnap {
		n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
		return
	}
	ents := m.GetEntries()
	app := m.GetType() == pb.MsgApp
	to := m.GetTo()
	size := msgHdr + 4 + len(m.GetContext())
	for _, e := range ents {
		size += 8 + 8 + 1 + 4
		if o, _, ok := batchOrigin(e.GetData()); !app || !ok || o != to {
			size += len(e.GetData())
		}
	}
	var h [4 + msgHdr]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(size))
	b := h[4:]
	b[0] = byte(m.GetType())
	if m.GetReject() {
		b[1] = 1
	}
	binary.LittleEndian.PutUint64(b[2:], m.GetFrom())
	binary.LittleEndian.PutUint64(b[10:], m.GetTerm())
	binary.LittleEndian.PutUint64(b[18:], m.GetLogTerm())
	binary.LittleEndian.PutUint64(b[26:], m.GetIndex())
	binary.LittleEndian.PutUint64(b[34:], m.GetCommit())
	binary.LittleEndian.PutUint64(b[42:], m.GetRejectHint())
	binary.LittleEndian.PutUint64(b[50:], m.GetVote())
	binary.LittleEndian.PutUint32(b[58:], uint32(len(ents)))
	s := &p.sock
	s.put(h[:])
	var eh [21]byte
	for _, e := range ents {
		d := e.GetData()
		binary.LittleEndian.PutUint64(eh[0:], e.GetTerm())
		binary.LittleEndian.PutUint64(eh[8:], e.GetIndex())
		eh[16] = byte(e.GetType())
		if o, seq, ok := batchOrigin(d); app && ok && o == to {
			// The peer proposed this batch: name it.
			eh[16] |= namedEntry
			binary.LittleEndian.PutUint32(eh[17:], seq)
			s.put(eh[:])
			continue
		}
		binary.LittleEndian.PutUint32(eh[17:], uint32(len(d)))
		s.put(eh[:])
		s.put(d)
	}
	var cl [4]byte
	binary.LittleEndian.PutUint32(cl[:], uint32(len(m.GetContext())))
	s.put(cl[:])
	s.put(m.GetContext())
	n.markDirty(s)
}

// put appends p to the record stream.
func (s *sock) put(p []byte) {
	for len(p) > 0 {
		if s.room() == 0 {
			s.seal()
			s.openRec()
		}
		k := min(len(p), s.room())
		s.out = append(s.out, p[:k]...)
		p = p[k:]
	}
}

// peerFrames reads the messages in pt and steps them.
func (n *node) peerFrames(p *peerIn) error {
	s := &p.sock
	if !p.hello {
		if s.pw-s.pr < 8 {
			return nil
		}
		p.from = binary.LittleEndian.Uint64(s.pt[s.pr:])
		s.pr += 8
		p.hello = true
	}
	for {
		avail := s.pw - s.pr
		if avail < 4 {
			return nil
		}
		l := int(binary.LittleEndian.Uint32(s.pt[s.pr:]))
		if avail < 4+l {
			return nil
		}
		f := s.pt[s.pr+4 : s.pr+4+l]
		s.pr += 4 + l
		m, err := n.decodeMsg(f)
		if err == errUnnamed {
			continue
		}
		if err != nil {
			return err
		}
		m.To = new(n.id)
		_ = n.rn.Step(m)
	}
}

func (n *node) decodeMsg(f []byte) (*pb.Message, error) {
	if len(f) < msgHdr+4 {
		return nil, errRecord
	}
	m := &pb.Message{
		Type:       pb.MessageType(f[0]).Enum(),
		From:       new(binary.LittleEndian.Uint64(f[2:])),
		Term:       new(binary.LittleEndian.Uint64(f[10:])),
		LogTerm:    new(binary.LittleEndian.Uint64(f[18:])),
		Index:      new(binary.LittleEndian.Uint64(f[26:])),
		Commit:     new(binary.LittleEndian.Uint64(f[34:])),
		RejectHint: new(binary.LittleEndian.Uint64(f[42:])),
		Vote:       new(binary.LittleEndian.Uint64(f[50:])),
	}
	if f[1]&1 != 0 {
		m.Reject = new(true)
	}
	k := int(binary.LittleEndian.Uint32(f[58:]))
	at := msgHdr
	if k > 0 {
		m.Entries = make([]*pb.Entry, k)
		for i := range k {
			if len(f) < at+21 {
				return nil, errRecord
			}
			l := int(binary.LittleEndian.Uint32(f[at+17:]))
			typ := f[at+16]
			e := &pb.Entry{
				Term:  new(binary.LittleEndian.Uint64(f[at:])),
				Index: new(binary.LittleEndian.Uint64(f[at+8:])),
				Type:  pb.EntryType(typ &^ namedEntry).Enum(),
			}
			at += 21
			if typ&namedEntry != 0 {
				seq := uint32(l)
				b := n.fwd[seq&(fwdSlots-1)]
				if b.seq != seq || b.data == nil {
					return nil, errUnnamed
				}
				e.Data = b.data
				m.Entries[i] = e
				continue
			}
			if len(f) < at+l {
				return nil, errRecord
			}
			if l > 0 {
				e.Data = n.rxCopy(f[at : at+l])
			}
			at += l
			m.Entries[i] = e
		}
	}
	if len(f) < at+4 {
		return nil, errRecord
	}
	cl := int(binary.LittleEndian.Uint32(f[at:]))
	at += 4
	if cl > 0 {
		m.Context = append([]byte(nil), f[at:at+cl]...)
	}
	return m, nil
}

const namedEntry = 0x80

var errUnnamed = errors.New("a named entry this node does not have")

// rxCopy copies received entry data into the receive ring.
func (n *node) rxCopy(p []byte) []byte {
	if n.rxPos+len(p) > len(n.rx) {
		n.rxPos = 0
	}
	d := n.rx[n.rxPos : n.rxPos+len(p) : n.rxPos+len(p)]
	copy(d, p)
	n.rxPos += len(p)
	return d
}
