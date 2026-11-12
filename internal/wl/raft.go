package wl

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"time"

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
	// stage mirrors the newest part of the log in page-aligned memory:
	// the loop copies each append there, and the helper writes its
	// blocks to the file with O_DIRECT and O_DSYNC, so the kernel neither
	// copies the log into its page cache nor writes it back.
	stage   []byte
	flushed int64 // the helper's: the log is durable up to here
	// lastKick is when the last sync started; deferred, that appends wait
	// for the next one at lastKick + syncGap.
	lastKick time.Time
	deferred bool
}

const syncGap = 800 * time.Microsecond

const (
	stageSize = 64 << 20
	block     = 4096
)

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
	// The log is a ring in a file written once with zeros: a sync into
	// blocks that are already allocated and written changes no metadata,
	// so ext4 commits no journal for it. Main's log gets the same from
	// the spare segments it recycles.
	zero := make([]byte, 4<<20)
	for off := int64(0); off < walRing; off += int64(len(zero)) {
		if _, err := unix.Pwrite(fd, zero, off); err != nil {
			return nil, err
		}
	}
	if err := unix.Fdatasync(fd); err != nil {
		return nil, err
	}
	_ = unix.Close(fd)
	if w.fd, err = unix.Open(path, unix.O_WRONLY|unix.O_DIRECT|unix.O_DSYNC|unix.O_CLOEXEC, 0); err != nil {
		return nil, err
	}
	w.stage = mmapBytes(stageSize)
	go w.helper()
	return w, nil
}

// walRing is the size of the log file; offsets in the log grow without
// end and wrap around in the file.
const walRing = 768 << 20

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
		// The blocks from the one that holds the durable end to the one
		// that holds target; the last is written again next time.
		from := w.flushed &^ (block - 1)
		to := (target + block - 1) &^ (block - 1)
		for from < to {
			pos := from % stageSize
			n := min(to-from, stageSize-pos)
			k, err := unix.Pwrite(w.fd, w.stage[pos:pos+n], from%walRing)
			if err == unix.EINTR {
				continue
			}
			if err != nil {
				panic(err)
			}
			from += int64(k)
		}
		w.flushed = target
		w.done.Store(target)
		binary.LittleEndian.PutUint64(b[:], 1)
		_, _ = unix.Write(w.wakeFD, b[:])
	}
}

// kick starts an fsync if none runs and appends wait, at most one every
// syncGap: under load each sync takes more appends, and the loop wakes
// for fewer completions, so its turns and the acks they send grow.
func (w *wal) kick() {
	if w.inflight || len(w.pending) == 0 {
		return
	}
	now := time.Now()
	if d := now.Sub(w.lastKick); d < syncGap {
		w.deferred = true
		return
	}
	w.deferred = false
	w.lastKick = now
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
	if w.off+int64(stageSize/2) < 0 || off-w.done.Load() > stageSize-block {
		panic("the log ran a whole stage ahead of the disk")
	}
	for _, v := range w.iov {
		for len(v) > 0 {
			pos := w.off % stageSize
			k := copy(w.stage[pos:], v)
			v = v[k:]
			w.off += int64(k)
			stats.walBytes += uint64(k)
		}
	}
	stats.walWrites++
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
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
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
	if m.GetType() == pb.MsgApp && len(p.out)-p.ow > peerBacklog {
		// The peer takes less than it is sent, as one that catches up:
		// Raft probes it again instead of piling up appends here.
		n.rn.ReportUnreachable(m.GetTo())
		return
	}
	if m.GetType() == pb.MsgSnap {
		n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
		return
	}
	ents := m.GetEntries()
	app := m.GetType() == pb.MsgApp
	to := m.GetTo()
	carry := time.Now().Before(n.carryUntil[to])
	size := msgHdr + 4 + len(m.GetContext())
	for _, e := range ents {
		size += 8 + 8 + 1 + 4
		if o, _, ok := batchOrigin(e.GetData()); app && ok && o != n.id && (o == to || !carry) {
			size += 4
		} else {
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
		if o, seq, ok := batchOrigin(d); app && ok && o != n.id && (o == to || !carry) {
			// A follower proposed this batch and sent it to every peer
			// itself: name it.
			dbgNamed++
			eh[16] |= namedEntry
			binary.LittleEndian.PutUint32(eh[17:], seq)
			s.put(eh[:])
			var ob [4]byte
			binary.LittleEndian.PutUint32(ob[:], uint32(o))
			s.put(ob[:])
			continue
		}
		binary.LittleEndian.PutUint32(eh[17:], uint32(len(d)))
		dbgCarried[m.GetType()]++
		dbgCarriedB[m.GetType()] += len(d)
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
		if len(p.held) > 0 && !(len(f) >= 9 && f[0] == frameBatch) {
			// Messages from a peer keep their order behind one held.
			p.held = append(p.held, append([]byte(nil), f...))
			continue
		}
		if len(f) == 1 && f[0] == frameCarry {
			// A follower lacks a batch the leader named: carry them to it.
			n.carryUntil[p.from] = time.Now().Add(time.Second)
			continue
		}
		if len(f) >= 9 && f[0] == frameBatch {
			// A batch a follower proposed, sent ahead of the leader's
			// append that names it.
			origin := uint64(binary.LittleEndian.Uint32(f[1:]))
			seq := binary.LittleEndian.Uint32(f[5:])
			b := &n.fwdOf(origin)[seq&(fwdSlots-1)]
			b.seq, b.data = seq, n.rxCopy(f[9:])
			continue
		}
		m, err := n.decodeMsg(f)
		if err == errUnnamed {
			// The batch it names comes on another connection, likely in
			// this same turn: try again after the turn's input.
			dbgHeld++
			if len(p.held) == 0 {
				p.heldSince = time.Now()
			}
			p.held = append(p.held, append([]byte(nil), f...))
			n.holding = true
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
				if len(f) < at+4 {
					return nil, errRecord
				}
				origin := uint64(binary.LittleEndian.Uint32(f[at:]))
				at += 4
				b := n.fwdOf(origin)[seq&(fwdSlots-1)]
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

// peerBacklog is the most a peer's send buffer holds before appends to it
// wait for Raft's next probe.
const peerBacklog = 64 << 20

// frameBatch marks a peer frame that carries a follower's batch;
// frameCarry asks the leader to carry batches instead of naming them.
const (
	frameBatch = 0xff
	frameCarry = 0xfe
)

func (n *node) askCarry(leader uint64) {
	if p := n.peersOut[leader]; p != nil && !p.dead {
		p.put([]byte{1, 0, 0, 0, frameCarry})
		n.markDirty(&p.sock)
	}
}

// sendBatch sends a batch this follower proposes to every peer but the
// leader, which gets it in the proposal.
func (n *node) sendBatch(data []byte) {
	seq := binary.LittleEndian.Uint32(data[4:])
	if dropBatches && seq%50 == 0 {
		return // a test of the leader's fallback to carrying batches
	}
	for id, p := range n.peersOut {
		if id == n.lead || p.dead {
			continue
		}
		var h [13]byte
		binary.LittleEndian.PutUint32(h[0:], uint32(9+len(data)))
		h[4] = frameBatch
		binary.LittleEndian.PutUint32(h[5:], uint32(n.id))
		binary.LittleEndian.PutUint32(h[9:], seq)
		p.put(h[:])
		p.put(data)
		n.markDirty(&p.sock)
	}
}

// fwdOf returns the table of batches that node origin proposed.
func (n *node) fwdOf(origin uint64) []fwdBatch {
	if origin == n.id {
		return n.fwd
	}
	t, ok := n.fwdPeers[origin]
	if !ok {
		t = make([]fwdBatch, fwdSlots)
		n.fwdPeers[origin] = t
	}
	return t
}

var errUnnamed = errors.New("a named entry this node does not have")

// retryHeld decodes again, in order, the messages held behind an append
// that named a batch not here yet.
func (n *node) retryHeld() {
	n.holding = false
	for _, x := range n.socks {
		p, ok := x.(*peerIn)
		if !ok || len(p.held) == 0 {
			continue
		}
		k := 0
		for ; k < len(p.held); k++ {
			m, err := n.decodeMsg(p.held[k])
			if err == errUnnamed && k == 0 && time.Since(p.heldSince) > 100*time.Millisecond {
				// The batch never came, as when its proposer had no
				// connection to this node yet: drop the append and ask
				// the leader to carry batches for a while. Raft resends
				// what this node then lacks.
				n.askCarry(p.from)
				continue
			}
			if err == errUnnamed {
				break
			}
			if err == nil {
				m.To = new(n.id)
				_ = n.rn.Step(m)
			}
		}
		clear(p.held[:k])
		p.held = append(p.held[:0], p.held[k:]...)
		if k > 0 {
			p.heldSince = time.Now()
		}
		if len(p.held) > 0 {
			n.holding = true
		}
	}
}

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

var dbgNamed int
var dbgCarried, dbgCarriedB [32]int
var dbgHeld int

// dropBatches, from WL_DROP_BATCHES, loses some follower batches on the way
// to the other peers, to test that the leader then carries them.
var dropBatches = os.Getenv("WL_DROP_BATCHES") != ""
