package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
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
func (n *node) ready() error {
	for i := 0; i < 64 && n.rn.HasReady(); i++ {
		rd := n.rn.Ready()
		if rd.SoftState != nil {
			was := n.leading()
			if rd.Lead != n.lead {
				n.logf("the leader is %d", rd.Lead)
			}
			n.lead = rd.Lead
			hasLeader := int64(0)
			if n.lead != 0 {
				hasLeader = 1
			}
			n.metrics.hasLeader.Store(hasLeader)
			if n.leading() != was {
				n.leaderChanged()
			}
			if rd.RaftState == raft.StateLeader {
				n.metrics.isLeader.Store(1)
			} else {
				n.metrics.isLeader.Store(0)
			}
		}
		for _, rs := range rd.ReadStates {
			n.readState(rs.RequestCtx, rs.Index)
		}
		for _, m := range rd.Messages {
			var err error
			switch m.GetTo() {
			case raft.LocalAppendThread:
				err = n.storageAppend(m)
			case raft.LocalApplyThread:
				err = n.storageApply(m)
			default:
				n.sendPeer(m)
			}
			if err != nil {
				return err
			}
		}
		if len(rd.ReadStates) > 0 {
			n.serveReads()
		}
	}
	return nil
}

// storageAppend writes an append into the stage, or, while the stage has
// no room for it, keeps it and those after it in order until the disk
// catches up.
func (n *node) storageAppend(m *pb.Message) error {
	n.nameEntries(m.GetEntries(), spanAppend)
	w := n.wal
	if len(w.waiting) > 0 || !w.fits(m.GetEntries()) {
		w.stats.waits++
		w.waiting = append(w.waiting, m)
		w.kick()
		return nil
	}
	return n.appendNow(m)
}

// unwait writes the kept appends that fit now.
func (n *node) unwait() error {
	w := n.wal
	k := 0
	for ; k < len(w.waiting) && w.fits(w.waiting[k].GetEntries()); k++ {
		if err := n.appendNow(w.waiting[k]); err != nil {
			return err
		}
		w.waiting[k] = nil
	}
	w.waiting = append(w.waiting[:0], w.waiting[k:]...)
	return nil
}

// appendNow writes an append into the stage, and hands its entries to
// Raft's storage with where the log holds their data. A snapshot it
// carries is installed first.
func (n *node) appendNow(m *pb.Message) error {
	if sn := m.GetSnapshot(); sn != nil && sn.GetMetadata().GetIndex() > 0 {
		if err := n.installSnapshot(sn); err != nil {
			return err
		}
	}
	ents := m.GetEntries()
	var hs []byte
	var st *pb.HardState
	if m.Term != nil {
		st = &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
		var err error
		if hs, err = proto.Marshal(st); err != nil {
			return err
		}
	}
	if len(ents) > 0 || len(hs) > 0 {
		if err := n.wal.write(ents, hs); err != nil {
			return err
		}
	}
	if err := n.storage.append(ents, n.wal.offs); err != nil {
		return err
	}
	n.nameEntries(ents, spanStored)
	if st != nil {
		n.term = st.GetTerm()
		n.commit = st.GetCommit()
		n.storage.setHardState(st)
	}
	n.wal.pending = append(n.wal.pending, walJob{end: n.wal.off, resps: m.Responses, hs: st})
	if !n.wal.inflight && n.wal.done.Load() >= n.wal.off {
		n.synced()
		return nil
	}
	n.wal.kick()
	return nil
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
		w.stable = w.pending[k].end
		if hs := w.pending[k].hs; hs != nil {
			w.durableHS = hs
		}
		w.pending[k] = walJob{}
	}
	w.pending = append(w.pending[:0], w.pending[k:]...)
	w.kick()
}

// fsyncDone is the helper's wake-up.
func (n *node) fsyncDone() error {
	var b [8]byte
	_, _ = unix.Read(n.wal.wakeFD, b[:])
	if err := n.wal.failed.Load(); err != nil {
		return *err
	}
	n.wal.inflight = false
	n.synced()
	return n.unwait()
}

func (n *node) storageApply(m *pb.Message) error {
	for _, e := range m.GetEntries() {
		idx := e.GetIndex()
		switch e.GetType() {
		case pb.EntryConfChange:
			var cc pb.ConfChange
			if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
				return fmt.Errorf("apply a membership change: %w", err)
			}
			if err := n.applyConfChange(&cc, idx); err != nil {
				return err
			}
		case pb.EntryConfChangeV2:
			// Nothing proposes one: the members change one at a time.
			return fmt.Errorf("apply entry %d: a ConfChangeV2", idx)
		case pb.EntryNormal:
			if d := e.GetData(); len(d) > 0 {
				if err := n.applyBatch(d, n.storage.offOf(idx)); err != nil {
					return err
				}
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
	return n.afterApply()
}

// afterApply keeps a window of entries for followers that lag and drops
// the rest up to the newest checkpoint, removes the log's segments before
// what the node still reads, and starts a checkpoint once the log has
// grown enough since the last.
func (n *node) afterApply() error {
	s := n.storage
	if first, _ := s.FirstIndex(); n.applied > first+2*storageWindow {
		s.compact(n.applied - storageWindow)
	}
	if s.snap != nil {
		if err := n.wal.cut(min(n.ckptCut, s.firstRecord(n.wal.off))); err != nil {
			return err
		}
	}
	return n.maybeCkpt()
}

// maybeCkpt starts a checkpoint once the log has grown enough since the
// last, or when Raft's storage wants one for a follower.
func (n *node) maybeCkpt() error {
	s := n.storage
	if n.ckpt != nil || n.ckptDir == "" || n.wal.off-n.ckptFrom < ckptMin && !s.wantCkpt {
		return nil
	}
	off := s.offOf(n.applied)
	if off < 0 {
		return nil
	}
	term, err := s.Term(n.applied)
	if err != nil {
		return err
	}
	s.wantCkpt = false
	return n.startCkpt(n.applied, term, off-walEntHdr)
}

// ---- peer messages

// msgHdr: type, flags, from, term, logTerm, index, commit, rejectHint,
// vote, entry count.
const msgHdr = 1 + 1 + 8*7 + 4

// sendPeer writes m into the send buffer of its peer.
func (n *node) sendPeer(m *pb.Message) {
	n.stats.peerMsgs++
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
		n.sendSnapshot(m)
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
			n.stats.named++
			eh[16] |= namedEntry
			binary.LittleEndian.PutUint32(eh[17:], seq)
			s.put(eh[:])
			var ob [4]byte
			binary.LittleEndian.PutUint32(ob[:], uint32(o))
			s.put(ob[:])
			continue
		}
		binary.LittleEndian.PutUint32(eh[17:], uint32(len(d)))
		n.stats.carried[m.GetType()]++
		n.stats.carriedBytes[m.GetType()] += len(d)
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
		if len(p.held) > 0 && (len(f) < 9 || f[0] != frameBatch) {
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
			b.seq, b.data = seq, n.rxCopy(f[9:], span{kind: spanUnnamed, at: n.now, fwd: true, origin: origin, seq: seq})
			continue
		}
		m, err := n.decodeMsg(f)
		if errors.Is(err, errUnnamed) {
			// The batch it names comes on another connection, likely in
			// this same turn: try again after the turn's input.
			n.stats.held++
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
				e.Data = n.rxCopy(f[at:at+l], entrySpan(m.GetType(), n.now))
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

// entrySpan is the span of an entry's data received in a message of type
// typ. A leader's Raft copies a proposal's entries as it steps them, and a
// follower's sends them on in this turn's Ready: no append ever holds the
// received bytes of a MsgProp.
func entrySpan(typ pb.MessageType, now time.Time) span {
	if typ == pb.MsgProp {
		return span{kind: spanFree}
	}
	return span{kind: spanUnnamed, at: now}
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
			if errors.Is(err, errUnnamed) && k == 0 && time.Since(p.heldSince) > 100*time.Millisecond {
				// The batch never came, as when its proposer had no
				// connection to this node yet: drop the append and ask
				// the leader to carry batches for a while. Raft resends
				// what this node then lacks.
				n.askCarry(p.from)
				continue
			}
			if errors.Is(err, errUnnamed) {
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

// rxCopy copies received entry data into the receive ring as span s, or
// to the heap when the ring has no room.
func (n *node) rxCopy(p []byte, s span) []byte {
	if len(p) == 0 {
		return nil
	}
	room := n.rx.reserve(len(p))
	if room == nil {
		n.rx.stats.heap++
		return append([]byte(nil), p...)
	}
	room = slices.Clip(append(room, p...))
	n.rx.commit(room, s)
	return room
}

// dropBatches, from WL_DROP_BATCHES, loses some follower batches on the way
// to the other peers, to test that the leader then carries them.
var dropBatches = os.Getenv("WL_DROP_BATCHES") != ""
