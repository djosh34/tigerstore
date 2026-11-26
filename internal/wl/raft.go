package wl

import (
	"bytes"
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
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/planted"
)

// Raft runs inline in the loop: etcd/raft's RawNode with asynchronous
// storage writes. An append is one pwritev of the entries straight from
// the log ring; one helper thread runs fdatasync and wakes the loop
// through an eventfd; the append's responses are stepped once it is
// durable.

// raftConfig is Raft's configuration of node id, with the buffers of z
// in a cluster that starts with members.
func raftConfig(id uint64, st raft.Storage, z sizes, members int) *raft.Config {
	return &raft.Config{
		ID:                        id,
		ElectionTick:              electionTicks,
		HeartbeatTick:             1,
		Storage:                   st,
		AsyncStorageWrites:        true,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		MaxInflightBytes:          z.inflight(members),
		MaxUncommittedEntriesSize: z.uncommitted,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
}

// ready handles everything Raft has to do now.
func (n *node) ready() error {
	rt := &n.turnReady
	for i := 0; i < 64 && n.rn.HasReady(); i++ {
		t := time.Now()
		rd := n.rn.Ready()
		rt.raft += time.Since(t)
		if err := n.storage.failed; err != nil {
			// What Raft read of the storage lacks an entry's data: nothing it
			// made of it may be sent, persisted or applied.
			return err
		}
		if n.putTimings != nil {
			// This Ready first hands committed entries to the local apply
			// thread. Observe that before its append/send/apply work.
			for _, m := range rd.Messages {
				if m.GetTo() == raft.LocalApplyThread {
					for _, e := range m.GetEntries() {
						n.tracePutBatch(e.GetData(), "commit", e.GetIndex())
					}
				}
			}
		}
		if rd.SoftState != nil {
			was := n.leading()
			if rd.Lead != n.lead {
				n.logf("the leader is %d at term %d", rd.Lead, n.rn.BasicStatus().GetTerm())
				if rd.Lead != 0 {
					n.gs.leaderChanges++
				}
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
			t := time.Now()
			switch m.GetTo() {
			case raft.LocalAppendThread:
				if len(n.votes) > 0 {
					m.Responses = append(m.Responses, n.votes...)
					clear(n.votes)
					n.votes = n.votes[:0]
				}
				err = n.storageAppend(m)
				rt.appends++
				rt.append += time.Since(t)
			case raft.LocalApplyThread:
				err = n.storageApply(m)
				rt.applies++
				rt.apply += time.Since(t)
			default:
				if m.GetType() == pb.MsgVote {
					// Sent once the campaign's term and vote are durable
					// (node.votes).
					n.votes = append(n.votes, m)
					continue
				}
				n.sendPeer(m)
				rt.send += time.Since(t)
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
	// Noted as Raft hands them out, also when they wait for the stage:
	// a change Raft holds is held for before the next goes (#689's
	// area-2 review).
	n.confAppended(m.GetEntries())
	// What Raft hands the log here holds every write and append taken so
	// far but the open batch's, as the turn's batch is proposed before its
	// Ready: it is in the log or waits for the stage now, which count it
	// (ringTaken, followerDrops). An append written in the turn's input,
	// as the stage drains, does not: the turn's writes taken before it are
	// in no append yet. A new pledge epoch begins: a batch of the one
	// before releases nothing (releasePledge).
	n.ringPledged, n.appendPledged, n.appendLast = n.batchPledge, 0, 0
	n.pledgeEpoch++
	w := n.wal
	if len(w.waiting) > 0 || !w.fits(m.GetEntries()) {
		w.stats.waits++
		n.turnReady.waits++
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
	var hs []byte
	var st *pb.HardState
	if m.Term != nil {
		st = &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
		var err error
		if hs, err = proto.Marshal(st); err != nil {
			return err
		}
	}
	// An append that only moves the commit (commitMove) and owes no
	// response is written and not synced: Raft asks no durability of an
	// append that carries no response (Config.AsyncStorageWrites), as a
	// leader's commit's. It is no job the log, or the simulator, waits
	// on. One that owes responses, a follower's, is held: its sync waits,
	// at most commitHold, for the next append's, which covers it (#802).
	move := n.commitMove(m, st)
	skip := move && len(m.Responses) == 0
	if !skip {
		n.simAppend(m)
	}
	if sn := m.GetSnapshot(); sn != nil && sn.GetMetadata().GetIndex() > 0 {
		t := time.Now()
		err := n.installSnapshot(sn)
		n.turnReady.install += time.Since(t)
		if err != nil {
			return err
		}
	}
	ents := m.GetEntries()
	n.noteMustSync(ents)
	dataEnd := n.wal.dataEnd
	if len(ents) > 0 || len(hs) > 0 {
		t := time.Now()
		err := n.wal.write(ents, hs)
		n.turnReady.write += time.Since(t)
		if err != nil {
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
	if skip {
		// Its record goes durable with the next sync or the idle mark
		// (markIdle). Synced, it took a sync of its own at every commit,
		// which the next write's sync waited behind (#802). A commit lost
		// to a crash is learned again from the leader; one whose range
		// holds what the replay of the log must keep is never skipped
		// (commitMove). Nothing relies on the record being durable, so no
		// mark need claim it (dataEnd).
		n.wal.dataEnd = dataEnd
		n.wal.stats.skipped++
		// A sync takes it commitHold later at most, if none came before
		// (kick): the loop wakes for it.
		if n.wal.skippedAt.IsZero() {
			n.wal.skippedAt = time.Now()
		}
		n.wal.kick()
		return nil
	}
	n.wal.pending = append(n.wal.pending, walJob{end: n.wal.off, resps: m.Responses, hs: st, at: time.Now(), ents: len(ents) > 0, held: move})
	if !n.wal.inflight && n.wal.done.Load() >= n.wal.off || n.plantedEarlyDurable() {
		n.synced()
		return nil
	}
	n.wal.kick()
	return nil
}

// commitMove reports whether append m, which writes hard state st, only
// moves the commit: it carries no entry and no snapshot, its hard
// state's term and vote are the durable one's, and the range it commits
// past the durable commit holds no entry the log's replay must keep
// (mustSyncCommit). Such an append's sync may be skipped or held
// (appendNow).
func (n *node) commitMove(m *pb.Message, st *pb.HardState) bool {
	d := n.wal.durableHS
	if len(m.GetEntries()) > 0 || m.GetSnapshot().GetMetadata().GetIndex() > 0 || st == nil || d == nil {
		return false
	}
	return st.GetTerm() == d.GetTerm() && st.GetVote() == d.GetVote() && !n.mustSyncCommit(d.GetCommit(), st.GetCommit())
}

// A node rebuilds what it applied from its newest checkpoint and the log's
// entries up to the durable commit. An entry whose apply a node answers
// for, and that no replay to a lower commit may leave out, is one whose
// commit is synced: a change of members, whose replay to a commit before
// it leaves a survivor of a removal waiting for the removed member's vote,
// and an auth op, whose replay before it serves reads with the auth of
// before (#821's review). So are the entries a node found in its log at
// its start, whose kind it did not read (recoveredLast).

// noteMustSync notes the entries of ents whose commit is synced
// (mustSyncCommit).
func (n *node) noteMustSync(ents []*pb.Entry) {
	for _, e := range ents {
		if t := e.GetType(); t == pb.EntryConfChange || t == pb.EntryConfChangeV2 || batchHasAuth(e.GetData()) {
			n.mustSync = append(n.mustSync, e.GetIndex())
		}
	}
}

// mustSyncCommit reports whether a commit moved from from to to covers an
// entry whose commit is synced.
func (n *node) mustSyncCommit(from, to uint64) bool {
	if to <= from {
		return false
	}
	if from < n.recoveredLast {
		return true
	}
	for _, i := range n.mustSync {
		if i > from && i <= to {
			return true
		}
	}
	return false
}

// batchHasAuth reports whether batch data holds an auth op.
func batchHasAuth(data []byte) bool {
	if !isBatch(data) {
		return false
	}
	for p := batchHdr; p+opHdr <= len(data); {
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		if data[p] == kAuthOp {
			return true
		}
		p += opHdr + l
	}
	return false
}

// deliver steps or sends an append's responses.
func (n *node) deliver(resps []*pb.Message) {
	for _, r := range resps {
		if r.GetTo() == n.id {
			_ = n.rn.Step(r)
		} else {
			n.sendPeer(r)
		}
	}
}

// synced delivers the responses of the appends that are durable now.
func (n *node) synced() {
	w := n.wal
	done := w.done.Load()
	if n.plantedEarlyDurable() {
		done = w.off
	}
	k := 0
	for ; k < len(w.pending) && w.pending[k].end <= done; k++ {
		n.deliver(w.pending[k].resps)
		n.simDurable(&w.pending[k])
		w.stable = w.pending[k].end
		if hs := w.pending[k].hs; hs != nil {
			w.durableHS = hs
		}
		w.pending[k] = walJob{}
	}
	if k > 0 {
		w.progressAt = n.now
		// What the durable commit covers needs its sync no more.
		if c := w.durableHS.GetCommit(); len(n.mustSync) > 0 {
			n.mustSync = slices.DeleteFunc(n.mustSync, func(i uint64) bool { return i <= c })
		}
	}
	w.pending = append(w.pending[:0], w.pending[k:]...)
	w.kick()
}

// plantedEarlyDurable reports whether planted bug B02 (on the leader) or
// B03 (on a follower) takes the log's appends as durable once written,
// before their sync.
func (n *node) plantedEarlyDurable() bool {
	return planted.On("B02") && n.lead == n.id || planted.On("B03") && n.lead != n.id
}

// fsyncDone is the helper's wake-up.
func (n *node) fsyncDone() error {
	var b [8]byte
	_, _ = sysRead(n.wal.wakeFD, b[:])
	if err := n.wal.failed.Load(); err != nil {
		return *err
	}
	n.wal.inflight = false
	if d := time.Duration(n.wal.lastSyncNs.Load()); d >= slowSync {
		// As main's nodes log them, for bench.yml's retry of a stall of
		// the runner's disk (cmd/benchreport, #747). The time is the
		// helper's own on the disk: the loop may come late to this
		// completion, and a fast sync must not look slow (#748).
		n.logf("slow log sync: %v", d)
		n.logf("slow log flush: %d bytes in %v", n.wal.lastBytes, d)
	}
	n.synced()
	// The appends let in may leave records no sync takes, and appends
	// waiting for the stage behind them: a kick after them starts the sync
	// that lets those in (#821's re-review).
	err := n.unwait()
	n.wal.kick()
	return err
}

// slowSync is the least time a sync of the log takes for the node to
// log it.
const slowSync = time.Second

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
			if idx == n.plantedSkip {
				// Planted (B23): the replay after a start stops one entry
				// short of the commit index it recovered.
				break
			}
			if d := e.GetData(); len(d) > 0 {
				if err := n.applyBatch(d, idx, n.storage.offOf(idx)); err != nil {
					return err
				}
			}
		}
		n.applied = idx
		n.simApplied(e)
		if n.applied > n.membershipAt+membershipSnapshotEntries {
			n.membershipAt = n.applied
			n.membershipSnapshot()
		}
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
	if first, _ := s.FirstIndex(); !n.wal.ringLow() && n.applied > first+2*n.catchup {
		s.compact(n.applied - n.catchup)
	}
	if planted.On("B20") && s.lastIndex() > n.applied+1 {
		// Planted: an entry not yet applied leaves memory.
		s.drop(n.applied + 1)
	}
	n.cutLog()
	return n.maybeCkpt()
}

// maybeCkpt starts a checkpoint once the log has grown enough since the
// last, or when Raft's storage wants one for a follower.
func (n *node) maybeCkpt() error {
	s := n.storage
	n.pruneSnapWaiters()
	clientDue := len(n.snapWaiters) > 0
	ringDue := n.ringDue()
	if n.ckpt != nil || n.ckptDir == "" || (n.wal.off-n.ckptFrom < n.ckptEvery() || n.now.Before(n.ckptNotBefore)) && !s.wantCkpt && !clientDue && !ringDue {
		return nil
	}
	index := n.applied
	if c := s.hs.GetCommit(); planted.On("B21") && c > index && c <= s.lastIndex() {
		// Planted: the checkpoint names the committed index, past the
		// applied one its store is at.
		index = c
	}
	if !ringDue && clientDue && s.snap != nil && index == s.snap.GetMetadata().GetIndex() {
		// No apply followed the newest checkpoint and no cut is due:
		// it is already a fresh source for waiting clients. Otherwise
		// the forced checkpoint takes them in and frees the ring first.
		waiters := n.snapWaiters
		n.snapWaiters = nil
		n.startClientSnaps(waiters, n.ckptName)
		return nil
	}
	off := s.offOf(index)
	if s.snap != nil && index == s.snap.GetMetadata().GetIndex() {
		if !ringDue && !s.wantCkpt {
			return nil
		}
		// The ring may need a forced cut at the newest's own index
		// (#815), or Raft may ask for another checkpoint there. Storage
		// may have compacted that entry already; the checkpoint keeps
		// its record's place so a new file can start at the same index.
		off = n.ckptFrom + walEntHdr
	}
	if off < 0 {
		return nil
	}
	term, err := s.Term(index)
	if err != nil {
		return err
	}
	if !n.ckptRetry() {
		// The last found no room (dropCkpt).
		return nil
	}
	return n.startCkpt(index, term, off-walEntHdr)
}

// ---- peer messages

// msgHdr: type, flags, from, term, logTerm, index, commit, rejectHint,
// vote, entry count.
const msgHdr = 1 + 1 + 8*7 + 4

// sendPeer writes m into the send buffer of its peer.
func (n *node) sendPeer(m *pb.Message) {
	if n.silent {
		// A leader on a stalled disk (silent.go). A snapshot it does
		// not send failed, so that Raft sends it again later.
		if m.GetType() == pb.MsgSnap {
			n.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
		}
		return
	}
	n.stats.peerMsgs++
	if m.GetType() == pb.MsgSnap {
		// A snapshot goes over a connection of its own, and is reported
		// to Raft however it ends: one dropped with the peer connection
		// below left Raft waiting for its report, and never sending the
		// follower anything again (seed 24040 of TestSeeds).
		n.sendSnapshot(m)
		return
	}
	p := n.peersOut[m.GetTo()]
	if n.onBeats(m, p) {
		if b := n.beatsOut[m.GetTo()]; b != nil && !b.dead {
			p = b
		}
	}
	if p == nil || p.dead {
		return
	}
	if m.GetType() == pb.MsgApp && int64(len(p.out)-p.ow) > n.sizes.peerBacklog(len(n.peersOut)) {
		// The peer takes less than it is sent, as one that catches up:
		// Raft probes it again instead of piling up appends here.
		n.rn.ReportUnreachable(m.GetTo())
		return
	}
	ents := m.GetEntries()
	app := m.GetType() == pb.MsgApp
	to := m.GetTo()
	carry := time.Now().Before(n.carryUntil[to])
	mctx := n.propContext(m)
	size := msgHdr + 4 + len(mctx)
	for _, e := range ents {
		size += 8 + 8 + 1 + 4
		if app && n.names(e.GetData(), carry) {
			size += 8
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
		if app && n.names(d, carry) {
			o, seq, _ := batchOrigin(d)
			n.stats.named++
			eh[16] |= namedEntry
			binary.LittleEndian.PutUint32(eh[17:], uint32(seq))
			s.put(eh[:])
			var ob [8]byte
			binary.LittleEndian.PutUint32(ob[:], uint32(o))
			binary.LittleEndian.PutUint32(ob[4:], uint32(seq>>32))
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
	binary.LittleEndian.PutUint32(cl[:], uint32(len(mctx)))
	s.put(cl[:])
	s.put(mctx)
	n.markDirty(s)
	if app && p.appSince.IsZero() {
		p.appSince = n.now
	}
}

// onBeats reports whether m goes on its peer's connection of heartbeats
// and votes rather than on p, the connection of the appends: an append of
// megabytes on a slow link would hold back the heartbeats behind it past
// an election timeout, and the followers would elect another leader
// (#715, TestHeartbeatsPassASlowAppend). Raft takes messages out of
// order, as a network may deliver them.
//
// Votes always go there: they are rare. A heartbeat goes there only while
// an append to its peer went unanswered for a tick (appSince), and its
// answer back on the connection it came on (beatAsked). Otherwise both go
// with the appends: the ReadIndex rounds are heartbeats, one or more
// every turn under load, and a connection of their own cost a write and a
// read of each per follower (0.24 µs of writes per op on the leader,
// #689's "0 ms: the bisect"). The appends and their answers keep their
// connection, and a leadership handover's MsgTimeoutNow stays behind the
// appends it waited for.
func (n *node) onBeats(m *pb.Message, p *peerOut) bool {
	t := m.GetType()
	if t == pb.MsgHeartbeat {
		return p != nil && !p.appSince.IsZero() && n.now.Sub(p.appSince) >= tickEvery
	}
	if t == pb.MsgHeartbeatResp {
		return n.beatAsked[m.GetTo()]
	}
	return t == pb.MsgPreVote || t == pb.MsgPreVoteResp || t == pb.MsgVote || t == pb.MsgVoteResp
}

// waitsBehindHeld reports whether peer frame f waits behind a message
// held before it, as messages keep their order. A batch never does: the
// held append waits for it. Nor does a heartbeat: it needs nothing an
// append brings, as its commit is at most what this follower answered it
// has, and on its own connection it is stepped out of order with the
// appends anyway (onBeats). Held behind appends that name batches a
// follower let go of, as after its disk stalled, heartbeats on the
// appends' connection kept it from its leader past an election timeout
// (TestWLStalledLeaderIsReplaced, seed 1, on 3489fdd1).
func waitsBehindHeld(f []byte) bool {
	batch := len(f) >= batchFrameHdr && f[0] == frameBatch
	heartbeat := len(f) >= msgHdr && f[0] == byte(pb.MsgHeartbeat)
	refusal := len(f) == refusedFrameLn && f[0] == frameRefused
	return !batch && !heartbeat && !refusal
}

// noteLanes notes, of a message from p, what onBeats needs: an answer to
// the appends sent to its peer, or the connection a heartbeat came on.
func (n *node) noteLanes(p *peerIn, m *pb.Message) {
	t := m.GetType()
	if t == pb.MsgAppResp {
		if o := n.peersOut[p.from]; o != nil {
			o.appSince = time.Time{}
		}
	}
	if t == pb.MsgHeartbeat && p.beat != n.beatAsked[p.from] {
		if n.beatAsked == nil {
			n.beatAsked = map[uint64]bool{}
		}
		n.beatAsked[p.from] = p.beat
	}
}

// names reports whether an append names an entry's batch instead of
// carrying it: a batch a follower proposed and sent every peer itself,
// unless the peer asked for batches to be carried. The proposer itself
// asks too when it lacks the batch, as after a restart.
func (n *node) names(data []byte, carry bool) bool {
	o, _, ok := batchOrigin(data)
	return ok && o != n.id && !carry
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
	if n.removed[p.from] {
		// Removed since it connected: its connection closes at the
		// turn's end (membersChanged), and nothing more of it is taken.
		return errPeerFrom
	}
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
		if len(p.held) > 0 && waitsBehindHeld(f) {
			// Messages from a peer keep their order behind one held.
			p.held = append(p.held, heldMsg{f: append([]byte(nil), f...), at: n.now})
			continue
		}
		if len(f) == 1 && f[0] == frameCarry {
			// A follower lacks a batch the leader named: carry them to it.
			n.carryUntil[p.from] = time.Now().Add(time.Second)
			continue
		}
		if n.isRefusal(f) {
			// The leader dropped a batch of this node's (refusal.go).
			n.refused(f)
			continue
		}
		if len(f) >= batchFrameHdr && f[0] == frameBatch {
			// A batch a follower proposed, sent ahead of the leader's
			// append that names it.
			origin := uint64(binary.LittleEndian.Uint32(f[1:]))
			seq := binary.LittleEndian.Uint64(f[5:])
			b := &n.fwdOf(origin)[seq&(fwdSlots-1)]
			b.seq, b.data = seq, n.rxCopy(f[batchFrameHdr:], span{kind: spanUnnamed, at: n.now, fwd: true, origin: origin, seq: seq})
			continue
		}
		m, err := n.decodeMsg(f, p.from)
		if errors.Is(err, errUnnamed) {
			// The batch it names comes on another connection, likely in
			// this same turn: try again after the turn's input.
			n.stats.held++
			p.held = append(p.held, heldMsg{f: append([]byte(nil), f...), at: n.now})
			n.holding = true
			continue
		}
		if err != nil {
			return err
		}
		m.To = new(n.id)
		if m.GetType() == pb.MsgProp && proposesConfChange(m) {
			n.proposeConfChange(m, n.now)
			continue
		}
		if !n.simDeliver(m) {
			continue
		}
		n.noteLanes(p, m)
		if n.leaderDrops(m, p.from) || n.followerDrops(m) {
			continue
		}
		n.step(m)
	}
}

// decodeMsg decodes a message of the peer that said it is from in its
// hello. A message that does not fit its frame, or says it is from
// another member, is errRecord or errPeerFrom: the connection closes, as
// etcd's peer handler drops a message whose sender is not the peer's.
func (n *node) decodeMsg(f []byte, from uint64) (*pb.Message, error) {
	if len(f) < msgHdr+4 {
		return nil, errRecord
	}
	if binary.LittleEndian.Uint64(f[2:]) != from {
		return nil, errPeerFrom
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
	if k > (len(f)-at)/entryHdr {
		return nil, errRecord // more entries than the frame holds
	}
	if k > 0 {
		m.Entries = make([]*pb.Entry, k)
		for i := range k {
			if len(f) < at+entryHdr {
				return nil, errRecord
			}
			l := int(binary.LittleEndian.Uint32(f[at+17:]))
			typ := f[at+16]
			if typ&^namedEntry > byte(pb.EntryConfChangeV2) {
				return nil, errRecord
			}
			e := &pb.Entry{
				Term:  new(binary.LittleEndian.Uint64(f[at:])),
				Index: new(binary.LittleEndian.Uint64(f[at+8:])),
				Type:  pb.EntryType(typ &^ namedEntry).Enum(),
			}
			at += entryHdr
			if typ&namedEntry != 0 {
				// The entry names a batch by its origin and sequence:
				// the length's place holds the sequence's low half.
				if len(f) < at+8 {
					return nil, errRecord
				}
				origin := uint64(binary.LittleEndian.Uint32(f[at:]))
				seq := uint64(binary.LittleEndian.Uint32(f[at+4:]))<<32 | uint64(uint32(l))
				at += 8
				b := n.fwdOf(origin)[seq&(fwdSlots-1)]
				if (b.seq != seq || b.data == nil) && planted.On("B33") {
					// Planted: the entry goes to Raft with its name, the
					// batch's key, as its data.
					e.Data = bytes.Clone(f[at-8 : at])
					m.Entries[i] = e
					continue
				}
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
	if len(f)-at < cl {
		return nil, errRecord
	}
	if cl > 0 {
		m.Context = append([]byte(nil), f[at:at+cl]...)
	}
	return m, nil
}

const namedEntry = 0x80

// entryHdr is an entry's header in a peer frame: its term, index, type
// and length.
const entryHdr = 8 + 8 + 1 + 4

// errPeerFrom is a peer's message that says it is from another member,
// or one from a member removed.
var errPeerFrom = errors.New("a peer's message from a member other than the peer, or removed")

// frameBatch marks a peer frame that carries a follower's batch, after
// [4 origin][8 sequence]; frameCarry asks the leader to carry batches
// instead of naming them.
const (
	batchFrameHdr = 1 + 4 + 8
	frameBatch    = 0xff
	frameCarry    = 0xfe
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
	seq := binary.LittleEndian.Uint64(data[4:])
	if dropBatches && seq%50 == 0 {
		return // a test of the leader's fallback to carrying batches
	}
	for id, p := range n.peersOut {
		if id == n.lead || p.dead {
			continue
		}
		var h [4 + batchFrameHdr]byte
		binary.LittleEndian.PutUint32(h[0:], uint32(batchFrameHdr+len(data)))
		h[4] = frameBatch
		binary.LittleEndian.PutUint32(h[5:], uint32(n.id))
		binary.LittleEndian.PutUint64(h[9:], seq)
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
// that named a batch not here yet. An append whose batch has not come
// within namedWait of the append itself is dropped, and the leader is
// asked to carry batches for a while; Raft resends what this node then
// lacks. Every such append in the queue goes at once: a follower that
// let go of many batches, as one whose disk stalled while the leader
// named them, would otherwise drop one append a turn while new
// heartbeats queue behind the rest, and lose its leader (#706).
func (n *node) retryHeld() {
	n.holding = false
	for _, x := range n.socks {
		p, ok := x.(*peerIn)
		if !ok || len(p.held) == 0 {
			continue
		}
		k, asked := 0, false
		for ; k < len(p.held); k++ {
			h := &p.held[k]
			if len(h.f) == 1 && h.f[0] == frameCarry {
				n.carryUntil[p.from] = time.Now().Add(time.Second)
				continue
			}
			m, err := n.decodeMsg(h.f, p.from)
			if errors.Is(err, errUnnamed) {
				if n.now.Sub(h.at) < namedWait {
					break
				}
				// The batch never came, as when its proposer had no
				// connection to this node yet, or this node let it go.
				if !asked {
					n.askCarry(p.from)
					asked = true
				}
				continue
			}
			if err == nil && n.simDeliver(m) {
				m.To = new(n.id)
				if m.GetType() == pb.MsgProp && proposesConfChange(m) {
					// Held as any proposal of a change, while Raft would
					// drop it (#689's area-4 review).
					n.proposeConfChange(m, h.at)
					continue
				}
				n.noteLanes(p, m)
				if n.leaderDrops(m, p.from) || n.followerDrops(m) {
					continue
				}
				n.step(m)
			}
		}
		clear(p.held[:k])
		p.held = append(p.held[:0], p.held[k:]...)
		if len(p.held) > 0 {
			n.holding = true
		}
	}
}

// namedWait is how long an append that names a batch not here waits for
// it: the batch comes on another peer's connection, usually in the same
// turn.
const namedWait = 100 * time.Millisecond

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
