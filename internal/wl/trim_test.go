package wl

import (
	"bytes"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// trimNet is a leader, 1, and a follower, 2, of three voters, in memory;
// 3 never answers. Each node stores what its Ready hands it before it
// sends the Ready's messages, as Raft's synchronous storage asks.
type trimNet struct {
	t     *testing.T
	nodes map[uint64]*raft.RawNode
	ms    map[uint64]*raft.MemoryStorage
	// app, when set, is called with each append to 2 before 2 steps it,
	// and reports whether 2 gets it: one it does not is lost.
	app func(m *pb.Message) bool
	// acks are 2's answers to appends.
	acks []*pb.Message
}

func newTrimNet(t *testing.T) *trimNet {
	t.Helper()
	tn := &trimNet{t: t, nodes: map[uint64]*raft.RawNode{}, ms: map[uint64]*raft.MemoryStorage{}}
	for _, id := range []uint64{1, 2} {
		ms := raft.NewMemoryStorage()
		if err := ms.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		}}); err != nil {
			t.Fatal(err)
		}
		rn, err := raft.NewRawNode(&raft.Config{ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: ms, Applied: 1, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256})
		if err != nil {
			t.Fatal(err)
		}
		tn.nodes[id], tn.ms[id] = rn, ms
	}
	if err := tn.nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	tn.pump()
	if st := tn.nodes[1].Status(); st.RaftState != raft.StateLeader {
		t.Fatalf("node 1 is %v after its campaign", st.RaftState)
	}
	return tn
}

// pump runs both nodes' Readies until neither has one.
func (tn *trimNet) pump() {
	for moved := true; moved; {
		moved = false
		for _, id := range []uint64{1, 2} {
			rn := tn.nodes[id]
			if !rn.HasReady() {
				continue
			}
			moved = true
			rd := rn.Ready()
			if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
				if err := tn.ms[id].SetHardState(rd.HardState); err != nil {
					tn.t.Fatal(err)
				}
			}
			if err := tn.ms[id].Append(rd.Entries); err != nil {
				tn.t.Fatal(err)
			}
			for _, m := range rd.Messages {
				to := m.GetTo()
				if to == 3 {
					continue
				}
				if to == 2 && m.GetType() == pb.MsgApp && tn.app != nil && !tn.app(m) {
					continue
				}
				if to == 1 && m.GetType() == pb.MsgAppResp {
					tn.acks = append(tn.acks, m)
				}
				if err := tn.nodes[to].Step(m); err != nil {
					tn.t.Fatal(err)
				}
			}
			rn.Advance(rd)
		}
	}
}

// TestATrimmedAppendIsAcknowledgedUpToItsPrefix is the trim of #814's
// follower (followerDrops): of an append of four entries, the follower's
// ring has room for two, which fitEntries keeps. The follower answers
// exactly the last it kept, its commit never passes it, the leader commits
// no further than it either, and the leader then sends the rest, which
// the follower takes once it has room.
func TestATrimmedAppendIsAcknowledgedUpToItsPrefix(t *testing.T) {
	tn := newTrimNet(t)
	leader, follower := tn.nodes[1], tn.nodes[2]
	base := leader.Status().GetCommit()
	if follower.Status().GetCommit() != base {
		t.Fatalf("the follower's commit %d, the leader's %d, before the append", follower.Status().GetCommit(), base)
	}
	const size = 1000
	ents := make([]*pb.Entry, 4)
	for i := range ents {
		ents[i] = &pb.Entry{Data: bytes.Repeat([]byte{byte('a' + i)}, size)}
	}
	// Room for two entries and part of a third. Once the trimmed append
	// went, the appends after it are lost until the test lets them go.
	room := int64(2*(size+walEntHdr) + size/2)
	var trimmed, rest *pb.Message
	held := false
	tn.app = func(m *pb.Message) bool {
		if len(m.GetEntries()) == 0 {
			return !held
		}
		if trimmed == nil {
			last, _ := tn.ms[2].LastIndex()
			keep, took, wanted := fitEntries(m.Entries, last, room)
			if keep != 2 || took != 2*(size+walEntHdr) || wanted != size+walEntHdr {
				t.Fatalf("fitEntries of %d entries in %d bytes: keep %d, took %d, wanted %d; want 2, %d, %d", len(m.Entries), room, keep, took, wanted, 2*(size+walEntHdr), size+walEntHdr)
			}
			m.Entries = m.Entries[:keep]
			trimmed, held = m, true
			return true
		}
		if held {
			return false
		}
		if rest == nil {
			rest = m
		}
		return true
	}
	tn.acks = nil
	if err := leader.Step(&pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(1)), Entries: ents}); err != nil {
		t.Fatal(err)
	}
	tn.pump()
	if trimmed == nil {
		t.Fatal("the leader sent the follower no append of the four entries")
	}
	prefix := trimmed.GetIndex() + 2
	if len(tn.acks) != 1 || tn.acks[0].GetIndex() != prefix || tn.acks[0].GetReject() {
		t.Fatalf("the follower's answers %v to the trimmed append after %d: want one ack of %d", tn.acks, trimmed.GetIndex(), prefix)
	}
	if c := follower.Status().GetCommit(); c > prefix {
		t.Fatalf("the follower committed %d, past the %d it kept", c, prefix)
	}
	if last, _ := tn.ms[2].LastIndex(); last != prefix {
		t.Fatalf("the follower's log ends at %d, want the %d it kept", last, prefix)
	}
	if c := leader.Status().GetCommit(); c != prefix {
		t.Fatalf("the leader committed %d with the follower at %d: want %d", c, prefix, prefix)
	}
	// The follower's commit stays within what it kept while the leader's
	// heartbeats come.
	for range 5 {
		leader.Tick()
		tn.pump()
		if c, last := follower.Status().GetCommit(), func() uint64 { l, _ := tn.ms[2].LastIndex(); return l }(); c > last {
			t.Fatalf("the follower committed %d, past its log's last %d", c, last)
		}
	}
	// Let the appends go: the leader sends the rest after the prefix, which
	// the follower, with room now, takes whole.
	held = false
	for i := 0; follower.Status().GetCommit() < prefix+2 && i < 50; i++ {
		leader.Tick()
		tn.pump()
	}
	if rest == nil || rest.GetIndex() != prefix || len(rest.GetEntries()) != 2 {
		t.Fatalf("the leader's first append after the trim that carried entries: %v; want the 2 entries after %d", rest, prefix)
	}
	if c, l := follower.Status().GetCommit(), leader.Status().GetCommit(); c != prefix+2 || l != prefix+2 {
		t.Fatalf("after the rest: the follower committed %d, the leader %d; want %d", c, l, prefix+2)
	}
	for i, want := range ents {
		got, err := tn.ms[2].Entries(prefix-1+uint64(i), prefix+uint64(i), 1<<20)
		if err != nil || len(got) != 1 || !bytes.Equal(got[0].GetData(), want.GetData()) {
			t.Fatalf("the follower's entry %d: %v %v", prefix-1+uint64(i), got, err)
		}
	}
}

// TestFitEntriesKeepsEntriesItHas checks fitEntries' edges: entries at the
// log's last index or before take no room; an append whose first entry is
// new and has no room keeps none; one that fits whole keeps all.
func TestFitEntriesKeepsEntriesItHas(t *testing.T) {
	ent := func(i uint64, n int) *pb.Entry { return &pb.Entry{Index: new(i), Data: make([]byte, n)} }
	one := int64(100 + walEntHdr)
	for _, c := range []struct {
		name         string
		ents         []*pb.Entry
		last         uint64
		room         int64
		keep         int
		took, wanted int64
	}{
		{"all fit", []*pb.Entry{ent(5, 100), ent(6, 100)}, 4, 2 * one, 2, 2 * one, 0},
		{"the first has no room", []*pb.Entry{ent(5, 100), ent(6, 100)}, 4, one - 1, 0, 0, one},
		{"held entries take none", []*pb.Entry{ent(4, 100), ent(5, 100), ent(6, 100)}, 5, one, 3, one, 0},
		{"held entries, then one with no room", []*pb.Entry{ent(4, 100), ent(5, 100)}, 4, 0, 1, 0, one},
		{"a prefix", []*pb.Entry{ent(5, 100), ent(6, 100), ent(7, 100)}, 4, 2*one + 1, 2, 2 * one, one},
	} {
		keep, took, wanted := fitEntries(c.ents, c.last, c.room)
		if keep != c.keep || took != c.took || wanted != c.wanted {
			t.Errorf("%s: keep %d, took %d, wanted %d; want %d, %d, %d", c.name, keep, took, wanted, c.keep, c.took, c.wanted)
		}
	}
}

// TestAFollowerTrimsDropsOrPassesAnAppend is followerDrops itself, on a
// ring of 32 segments of 16 KiB with 11 free and the log's end at a
// segment's start: room for 196608 bytes, less appendMargin. An append of
// four entries of 60000 bytes is cut to the three that fit, and the bytes
// and last index they take are counted; one whose first entry has no
// room is dropped whole; one whose entry before them the follower lacks
// goes to Raft untouched, which rejects it and writes none of it.
func TestAFollowerTrimsDropsOrPassesAnAppend(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	const segs = 32
	w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, segs*(16<<10), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	w.off = 20 * segSize
	if room := w.ringRoom(); room != 12*segSize {
		t.Fatalf("the ring's room is %d, want %d", room, 12*segSize)
	}
	n := &node{wal: w, storage: newLogStorage(nil)}
	n.storage.snapIndex, n.storage.snapTerm = 5, 2
	n.storage.hs = &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(5))}
	rn, err := raft.NewRawNode(&raft.Config{ID: 2, ElectionTick: 10, HeartbeatTick: 1, Storage: n.storage, Applied: 5, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256})
	if err != nil {
		t.Fatal(err)
	}
	n.rn = rn
	app := func(prev uint64, size, k int) *pb.Message {
		m := &pb.Message{Type: pb.MsgApp.Enum(), Term: new(uint64(2)), Index: new(prev), LogTerm: new(uint64(2))}
		for i := range k {
			m.Entries = append(m.Entries, &pb.Entry{Term: new(uint64(2)), Index: new(prev + 1 + uint64(i)), Data: make([]byte, size)})
		}
		return m
	}
	// An append whose predecessor is of another term, and one of a leader
	// before, go to Raft untouched, and count as taken nothing.
	m := app(5, 60000, 4)
	m.LogTerm = new(uint64(1))
	if n.followerDrops(m) || len(m.Entries) != 4 || n.appendPledged != 0 || n.appendLast != 0 {
		t.Fatalf("an append after an entry of another term: %d entries kept, pledged %d, last %d; want it untouched and nothing taken", len(m.Entries), n.appendPledged, n.appendLast)
	}
	m = app(5, 60000, 4)
	m.Term = new(uint64(1))
	if n.followerDrops(m) || len(m.Entries) != 4 || n.appendPledged != 0 || n.appendLast != 0 {
		t.Fatalf("an append of a leader before: %d entries kept, pledged %d, last %d; want it untouched and nothing taken", len(m.Entries), n.appendPledged, n.appendLast)
	}
	one := int64(60000 + walEntHdr)

	m = app(5, 60000, 4)
	if n.followerDrops(m) {
		t.Fatal("an append whose first three entries fit was dropped")
	}
	if len(m.Entries) != 3 || m.Entries[2].GetIndex() != 8 {
		t.Fatalf("the append kept %d entries, want the 3 that fit, 6 to 8", len(m.Entries))
	}
	if n.appendPledged != 3*one || n.appendLast != 8 || n.appendLastTerm != 2 || n.stats.appendsTrimmed != 1 || w.wanted != 4*one+appendMargin {
		t.Fatalf("after the trim: pledged %d, last %d of term %d, trimmed %d, wanted %d; want %d, 8 of term 2, 1, %d", n.appendPledged, n.appendLast, n.appendLastTerm, n.stats.appendsTrimmed, w.wanted, 3*one, 4*one+appendMargin)
	}

	// The next append, after the three, has room for none.
	m = app(8, 60000, 1)
	if !n.followerDrops(m) || n.stats.appendsDropped != 1 || len(m.Entries) != 1 {
		t.Fatalf("an append with no room for its first entry: dropped %d, entries %d; want dropped whole", n.stats.appendsDropped, len(m.Entries))
	}

	// An append past what the follower holds goes to Raft whole.
	m = app(12, 60000, 4)
	if n.followerDrops(m) || len(m.Entries) != 4 || n.appendPledged != 3*one {
		t.Fatalf("an append past the follower's last entry: %d entries kept, pledged %d; want it untouched", len(m.Entries), n.appendPledged)
	}

	// Raft handed the log what it took: the room counts it from the log
	// (storageAppend), and an append that fits whole is kept whole.
	n.appendPledged, n.appendLast = 0, 0
	m = app(5, 100, 2)
	if n.followerDrops(m) || len(m.Entries) != 2 || w.wanted != 0 {
		t.Fatalf("an append that fits: %d entries kept, wanted %d; want both, 0", len(m.Entries), w.wanted)
	}
}

// TestAFollowerCountsTheEntriesAConflictRewrites is #817: a follower that
// holds entries 6 to 9 of term 2 gets an append of a new leader, of term
// 3, after entry 5, with entries 6 to 9 of term 3. Raft truncates the log
// at 6 and writes all four (findConflict), so the room counts them, from
// the first conflict on: on the ring of TestAFollowerTrimsDropsOrPassesAnAppend,
// with room for three entries of 60000 bytes, the append is cut to three.
// The follower holds the old entries in Raft's storage, or in an append
// that waits for the stage, which Raft's unstable log holds as well
// (#946's review). Before the fix, the entries at the follower's last
// index or before took no room, and all four went to a log with room for
// three.
func TestAFollowerCountsTheEntriesAConflictRewrites(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	entries := func(term uint64, size int) []*pb.Entry {
		var ents []*pb.Entry
		for i := range uint64(4) {
			ents = append(ents, &pb.Entry{Term: new(term), Index: new(6 + i), Data: make([]byte, size)})
		}
		return ents
	}
	for _, waiting := range []bool{false, true} {
		name := "held in storage"
		if waiting {
			name = "waiting for the stage"
		}
		t.Run(name, func(t *testing.T) {
			w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, 32*(16<<10), 0, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(w.close)
			w.off = 20 * segSize
			n := &node{wal: w, storage: newLogStorage(nil)}
			n.storage.snapIndex, n.storage.snapTerm = 5, 2
			n.storage.hs = &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(5))}
			if waiting {
				w.waiting = []*pb.Message{{Type: pb.MsgStorageAppend.Enum(), Entries: entries(2, 100)}}
			} else if err := n.storage.append(entries(2, 100), make([]int64, 4)); err != nil {
				t.Fatal(err)
			}
			rn, err := raft.NewRawNode(&raft.Config{ID: 2, ElectionTick: 10, HeartbeatTick: 1, Storage: n.storage, Applied: 5, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256})
			if err != nil {
				t.Fatal(err)
			}
			n.rn = rn
			one := int64(60000 + walEntHdr)
			app := func(term uint64) *pb.Message {
				return &pb.Message{Type: pb.MsgApp.Enum(), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: entries(term, 60000)}
			}
			// The same entries again, as Raft sends them again, write nothing.
			if m := app(2); n.followerDrops(m) || len(m.Entries) != 4 || n.appendPledged != 0 {
				t.Fatalf("entries the follower holds, sent again: %d kept, pledged %d; want all 4 and nothing taken", len(m.Entries), n.appendPledged)
			}
			m := app(3)
			if n.followerDrops(m) {
				t.Fatal("an append whose first three entries fit was dropped")
			}
			if len(m.Entries) != 3 || n.appendPledged != 3*one || n.stats.appendsTrimmed != 1 {
				t.Fatalf("an append that rewrites 4 entries, with room for 3: %d kept, pledged %d, trimmed %d; want 3, %d, 1", len(m.Entries), n.appendPledged, n.stats.appendsTrimmed, 3*one)
			}
		})
	}
}

// TestNudgeRingProposesOneAtATime is nudgeRing's bound: a leader whose ring
// a checkpoint cannot free (it applied every entry, its newest checkpoint
// at the last) and that refuses five writes in one turn, before its Ready
// hands Raft's storage the first empty entry, proposes one, not five
// (#818's re-check); one lost is sent again once requestTimeout passed.
func TestNudgeRingProposesOneAtATime(t *testing.T) {
	was := segSize
	segSize = 16 << 10
	t.Cleanup(func() { segSize = was })
	w, err := openWALRing(sysEnv{}, t.TempDir(), 64<<10, 8*(16<<10), 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	n := &node{id: 1, wal: w, storage: newLogStorage(nil), applied: 5, lead: 1}
	n.storage.snapIndex, n.storage.snapTerm = 5, 1
	n.storage.snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}}}}
	n.storage.hs = &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}
	n.storage.cs = &pb.ConfState{Voters: []uint64{1}}
	if n.ringDue() {
		t.Fatal("a checkpoint is due at the newest checkpoint's own index")
	}
	rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: n.storage, Applied: 5, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	// Its vote for itself counts once the Ready that holds it is handled.
	for i := 0; i < 4 && rn.HasReady() && rn.BasicStatus().RaftState != raft.StateLeader; i++ {
		rn.Advance(rn.Ready())
	}
	if rn.BasicStatus().RaftState != raft.StateLeader {
		t.Fatalf("the lone voter is %v after its campaign", rn.BasicStatus().RaftState)
	}
	n.rn = rn
	n.now = time.Now()
	for range 5 {
		n.nudgeRing()
	}
	if got := n.stats.ringNudges; got != 1 {
		t.Fatalf("five refusals in one turn proposed %d empty entries, want 1", got)
	}
	n.now = n.now.Add(requestTimeout / 2)
	n.nudgeRing()
	if got := n.stats.ringNudges; got != 1 {
		t.Fatalf("a refusal before requestTimeout passed proposed another empty entry: %d", got)
	}
	n.now = n.now.Add(requestTimeout)
	n.nudgeRing()
	if got := n.stats.ringNudges; got != 2 {
		t.Fatalf("once requestTimeout passed, the empty entry was not sent again: %d proposed, want 2", got)
	}
}
