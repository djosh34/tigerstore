package wl

import (
	"bytes"
	"testing"

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
