package audit_test

import (
	"bytes"
	"io"
	"log"
	"math"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type configuredStorage struct{ *raft.MemoryStorage }

func (s configuredStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := s.MemoryStorage.InitialState()
	return hs, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}, err
}

func entry(term, index uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Term: new(term), Index: new(index), Data: []byte(data)}
}

func follower(t *testing.T) (*raft.RawNode, configuredStorage) {
	t.Helper()
	s := configuredStorage{raft.NewMemoryStorage()}
	if err := s.Append([]*raftpb.Entry{entry(1, 1, "base")}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetHardState(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: s, Applied: 1,
		AsyncStorageWrites: true, MaxSizePerMsg: math.MaxUint64, MaxInflightMsgs: 256,
		Logger: &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, s
}

func step(t *testing.T, rn *raft.RawNode, m *raftpb.Message) {
	t.Helper()
	if err := rn.Step(m); err != nil {
		t.Fatal(err)
	}
}
func appendMessage(t *testing.T, rd raft.Ready) *raftpb.Message {
	t.Helper()
	for _, m := range rd.Messages {
		if m.GetTo() == raft.LocalAppendThread {
			return m
		}
	}
	t.Fatalf("no append message in %+v", rd)
	return nil
}
func persist(t *testing.T, rn *raft.RawNode, s configuredStorage, m *raftpb.Message) {
	t.Helper()
	if err := s.Append(m.Entries); err != nil {
		t.Fatal(err)
	}
	if m.Vote != nil {
		if err := s.SetHardState(&raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range m.Responses {
		if r.GetTo() == 1 {
			step(t, rn, r)
		}
	}
}
func appendFromLeader(term, prev, prevTerm, index, commit uint64, value string) *raftpb.Message {
	return &raftpb.Message{Type: raftpb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(term), Index: new(prev), LogTerm: new(prevTerm), Commit: new(commit), Entries: []*raftpb.Entry{entry(term, index, value)}}
}

func TestCommittedUnstableDoesNotApply(t *testing.T) {
	rn, s := follower(t)
	step(t, rn, appendFromLeader(1, 1, 1, 2, 2, "new"))
	rd := rn.Ready()
	if rn.Status().GetCommit() != 2 {
		t.Fatal("entry did not commit")
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatal("unstable entry was offered for apply")
	}
	for _, m := range rd.Messages {
		if m.GetTo() == raft.LocalApplyThread {
			t.Fatal("unstable apply message")
		}
	}
	a := appendMessage(t, rd)
	if rn.HasReady() {
		t.Fatal("Ready did not accept its entries and messages")
	}
	persist(t, rn, s, a)
	if !rn.HasReady() {
		t.Fatal("durability did not unblock apply")
	}
	applied := rn.Ready()
	if len(applied.CommittedEntries) != 1 || applied.CommittedEntries[0].GetIndex() != 2 {
		t.Fatal("stable entry not offered for apply")
	}
	t.Log("commit advanced to 2, but apply waited for append response; Ready immediately consumed pending work")
}

func TestEmptyAppendResponseAfterTermChangeUnblocksApply(t *testing.T) {
	rn, s := follower(t)
	step(t, rn, appendFromLeader(1, 1, 1, 2, 2, "new"))
	a := appendMessage(t, rn.Ready())
	step(t, rn, &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Commit: new(uint64(2))})
	b := appendMessage(t, rn.Ready())
	if len(b.Entries) != 0 {
		t.Fatal("expected hard-state-only append")
	}
	var stabilizer *raftpb.Message
	for _, m := range b.Responses {
		if m.GetType() == raftpb.MsgStorageAppendResp {
			stabilizer = m
		}
	}
	if stabilizer == nil || stabilizer.GetTerm() != 2 || stabilizer.GetIndex() != 2 || stabilizer.GetLogTerm() != 1 {
		t.Fatalf("no current-term stability witness: %v", stabilizer)
	}
	persist(t, rn, s, a)
	if rn.HasReady() {
		t.Fatal("old-term append response stabilized entries")
	}
	persist(t, rn, s, b)
	if !rn.HasReady() || len(rn.Ready().CommittedEntries) != 1 {
		t.Fatal("current-term empty append response did not unblock apply")
	}
	t.Log("old completion was ignored; exact response on entry-free append was necessary")
}

func TestReadyEntriesRemainReadOnlyAcrossSuffixReplacement(t *testing.T) {
	rn, _ := follower(t)
	// Keep index 2 committed, then replace only the uncommitted index 3.
	step(t, rn, appendFromLeader(1, 1, 1, 2, 2, "committed"))
	a := appendMessage(t, rn.Ready())
	step(t, rn, appendFromLeader(1, 2, 1, 3, 2, "old"))
	b := appendMessage(t, rn.Ready())
	step(t, rn, appendFromLeader(2, 2, 1, 3, 2, "replacement"))
	c := appendMessage(t, rn.Ready())
	if !bytes.Equal(a.Entries[0].Data, []byte("committed")) || !bytes.Equal(b.Entries[0].Data, []byte("old")) || !bytes.Equal(c.Entries[0].Data, []byte("replacement")) {
		t.Fatal("later Step altered accepted Ready entries")
	}
	if b.Entries[0] == c.Entries[0] {
		t.Fatal("replacement reuses mutable entry object")
	}
	t.Log("accepted Ready entries survive later suffix replacement, but remain shared read-only objects")
}
