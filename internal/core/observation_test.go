package core

import (
	"reflect"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestObservationCopiesQueuesWithoutProgress(t *testing.T) {
	n, err := New(DefaultConfig(1, []uint64{1, 2, 3}), &wal.Recovered{})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(10))}); err != nil {
		t.Fatal(err)
	}
	// Include expired physical references to fix the queue definitions.
	p, done := &pending{}, &pending{done: true}
	n.waiting[1], n.waiting[2] = p, p
	n.proposals = []*pending{p, done}
	n.reads = []*pending{p}
	n.readBatches = []*readBatch{{reads: []*pending{p, done}}, {reads: []*pending{p}}}
	n.applied = 7
	before := n.out
	want := Observation{
		PendingProposals: 2, QueuedWrites: 2, QueuedReads: 1,
		ReadIndexReads: 3, ReadIndexBatches: 2,
		Leader: 2, Term: 10, Applied: 7, Revision: n.kv.Rev(), RaftProgress: []RaftProgress{},
	}
	for range 3 {
		if got := n.Observe(); !reflect.DeepEqual(got, want) {
			t.Fatalf("observation %+v, want %+v", got, want)
		}
	}
	if !reflect.DeepEqual(n.out, before) || n.now != 0 || n.term == 10 || len(n.proposals) != 2 || !done.done {
		t.Fatal("observation changed core progress, effects or queues")
	}
}

func TestObservationRaftProgressCountAndByteFull(t *testing.T) {
	countFull := tracker.NewInflights(2, 1000)
	countFull.Add(10, 1)
	countFull.Add(11, 1)
	byteFull := tracker.NewInflights(256, 1000)
	byteFull.Add(20, 1000)
	st := raft.Status{Progress: map[uint64]tracker.Progress{
		3: {Match: 19, Next: 21, State: tracker.StateReplicate, RecentActive: true, MsgAppFlowPaused: true, Inflights: byteFull},
		2: {Match: 9, Next: 12, State: tracker.StateReplicate, MsgAppFlowPaused: true, Inflights: countFull},
		1: {Match: 21, Next: 22, State: tracker.StateProbe, RecentActive: true},
	}}
	got := observeRaftProgress(st)
	want := []RaftProgress{
		{ID: 1, Match: 21, Next: 22, State: "StateProbe", RecentActive: true},
		{ID: 2, Match: 9, Next: 12, State: "StateReplicate", MsgAppFlowPaused: true, InflightCount: 2, InflightFull: true},
		{ID: 3, Match: 19, Next: 21, State: "StateReplicate", RecentActive: true, MsgAppFlowPaused: true, InflightCount: 1, InflightFull: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("progress %+v, want %+v", got, want)
	}
	byteFull.FreeLE(20)
	countFull.FreeLE(11)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("progress snapshot retained inflights pointers")
	}
}

func TestObservationRaftStatusClone(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	o := s.n.Observe()
	if len(o.RaftProgress) != 1 || o.RaftProgress[0].ID != 1 || o.RaftProgress[0].State != "StateReplicate" || !o.RaftProgress[0].RecentActive {
		t.Fatalf("leader progress: %+v", o.RaftProgress)
	}
	// Status exposes clones. Mutating one cannot change a later sample
	// or live Raft inflights, and the observation itself retains scalars.
	st := s.n.rn.Status()
	p := st.Progress[1]
	p.Inflights.Add(o.RaftProgress[0].Next, 100)
	o.RaftProgress[0].Match = 999
	after := s.n.Observe()
	if after.RaftProgress[0].InflightCount != 0 || after.RaftProgress[0].Match == 999 || s.n.rn.Status().Progress[1].Inflights.Count() != 0 {
		t.Fatal("status sampling shares live progress")
	}
}
