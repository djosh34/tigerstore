package core

import (
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

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
		Leader: 2, Term: 10, Applied: 7, Revision: n.kv.Rev(),
	}
	for range 3 {
		if got := n.Observe(); got != want {
			t.Fatalf("observation %+v, want %+v", got, want)
		}
	}
	if !reflect.DeepEqual(n.out, before) || n.now != 0 || n.term == 10 || len(n.proposals) != 2 || !done.done {
		t.Fatal("observation changed core progress, effects or queues")
	}
}
