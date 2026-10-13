package peer

import (
	"context"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestObserveQueuesUsesExistingBudgets(t *testing.T) {
	tr := &TCP{cfg: Config{Unreachable: func(uint64) {}}, senders: map[uint64]*sender{}}
	for _, id := range []uint64{3, 2} {
		tr.senders[id] = &sender{t: tr, id: id, ctrl: make(chan *raftpb.Message, 4), data: make(chan *raftpb.Message, 4)}
	}
	data := &raftpb.Message{Type: raftpb.MsgApp.Enum(), To: new(uint64(2)), Context: []byte("abc"), Entries: []*raftpb.Entry{{Data: []byte("value")}}}
	ctrl := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), To: new(uint64(3))}
	tr.Send(data)
	tr.Send(ctrl)
	got := tr.ObserveQueues()
	if len(got) != 2 || got[0].ID != 2 || got[1].ID != 3 || got[0].DataBytes != size(data) || got[0].DataMessages != 1 || got[1].ControlBytes != size(ctrl) || got[1].ControlMessages != 1 {
		t.Fatalf("queue snapshots: %+v", got)
	}
	// Sampling must not dequeue, adjust budgets, or retain mutable state.
	got[0].DataBytes = -1
	if tr.ObserveQueues()[0].DataBytes != size(data) {
		t.Fatal("returned snapshot shares accounting")
	}
	m, ok := tr.senders[2].next(context.Background())
	if !ok || m != data || tr.ObserveQueues()[0].DataBytes != 0 || tr.ObserveQueues()[0].DataMessages != 0 {
		t.Fatal("dequeue accounting changed")
	}
	tr.senders[3].drop()
	if got := tr.ObserveQueues()[1]; got.ControlBytes != 0 || got.ControlMessages != 0 {
		t.Fatalf("drop accounting changed: %+v", got)
	}
}
