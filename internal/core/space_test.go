package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// TestTakesSpace checks which requests etcd v3.7.2 refuses while its
// NOSPACE alarm is active.
func TestTakesSpace(t *testing.T) {
	put := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k")}}}
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("k")}}}
	get := &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k")}}}
	nested := &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: []*pb.RequestOp{del}}}}
	for _, tc := range []struct {
		op   proto.Message
		want bool
	}{
		{nil, true}, // a put on the fast path
		{&pb.PutRequest{Key: []byte("k")}, true},
		{&pb.LeaseGrantRequest{TTL: 5}, true},
		{&pb.TxnRequest{Failure: []*pb.RequestOp{get, put}}, true},
		// The capped applier refuses deletes inside a transaction too.
		{&pb.TxnRequest{Success: []*pb.RequestOp{del}}, true},
		{&pb.TxnRequest{Success: []*pb.RequestOp{nested}}, true},
		{&pb.TxnRequest{Success: []*pb.RequestOp{get}, Failure: []*pb.RequestOp{get}}, false},
		{&pb.DeleteRangeRequest{Key: []byte("k")}, false},
		{&pb.CompactionRequest{Revision: 2}, false},
		{&pb.RangeRequest{Key: []byte("k")}, false},
	} {
		if got := TakesSpace(Request{Op: tc.op}); got != tc.want {
			t.Errorf("TakesSpace(%T %v) = %v", tc.op, tc.op, got)
		}
	}
}

// TestForwardedWritesGetNoSpace has a leader past its budget take writes
// that a follower forwards: the put and the lease grant are answered with
// NOSPACE and change nothing, the delete applies.
func TestForwardedWritesGetNoSpace(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.nodes[0].SetDisk(1, 1)
	f := c.nodes[1]
	f.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	f.Submit(Request{ID: 2, Op: &pb.LeaseGrantRequest{TTL: 10}})
	f.Submit(Request{ID: 3, Op: &pb.DeleteRangeRequest{Key: []byte("k")}})
	for range 100 {
		if c.noSpace+c.replies == 3 {
			break
		}
		c.step()
	}
	if c.noSpace != 2 || c.replies != 1 {
		t.Fatalf("%d NOSPACE answers and %d others, want 2 and 1", c.noSpace, c.replies)
	}
	for i, n := range c.nodes {
		if n.Store().Rev() != 1 || n.Hash() != c.nodes[0].Hash() {
			t.Fatalf("node %d is at revision %d", i+1, n.Store().Rev())
		}
	}
	c.nodes[0].SetDisk(0, 1)
	f.Submit(Request{ID: 4, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for range 100 {
		if c.replies == 2 {
			break
		}
		c.step()
	}
	if c.replies != 2 || f.Store().Rev() != 2 {
		t.Fatalf("the put after the budget: %d answers, revision %d", c.replies, f.Store().Rev())
	}
}

// TestQueuedWritesGetNoSpace has the leader take 100 puts while it may
// propose only a few, then pass its budget: the puts not proposed yet get
// NOSPACE.
func TestQueuedWritesGetNoSpace(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.UnappliedBytes, cfg.MaxBatchBytes = 8<<10, 2<<10 })
	l := c.nodes[0]
	for i := range 100 {
		l.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 1<<10)}})
	}
	c.step()
	l.SetDisk(1, 1)
	for range 100 {
		if c.noSpace+c.replies == 100 {
			break
		}
		c.step()
	}
	if c.noSpace+c.replies != 100 || c.replies > 10 {
		t.Fatalf("%d NOSPACE answers and %d others, want at most 10 others", c.noSpace, c.replies)
	}
}

// TestFollowerPastBudgetStopsWrites has a follower pass its budget (#170).
// It raises its NOSPACE alarm, so the leader refuses a put too, which the
// follower would otherwise append to its full disk. Once the follower's
// disk is low again it clears the alarm, and a put goes through.
func TestFollowerPastBudgetStopsWrites(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	l, f := c.nodes[0], c.nodes[2]
	f.SetDisk(1, 1)
	alarmed := func() bool {
		for _, a := range l.state.alarms {
			if a.Alarm == pb.AlarmType_NOSPACE && a.MemberID == f.cfg.ID {
				return true
			}
		}
		return false
	}
	// The alarm waits for the cluster version, which waits for every
	// node's announcement, which ticks send.
	tick := func() {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	for i := 0; i < 100 && !alarmed(); i++ {
		tick()
	}
	if !alarmed() {
		t.Fatal("the follower past its budget raised no alarm")
	}
	l.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for range 10 {
		c.step()
	}
	if c.noSpace != 1 || l.Store().Rev() != 1 {
		t.Fatalf("%d NOSPACE answers, revision %d; want the put refused", c.noSpace, l.Store().Rev())
	}
	f.SetDisk(0, 1)
	for i := 0; i < 100 && alarmed(); i++ {
		tick()
	}
	if alarmed() {
		t.Fatal("the follower kept its alarm with its disk low")
	}
	before := c.replies
	l.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for i := 0; i < 100 && c.replies == before; i++ {
		c.step()
	}
	if c.replies == before || l.Store().Rev() != 2 {
		t.Fatalf("the put after the alarm: %d answers, revision %d", c.replies-before, l.Store().Rev())
	}
}
