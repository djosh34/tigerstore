package core

import (
	"slices"
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

// capable ticks the cluster until every node runs a cluster version with
// alarm entries, which waits for every node's announcement.
func (c *memCluster) capable() {
	for range 100 {
		if c.nodes[0].capable() && c.nodes[1].capable() && c.nodes[2].capable() {
			return
		}
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	c.t.Fatal("the cluster version allows no alarms")
}

// alarmed reports whether n has applied a NOSPACE alarm of member id.
func alarmed(n *Node, id uint64) bool {
	return slices.ContainsFunc(n.state.alarms, func(a *pb.AlarmMember) bool {
		return a.Alarm == pb.AlarmType_NOSPACE && a.MemberID == id
	})
}

// TestQuotaAlarmStays has a put reach the quota: its node raises its
// NOSPACE alarm, which stays while its disk is within the budget, as
// etcd's stays until an operator disarms it. After the disarm a put goes
// through.
func TestQuotaAlarmStays(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.QuotaBytes = 4 << 10 })
	c.capable()
	l := c.nodes[0]
	l.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 8<<10)}})
	for i := 0; i < 100 && !alarmed(l, l.cfg.ID); i++ {
		c.step()
	}
	if c.noSpace != 1 || !alarmed(l, l.cfg.ID) {
		t.Fatalf("%d NOSPACE answers, alarm %v; want the put refused and the alarm up", c.noSpace, alarmed(l, l.cfg.ID))
	}
	for range 3 * leaseRetryTicks {
		l.SetDisk(0, 1)
		l.Tick()
		c.step()
	}
	if !alarmed(l, l.cfg.ID) {
		t.Fatal("the quota's alarm cleared itself")
	}
	l.Submit(Request{ID: 2, Op: &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: l.cfg.ID}})
	for i := 0; i < 100 && alarmed(l, l.cfg.ID); i++ {
		c.step()
	}
	before := c.replies
	l.Submit(Request{ID: 3, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for i := 0; i < 100 && c.replies == before; i++ {
		c.step()
	}
	if alarmed(l, l.cfg.ID) || c.noSpace != 1 || l.Store().Rev() != 2 {
		t.Fatalf("after the disarm: alarm %v, %d NOSPACE answers, revision %d", alarmed(l, l.cfg.ID), c.noSpace, l.Store().Rev())
	}
}

// TestQueuedWritesGetNoSpace has the leader take 100 puts while it may
// propose only a few, then pass its budget: its alarm goes ahead of the
// puts not proposed yet, which get NOSPACE.
func TestQueuedWritesGetNoSpace(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.UnappliedBytes, cfg.MaxBatchBytes = 8<<10, 2<<10 })
	c.capable()
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
	up := func() bool { return alarmed(l, f.cfg.ID) }
	// The alarm waits for the cluster version, which waits for every
	// node's announcement, which ticks send.
	tick := func() {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	for i := 0; i < 100 && !up(); i++ {
		tick()
	}
	if !up() {
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
	for i := 0; i < 100 && up(); i++ {
		tick()
	}
	if up() {
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
