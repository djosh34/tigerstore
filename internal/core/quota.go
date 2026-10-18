package core

import (
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The space quota follows etcd v3.7.2's (server/storage/quota.go and its
// users in v3rpc/quota.go, apply/quota.go and EtcdServer.apply). etcd
// compares the size of its database file with --quota-backend-bytes;
// tigerstore compares what that database would hold for the versions in
// the index (mvcc.Store.Size), which is the same on every member. The log
// on disk is bounded by the disk budget instead.
//
// A put, a transaction with a put or a lease grant is refused with
// ErrNoSpace before it is proposed if the size plus its cost reaches the
// quota, as etcd's gRPC layer does, and the node raises a NOSPACE alarm
// for itself. One that was let in and finds the quota reached when it
// applies still applies, as in etcd, but its client gets ErrNoSpace, and
// every member raises its own NOSPACE alarm. While a NOSPACE alarm is
// active, such writes fail when they apply (alarm.go), and every member
// refuses them before it proposes them, until an operator disarms it. A
// node past its disk budget raises the same alarm for itself, and clears
// it once its files are below the budget again (SetDisk).

// QuotaBytes is etcd's default --quota-backend-bytes.
const QuotaBytes = 2 << 30

// etcd's cost estimates of a lease and of the metadata of a key.
const (
	leaseOverhead = 64
	kvOverhead    = 256
)

// quotaCost is etcd's cost of a request: 0 for one that adds nothing.
func quotaCost(op proto.Message) int64 {
	switch op := op.(type) {
	case *pb.PutRequest:
		return kvOverhead + int64(len(op.Key)+len(op.Value))
	case *pb.TxnRequest:
		return txnCost(op)
	case *pb.LeaseGrantRequest:
		return leaseOverhead
	}
	return 0
}

// putCost is quotaCost of a put in wire form.
func putCost(p *mvcc.PutOp) int64 { return kvOverhead + int64(len(p.Key)+len(p.Value)) }

// txnCost is the cost of the costlier branch, nested transactions
// included.
func txnCost(t *pb.TxnRequest) int64 {
	var costs [2]int64
	for i, ops := range [][]*pb.RequestOp{t.Success, t.Failure} {
		for _, o := range ops {
			switch {
			case o.GetRequestPut() != nil:
				costs[i] += quotaCost(o.GetRequestPut())
			case o.GetRequestTxn() != nil:
				costs[i] += txnCost(o.GetRequestTxn())
			}
		}
	}
	return max(costs[0], costs[1])
}

// quotaLeft reports whether a request of this cost fits the quota.
func (n *Node) quotaLeft(cost int64) bool {
	if cost == 0 || n.cfg.QuotaBytes < 0 {
		return true
	}
	quota := n.cfg.QuotaBytes
	if quota == 0 {
		quota = QuotaBytes
	}
	return n.kv.Size()+cost < quota
}

// refuseNoSpace refuses request id, which does not fit the quota, and
// raises the node's NOSPACE alarm. Like etcd's gRPC layer, it answers
// once the alarm applied, if it proposes it.
func (n *Node) refuseNoSpace(id uint64) {
	n.diskAlarm = false
	if !n.raiseNoSpace(id) {
		n.reply(id, rpctypes.ErrGRPCNoSpace)
	}
}

// overQuota is the answer to a write that applied past the quota.
func (n *Node) overQuota() error {
	n.diskAlarm = false
	n.raiseNoSpace(0)
	return rpctypes.ErrGRPCNoSpace
}

// raiseNoSpace proposes this node's NOSPACE alarm, unless it is active or
// a proposal of it is under way: one from the last leaseRetryTicks that
// has not applied. A node that replays its log proposes nothing: it
// raised the alarm when it applied the entry first. The alarm goes ahead
// of the writes that wait to be proposed, which it then refuses. If it
// proposes the alarm and id is not 0, request id gets ErrNoSpace once the
// alarm applied.
func (n *Node) raiseNoSpace(id uint64) bool {
	if !n.noSpaceDue(true) {
		return false
	}
	p := &pending{
		req:      Request{ID: id, Op: &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: n.cfg.ID}},
		deadline: n.now + int64(n.cfg.RequestTicks),
		refuse:   rpctypes.ErrGRPCNoSpace,
	}
	n.queueFeature(p)
	if p.queued {
		copy(n.proposals[1:], n.proposals[:len(n.proposals)-1])
		n.proposals[0] = p
	}
	if id != 0 {
		n.timeouts = append(n.timeouts, p)
	}
	return id != 0
}

// clearNoSpace proposes clearing this node's NOSPACE alarm, if it is
// active and no proposal of a change to it is under way (SetDisk).
func (n *Node) clearNoSpace() {
	if !n.noSpaceDue(false) {
		return
	}
	n.queueFeature(&pending{
		req:      Request{Op: &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: n.cfg.ID}},
		deadline: n.now + int64(n.cfg.RequestTicks),
	})
}

// noSpaceDue reports whether the node proposes setting its own NOSPACE
// alarm to active, and notes the proposal: not while it replays its log,
// not if the alarm is already so, and not within
// leaseRetryTicks of its last proposal of a change, which may not have
// applied yet.
func (n *Node) noSpaceDue(active bool) bool {
	if n.rn == nil || n.readInline {
		return false
	}
	if slices.ContainsFunc(n.state.alarms, func(a *pb.AlarmMember) bool {
		return a.Alarm == pb.AlarmType_NOSPACE && a.MemberID == n.cfg.ID
	}) == active {
		return false
	}
	if n.noSpaceAt >= 0 && n.now-n.noSpaceAt < leaseRetryTicks {
		return false
	}
	n.noSpaceAt = n.now
	return true
}
