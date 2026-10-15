package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/planted"
)

// Only the leader arms lease expiry, in ticks of its own clock, as etcd's
// primary lessor does. When a lease expires, the leader proposes its
// expiry like any write, so every replica deletes the keys at the same
// revision. The expiry applies only if the lease was not granted again or
// kept alive after the leader decided (newExpiry). A new leader gives
// every lease its full TTL again, so a leader change can make a lease live
// longer, never shorter.
//
// With LeaseCheckpointTicks, as with etcd's LeaseCheckpoint feature, the
// leader writes a lease's remaining TTL through Raft that long after it
// armed it, again after each checkpoint, while more than that is left.
// The remaining TTL is replicated state, held in checkpoints, and a new
// leader arms a lease for it instead of its full TTL; a keepalive ends
// it. etcd checkpoints a lease whose remaining TTL is below its TTL only.

// leaseRetryTicks is how long the leader waits for an expiry revoke to
// apply before it proposes it again; Raft may drop a proposal.
const leaseRetryTicks = 10

// arm starts or renews a lease's expiry on the leader. The current tick
// may be nearly over, so the lease gets one tick more than its TTL and
// never expires early.
func (n *Node) arm(id int64) {
	if l, ok := n.state.leases[id]; ok {
		l.expiry = n.now + l.left()*int64(n.cfg.TicksPerSecond) + 1
		l.revokeAt = 0
	}
}

// left is the TTL a lease gets when the leader arms it: the remaining
// TTL of its last checkpoint, or else its TTL.
func (l *lease) left() int64 {
	if l.remaining > 0 {
		return l.remaining
	}
	return l.ttl
}

// scheduleCheckpoint is etcd's scheduleCheckpointIfNeeded: on the leader,
// a lease with more than LeaseCheckpointTicks left gets a checkpoint
// then.
func (n *Node) scheduleCheckpoint(id int64) {
	every := int64(n.cfg.LeaseCheckpointTicks)
	l, ok := n.state.leases[id]
	if !ok || every == 0 || !n.isLeader || l.left()*int64(n.cfg.TicksPerSecond) <= every {
		return
	}
	l.checkpointAt = n.now + every
}

// checkpointLeases proposes the checkpoints that are due, on the leader,
// with each lease's remaining TTL in whole seconds, rounded up, as etcd's
// findDueScheduledCheckpoints does.
func (n *Node) checkpointLeases() {
	if n.cfg.LeaseCheckpointTicks == 0 || !n.isLeader || !n.capable() {
		return
	}
	tps := int64(n.cfg.TicksPerSecond)
	var cps []*pb.LeaseCheckpoint
	for _, l := range n.state.sortedLeases() {
		if l.checkpointAt == 0 || l.checkpointAt > n.now || l.expiry == 0 {
			continue
		}
		l.checkpointAt = 0
		left := (l.expiry - n.now - 1 + tps - 1) / tps
		if left >= l.ttl {
			continue
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: l.id, Remaining_TTL: left})
		if len(cps) == maxLeaseCheckpoints {
			n.proposeInternal(&pb.LeaseCheckpointRequest{Checkpoints: cps})
			cps = nil
		}
	}
	if len(cps) > 0 {
		n.proposeInternal(&pb.LeaseCheckpointRequest{Checkpoints: cps})
	}
}

// maxLeaseCheckpoints is etcd's maxLeaseCheckpointBatchSize.
const maxLeaseCheckpoints = 1000

// leadershipChanged arms every lease when this node becomes leader and
// disarms them when it stops being leader.
func (n *Node) leadershipChanged() {
	for _, l := range n.state.sortedLeases() {
		l.expiry, l.revokeAt, l.checkpointAt = 0, 0, 0
		if n.isLeader {
			n.arm(l.id)
			n.scheduleCheckpoint(l.id)
		}
	}
}

// remaining is the leader's count of whole seconds left on a lease,
// rounded down as in etcd, or -1 on other nodes, which report the granted
// TTL.
func (n *Node) remaining(id int64) int64 {
	l, ok := n.state.leases[id]
	if !ok || !n.isLeader || l.expiry == 0 {
		return -1
	}
	tps := int64(n.cfg.TicksPerSecond)
	return max(0, (l.expiry-n.now-1)/tps)
}

// expireLeases proposes the revoke of every expired lease, on the leader.
func (n *Node) expireLeases() {
	if !n.isLeader && planted.On("B10") {
		for _, l := range n.state.sortedLeases() {
			if l.expiry != 0 && l.expiry <= n.now {
				if _, err := n.state.revoke(l.id); err != nil {
					n.fail(err)
				}
			}
		}
		return
	}
	if !n.isLeader {
		return
	}
	for _, l := range n.state.sortedLeases() {
		if l.expiry == 0 || l.expiry > n.now {
			continue
		}
		if l.revokeAt != 0 && n.now-l.revokeAt < leaseRetryTicks {
			continue
		}
		l.revokeAt = n.now
		n.proposeInternal(newExpiry(l.id, l.stamp))
	}
}

// proposeInternal queues a write that no client waits for. It carries
// request ID 0, which no client request has.
func (n *Node) proposeInternal(op proto.Message) {
	n.queueWrite(&pending{req: Request{ID: 0, Op: op}})
}

// Leases returns the IDs of the granted leases in order.
func (n *Node) Leases() []int64 {
	var ids []int64
	for _, l := range n.state.sortedLeases() {
		ids = append(ids, l.id)
	}
	return ids
}
