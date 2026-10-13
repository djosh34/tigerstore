package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Only the leader arms lease expiry, in ticks of its own clock, as etcd's
// primary lessor does. When a lease expires, the leader proposes its
// expiry like any write, so every replica deletes the keys at the same
// revision. The expiry applies only if the lease was not granted again or
// kept alive after the leader decided (newExpiry). A new leader gives
// every lease its full TTL again, so a leader change can make a lease live
// longer, never shorter.

// leaseRetryTicks is how long the leader waits for an expiry revoke to
// apply before it proposes it again; Raft may drop a proposal.
const leaseRetryTicks = 10

// arm starts or renews a lease's expiry on the leader. The current tick
// may be nearly over, so the lease gets one tick more than its TTL and
// never expires early.
func (n *Node) arm(id int64) {
	if l, ok := n.state.leases[id]; ok {
		l.expiry = n.now + l.ttl*int64(n.cfg.TicksPerSecond) + 1
		l.revokeAt = 0
	}
}

// leadershipChanged arms every lease when this node becomes leader and
// disarms them when it stops being leader.
func (n *Node) leadershipChanged() {
	for _, l := range n.state.sortedLeases() {
		l.expiry, l.revokeAt = 0, 0
		if n.isLeader {
			n.arm(l.id)
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
func (n *Node) proposeInternal(op *pb.LeaseCheckpoint) {
	n.proposals = append(n.proposals, &pending{req: Request{ID: 0, Op: op}})
}

// Leases returns the IDs of the granted leases in order.
func (n *Node) Leases() []int64 {
	var ids []int64
	for _, l := range n.state.sortedLeases() {
		ids = append(ids, l.id)
	}
	return ids
}
