package core

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
// it, and schedules a checkpoint if none is due. etcd checkpoints a lease
// whose remaining TTL is below its TTL only.

// leaseRetryTicks is how long the leader waits for an expiry revoke to
// apply before it proposes it again; Raft may drop a proposal.
const leaseRetryTicks = 10

// arm starts or renews a lease's expiry on the leader. The current tick
// may be nearly over, so the lease gets one tick more than its TTL and
// never expires early.
func (n *Node) arm(id int64) {
	if l, ok := n.state.leases[id]; ok {
		l.expiry = n.now + n.cfg.ticksOf(l.left()) + 1
		l.revokeAt = 0
		n.expiries.set(l)
	}
}

// due is the tick at which the leader next proposes the lease's expiry:
// at its expiry, or once its last proposal had leaseRetryTicks to apply.
func (l *lease) due() int64 {
	if l.revokeAt != 0 {
		return l.revokeAt + leaseRetryTicks
	}
	return l.expiry
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
	if !ok || every == 0 || !n.isLeader || n.cfg.ticksOf(l.left()) <= every {
		return
	}
	l.checkpointAt = n.now + every
	n.checkpoints.set(l)
}

// LeaseTTLs returns the granted leases' TTLs by their IDs, for the
// offline upgrade to internal/wl, which takes a snapshot of this engine.
func (n *Node) LeaseTTLs() map[int64]int64 {
	ttls := make(map[int64]int64, len(n.state.leases))
	for _, l := range n.state.leases {
		ttls[l.id] = l.ttl
	}
	return ttls
}
