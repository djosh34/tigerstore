package core

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// A revoke of a lease with many keys, by a client or by its expiry,
// deletes them a step at a time, revokeKeys at most, all at one revision
// (#505). Ordered apply waits behind it, so nothing else writes
// meanwhile; the store moves to the revision only at the last step, so a
// read sees the revoke whole or not at all. A watcher that takes its
// keys falls behind and reads the revision from the history once it is
// whole.

// revokeWrite is a revoke that goes in steps: the write of entry index
// that applies it, the request that waits for it, if any, its op and the
// lease it found, and whether the quota had room for it.
type revokeWrite struct {
	index uint64
	write int
	p     *pending
	op    proto.Message
	lease *lease
	room  bool
}

// revokeStep takes the next step of the revoke under way. Its last
// answers the request that waits for it, if any, and lets ordered apply
// go on from the write after it.
func (n *Node) revokeStep() {
	r := n.revoke
	resp, err := n.state.RevokePiece()
	n.matchWatches()
	if errors.Is(err, errRevoking) {
		return
	}
	n.revoke = nil
	resp, err = n.afterApply(r.op, r.lease, resp, err)
	if errors.Is(err, errInternal) {
		n.fail(err)
		return
	}
	if err == nil && !r.room {
		resp, err = nil, n.overQuota(r.p)
	}
	n.timed = timedApply{op: opName(r.op), err: err}
	if r.p != nil {
		n.answer(r.p, Reply{ID: r.p.req.ID, Resp: resp, Err: err}, nil)
	}
	n.resume = &pausedWrite{index: r.index, write: r.write + 1}
	if !n.readInline {
		n.runApply()
		n.resume = nil
	}
}

// applyWaits reports whether ordered apply waits: for values from the
// log, or for the steps of a revoke.
func (n *Node) applyWaits() bool { return n.paused != nil || n.revoke != nil }

// Working reports whether the node has work for its next turn already: a
// watcher that catches up with room for its next piece, or the next step
// of a revoke. The driver then gives it that turn at once.
func (n *Node) Working() bool { return n.revoke != nil || n.CatchingUp() }
