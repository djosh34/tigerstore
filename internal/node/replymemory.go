package node

import "sync/atomic"

// ReplyHold keeps a Range's memory reservation until every owner has
// dropped its reference. A writer retains one while using the answer in
// socket writes, so a concurrent stream reset cannot release it early.
type ReplyHold struct {
	bytes   int64
	release func(int64)
	refs    atomic.Int64
}

// NewReplyHold transfers a reservation to the transport. release reports
// its bytes to the driver when the last reference goes away.
func NewReplyHold(bytes int64, release func(int64)) *ReplyHold {
	h := &ReplyHold{bytes: bytes, release: release}
	h.refs.Store(1)
	return h
}

// Retain keeps the reservation for another owner. Nil holds do nothing.
func (h *ReplyHold) Retain() {
	if h != nil {
		h.refs.Add(1)
	}
}

// Release drops one owner's reference. Nil holds do nothing.
func (h *ReplyHold) Release() {
	if h != nil && h.refs.Add(-1) == 0 {
		h.release(h.bytes)
	}
}

func (n *Node) holdReply(bytes int64) *ReplyHold {
	if bytes == 0 {
		return nil
	}
	n.replyIssued += bytes
	return NewReplyHold(bytes, func(bytes int64) {
		n.replyFreed.Add(bytes)
		// Wake the committer without waiting for it. A full inbox already
		// guarantees a turn that consumes the accumulated releases.
		select {
		case n.inbox <- input{}:
		default:
		}
	})
}
