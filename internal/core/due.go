package core

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/planted"
)

// The core's time-based work runs on deadlines, not on every tick (#488,
// #504). Each part knows when it next has to act: the leader's next
// heartbeat, a follower's earliest election timeout, a request's deadline,
// a read index to ask for again, a token to drop, a progress notification.
// Due is the earliest of them. Until then a Tick only moves the clocks and
// Raft's election clock, and its checks find nothing to do, so a driver
// may hold the ticks back and wake only when one is due or something
// happens. It then gives the core every tick it held back, each a Tick, in
// order, before anything else, and Raft sees the same ticks as if each had
// come on time.

// Due is how many ticks may pass before a tick has work to do, at least
// 1: Tick does nothing that shows before then.
func (n *Node) Due() int64 {
	due := n.raftDue()
	if due <= n.now+1 {
		return 1
	}
	if n.removeAt != 0 {
		due = min(due, n.removeAt)
	}
	for _, p := range n.timeouts {
		if !p.done && !p.held {
			due = min(due, n.now+p.deadline-n.clock)
			break
		}
	}
	for _, w := range n.tokenWaits {
		// Process times them out, and the turn of a due tick runs it.
		due = min(due, n.now+w.deadline-n.clock)
	}
	for _, b := range n.readBatches {
		if b.index == 0 {
			due = min(due, b.sent+readRetryTicks)
		}
	}
	if n.wantsPublish() {
		due = min(due, n.again(n.publishedAt))
	}
	if n.wantsAnnounce() {
		due = min(due, n.again(n.announcedAt))
	}
	if at := n.noSpaceAt + leaseRetryTicks; n.noSpaceAt >= 0 && at > n.now {
		// The driver's turn then proposes the alarm change again
		// (SetDisk), if it still has to; from then on any turn does.
		due = min(due, at)
	}
	if len(n.tokens) > 0 {
		due = min(due, n.secondEnd(n.tokenDue))
	}
	if p := int64(n.cfg.ProgressTicks); p > 0 && len(n.watchers) > 0 {
		// Notifications go out every p ticks, counted from the node's
		// start; with no watcher there is nothing to notify.
		due = min(due, (n.now/p+1)*p)
	}
	return max(due-n.now, 1)
}

// again is the tick at which a proposal made at tick at, or never if 0,
// goes out again: an election timeout later, as publish and
// announceVersion retry.
func (n *Node) again(at int64) int64 {
	if at == 0 {
		return n.now + 1
	}
	return at + int64(n.cfg.ElectionTicks)
}

// raftDue is the next tick at which Raft may act on a tick. A leader acts
// on every tick: heartbeats, and the check that a quorum is still there.
// Any other node only campaigns, once its election clock passes a timeout
// Raft draws between ElectionTicks and twice that; the clock starts again
// from 0 whenever Raft resets it (electionFrom).
func (n *Node) raftDue() int64 {
	if n.raftState == raft.StateLeader || planted.On("B10") {
		return n.now + 1
	}
	return n.electionFrom + int64(n.cfg.ElectionTicks)
}

// noteRaft notes a reset of Raft's election clock. Raft resets it when the
// node changes its role, term or leader, and when a follower hears from
// its leader in its term: an append, a heartbeat or a snapshot. It resets
// it at other times too, which noteRaft does not see: electionFrom is then
// older than Raft's, and the node only wakes earlier than it must. m, if
// set, is the message Raft just stepped.
func (n *Node) noteRaft(m *raftpb.Message) {
	st := n.rn.BasicStatus()
	if st.RaftState != n.raftState || st.GetTerm() != n.raftTerm || st.Lead != n.raftLead {
		n.raftState, n.raftTerm, n.raftLead = st.RaftState, st.GetTerm(), st.Lead
		n.electionFrom = n.now
		return
	}
	if m == nil || st.RaftState != raft.StateFollower || m.GetTerm() != st.GetTerm() {
		return
	}
	if t := m.GetType(); t == raftpb.MsgApp || t == raftpb.MsgHeartbeat || t == raftpb.MsgSnap {
		n.electionFrom = n.now
	}
}

// secondEnd is the first tick after now, and at or after t, that ends a
// second (Config.every), when Tick runs the work it does once a second:
// the first tick of second j is the first whose end reaches j seconds.
func (n *Node) secondEnd(t int64) int64 {
	t = max(t, n.now+1)
	tm := n.cfg.TickMillis
	j := (t-1)*tm/1000 + 1
	return (j*1000 + tm - 1) / tm
}
