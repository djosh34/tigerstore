package core

import (
	"cmp"
	"slices"

	"go.etcd.io/raft/v3"
)

// RaftProgress copies the leader's replication progress for one tracked
// member, including itself. Inflights bytes are not exported by Raft.
type RaftProgress struct {
	ID               uint64 `json:"id"`
	Match            uint64 `json:"match"`
	Next             uint64 `json:"next"`
	State            string `json:"state"`
	RecentActive     bool   `json:"recent_active"`
	MsgAppFlowPaused bool   `json:"msg_app_flow_paused"`
	InflightCount    int    `json:"inflight_count"`
	InflightFull     bool   `json:"inflight_full"`
}

// Observation copies queue lengths and Raft/application gauges. Only the
// goroutine that owns Node may call Observe. It does not advance Raft or
// allocate responses, and contains no references to mutable core state.
type Observation struct {
	PendingProposals int            `json:"pending_proposals"`
	QueuedWrites     int            `json:"queued_writes"`
	QueuedReads      int            `json:"queued_reads"`
	ReadIndexReads   int            `json:"read_index_reads"`
	ReadIndexBatches int            `json:"read_index_batches"`
	Leader           uint64         `json:"leader"`
	Term             uint64         `json:"term"`
	Applied          uint64         `json:"applied"`
	Revision         int64          `json:"revision"`
	RaftProgress     []RaftProgress `json:"raft_progress"`
}

// Observe reports physical queue slots, including timed-out references
// awaiting the existing cleanup. PendingProposals is the live waiting-write
// map and overlaps QueuedWrites. ReadIndexReads costs one length lookup per
// read batch; it does not walk requests or keys. Status clones Raft's full
// membership configuration and, on leaders, every progress/inflight buffer.
// This extra allocation/copy work occurs only on observation requests.
func (n *Node) Observe() Observation {
	st := n.rn.Status()
	o := Observation{
		PendingProposals: len(n.waiting), QueuedWrites: len(n.proposals),
		QueuedReads: len(n.reads), ReadIndexBatches: len(n.readBatches),
		Leader: st.Lead, Term: st.GetTerm(), Applied: n.applied, Revision: n.kv.Rev(),
		RaftProgress: observeRaftProgress(st),
	}
	for _, b := range n.readBatches {
		o.ReadIndexReads += len(b.reads)
	}
	return o
}

func observeRaftProgress(st raft.Status) []RaftProgress {
	out := make([]RaftProgress, 0, len(st.Progress))
	for id, p := range st.Progress {
		r := RaftProgress{
			ID: id, Match: p.Match, Next: p.Next, State: p.State.String(),
			RecentActive: p.RecentActive, MsgAppFlowPaused: p.MsgAppFlowPaused,
		}
		if p.Inflights != nil {
			r.InflightCount, r.InflightFull = p.Inflights.Count(), p.Inflights.Full()
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b RaftProgress) int { return cmp.Compare(a.ID, b.ID) })
	return out
}
