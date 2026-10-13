package core

// Observation copies queue lengths and Raft/application gauges. Only the
// goroutine that owns Node may call Observe. It does not advance Raft or
// allocate responses, and contains no references to mutable core state.
type Observation struct {
	PendingProposals int    `json:"pending_proposals"`
	QueuedWrites     int    `json:"queued_writes"`
	QueuedReads      int    `json:"queued_reads"`
	ReadIndexReads   int    `json:"read_index_reads"`
	ReadIndexBatches int    `json:"read_index_batches"`
	Leader           uint64 `json:"leader"`
	Term             uint64 `json:"term"`
	Applied          uint64 `json:"applied"`
	Revision         int64  `json:"revision"`
}

// Observe reports physical queue slots, including timed-out references
// awaiting the existing cleanup. PendingProposals is the live waiting-write
// map and overlaps QueuedWrites. ReadIndexReads costs one length lookup per
// read batch; it does not walk requests or keys.
func (n *Node) Observe() Observation {
	st := n.rn.BasicStatus()
	o := Observation{
		PendingProposals: len(n.waiting), QueuedWrites: len(n.proposals),
		QueuedReads: len(n.reads), ReadIndexBatches: len(n.readBatches),
		Leader: st.Lead, Term: st.GetTerm(), Applied: n.applied, Revision: n.kv.Rev(),
	}
	for _, b := range n.readBatches {
		o.ReadIndexReads += len(b.reads)
	}
	return o
}
