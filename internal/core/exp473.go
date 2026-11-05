package core

// Exp473 counts the work of node starts (research #473, exp branch only):
// starts, checkpoints loaded and the keys and versions in them, entries
// replayed after the checkpoint, and entries after the commit index read
// into Raft's storage.
var Exp473 struct {
	Starts, CkptLoads, CkptKeys, CkptVersions, Replayed, Tail int64
}

// Exp473Index returns the keys and versions of the node's index.
func (n *Node) Exp473Index() (keys, versions int) { return n.kv.Exp473Size() }

// Exp473Leases returns the number of leases in the state, and how many of
// them hold no key.
func (n *Node) Exp473Leases() (leases, empty int) {
	for _, l := range n.state.leases {
		leases++
		if len(l.keys) == 0 {
			empty++
		}
	}
	return leases, empty
}
