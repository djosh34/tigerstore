package core

import "fmt"

// Exp473 counts the work of node starts (research #473, exp branch only):
// starts, checkpoints loaded and the keys and versions in them, entries
// replayed after the checkpoint, and entries after the commit index read
// into Raft's storage.
var Exp473 struct {
	Starts, CkptLoads, CkptKeys, CkptVersions, Replayed, Tail int64
	// LeaseCkpts counts lease checkpoint proposals, LeaseCkptCfg the
	// LeaseCheckpointTicks of the last node started.
	LeaseCkpts, LeaseCkptCfg int64
}

// Exp473NoSort makes State.Hash fold the leases in without sorting them.
var Exp473NoSort bool

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

// Exp473LeaseState reports whether the node leads, the cluster version,
// and how many leases have a checkpoint scheduled and an expiry armed.
func (n *Node) Exp473LeaseState() string {
	sched, armed := 0, 0
	for _, l := range n.state.leases {
		if l.checkpointAt != 0 {
			sched++
		}
		if l.expiry != 0 {
			armed++
		}
	}
	return fmt.Sprintf("leader:%v,version:%d,capable:%v,sched:%d,armed:%d,now:%d", n.isLeader, n.state.version, n.capable(), sched, armed, n.now)
}
