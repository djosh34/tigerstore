//go:build sim || simio

package wl

import (
	"encoding/binary"
	"hash/fnv"
	"math"

	pb "go.etcd.io/raft/v3/raftpb"
)

// The simulator's hooks (sys.go's Hooks): each call hands the node's
// Config.Hooks, if set, what it recovered, appended, applied and made
// durable, and the end of each turn of its loop.

// simState is the simulator's state of a node: the store's revision
// before the entry being applied, whose changes come after it.
type simState struct{ rev int64 }

func (n *node) simRecovered() {
	n.sys.sim.rev = n.st.rev
	if h := n.sys.h; h != nil {
		h.Recovered(n.storage.lastIndex(), n.storage.hs)
	}
}

func (n *node) simAppend(m *pb.Message) {
	h := n.sys.h
	if h == nil {
		return
	}
	sn, ents := m.GetSnapshot(), m.GetEntries()
	last := uint64(0)
	if len(ents) > 0 {
		last = ents[len(ents)-1].GetIndex()
	}
	switch {
	case sn != nil && sn.GetMetadata().GetIndex() > 0:
		h.Append(sn.GetMetadata().GetIndex()+1, max(last, sn.GetMetadata().GetIndex()), true)
	case len(ents) > 0:
		h.Append(ents[0].GetIndex(), last, false)
	default:
		h.Append(math.MaxUint64, 0, false)
	}
}

// simApplied hashes the changes entry e made: those after the revision
// before it, each key's name and its record at the change's revision, or
// its deletion.
func (n *node) simApplied(e *pb.Entry) {
	st := n.st
	before := n.sys.sim.rev
	n.sys.sim.rev = st.rev
	h := n.sys.h
	if h == nil {
		return
	}
	sum := fnv.New64a()
	j := searchChanges(st.changes, before+1)
	var b [17]byte
	for _, ch := range st.changes[j:] {
		binary.LittleEndian.PutUint64(b[:], uint64(ch.rev))
		binary.LittleEndian.PutUint64(b[8:], uint64(len(st.key(ch.key))))
		b[16] = 0
		if ch.del {
			b[16] = 1
		}
		_, _ = sum.Write(b[:])
		_, _ = sum.Write(st.key(ch.key))
		if !ch.del {
			rec, err := st.at(ch.key, ch.rev)
			if err != nil {
				// A record the store cannot read stops the node elsewhere;
				// the hash says so.
				_, _ = sum.Write([]byte(err.Error()))
				continue
			}
			_, _ = sum.Write(rec)
		}
	}
	h.Applied(e, sum.Sum64())
}

func (n *node) simDurable(j *walJob) {
	h := n.sys.h
	if h == nil {
		return
	}
	for _, r := range j.resps {
		if r.GetTo() != n.id || r.GetType() != pb.MsgStorageAppendResp {
			continue
		}
		if sn := r.GetSnapshot(); sn != nil && sn.GetMetadata().GetIndex() > 0 {
			h.Durable(max(r.GetIndex(), sn.GetMetadata().GetIndex()), true, j.hs)
			return
		}
		h.Durable(r.GetIndex(), false, j.hs)
		return
	}
	h.Durable(0, false, j.hs)
}

func (n *node) simTurn() {
	h := n.sys.h
	if h == nil {
		return
	}
	first, _ := n.storage.FirstIndex()
	watchers := 0
	for i := range n.w.ws {
		if n.w.ws[i].active {
			watchers++
		}
	}
	h.Turn(TurnState{
		Lead: n.lead, Term: n.term, Commit: n.commit, Applied: n.applied,
		First: first, Format: n.format, Rev: n.st.rev, Watchers: watchers,
	})
}

// simInstalled starts the changes from the store a snapshot installed.
func (n *node) simInstalled() { n.sys.sim.rev = n.st.rev }
