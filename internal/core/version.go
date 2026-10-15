package core

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The cluster version is the entry format every node writes. Each node
// announces through Raft the newest version it supports, and once every
// voter has announced, the cluster version rises to the lowest of their
// versions. It is replicated state, held in checkpoints, and never goes
// down: a node announcing a lower version before then holds it back, and
// a binary older than the cluster version refuses to start.

// MaxVersion is the newest cluster version, and so entry format, this
// build supports. It reads every format up to it.
const MaxVersion = 2

// ErrTooOld stops a node whose cluster runs a newer version than its
// binary supports.
var ErrTooOld = errors.New("core: this binary is too old for the cluster")

func tooOld(cluster, own uint32) error {
	return fmt.Errorf("%w: the cluster runs version %d and this binary supports versions up to %d; a version never goes down once active, so start a binary that supports version %d",
		ErrTooOld, cluster, own, cluster)
}

// announce records the newest version a voter supports and raises the
// cluster version to the lowest version of all voters, once each has
// announced one.
func (st *State) announce(id uint64, v uint32, voters []uint64) {
	if !slices.Contains(voters, id) {
		return
	}
	st.supports[id] = v
	low := v
	for _, voter := range voters {
		s, ok := st.supports[voter]
		if !ok {
			return
		}
		low = min(low, s)
	}
	st.version = max(st.version, low)
}

// maxVersion is the newest version a node supports.
func maxVersion(cfg Config) uint32 { return cmp.Or(cfg.MaxVersion, MaxVersion) }

// ActiveVersion is the cluster version this node has applied.
func (n *Node) ActiveVersion() uint32 { return n.state.version }

// applyVersion applies a node's announcement. A node whose binary is
// older than the version it makes active stops.
func (n *Node) applyVersion(origin uint64, b []byte) error {
	var v wrapperspb.UInt32Value
	if err := proto.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	n.state.announce(origin, v.Value, n.cfg.Peers)
	if n.state.version > maxVersion(n.cfg) {
		return tooOld(n.state.version, maxVersion(n.cfg))
	}
	return nil
}

// announceVersion proposes the newest version this node supports until
// the state holds it, again after each election timeout, as Raft may
// drop a proposal.
func (n *Node) announceVersion() {
	own := maxVersion(n.cfg)
	if n.state.supports[n.cfg.ID] == own || n.leader == raft.None {
		return
	}
	if n.announcedAt != 0 && n.now-n.announcedAt < int64(n.cfg.ElectionTicks) {
		return
	}
	n.announcedAt = n.now
	n.queueWrite(&pending{req: Request{Op: wrapperspb.UInt32(own)}})
}
