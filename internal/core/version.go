package core

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The cluster version is the entry format every node writes. Each node
// announces through Raft the newest version it supports, and once every
// voter has announced, the cluster version rises to the lowest of their
// versions. It is replicated state, held in checkpoints, and never goes
// down: a node whose announcement of a lower version applies before then
// holds it back, and a binary older than the cluster version refuses to
// start.

// MaxVersion is the newest cluster version, and so entry format, this
// build supports. It reads every format up to it.
const MaxVersion = 3

// featureVersion is the cluster version, and entry format, from which
// entries may hold the writes of etcd's features that format 2 has no
// kind for: alarm changes, members' attributes and member changes. Until it is active, such writes wait for it,
// up to their deadline, as writes wait for a leader. A fresh cluster
// activates it once every voter's announcement applied.
const featureVersion = 3

// capable reports whether the cluster version allows the kinds of
// featureVersion.
func (n *Node) capable() bool { return n.state.version >= featureVersion }

// queueFeature queues a write that needs featureVersion, or keeps it
// until that version is active.
func (n *Node) queueFeature(p *pending) {
	if n.capable() {
		n.queueWrite(p)
		return
	}
	p.early = true
	n.early = append(n.early, p)
}

// releaseEarly queues the writes that waited for featureVersion once it
// is active.
func (n *Node) releaseEarly() {
	if len(n.early) == 0 || !n.capable() {
		return
	}
	for _, p := range n.early {
		switch {
		case p.done:
		case p.conf != nil:
			p.early = false
			n.confs = append(n.confs, p)
		default:
			p.early = false
			n.queueWrite(p)
		}
	}
	n.early = nil
}

// ErrTooOld stops a node whose cluster runs a newer version than its
// binary supports.
var ErrTooOld = errors.New("core: this binary is too old for the cluster")

func tooOld(cluster, own uint32) error {
	return fmt.Errorf("%w: the cluster runs version %d and this binary supports versions up to %d; a version never goes down once active, so start a binary that supports version %d",
		ErrTooOld, cluster, own, cluster)
}

// An announcement is a wrapperspb.UInt32Value with the version, and in
// field 2 the applied index of the node that proposed it, which a parser
// of the plain message keeps as an unknown field. A run's announcement can
// reach the leader late, after the node restarted with another binary and
// announced again; base tells it apart: it is below the index at which
// the newer announcement applied, which the node had not seen (#174).
const announceBaseField = 2

// announcement is the announcement of version v, proposed at applied
// index base.
func announcement(v uint32, base uint64) *wrapperspb.UInt32Value {
	m := wrapperspb.UInt32(v)
	b := protowire.AppendTag(nil, announceBaseField, protowire.VarintType)
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(b, base))
	return m
}

// parseAnnouncement returns the version and base of an announcement. One
// without a base, from a build before #174, has base 0.
func parseAnnouncement(b []byte) (uint32, uint64, error) {
	var m wrapperspb.UInt32Value
	if err := proto.Unmarshal(b, &m); err != nil {
		return 0, 0, fmt.Errorf("%w: %w", errBadBatch, err)
	}
	var base uint64
	for u := m.ProtoReflect().GetUnknown(); len(u) > 0; {
		num, typ, n := protowire.ConsumeTag(u)
		if n < 0 {
			return 0, 0, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
		if num == announceBaseField && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(u)
			if n < 0 {
				return 0, 0, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
			}
			base, u = v, u[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, u)
		if n < 0 {
			return 0, 0, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
	}
	return m.Value, base, nil
}

// announce records the newest version a voter supports, from an
// announcement proposed at applied index base that applies at index, and
// raises the cluster version to the lowest version of all voters, once
// each has announced one. An announcement proposed before the voter's
// current one applied is from before it, maybe from an earlier run, and
// changes nothing.
func (st *State) announce(id uint64, v uint32, base, index uint64, voters []uint64) {
	if !slices.Contains(voters, id) || base < st.announced[id] {
		return
	}
	st.supports[id], st.announced[id] = v, index
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

// ownVersion is the newest version this node supports.
func ownVersion(cfg Config) uint32 { return cmp.Or(cfg.MaxVersion, MaxVersion) }

// ActiveVersion is the cluster version this node has applied.
func (n *Node) ActiveVersion() uint32 { return n.state.version }

// applyVersion applies a node's announcement at index. A node whose
// binary is older than the version it makes active stops.
func (n *Node) applyVersion(origin, index uint64, b []byte) error {
	v, base, err := parseAnnouncement(b)
	if err != nil {
		return err
	}
	n.state.announce(origin, v, base, index, n.state.voters())
	if n.state.version > ownVersion(n.cfg) {
		return tooOld(n.state.version, ownVersion(n.cfg))
	}
	return nil
}

// announceVersion proposes the newest version this node supports until
// the state holds it, again after each election timeout, as Raft may
// drop a proposal.
func (n *Node) announceVersion() {
	own := ownVersion(n.cfg)
	if n.state.supports[n.cfg.ID] == own || n.leader == raft.None {
		return
	}
	if n.announcedAt != 0 && n.now-n.announcedAt < int64(n.cfg.ElectionTicks) {
		return
	}
	n.announcedAt = n.now
	n.proposeInternal(announcement(own, n.applied))
}
