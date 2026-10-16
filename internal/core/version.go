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
// field 2 the number of the run of the node that proposed it (Config.Run),
// which a parser of the plain message keeps as an unknown field. A run's
// announcement can reach the leader late, after the node restarted with
// another binary and announced again, and apply before or after the newer
// one; the run number tells them apart (#174).
const announceRunField = 2

// announcement is the announcement of version v by run run.
func announcement(v uint32, run uint64) *wrapperspb.UInt32Value {
	m := wrapperspb.UInt32(v)
	b := protowire.AppendTag(nil, announceRunField, protowire.VarintType)
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(b, run))
	return m
}

// parseAnnouncement returns the version and run of an announcement, and
// whether it has a run: one from a build before #174 has none.
func parseAnnouncement(b []byte) (v uint32, run uint64, hasRun bool, err error) {
	var m wrapperspb.UInt32Value
	if err := proto.Unmarshal(b, &m); err != nil {
		return 0, 0, false, fmt.Errorf("%w: %w", errBadBatch, err)
	}
	for u := m.ProtoReflect().GetUnknown(); len(u) > 0; {
		num, typ, n := protowire.ConsumeTag(u)
		if n < 0 {
			return 0, 0, false, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
		if num == announceRunField && typ == protowire.VarintType {
			x, n := protowire.ConsumeVarint(u)
			if n < 0 {
				return 0, 0, false, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
			}
			run, hasRun, u = x, true, u[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, u)
		if n < 0 {
			return 0, 0, false, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
	}
	return m.Value, run, hasRun, nil
}

// announce records the newest version a voter supports, from an
// announcement by its run run, and raises the cluster version to the
// lowest version of all voters, once each has announced one. One from an
// earlier run than the voter's current announcement changes nothing,
// whenever it applies. One without a run, from a build before #174,
// always counts, as it did there.
func (st *State) announce(id uint64, v uint32, run uint64, hasRun bool, voters []uint64) {
	if !slices.Contains(voters, id) || hasRun && run < st.runs[id] {
		return
	}
	st.supports[id] = v
	if hasRun {
		st.runs[id] = run
	}
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

// applyVersion applies a node's announcement. A node whose binary is
// older than the version it makes active stops.
func (n *Node) applyVersion(origin uint64, b []byte) error {
	v, run, hasRun, err := parseAnnouncement(b)
	if err != nil {
		return err
	}
	n.state.announce(origin, v, run, hasRun, n.state.voters())
	if n.state.version > ownVersion(n.cfg) {
		return tooOld(n.state.version, ownVersion(n.cfg))
	}
	return nil
}

// announceVersion proposes the newest version this node supports until
// the state holds it, from this run, again after each election timeout,
// as Raft may drop a proposal.
func (n *Node) announceVersion() {
	own := ownVersion(n.cfg)
	// A new run announces even an unchanged version, so that its run
	// supersedes those before it: a late announcement of an earlier one
	// must not count.
	if n.state.supports[n.cfg.ID] == own && n.state.runs[n.cfg.ID] >= n.cfg.Run || n.leader == raft.None {
		return
	}
	if n.announcedAt != 0 && n.now-n.announcedAt < int64(n.cfg.ElectionTicks) {
		return
	}
	n.announcedAt = n.now
	n.proposeInternal(announcement(own, n.cfg.Run))
}
