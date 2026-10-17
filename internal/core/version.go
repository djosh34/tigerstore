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
// announces through Raft the newest version it supports. Before raising
// the version, every voter must confirm the same set of announcements.
// A stopped voter cannot approve an upgrade using its earlier support.
// The version never goes down; an older binary refuses to start.

// MaxVersion is the newest cluster version, and so entry format, this
// build supports. It reads every format up to it.
const MaxVersion = 4

// featureVersion is the cluster version, and entry format, from which
// entries may hold the writes of etcd's features that format 2 has no
// kind for: alarm changes, members' attributes and member changes. Until it is active, such writes wait for it,
// up to their deadline, as writes wait for a leader. A fresh cluster
// activates it once every voter has confirmed its support.
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

// Announcements extend UInt32Value with the durable run number and the
// epoch of the support table the sender has applied. The latter prevents
// support from a stopped run from activating a later upgrade (#174).
const (
	announceRunField   = 2
	announceEpochField = 3
)

// announcement is the announcement format used before confirmations.
// Keep it for replay of old logs.
func announcement(v uint32, run uint64) *wrapperspb.UInt32Value {
	m := wrapperspb.UInt32(v)
	b := protowire.AppendTag(nil, announceRunField, protowire.VarintType)
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(b, run))
	return m
}

func confirmation(v uint32, run, epoch uint64) *wrapperspb.UInt32Value {
	m := announcement(v, run)
	b := protowire.AppendTag(m.ProtoReflect().GetUnknown(), announceEpochField, protowire.VarintType)
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(b, epoch))
	return m
}

type versionAnnouncement struct {
	version          uint32
	run, epoch       uint64
	hasRun, hasEpoch bool
}

func parseAnnouncement(b []byte) (a versionAnnouncement, err error) {
	var m wrapperspb.UInt32Value
	if err := proto.Unmarshal(b, &m); err != nil {
		return a, fmt.Errorf("%w: %w", errBadBatch, err)
	}
	for u := m.ProtoReflect().GetUnknown(); len(u) > 0; {
		num, typ, n := protowire.ConsumeTag(u)
		if n < 0 {
			return a, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
		if (num == announceRunField || num == announceEpochField) && typ == protowire.VarintType {
			x, n := protowire.ConsumeVarint(u)
			if n < 0 {
				return a, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
			}
			if num == announceRunField {
				a.run, a.hasRun = x, true
			} else {
				a.epoch, a.hasEpoch = x, true
			}
			u = u[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, u)
		if n < 0 {
			return a, fmt.Errorf("%w: announcement: %w", errBadBatch, protowire.ParseError(n))
		}
		u = u[n:]
	}
	a.version = m.Value
	return a, nil
}

// recordVersion rejects superseded runs. Any change of support or run
// invalidates all confirmations, including ones delayed in the network.
// Use its log index, not a counter of changes: old checkpoints contain no
// epoch, so a counter would differ between checkpoint and full-log replay.
// Legacy announcements use zero and keep their original apply semantics.
func (st *State) recordVersion(id uint64, v uint32, run uint64, hasRun bool, voters []uint64, index uint64) bool {
	if !slices.Contains(voters, id) || hasRun && run < st.runs[id] {
		return false
	}
	if st.supports[id] != v || hasRun && st.runs[id] != run || index != 0 && st.versionEpoch == 0 {
		st.versionEpoch = index
		clear(st.confirmed)
	}
	st.supports[id] = v
	if hasRun {
		st.runs[id] = run
	}
	return true
}

func (st *State) supportedVersion(voters []uint64) uint32 {
	if len(voters) == 0 {
		return st.version
	}
	low := st.supports[voters[0]]
	for _, id := range voters[1:] {
		low = min(low, st.supports[id])
	}
	return low
}

// announce preserves the apply semantics of entries written before the
// confirmation protocol. Replaying an old log must activate its formats
// at the same entries as the build that wrote it.
func (st *State) announce(id uint64, v uint32, run uint64, hasRun bool, voters []uint64) {
	if st.recordVersion(id, v, run, hasRun, voters, 0) {
		st.version = max(st.version, st.supportedVersion(voters))
	}
}

func (st *State) confirmVersion(index, id uint64, a versionAnnouncement, voters []uint64) {
	if !st.recordVersion(id, a.version, a.run, a.hasRun, voters, index) || a.epoch != st.versionEpoch {
		return
	}
	st.confirmed[id] = a.epoch
	for _, voter := range voters {
		if st.confirmed[voter] != st.versionEpoch {
			return
		}
	}
	st.version = max(st.version, st.supportedVersion(voters))
}

// ownVersion is the newest version this node supports.
func ownVersion(cfg Config) uint32 { return cmp.Or(cfg.MaxVersion, MaxVersion) }

// ActiveVersion is the cluster version this node has applied.
func (n *Node) ActiveVersion() uint32 { return n.state.version }

func (n *Node) applyVersion(index, origin uint64, b []byte) error {
	a, err := parseAnnouncement(b)
	if err != nil {
		return err
	}
	if a.hasEpoch {
		n.state.confirmVersion(index, origin, a, n.state.voters())
	} else {
		n.state.announce(origin, a.version, a.run, a.hasRun, n.state.voters())
	}
	if n.state.version > ownVersion(n.cfg) {
		return tooOld(n.state.version, ownVersion(n.cfg))
	}
	return nil
}

// announceVersion retries after each election timeout, as Raft may drop
// proposals. Once all voters support a newer format, each must confirm
// that epoch. A node that has rolled back sends its lower support instead.
func (n *Node) announceVersion() {
	own := ownVersion(n.cfg)
	st := n.state
	if n.leader == raft.None || st.runs[n.cfg.ID] > n.cfg.Run {
		return
	}
	unchanged := st.supports[n.cfg.ID] == own && st.runs[n.cfg.ID] == n.cfg.Run
	needsConfirmation := st.supportedVersion(st.voters()) > st.version && (st.versionEpoch == 0 || st.confirmed[n.cfg.ID] != st.versionEpoch)
	if unchanged && !needsConfirmation {
		return
	}
	if n.announcedAt != 0 && n.now-n.announcedAt < int64(n.cfg.ElectionTicks) {
		return
	}
	n.announcedAt = n.now
	n.proposeInternal(confirmation(own, n.cfg.Run, st.versionEpoch))
}
