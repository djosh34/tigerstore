package core

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"go.etcd.io/raft/v3/raftpb"
)

// Held entries (#602). A follower forwards the writes its clients send it
// to the leader, which appends them and sends them to every follower,
// the one that forwarded them included. On Kubernetes' mix that was two
// thirds of the bytes the leader sent, and every byte the leader sends on
// loopback costs its core twice: once to write it and once to run the
// receiver's TCP path.
//
// So a follower that forwards a proposal also sends its entries straight
// to the other voters, a side copy, and keeps them itself. The leader's
// appends then name each entry a follower proposed in the leader's term
// by its key instead of carrying its bytes: the header every entry
// starts with, which names the node and run that proposed it and its
// first write's request ID (codec.go), with the entry's size and CRC-32C.
// A request ID is never reused within a run, and the leader names only
// entries whose first write has one, so a key names one entry.
//
// A follower hands Raft an append only once it holds every entry the
// append names, so it never acknowledges, persists or applies an entry
// it does not have. An append that names an entry the follower does not
// hold waits, with every later append, for the side copy; after
// heldWaitTicks it is dropped, and the follower asks the leader to send
// those entries in full. Raft then sends them again, as it does after any
// lost append. A side copy may be lost, late or reordered, and a
// forwarder may crash and forget what it held: each only costs that wait.
//
// The forms travel only on links of peer protocol 4 and later
// (Config.HeldEntries), so a node of an older version never sees them.

const (
	// heldWaitTicks is how long appends wait for entries they name.
	heldWaitTicks = 2
	// heldKeepTicks is how long a node holds an entry for appends to come,
	// and heldMaxBytes bounds the bytes it holds.
	heldKeepTicks = 10
	heldMaxBytes  = 64 << 20
)

// heldTag starts the Context of a message of this file; the byte after
// it is the message's form.
var heldTag = []byte("TSH")

const (
	// heldSideCopy is a MsgProp to a follower with the entries another
	// follower forwards to the leader.
	heldSideCopy byte = 1
	// heldNamed is a MsgApp in which the entries at the positions the
	// Context lists carry their key instead of their bytes.
	heldNamed byte = 2
	// heldAskFull is a MsgProp to the leader that asks it to send the
	// entries up to the index in the Context in full to this follower.
	heldAskFull byte = 3
)

// HeldForm reports whether m is a message of this file, which only a peer
// of protocol 4 or later may get.
func HeldForm(m *raftpb.Message) bool { return bytes.HasPrefix(m.GetContext(), heldTag) }

// heldKey names an entry.
type heldKey struct {
	origin, boot, id uint64
	size, sum        uint32
}

// entryKey returns the key of an entry's data, false for data whose
// first write has no request ID, which a key could not tell apart.
func entryKey(data []byte) (heldKey, bool) {
	_, rest, err := skipFormat(data)
	if err != nil {
		return heldKey{}, false
	}
	count, rest, ok := uvarint(rest)
	if !ok || count == 0 {
		return heldKey{}, false
	}
	var k heldKey
	if k.origin, rest, ok = uvarint(rest); !ok {
		return heldKey{}, false
	}
	if k.boot, rest, ok = uvarint(rest); !ok {
		return heldKey{}, false
	}
	// Every format writes the first write's origin and boot, then its
	// request ID.
	if k.id, _, ok = uvarint(rest); !ok || k.id == 0 {
		return heldKey{}, false
	}
	k.size = uint32(len(data))
	k.sum = crc32.Checksum(data, castagnoli)
	return k, true
}

func (k heldKey) append(b []byte) []byte {
	b = binary.AppendUvarint(b, k.origin)
	b = binary.AppendUvarint(b, k.boot)
	b = binary.AppendUvarint(b, k.id)
	b = binary.BigEndian.AppendUint32(b, k.size)
	return binary.BigEndian.AppendUint32(b, k.sum)
}

func readKey(b []byte) (heldKey, bool) {
	var k heldKey
	var ok bool
	if k.origin, b, ok = uvarint(b); !ok {
		return heldKey{}, false
	}
	if k.boot, b, ok = uvarint(b); !ok {
		return heldKey{}, false
	}
	if k.id, b, ok = uvarint(b); !ok || len(b) != 8 {
		return heldKey{}, false
	}
	k.size, k.sum = binary.BigEndian.Uint32(b), binary.BigEndian.Uint32(b[4:])
	return k, true
}

// keptEntries is a node's state for held entries.
type keptEntries struct {
	// data are the entries a follower holds by key, order their keys
	// with the tick each came, oldest first, and bytes what data holds.
	data  map[heldKey][]byte
	order []keptAt
	bytes int64
	// parked are the appends that wait for entries, in the order they
	// came, since the tick parkedAt.
	parked   []*raftpb.Message
	parkedAt int64
	// full is, on the leader, the last index of the entries it sends each
	// follower in full, in term fullTerm.
	full     map[uint64]uint64
	fullTerm uint64
}

type keptAt struct {
	key heldKey
	at  int64
}

// HeldCounts are a node's counters of held entries (Counters).
type HeldCounts struct{ Named, Parked, Asked int64 }

// Held returns the node's counters of held entries.
func (n *Node) Held() HeldCounts {
	return HeldCounts{n.counters.HeldNamed, n.counters.HeldParked, n.counters.HeldAsked}
}

// heldOn reports whether the link to peer carries the forms of this file.
func (n *Node) heldOn(peer uint64) bool {
	return n.cfg.HeldEntries != nil && n.cfg.HeldEntries(peer)
}

// keep holds an entry's data under its key, and drops the oldest while
// the node holds more than heldMaxBytes. An entry stays held for
// heldKeepTicks, not only until an append names it: Raft may send the
// same append again.
func (n *Node) keep(k heldKey, data []byte) {
	h := &n.kept
	if h.data == nil {
		h.data = map[heldKey][]byte{}
	}
	if _, ok := h.data[k]; ok {
		return
	}
	h.data[k] = data
	h.order = append(h.order, keptAt{k, n.now})
	h.bytes += int64(len(data))
	for h.bytes > heldMaxBytes {
		n.dropOldest()
	}
}

// dropOldest stops holding the entry held longest.
func (n *Node) dropOldest() {
	h := &n.kept
	k := h.order[0].key
	h.bytes -= int64(len(h.data[k]))
	delete(h.data, k)
	h.order[0] = keptAt{}
	h.order = h.order[1:]
	if len(h.order) == 0 {
		h.order = nil
	}
}

// forwarded keeps the entries of a proposal this follower forwards to the
// leader and sends them to the other voters that take side copies.
func (n *Node) forwarded(m *raftpb.Message) {
	if !n.heldOn(m.GetTo()) {
		return
	}
	kept := false
	for _, e := range m.Entries {
		if e.GetType() != raftpb.EntryNormal {
			continue
		}
		if k, ok := entryKey(e.GetData()); ok && k.origin == n.cfg.ID {
			n.keep(k, e.GetData())
			kept = true
		}
	}
	if !kept {
		return
	}
	for _, id := range n.state.voters() {
		if id == n.cfg.ID || id == m.GetTo() || !n.heldOn(id) {
			continue
		}
		n.send(&raftpb.Message{
			Type: raftpb.MsgProp.Enum(), From: new(n.cfg.ID), To: new(id),
			Context: append(append([]byte(nil), heldTag...), heldSideCopy),
			Entries: m.Entries,
		})
	}
}

// nameHeld returns the append m with each entry that a follower proposed
// in this term named by its key, or m itself if it names none. The leader
// sends in full what the follower asked for (heldAskFull).
func (n *Node) nameHeld(m *raftpb.Message) *raftpb.Message {
	if !n.heldOn(m.GetTo()) {
		return m
	}
	if n.kept.fullTerm != n.term {
		n.kept.full, n.kept.fullTerm = nil, n.term
	}
	full := n.kept.full[m.GetTo()]
	var named []byte
	var entries []*raftpb.Entry
	for i, e := range m.Entries {
		if e.GetTerm() != n.term || e.GetType() != raftpb.EntryNormal || e.GetIndex() <= full {
			continue
		}
		k, ok := entryKey(e.GetData())
		if !ok || k.origin == n.cfg.ID {
			continue
		}
		if entries == nil {
			entries = append([]*raftpb.Entry(nil), m.Entries...)
			named = append(append([]byte(nil), heldTag...), heldNamed)
		}
		entries[i] = &raftpb.Entry{Term: e.Term, Index: e.Index, Type: e.Type, Data: k.append(nil)}
		named = binary.AppendUvarint(named, uint64(i))
		n.counters.HeldNamed++
	}
	if entries == nil {
		return m
	}
	return withEntries(m, entries, named)
}

// withEntries returns a copy of m with other entries and Context. The
// copy shares m's other fields, which nothing changes once sent.
func withEntries(m *raftpb.Message, entries []*raftpb.Entry, ctx []byte) *raftpb.Message {
	return &raftpb.Message{
		Type: m.Type, To: m.To, From: m.From, Term: m.Term, LogTerm: m.LogTerm, Index: m.Index,
		Entries: entries, Commit: m.Commit, Vote: m.Vote, Snapshot: m.Snapshot,
		Reject: m.Reject, RejectHint: m.RejectHint, Context: ctx, Responses: m.Responses,
	}
}

// stepHeld handles a message of this file, or an append while earlier
// ones wait for entries. It reports whether it took m.
func (n *Node) stepHeld(m *raftpb.Message) (bool, error) {
	if !HeldForm(m) {
		if m.GetType() == raftpb.MsgApp && len(n.kept.parked) > 0 {
			n.park(m)
			return true, nil
		}
		return false, nil
	}
	ctx := m.GetContext()[len(heldTag):]
	if len(ctx) == 0 {
		return true, nil
	}
	switch ctx[0] {
	case heldSideCopy:
		for _, e := range m.Entries {
			if k, ok := entryKey(e.GetData()); ok {
				n.keep(k, e.GetData())
			}
		}
		return true, n.unpark()
	case heldAskFull:
		if last, _, ok := uvarint(ctx[1:]); ok && n.isLeader {
			if n.kept.fullTerm != n.term {
				n.kept.full, n.kept.fullTerm = nil, n.term
			}
			if n.kept.full == nil {
				n.kept.full = map[uint64]uint64{}
			}
			n.kept.full[m.GetFrom()] = max(n.kept.full[m.GetFrom()], last)
		}
		return true, nil
	case heldNamed:
		if m.GetType() != raftpb.MsgApp {
			return true, nil
		}
		if len(n.kept.parked) == 0 {
			if full, ok := n.fill(m); ok {
				return true, n.stepRaft(full)
			}
		}
		n.counters.HeldParked++
		n.park(m)
		return true, nil
	}
	return true, nil
}

func (n *Node) park(m *raftpb.Message) {
	if len(n.kept.parked) == 0 {
		n.kept.parkedAt = n.now
	}
	n.kept.parked = append(n.kept.parked, m)
}

// unpark steps the parked appends, oldest first, until one names an entry
// this node does not hold.
func (n *Node) unpark() error {
	h := &n.kept
	for len(h.parked) > 0 {
		m := h.parked[0]
		if HeldForm(m) {
			var ok bool
			if m, ok = n.fill(m); !ok {
				return nil
			}
		}
		h.parked[0] = nil
		h.parked = h.parked[1:]
		h.parkedAt = n.now
		if err := n.stepRaft(m); err != nil {
			return err
		}
	}
	h.parked = nil
	return nil
}

// fill returns the append m with the entries it names filled in, and
// false if this node does not hold one of them.
func (n *Node) fill(m *raftpb.Message) (*raftpb.Message, bool) {
	ctx := m.GetContext()[len(heldTag)+1:]
	entries := append([]*raftpb.Entry(nil), m.Entries...)
	for len(ctx) > 0 {
		i, rest, ok := uvarint(ctx)
		if !ok || i >= uint64(len(entries)) {
			return nil, false
		}
		ctx = rest
		k, ok := readKey(entries[i].GetData())
		if !ok {
			return nil, false
		}
		data, ok := n.kept.data[k]
		if !ok {
			return nil, false
		}
		e := entries[i]
		entries[i] = &raftpb.Entry{Term: e.Term, Index: e.Index, Type: e.Type, Data: data}
	}
	return withEntries(m, entries, nil), true
}

// heldTick stops holding entries held for heldKeepTicks, and asks the
// leader to send in full what parked appends have waited for for
// heldWaitTicks, and drops them: Raft sends them again.
func (n *Node) heldTick() {
	h := &n.kept
	for len(h.order) > 0 && n.now-h.order[0].at > heldKeepTicks {
		n.dropOldest()
	}
	if len(h.parked) == 0 || n.now-h.parkedAt < heldWaitTicks {
		return
	}
	var last uint64
	for _, m := range h.parked {
		for _, e := range m.Entries {
			last = max(last, e.GetIndex())
		}
	}
	to := h.parked[0].GetFrom()
	clear(h.parked)
	h.parked = nil
	n.counters.HeldAsked++
	n.send(&raftpb.Message{
		Type: raftpb.MsgProp.Enum(), From: new(n.cfg.ID), To: new(to),
		Context: binary.AppendUvarint(append(append([]byte(nil), heldTag...), heldAskFull), last),
	})
}
