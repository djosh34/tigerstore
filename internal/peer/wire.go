package peer

import (
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// A peer frame holds a raftpb.Message in protobuf's wire form, so peers of
// every version read each other. The hot messages, appends, their answers
// and heartbeats, are written and read here without protobuf's reflection
// (part 3 of docs/wire-loop.md). A received message takes at most four
// allocations, whatever its entries: the message with its fields, the
// entries, their list and one copy of their data, which the entries share.
// protobuf's Unmarshal took one allocation per field and three per entry,
// one of them a copy of its data. A message with a snapshot, responses or
// fields this code does not know goes through protobuf, both ways.

// The fields of raftpb.Message and raftpb.Entry.
const (
	fType       = 1
	fTo         = 2
	fFrom       = 3
	fTerm       = 4
	fLogTerm    = 5
	fIndex      = 6
	fEntries    = 7
	fCommit     = 8
	fReject     = 10
	fRejectHint = 11
	fContext    = 12
	fVote       = 13

	fEntryType  = 1
	fEntryTerm  = 2
	fEntryIndex = 3
	fEntryData  = 4
)

// message is a raftpb.Message with room for the fields it points to, so
// that it takes one allocation.
type message struct {
	m                                                   raftpb.Message
	typ                                                 raftpb.MessageType
	to, from, term, logTerm, index, commit, hint, vote uint64
	reject                                              bool
}

// entry is a raftpb.Entry with room for the fields it points to.
type entry struct {
	e           raftpb.Entry
	typ         raftpb.EntryType
	term, index uint64
}

// decodeMessage reads a message from b, which it does not keep.
func decodeMessage(b []byte) (*raftpb.Message, error) {
	if m, ok := decodeFast(b); ok {
		return m, nil
	}
	m := &raftpb.Message{}
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, err
	}
	return m, nil
}

// decodeFast reads b as decodeMessage does, and reports false for a
// message it leaves to protobuf: one with a field it does not read, an
// enum value raftpb does not name, or bytes that do not parse.
func decodeFast(b []byte) (*raftpb.Message, bool) {
	ents, data, ok := measure(b)
	if !ok {
		return nil, false
	}
	x := &message{}
	m := &x.m
	var buf []byte
	if data > 0 {
		buf = make([]byte, 0, data)
	}
	var boxes []entry
	if ents > 0 {
		boxes = make([]entry, ents)
		m.Entries = make([]*raftpb.Entry, 0, ents)
	}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		b = b[n:]
		if typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			b = b[n:]
			switch num {
			case fEntries:
				e := &boxes[len(m.Entries)]
				if buf, ok = decodeEntry(v, e, buf); !ok {
					return nil, false
				}
				m.Entries = append(m.Entries, &e.e)
			case fContext:
				buf, m.Context = keep(buf, v)
			}
			continue
		}
		v, n := protowire.ConsumeVarint(b)
		b = b[n:]
		switch num {
		case fType:
			x.typ = raftpb.MessageType(int32(v))
			if _, ok := raftpb.MessageType_name[int32(x.typ)]; !ok {
				return nil, false
			}
			m.Type = &x.typ
		case fTo:
			x.to, m.To = v, &x.to
		case fFrom:
			x.from, m.From = v, &x.from
		case fTerm:
			x.term, m.Term = v, &x.term
		case fLogTerm:
			x.logTerm, m.LogTerm = v, &x.logTerm
		case fIndex:
			x.index, m.Index = v, &x.index
		case fCommit:
			x.commit, m.Commit = v, &x.commit
		case fReject:
			x.reject, m.Reject = v != 0, &x.reject
		case fRejectHint:
			x.hint, m.RejectHint = v, &x.hint
		case fVote:
			x.vote, m.Vote = v, &x.vote
		}
	}
	return m, true
}

// measure checks that b holds only fields decodeFast reads, of the wire
// types it expects, and returns how many entries b has and at most how
// many bytes their data and the context take.
func measure(b []byte) (ents, data int, ok bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, 0, false
		}
		b = b[n:]
		switch {
		case num == fEntries || num == fContext:
			if typ != protowire.BytesType {
				return 0, 0, false
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, 0, false
			}
			b = b[n:]
			data += len(v)
			if num == fEntries {
				ents++
			}
		case num >= fType && num <= fCommit || num >= fReject && num <= fVote && num != fContext:
			if typ != protowire.VarintType {
				return 0, 0, false
			}
			_, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, 0, false
			}
			b = b[n:]
		default:
			return 0, 0, false
		}
	}
	return ents, data, true
}

// decodeEntry reads entry e from b, its data into buf, which it returns.
func decodeEntry(b []byte, e *entry, buf []byte) ([]byte, bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return buf, false
		}
		b = b[n:]
		if num == fEntryData {
			if typ != protowire.BytesType {
				return buf, false
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return buf, false
			}
			b = b[n:]
			buf, e.e.Data = keep(buf, v)
			continue
		}
		if typ != protowire.VarintType || num < fEntryType || num > fEntryIndex {
			return buf, false
		}
		v, n := protowire.ConsumeVarint(b)
		if n < 0 {
			return buf, false
		}
		b = b[n:]
		switch num {
		case fEntryType:
			e.typ = raftpb.EntryType(int32(v))
			if _, ok := raftpb.EntryType_name[int32(e.typ)]; !ok {
				return buf, false
			}
			e.e.Type = &e.typ
		case fEntryTerm:
			e.term, e.e.Term = v, &e.term
		case fEntryIndex:
			e.index, e.e.Index = v, &e.index
		}
	}
	return buf, true
}

// keep copies v to the end of buf and returns buf and the copy, which is
// not nil even when empty, as protobuf marks a bytes field that is there.
func keep(buf, v []byte) ([]byte, []byte) {
	if len(v) == 0 {
		return buf, []byte{}
	}
	start := len(buf)
	buf = append(buf, v...)
	return buf, buf[start:len(buf):len(buf)]
}

// appendMessage appends m in protobuf's wire form, its fields in the order
// of their numbers, as protobuf's Marshal writes them.
func appendMessage(b []byte, m *raftpb.Message) ([]byte, error) {
	if !plain(m) {
		return proto.MarshalOptions{}.MarshalAppend(b, m)
	}
	if m.Type != nil {
		b = appendVarint(b, fType, uint64(int64(*m.Type)))
	}
	for _, f := range [...]struct {
		num protowire.Number
		v   *uint64
	}{{fTo, m.To}, {fFrom, m.From}, {fTerm, m.Term}, {fLogTerm, m.LogTerm}, {fIndex, m.Index}} {
		if f.v != nil {
			b = appendVarint(b, f.num, *f.v)
		}
	}
	for _, e := range m.Entries {
		b = protowire.AppendTag(b, fEntries, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(entrySize(e)))
		b = appendEntry(b, e)
	}
	if m.Commit != nil {
		b = appendVarint(b, fCommit, *m.Commit)
	}
	if m.Reject != nil {
		v := uint64(0)
		if *m.Reject {
			v = 1
		}
		b = appendVarint(b, fReject, v)
	}
	if m.RejectHint != nil {
		b = appendVarint(b, fRejectHint, *m.RejectHint)
	}
	if m.Context != nil {
		b = protowire.AppendTag(b, fContext, protowire.BytesType)
		b = protowire.AppendBytes(b, m.Context)
	}
	if m.Vote != nil {
		b = appendVarint(b, fVote, *m.Vote)
	}
	return b, nil
}

// plain reports whether appendMessage writes m itself: m has no snapshot,
// no responses and no fields that protobuf kept unread.
func plain(m *raftpb.Message) bool {
	if m.Snapshot != nil || len(m.Responses) > 0 || len(m.ProtoReflect().GetUnknown()) > 0 {
		return false
	}
	for _, e := range m.Entries {
		if len(e.ProtoReflect().GetUnknown()) > 0 {
			return false
		}
	}
	return true
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func appendEntry(b []byte, e *raftpb.Entry) []byte {
	if e.Type != nil {
		b = appendVarint(b, fEntryType, uint64(int64(*e.Type)))
	}
	if e.Term != nil {
		b = appendVarint(b, fEntryTerm, *e.Term)
	}
	if e.Index != nil {
		b = appendVarint(b, fEntryIndex, *e.Index)
	}
	if e.Data != nil {
		b = protowire.AppendTag(b, fEntryData, protowire.BytesType)
		b = protowire.AppendBytes(b, e.Data)
	}
	return b
}

func entrySize(e *raftpb.Entry) int {
	n := 0
	if e.Type != nil {
		n += 1 + protowire.SizeVarint(uint64(int64(*e.Type)))
	}
	if e.Term != nil {
		n += 1 + protowire.SizeVarint(*e.Term)
	}
	if e.Index != nil {
		n += 1 + protowire.SizeVarint(*e.Index)
	}
	if e.Data != nil {
		n += 1 + protowire.SizeBytes(len(e.Data))
	}
	return n
}
