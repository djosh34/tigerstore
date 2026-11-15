package mvcc

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// The store writes what it answers in etcd's wire form itself: a KeyValue
// or a watch Event goes straight from the store's bytes into a reply, with
// no message built and no copy of a value in between (part 4 of
// docs/wire-loop.md). Fields come in the order of their numbers and fields
// at their zero value are left out, as protobuf's Marshal writes them, so
// a size from Size or EventSize is the size written.

// Append appends kv as a KeyValue message, without a tag or length.
func (kv *KV) Append(b []byte) []byte {
	if len(kv.Key) > 0 {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, kv.Key)
	}
	for i, v := range [...]int64{kv.CreateRevision, kv.ModRevision, kv.Version} {
		if v != 0 {
			b = protowire.AppendTag(b, protowire.Number(i+2), protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(v))
		}
	}
	if len(kv.Value) > 0 {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, kv.Value)
	}
	if kv.Lease != 0 {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(kv.Lease))
	}
	return b
}

// appendField appends kv as field num of a message: its tag, its length
// and the KeyValue.
func (kv *KV) appendField(b []byte, num protowire.Number) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(kv.Size(len(kv.Value))))
	return kv.Append(b)
}

// AppendEvent appends the watch event of the change as an mvccpb.Event
// message, without a tag or length, as Event builds it. It reports false,
// and appends nothing, if a value the event needs is only on disk: the
// caller then builds it with Event, whose values come as fills.
func (c Change) AppendEvent(b []byte, withPrev bool) ([]byte, bool, error) {
	s := c.s
	start := len(b)
	v := &c.k.vers()[c.i]
	kv := KV{Key: s.key(c.k), ModRevision: v.mod.main}
	if v.ver == 0 {
		// The type field: DELETE is 1, PUT the default 0.
		b = protowire.AppendTag(b, 1, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	} else {
		kv = s.kvValue(c.k, v)
		val, ok, err := s.peek(c.k.id, kv.Key, v.value)
		if err != nil || !ok {
			return b[:start], false, err
		}
		kv.Value = val
	}
	// Each value goes into b before the next peek, which may reuse the
	// room of the last.
	b = kv.appendField(b, 2)
	if p := c.prev(withPrev); p != nil {
		prev := s.kvValue(c.k, p)
		val, ok, err := s.peek(c.k.id, prev.Key, p.value)
		if err != nil || !ok {
			return b[:start], false, err
		}
		prev.Value = val
		b = prev.appendField(b, 3)
	}
	return b, true, nil
}

// peek returns the bytes of a value of key, in slot id, if they are in
// memory, as value does, but without copying them: they are valid only
// until the store's next call.
func (s *Store) peek(id uint32, key []byte, r Ref) ([]byte, bool, error) {
	if r.Len == 0 {
		return nil, true, nil
	}
	if b, ok := s.heldValue(id, r); ok {
		s.vals.hits++
		return b, true, nil
	}
	s.scratch = s.scratch[:0]
	b, ok, err := s.values.Read(r, &s.scratch)
	if cap(s.scratch) > maxScratch {
		s.scratch = nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("mvcc: read the value of %q: %w", key, err)
	}
	if !ok {
		return nil, false, nil
	}
	if err := check(key, r, b); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// maxScratch is the most room the store keeps for the values peek copies
// out of Raft entries.
const maxScratch = 1 << 20
