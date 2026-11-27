package wl

import "errors"

// A resident probe must not turn a cold record into a new slab allocation:
// that could evict a record it already selected. Nothing is emitted until
// selection succeeds for the whole reply in this non-yielding logical turn.
var errResidentCold = errors.New("the reply needs a value fetched")

func (st *store) residentAt(id uint32, rev int64) ([]byte, error) {
	k := &st.keys[id]
	if k.create != 0 && k.rec != 0 && (rev == 0 || k.mod <= rev) {
		return st.slab.bytes(k.rec, int(k.rlen)), nil
	}
	ks, ok := st.stateAt(id, rev)
	if !ok {
		return nil, nil
	}
	return st.residentState(id, &ks)
}

func (st *store) residentPutAt(id uint32, rev int64) ([]byte, error) {
	k := &st.keys[id]
	if k.rec != 0 && k.mod == rev && k.create != 0 {
		return st.slab.bytes(k.rec, int(k.rlen)), nil
	}
	ks, ok := st.putStateAt(id, rev)
	if !ok {
		return nil, nil
	}
	return st.residentState(id, &ks)
}

func (st *store) residentState(id uint32, ks *ckptState) ([]byte, error) {
	k := &st.keys[id]
	if k.rec != 0 && k.mod == ks.mod {
		return st.slab.bytes(k.rec, int(k.rlen)), nil
	}
	if ks.vlen == 0 {
		return appendKV(nil, st.key(id), nil, ks.create, ks.mod, ks.ver, ks.lease), nil
	}
	return nil, errResidentCold
}

// tryResidentRange uses main's selection/representation and byte writer.
// Output loans/protected output still require replyRoom's complete P funding;
// direct means no cursor/source plan, not unaccounted output buffering.
// at is explicit so the cold-during-selection boundary is tested without a
// persistent production hook or a second snapshot.
func (n *node) tryResidentRange(c *client, s *stream, r *rangeReq, msg []byte, at func(uint32, int64) ([]byte, error)) bool {
	st := n.st
	if n.legacyRetry != nil || st.legacyView != nil || (s.kind != kRange && s.kind != kRangeStream) || r.rev > st.rev || r.rev > 0 && r.rev < st.compactRev {
		return false
	}
	kvs, count, more, err := n.selectRangeKVs(r, n.kvs[:0], n.oneShot(), at)
	if err != nil {
		clear(kvs)
		n.kvs = kvs[:0]
		return false
	}
	if r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = keysOnly(kv, r)
		}
	}
	header, term := st.rev, n.term
	body := sizeBytesField(1, n.hdrSizeTerm(header, term)) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	if s.kind == kRangeStream {
		body += 32 * (len(kvs)/initialStreamChunk + 1)
	}
	wire := body + 5
	if s.kind == kRangeStream {
		wire = n.residentStreamWire(r, kvs, count, more, header, term)
	}
	if n.cannotFit(int64(body + 5)) {
		clear(kvs)
		n.kvs = kvs[:0]
		return false
	}
	defer n.dropReplyStage(c)
	if !n.replyRoom(c, s, int64(wire), int64(body+5)) {
		clear(kvs)
		n.kvs = kvs[:0]
		n.waitRoom(c, s, msg, body-sizeBytesField(1, n.hdrSizeTerm(header, term)))
		return true
	}
	n.gs.rangesN++
	if s.kind == kRangeStream {
		n.streamRange(c, s, r, kvs, count, more)
		clear(kvs)
		n.kvs = kvs[:0]
	} else {
		n.writeRangeReply(c, s, kvs, count, more, header, term, body)
	}
	return true
}
