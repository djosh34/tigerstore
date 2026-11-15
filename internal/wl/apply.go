package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// The apply: each committed batch changes the store in order, and the
// node that proposed it answers its clients. An error here is one the
// node cannot go on from, such as a full store: the loop stops.

// applyBatch applies one entry's batch; walOff is where its data lies in
// the WAL.
func (n *node) applyBatch(data []byte, walOff int64) error {
	if len(data) < batchHdr || binary.LittleEndian.Uint32(data[12:]) != magic {
		return nil
	}
	origin := uint64(binary.LittleEndian.Uint32(data))
	nops := int(binary.LittleEndian.Uint32(data[8:]))
	p := batchHdr
	mine := origin == n.id
	// The node that proposed the batch knows its clients' handles; they
	// are not in the batch.
	var handles []handle
	if mine {
		seq := binary.LittleEndian.Uint32(data[4:])
		if f := &n.fwd[seq&(fwdSlots-1)]; f.seq == seq && len(f.handles) == nops {
			handles = f.handles
		} else {
			mine = false
		}
	}
	for i := range nops {
		kind := data[p]
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		req := data[p+opHdr : p+opHdr+l : p+opHdr+l]
		voff := walOff + int64(p+opHdr)
		p += opHdr + l
		dbgOps[kind]++
		dbgBytes[kind] += l
		var c *client
		var s *stream
		if mine {
			c, s = n.clientOf(handles[i])
		}
		var err error
		switch kind {
		case kPut:
			err = n.applyPut(req, voff, c, s)
		case kDel:
			err = n.applyDel(req, c, s)
		case kTxn:
			err = n.applyTxn(req, voff, c, s)
		case kCompact:
			n.applyCompact(req, c, s)
		case kLeaseGrant:
			n.applyLeaseGrant(req, c, s)
		case kLeaseRevoke:
			n.applyLeaseRevoke(req, c, s)
		}
		if bad, ok := errors.AsType[*badRequestError](err); ok {
			// Every node reads the same bytes and rejects them alike.
			if c != nil {
				n.replyError(c, s.id, codeInvalidArgument, bad.Error())
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// badRequestError is a request the apply cannot read: its client gets
// an error, and the node goes on.
type badRequestError struct{ err error }

func (e *badRequestError) Error() string { return "etcdserver: bad request: " + e.err.Error() }

func (e *badRequestError) Unwrap() error { return e.err }

func badRequest(err error) error { return &badRequestError{err: err} }

// replyHeader answers a request whose response is only the header.
func (n *node) replyHeader(c *client, s *stream) {
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize()))
	n.writeHdr(&w)
	n.endReply(c, s, &w)
}

func (n *node) applyCompact(req []byte, c *client, s *stream) {
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			break
		}
		if num == 1 {
			if int64(v) > n.st.compactRev && int64(v) <= n.st.rev {
				n.st.compactRev = int64(v)
			} else if c != nil && int64(v) <= n.st.compactRev {
				n.replyError(c, s.id, codeOutOfRange, msgCompacted)
				c = nil
			}
		}
		m = rest
	}
	if c != nil {
		n.replyHeader(c, s)
	}
}

func (n *node) applyLeaseGrant(req []byte, c *client, s *stream) {
	ttl, id := int64(0), int64(0)
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			break
		}
		switch num {
		case 1:
			ttl = int64(v)
		case 2:
			id = int64(v)
		}
		m = rest
	}
	if id == 0 {
		id = int64(n.applied+1)<<20 | int64(len(n.st.leases)&0xfffff)
		for n.st.leases[id] != 0 {
			id++
		}
	}
	n.st.leases[id] = ttl
	if c != nil {
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(id)) + sizeVarintField(3, uint64(ttl))
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(id))
		writeVarintNZ(&w, 3, uint64(ttl))
		n.endReply(c, s, &w)
	}
}

func (n *node) applyLeaseRevoke(req []byte, c *client, s *stream) {
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			break
		}
		if num == 1 {
			delete(n.st.leases, int64(v))
		}
		m = rest
	}
	if c != nil {
		n.replyHeader(c, s)
	}
}

func voffOf(req, sub []byte, base int64) int64 {
	return base + int64(cap(req)-cap(sub))
}

// putKey writes r under its key at revision rev; req is the request r
// was read from, which lies at voff in the WAL. It returns the record the
// key had, if it was live.
func (n *node) putKey(r *putReq, req []byte, voff, rev int64) ([]byte, error) {
	st := n.st
	id, err := st.intern(r.key)
	if err != nil {
		return nil, err
	}
	value, vo := r.value, voffOf(req, r.value, voff)
	if r.ignoreValue || r.ignoreLease {
		if cur := st.live(id); cur != nil {
			if r.ignoreValue {
				value = bytes.Clone(recValue(cur))
				vo = st.keys[id].voff
			}
			if r.ignoreLease {
				r.lease = st.keys[id].lease
			}
		}
	}
	prev, err := st.put(id, value, r.lease, rev, vo)
	if err != nil {
		return nil, err
	}
	n.w.record(st, id, false, rev, st.live(id), prev)
	return prev, nil
}

// deleteRange deletes the live keys of r at revision rev and appends
// their records to kvs if r asks for them.
func (n *node) deleteRange(r *delReq, rev int64, kvs [][]byte) ([][]byte, int64, error) {
	st := n.st
	var deleted int64
	del := func(id uint32) error {
		prev, err := st.del(id, rev)
		if err != nil || prev == nil {
			return err
		}
		deleted++
		n.w.record(st, id, true, rev, nil, prev)
		if r.prevKV {
			kvs = append(kvs, prev)
		}
		return nil
	}
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			if err := del(id); err != nil {
				return kvs, deleted, err
			}
		}
		return kvs, deleted, nil
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	var ids []uint32
	st.tree.scan(r.key, end, func(id uint32) bool { ids = append(ids, id); return true })
	for _, id := range ids {
		if err := del(id); err != nil {
			return kvs, deleted, err
		}
	}
	return kvs, deleted, nil
}

func (n *node) applyPut(req []byte, voff int64, c *client, s *stream) error {
	var r putReq
	if err := parsePut(req, &r); err != nil {
		return badRequest(err)
	}
	rev := n.st.rev + 1
	prev, err := n.putKey(&r, req, voff, rev)
	if err != nil {
		return err
	}
	n.st.rev = rev
	if c != nil {
		var w msgW
		size := sizeBytesField(1, n.hdrSize())
		if r.prevKV && prev != nil {
			size += sizeBytesField(2, len(prev))
		}
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		if r.prevKV && prev != nil {
			w.bytesField(2, prev)
		}
		n.endReply(c, s, &w)
	}
	return nil
}

func (n *node) applyDel(req []byte, c *client, s *stream) error {
	var r delReq
	if err := parseDel(req, &r); err != nil {
		return badRequest(err)
	}
	rev := n.st.rev + 1
	kvs, deleted, err := n.deleteRange(&r, rev, n.kvs[:0])
	if err != nil {
		return err
	}
	if deleted > 0 {
		n.st.rev = rev
	}
	if c != nil {
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(deleted))
		for _, kv := range kvs {
			size += sizeBytesField(3, len(kv))
		}
		var w msgW
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(deleted))
		for _, kv := range kvs {
			w.bytesField(3, kv)
		}
		n.endReply(c, s, &w)
	}
	clear(kvs)
	n.kvs = kvs[:0]
	return nil
}

// cmpHolds evaluates one compare against key id's state.
func (n *node) cmpHolds(c *compare) bool {
	st := n.st
	check := func(id uint32, live bool) bool {
		var have int64
		if !live {
			if c.target == 3 {
				return false
			}
		}
		mod, create, ver, lease := st.state(id)
		switch c.target {
		case 0:
			have = ver
		case 1:
			have = create
		case 2:
			have = mod
		case 4:
			have = lease
		case 3:
			r := bytes.Compare(recValue(st.live(id)), c.value)
			return result(c.result, r)
		}
		r := 0
		if have < c.num {
			r = -1
		} else if have > c.num {
			r = 1
		}
		return result(c.result, r)
	}
	if len(c.end) == 0 {
		id, ok := st.find(c.key)
		if !ok {
			if c.target == 3 {
				return false
			}
			r := 0
			if c.num > 0 {
				r = -1
			} else if c.num < 0 {
				r = 1
			}
			return result(c.result, r)
		}
		return check(id, st.live(id) != nil)
	}
	end := c.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	ok := true
	st.tree.scan(c.key, end, func(id uint32) bool {
		ok = check(id, true)
		return ok
	})
	return ok
}

func result(want int, r int) bool {
	switch want {
	case 0:
		return r == 0
	case 1:
		return r > 0
	case 2:
		return r < 0
	case 3:
		return r != 0
	}
	return false
}

func (n *node) applyTxn(req []byte, voff int64, c *client, s *stream) error {
	st := n.st
	ok := true
	var cmp compare
	for m := req; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		if num == 1 && ok {
			if err := parseCompare(p, &cmp); err != nil {
				return badRequest(err)
			}
			ok = n.cmpHolds(&cmp)
		}
		m = rest
	}
	branch := 2
	if !ok {
		branch = 3
	}
	rev := st.rev + 1
	changed := false
	res := n.txnRes[:0]
	kvs := n.kvs[:0]
	ops := req
	if !ok && s != nil && s.fail != nil {
		ops = s.fail
	}
	for m := ops; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		if num != branch {
			continue
		}
		// p is a RequestOp: one field.
		onum, _, _, op, _, err := field(p)
		if err != nil {
			return badRequest(err)
		}
		switch onum {
		case 1:
			var r rangeReq
			if err := parseRange(op, &r); err != nil {
				return badRequest(err)
			}
			k0 := len(kvs)
			var count int64
			if kvs, count, err = n.rangeKVs(&r, kvs); err != nil {
				return err
			}
			res = append(res, opRes{kind: kRange, kvs0: k0, kvs1: len(kvs), count: count, more: int64(len(kvs)-k0) < count && !r.countOnly})
		case 2:
			var r putReq
			if err := parsePut(op, &r); err != nil {
				return badRequest(err)
			}
			prev, err := n.putKey(&r, req, voff, rev)
			if err != nil {
				return err
			}
			changed = true
			x := opRes{kind: kPut}
			if r.prevKV {
				x.prev = prev
			}
			res = append(res, x)
		case 3:
			var r delReq
			if err := parseDel(op, &r); err != nil {
				return badRequest(err)
			}
			k0 := len(kvs)
			var deleted int64
			if kvs, deleted, err = n.deleteRange(&r, rev, kvs); err != nil {
				return err
			}
			if deleted > 0 {
				changed = true
			}
			res = append(res, opRes{kind: kDel, kvs0: k0, kvs1: len(kvs), deleted: deleted})
		default:
			res = append(res, opRes{kind: kTxn})
		}
	}
	if changed {
		st.rev = rev
	}
	if c != nil {
		n.replyTxn(c, s, ok, res, kvs)
	}
	clear(kvs)
	n.kvs = kvs[:0]
	n.txnRes = res[:0]
	return nil
}

func (n *node) opSize(x *opRes, kvs [][]byte) int {
	switch x.kind {
	case kRange:
		b := sizeVarintField(4, uint64(x.count))
		if x.more {
			b += 2
		}
		for _, kv := range kvs[x.kvs0:x.kvs1] {
			b += sizeBytesField(2, len(kv))
		}
		return b
	case kPut:
		if x.prev != nil {
			return sizeBytesField(2, len(x.prev))
		}
		return 0
	case kDel:
		b := sizeVarintField(2, uint64(x.deleted))
		for _, kv := range kvs[x.kvs0:x.kvs1] {
			b += sizeBytesField(3, len(kv))
		}
		return b
	}
	return 0
}

var opField = map[uint8]int{kRange: 1, kPut: 2, kDel: 3, kTxn: 4}

func (n *node) replyTxn(c *client, s *stream, ok bool, res []opRes, kvs [][]byte) {
	size := sizeBytesField(1, n.hdrSize())
	if ok {
		size += 2
	}
	for i := range res {
		inner := n.opSize(&res[i], kvs)
		size += sizeBytesField(3, sizeBytesField(opField[res[i].kind], inner))
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	if ok {
		w.varintField(2, 1)
	}
	for i := range res {
		x := &res[i]
		inner := n.opSize(x, kvs)
		w.lenField(3, sizeBytesField(opField[x.kind], inner))
		w.lenField(opField[x.kind], inner)
		switch x.kind {
		case kRange:
			for _, kv := range kvs[x.kvs0:x.kvs1] {
				w.bytesField(2, kv)
			}
			if x.more {
				w.varintField(3, 1)
			}
			writeVarintNZ(&w, 4, uint64(x.count))
		case kPut:
			if x.prev != nil {
				w.bytesField(2, x.prev)
			}
		case kDel:
			writeVarintNZ(&w, 2, uint64(x.deleted))
			for _, kv := range kvs[x.kvs0:x.kvs1] {
				w.bytesField(3, kv)
			}
		}
	}
	n.endReply(c, s, &w)
}
