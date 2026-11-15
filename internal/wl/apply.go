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
			// Answered here, the batch's clients are not timed out.
			defer func() { f.handles = f.handles[:0] }()
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
		n.stats.opsByKind[kind]++
		n.stats.bytesByKind[kind] += l
		var c *client
		var s *stream
		if mine {
			c, s = n.clientOf(handles[i])
		}
		var err error
		if n.noSpace || n.corrupt {
			err = n.alarmGate(kind, req)
		}
		switch {
		case err != nil:
		case kind == kPut:
			err = n.applyPut(req, voff, c, s)
		case kind == kDel:
			err = n.applyDel(req, c, s)
		case kind == kTxn:
			err = n.applyTxn(req, voff, c, s)
		case kind == kCompact:
			err = n.applyCompact(req, c, s)
		case kind == kLeaseGrant:
			err = n.applyLeaseGrant(req, c, s)
		case kind == kLeaseRevoke:
			err = n.applyLeaseRevoke(req, c, s)
		case kind == kLeaseKeepAlive:
			err = n.applyLeaseKeepAlive(req, c, s)
		case kind == kMemberAttrs:
			err = n.applyMemberAttrs(req)
		case kind == kAlarm:
			err = n.applyAlarm(req, c, s)
		}
		if re, ok := errors.AsType[*requestError](err); ok {
			// Every node applies the same bytes and rejects them alike.
			if c != nil {
				n.replyError(c, s.id, re.code, re.msg)
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// requestError is a request the apply rejects before it changes anything:
// its client gets the error, and the node goes on.
type requestError struct {
	code int
	msg  string
}

func (e *requestError) Error() string { return e.msg }

func requestErr(code int, msg string) error { return &requestError{code: code, msg: msg} }

// badRequest is a request the apply cannot read.
func badRequest(err error) error {
	return requestErr(codeInvalidArgument, "etcdserver: bad request: "+err.Error())
}

// replyHeader answers a request whose response is only the header.
func (n *node) replyHeader(c *client, s *stream) {
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize()))
	n.writeHdr(&w)
	n.endReply(c, s, &w)
}

func (n *node) applyCompact(req []byte, c *client, s *stream) error {
	rev := int64(0)
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		if num == 1 {
			rev = int64(v)
		}
		m = rest
	}
	switch {
	case rev <= n.st.compactRev:
		return requestErr(codeOutOfRange, msgCompacted)
	case rev > n.st.rev:
		return requestErr(codeOutOfRange, msgFuture)
	}
	n.st.compact(rev)
	if c != nil {
		n.replyHeader(c, s)
	}
	return nil
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
		cur, err := st.live(id)
		if err != nil {
			return nil, err
		}
		if cur != nil {
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
	rec, err := st.live(id)
	if err != nil {
		return nil, err
	}
	n.w.record(st, id, false, rev, rec, prev)
	return prev, nil
}

// checkPutKey fails a put to a lease that does not exist, or one that
// keeps the value or lease of a key that does not exist: etcd's checkPut.
func (n *node) checkPutKey(r *putReq) error {
	if !r.ignoreLease {
		if err := n.checkLease(r.lease); err != nil {
			return err
		}
	}
	if r.ignoreValue || r.ignoreLease {
		id, ok := n.st.find(r.key)
		if !ok {
			return invalid(msgKeyNotFound)
		}
		cur, err := n.st.live(id)
		if err != nil {
			return err
		}
		if cur == nil {
			return invalid(msgKeyNotFound)
		}
	}
	return nil
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
	if err := n.checkPutKey(&r); err != nil {
		return err
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

// cmpHolds evaluates one Txn compare against the store as it is.
func (n *node) cmpHolds(c *compare) (bool, error) {
	st := n.st
	check := func(id uint32) (bool, error) {
		mod, create, ver, lease := st.state(id)
		var have int64
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
			if !st.exists(id) {
				return false, nil
			}
			rec, err := st.live(id)
			if err != nil {
				return false, err
			}
			return result(c.result, bytes.Compare(recValue(rec), c.value)), nil
		}
		r := 0
		if have < c.num {
			r = -1
		} else if have > c.num {
			r = 1
		}
		return result(c.result, r), nil
	}
	if len(c.end) == 0 {
		id, ok := st.find(c.key)
		if !ok {
			if c.target == 3 {
				return false, nil
			}
			r := 0
			if c.num > 0 {
				r = -1
			} else if c.num < 0 {
				r = 1
			}
			return result(c.result, r), nil
		}
		return check(id)
	}
	end := c.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	ok := true
	var err error
	st.tree.scan(c.key, end, func(id uint32) bool {
		if !st.exists(id) {
			return true
		}
		ok, err = check(id)
		return ok && err == nil
	})
	return ok, err
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

// txnCompares evaluates a Txn's compares against the store as it is.
func (n *node) txnCompares(req []byte) (bool, error) {
	var cmp compare
	for m := req; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return false, badRequest(err)
		}
		m = rest
		if num != 1 {
			continue
		}
		if err := parseCompare(p, &cmp); err != nil {
			return false, badRequest(err)
		}
		ok, err := n.cmpHolds(&cmp)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// txnRun is the state of one Txn's apply: its revision, whether it
// changed the store, and the results and records of its operations.
type txnRun struct {
	req     []byte // the Txn as replicated, where the put values lie
	voff    int64  // where req lies in the log
	rev     int64
	changed bool
	res     []opRes
	kvs     [][]byte
}

func (n *node) applyTxn(req []byte, voff int64, c *client, s *stream) error {
	ok, err := n.txnCompares(req)
	if err != nil {
		return err
	}
	// A failure branch kept by this node's stream is not in req.
	ops := req
	if !ok && s != nil && s.fail != nil {
		ops = s.fail
	}
	if err := n.checkTxnOps(ops, ok); err != nil {
		return err
	}
	t := txnRun{req: req, voff: voff, rev: n.st.rev + 1, res: n.txnRes[:0], kvs: n.kvs[:0]}
	r0, r1, err := n.runBranch(&t, ops, ok, 0)
	if err != nil {
		return err
	}
	if t.changed {
		n.st.rev = t.rev
	}
	if c != nil {
		n.replyTxn(c, s, ok, t.res, r0, r1, t.kvs)
	}
	clear(t.kvs)
	n.kvs = t.kvs[:0]
	clear(t.res)
	n.txnRes = t.res[:0]
	return nil
}

// seen is the revision a Txn's operation sees: the Txn's own once it
// changed something, else the store's before it.
func (t *txnRun) seen() int64 {
	if t.changed {
		return t.rev
	}
	return t.rev - 1
}

// runBranch applies the branch of ops that ok picks, and returns where
// its results lie in t.res: a nested Txn's results lie before its own.
// depth is how deep the Txn is nested, for its scratch.
func (n *node) runBranch(t *txnRun, ops []byte, ok bool, depth int) (int, int, error) {
	branch := 2
	if !ok {
		branch = 3
	}
	for len(n.txnScratch) <= depth {
		n.txnScratch = append(n.txnScratch, nil)
	}
	local := n.txnScratch[depth][:0]
	for m := ops; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return 0, 0, badRequest(err)
		}
		m = rest
		if num != branch {
			continue
		}
		// p is a RequestOp: one field.
		onum, _, _, op, _, err := field(p)
		if err != nil {
			return 0, 0, badRequest(err)
		}
		switch onum {
		case 1:
			var r rangeReq
			if err := parseRange(op, &r); err != nil {
				return 0, 0, badRequest(err)
			}
			k0 := len(t.kvs)
			var count int64
			var more bool
			if t.kvs, count, more, err = n.rangeKVs(&r, t.kvs); err != nil {
				return 0, 0, err
			}
			if r.keysOnly {
				for i := k0; i < len(t.kvs); i++ {
					t.kvs[i] = keysOnly(t.kvs[i], &r)
				}
			}
			local = append(local, opRes{kind: kRange, rev: t.seen(), kvs0: k0, kvs1: len(t.kvs), count: count, more: more})
		case 2:
			var r putReq
			if err := parsePut(op, &r); err != nil {
				return 0, 0, badRequest(err)
			}
			prev, err := n.putKey(&r, t.req, t.voff, t.rev)
			if err != nil {
				return 0, 0, err
			}
			t.changed = true
			x := opRes{kind: kPut, rev: t.rev}
			if r.prevKV {
				x.prev = prev
			}
			local = append(local, x)
		case 3:
			var r delReq
			if err := parseDel(op, &r); err != nil {
				return 0, 0, badRequest(err)
			}
			k0 := len(t.kvs)
			var deleted int64
			if t.kvs, deleted, err = n.deleteRange(&r, t.rev, t.kvs); err != nil {
				return 0, 0, err
			}
			t.changed = t.changed || deleted > 0
			local = append(local, opRes{kind: kDel, rev: t.seen(), kvs0: k0, kvs1: len(t.kvs), deleted: deleted})
		case 4:
			sub, err := n.txnCompares(op)
			if err != nil {
				return 0, 0, err
			}
			c0, c1, err := n.runBranch(t, op, sub, depth+1)
			if err != nil {
				return 0, 0, err
			}
			local = append(local, opRes{kind: kTxn, ok: sub, sub0: c0, sub1: c1})
		}
	}
	r0 := len(t.res)
	t.res = append(t.res, local...)
	clear(local)
	n.txnScratch[depth] = local[:0]
	return r0, len(t.res), nil
}

// checkTxnOps fails a Txn whose branch puts to a lease that does not
// exist, or reads at a revision compacted or still to come, before it
// changes anything. A nested Txn's branch is the one its compares pick
// on the store before the Txn, as in etcd.
func (n *node) checkTxnOps(ops []byte, ok bool) error {
	branch := 2
	if !ok {
		branch = 3
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
			switch {
			case r.rev == 0:
			case r.rev > n.st.rev:
				return requestErr(codeOutOfRange, msgFuture)
			case r.rev < n.st.compactRev:
				return requestErr(codeOutOfRange, msgCompacted)
			}
		case 2:
			var r putReq
			if err := parsePut(op, &r); err != nil {
				return badRequest(err)
			}
			if err := n.checkPutKey(&r); err != nil {
				return err
			}
		case 4:
			sub, err := n.txnCompares(op)
			if err != nil {
				return err
			}
			if err := n.checkTxnOps(op, sub); err != nil {
				return err
			}
		}
	}
	return nil
}

// opSize is the size of an operation's Response message.
func (n *node) opSize(x *opRes, res []opRes, kvs [][]byte) int {
	// etcd gives each a header with only its revision; a nested Txn's is
	// empty.
	return sizeBytesField(1, sizeVarintField(3, uint64(x.rev))) + n.opBodySize(x, res, kvs)
}

func (n *node) opBodySize(x *opRes, res []opRes, kvs [][]byte) int {
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
	case kTxn:
		return n.responsesSize(x.ok, res, x.sub0, x.sub1, kvs)
	}
	return 0
}

// responsesSize is the size of a TxnResponse's succeeded and responses.
func (n *node) responsesSize(ok bool, res []opRes, r0, r1 int, kvs [][]byte) int {
	size := 0
	if ok {
		size += 2
	}
	for i := r0; i < r1; i++ {
		size += sizeBytesField(3, sizeBytesField(opField[res[i].kind], n.opSize(&res[i], res, kvs)))
	}
	return size
}

var opField = map[uint8]int{kRange: 1, kPut: 2, kDel: 3, kTxn: 4}

func (n *node) replyTxn(c *client, s *stream, ok bool, res []opRes, r0, r1 int, kvs [][]byte) {
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize())+n.responsesSize(ok, res, r0, r1, kvs))
	n.writeHdr(&w)
	n.writeResponses(&w, ok, res, r0, r1, kvs)
	n.endReply(c, s, &w)
}

// writeResponses writes a TxnResponse's succeeded and responses.
func (n *node) writeResponses(w *msgW, ok bool, res []opRes, r0, r1 int, kvs [][]byte) {
	if ok {
		w.varintField(2, 1)
	}
	for i := r0; i < r1; i++ {
		x := &res[i]
		inner := n.opSize(x, res, kvs)
		w.lenField(3, sizeBytesField(opField[x.kind], inner))
		w.lenField(opField[x.kind], inner)
		w.lenField(1, sizeVarintField(3, uint64(x.rev)))
		writeVarintNZ(w, 3, uint64(x.rev))
		switch x.kind {
		case kRange:
			for _, kv := range kvs[x.kvs0:x.kvs1] {
				w.bytesField(2, kv)
			}
			if x.more {
				w.varintField(3, 1)
			}
			writeVarintNZ(w, 4, uint64(x.count))
		case kPut:
			if x.prev != nil {
				w.bytesField(2, x.prev)
			}
		case kDel:
			writeVarintNZ(w, 2, uint64(x.deleted))
			for _, kv := range kvs[x.kvs0:x.kvs1] {
				w.bytesField(3, kv)
			}
		case kTxn:
			n.writeResponses(w, x.ok, res, x.sub0, x.sub1, kvs)
		}
	}
}
