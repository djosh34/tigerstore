package wl

import (
	"bytes"
	"errors"
	"slices"
	"unsafe"
)

// The reserve (#727, ruled: a shared reserve within the client pool).
// What waits to be sent to a client, in its send buffer and held for
// want of HTTP/2 window, stays within its share (sizes.connOut). A message
// that would take it past the share takes the room it passes it by from
// the reserve (sizes.reserve), shared by every client, before it is
// built, and the room goes back as the client's bytes leave for its
// socket (giveRoom). A message within its share takes no new step.
//
// When the reserve is spent:
//   - a range's reply waits unbuilt, its request alone kept, until room
//     comes back, as a client waits for the read half (roomWait);
//   - a live watch event waits as a slow watcher's do, and its watch
//     catches up later from the change index;
//   - a watch's catch-up waits for its next turn;
//   - a write's reply, its write applied, waits unbuilt too (heldReply):
//     it keeps its records' keys and revisions, not their bytes, and the
//     store keeps their states (holdRead) until it is built. It is never
//     refused: a client told "too many requests" for a write that
//     applied would retry it, and a compare-and-put's retry fails its
//     compare.
//
// One message is at most maxMsg, which the reserve holds, so none can
// pass a share by more than the whole reserve. Messages of up to
// smallMsg, headers, trailers and a few fields, take no room: each stream
// of the client has at most one of them under way.

// smallMsg is the largest message that takes no room of the reserve.
const smallMsg = 1 << 10

// msgRoom reports whether a message of m bytes may be built for client c
// now: within its share, or with the room the reserve gives it for what
// passes it, which it takes.
func (n *node) msgRoom(c *client, m int64) bool {
	past := n.roomNeeded(c, m)
	if past == 0 {
		return true
	}
	if n.reserved+past > n.sizes.reserve() {
		n.stats.roomWaits++
		n.roomShort = true
		return false
	}
	c.roomHeld += past
	n.reserved += past
	return true
}

// roomNeeded is the extra room a message of m bytes needs from the
// reserve, beyond what client c holds already.
func (n *node) roomNeeded(c *client, m int64) int64 {
	if m <= smallMsg {
		return 0
	}
	return max(0, c.waiting()+c.flow.bytes+m-n.connOut-c.roomHeld)
}

// giveRoom gives back the room of the reserve that client c holds past
// what it holds over its share now, all of it once c is closed.
func (n *node) giveRoom(c *client, closed bool) {
	if closed && len(n.heldReplies) > 0 {
		// A reply it waited for lets go of what it holds.
		n.roomFreed = true
	}
	if !closed && c.roomWaiters > 0 && c.waiting()+c.flow.bytes <= n.connOut/2 {
		// Its messages waiting may have waited behind its own bytes, with
		// no room of the reserve held, and those have gone down to half
		// its share (#689's re-check, B2).
		n.roomFreed, n.roomShort = true, false
	}
	if c.roomHeld == 0 {
		return
	}
	keep := min(c.roomHeld, max(0, c.waiting()+c.flow.bytes-n.connOut))
	if closed {
		keep = 0
	}
	if keep < c.roomHeld {
		n.roomFreed = n.roomFreed || len(n.roomWait) > 0 || len(n.heldReplies) > 0
		n.roomShort = false
	}
	n.reserved -= c.roomHeld - keep
	c.roomHeld = keep
}

// roomRead is a range waiting for reply room. Its body size, without the
// header, stays right while its store, revision and compacted revision
// stay the same. No record or value is kept.
type roomRead struct {
	awayRead
	tag             *byte
	rev, compactRev int64
	body            int
}

// waitRoom keeps range request msg of stream s of client c until the
// reserve has room for its reply, whose body size excludes the header.
func (n *node) waitRoom(c *client, s *stream, msg []byte, body int) {
	c.roomWaiters++
	// Its request, kept, counts in the client's pool (B9).
	c.growOut(len(msg))
	c.heldCounted += int64(len(msg))
	r := roomRead{h: mkHandle(c, s.id), msg: bytes.Clone(msg), body: body}
	if n.st != nil {
		r.tag, r.rev, r.compactRev = n.st.tag, n.st.rev, n.st.compactRev
	}
	n.roomWait = append(n.roomWait, r)
}

// cannotFit reports whether a message of m bytes could never be built for
// a client, past its share by more than the whole reserve: it must go in
// pieces, as waiting for room would wait for good (#689's re-check, B1).
func (n *node) cannotFit(m int64) bool { return m-n.connOut > n.sizes.reserve() }

// dropClientWaits lets go of the ranges client c, closed, waited for room
// with: their requests count in the pool.
func (n *node) dropClientWaits(c *client) {
	n.dropAway(c, 0)
	k := 0
	for _, r := range n.roomWait {
		if idx, gen, _ := r.h.parts(); idx == c.idx && gen == c.gen {
			c.growOut(-len(r.msg))
			c.heldCounted -= int64(len(r.msg))
			c.roomWaiters--
			continue
		}
		n.roomWait[k] = r
		k++
	}
	clear(n.roomWait[k:])
	n.roomWait = n.roomWait[:k]
}

// dropWaits lets go of what stream sid of client c waits for room with:
// the client reset it.
func (n *node) dropWaits(c *client, sid uint32) {
	n.dropAway(c, sid)
	h := mkHandle(c, sid)
	k := 0
	for _, r := range n.roomWait {
		if r.h == h {
			c.roomWaiters--
			c.growOut(-len(r.msg))
			c.heldCounted -= int64(len(r.msg))
			continue
		}
		n.roomWait[k] = r
		k++
	}
	clear(n.roomWait[k:])
	n.roomWait = n.roomWait[:k]
	k = 0
	for _, hr := range n.heldReplies {
		if hr.h == h {
			n.endHeld(hr)
			continue
		}
		n.heldReplies[k] = hr
		k++
	}
	clear(n.heldReplies[k:])
	n.heldReplies = n.heldReplies[:k]
}

// serveRoomWait serves again the ranges that waited for room, once some
// came back; one that finds none waits again.
func (n *node) serveRoomWait() {
	if !n.roomFreed {
		return
	}
	n.roomFreed = false
	n.serveHeld()
	reads := n.roomWait
	n.roomWait = nil
	for _, r := range reads {
		c, s := n.clientOf(r.h)
		if c == nil {
			continue
		}
		if r.tag != nil && r.tag == n.st.tag && r.rev == n.st.rev && r.compactRev == n.st.compactRev {
			// A little room came back, not necessarily enough for this
			// reply. Reading its values again just to find that out built
			// gigabytes of discarded records under concurrent ranges
			// (#833). Check its size first, with the header as it is now.
			m := int64(5 + sizeBytesField(1, n.hdrSize()) + r.body)
			if past := n.roomNeeded(c, m); past > 0 && n.reserved+past > n.sizes.reserve() && !n.cannotFit(m) {
				n.stats.roomWaits++
				n.roomShort = true
				n.roomWait = append(n.roomWait, r)
				continue
			}
		}
		c.roomWaiters--
		c.growOut(-len(r.msg))
		c.heldCounted -= int64(len(r.msg))
		var req rangeReq
		if err := parseRange(r.msg, &req); err != nil {
			continue
		}
		n.serveRange(c, s, &req, r.msg)
	}
}

// heldReply is the reply of an applied write that waits for room in the
// reserve, unbuilt: a Put's with prev_kv, a DeleteRange's with prev_kv,
// or a Txn's with ranges or either. It holds none of its records' bytes:
// each is named by its key and the revision of its put (recRef), read
// again when the reply is built, and its store keeps their states until
// then, as a range sent in pieces holds its revision (bigrange.go).
type heldReply struct {
	h      handle
	c      *client
	st     *store // the store it reads
	kind   uint8  // kPut, kDel or kTxn
	hdrRev int64  // the revision its header names
	// hold is the revision it holds in st (store.holdTrim), -1 if none:
	// its records' least, at least the compacted one. Each record's own
	// state it holds too (store.holdState).
	hold int64
	body int // its size less its header's
	// A DeleteRange's keys deleted; a Txn's results, where a put's value
	// before is refs[kvs0] when kvs1 > kvs0, and whether it succeeded.
	deleted  int64
	res      []opRes
	r0, r1   int
	ok       bool
	refs     []recRef
	fetching bool // it waits for values read off the loop
	counted  int  // what it holds, in its client's pool
}

// recRef names a record of a held reply: the put of key id at revision
// mod, or, for a record that has no value (a range's keys_only), the
// record itself, a key and a few fields.
type recRef struct {
	id  uint32
	mod int64
	rec []byte
}

// holdPut holds the reply of an applied Put, prev its record before.
func (n *node) holdPut(c *client, s *stream, prev []byte) {
	var recs [][]byte
	if prev != nil {
		recs = [][]byte{prev}
	}
	n.holdReply(c, s, &heldReply{kind: kPut}, recs)
}

// holdDel holds the reply of an applied DeleteRange, kvs its records
// before.
func (n *node) holdDel(c *client, s *stream, deleted int64, kvs [][]byte) {
	n.holdReply(c, s, &heldReply{kind: kDel, deleted: deleted}, kvs)
}

// holdTxn holds the reply of an applied Txn.
func (n *node) holdTxn(c *client, s *stream, ok bool, res []opRes, r0, r1 int, kvs [][]byte) {
	hr := &heldReply{kind: kTxn, ok: ok, res: slices.Clone(res), r0: r0, r1: r1, hdrRev: n.st.rev}
	// Sized with its puts' values before, which the loop below moves into
	// its records: sized after, a put's prev counted nothing (#689's
	// re-check, B3).
	hr.body = n.heldSize(hr, kvs) - sizeBytesField(1, n.hdrSizeAt(hr.hdrRev))
	recs := kvs
	for i := range hr.res {
		if x := &hr.res[i]; x.kind == kPut && x.prev != nil {
			if len(recs) == len(kvs) {
				recs = slices.Clone(kvs)
			}
			x.kvs0, x.kvs1 = len(recs), len(recs)+1
			recs = append(recs, x.prev)
			x.prev = nil
		}
	}
	n.holdReply(c, s, hr, recs)
}

// holdReply keeps hr, the reply of stream s of client c with records
// recs, until the reserve has room for it.
func (n *node) holdReply(c *client, s *stream, hr *heldReply, recs [][]byte) {
	st := n.st
	hr.h, hr.c, hr.st, hr.hdrRev, hr.hold = mkHandle(c, s.id), c, st, st.rev, -1
	if hr.kind != kTxn {
		hr.body = n.heldSize(hr, recs) - sizeBytesField(1, n.hdrSizeAt(hr.hdrRev))
	}
	c.roomWaiters++
	hr.refs = make([]recRef, 0, len(recs))
	for _, rec := range recs {
		key, mod, value := recIdent(rec)
		id, found := st.find(key)
		if !value || !found {
			// A record of no value is kept as it is; a key the tree
			// does not have, never, since the apply just read it.
			hr.refs = append(hr.refs, recRef{rec: bytes.Clone(rec)})
			hr.counted += len(rec)
			continue
		}
		hr.refs = append(hr.refs, recRef{id: id, mod: mod})
		// Trimming keeps the states its records were read in from the
		// least one's revision on, at the compacted one if older: the Txn
		// read each at a revision since, at which it was the newest. A
		// checkpoint takes in each record's own state, not every state a
		// read at those revisions sees (the re-check of e710ebb9).
		if rev := max(mod, st.compactRev); hr.hold < 0 || rev < hr.hold {
			hr.hold = rev
		}
		st.holdState(id, mod)
		hr.counted += heldStateSize
	}
	if hr.hold >= 0 {
		st.holdTrim(hr.hold)
	}
	hr.counted += cap(hr.refs)*int(unsafe.Sizeof(recRef{})) + cap(hr.res)*int(unsafe.Sizeof(opRes{}))
	c.growOut(hr.counted)
	c.heldCounted += int64(hr.counted)
	n.stats.heldReplies++
	n.heldReplies = append(n.heldReplies, hr)
}

// recIdent returns the key of record rec, the revision of its put, and
// whether it has a value.
func recIdent(rec []byte) (key []byte, mod int64, value bool) {
	for m := rec; len(m) > 0; {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return key, mod, false
		}
		switch num {
		case 1:
			key = p
		case 3:
			mod = int64(v)
		case 5:
			value = true
		}
		m = rest
	}
	return key, mod, value
}

// heldSize is the size of held reply hr with records recs.
func (n *node) heldSize(hr *heldReply, recs [][]byte) int {
	switch hr.kind {
	case kPut:
		return n.putReplySize(hr.hdrRev, firstRec(recs))
	case kDel:
		return n.delReplySize(hr.hdrRev, hr.deleted, recs)
	}
	return n.txnReplySize(hr.hdrRev, hr.ok, hr.res, hr.r0, hr.r1, recs)
}

func firstRec(recs [][]byte) []byte {
	if len(recs) == 0 {
		return nil
	}
	return recs[0]
}

// serveHeld builds and sends the held replies the reserve has room for
// now; one that finds none, or whose values are away, waits again.
func (n *node) serveHeld() {
	held := n.heldReplies
	n.heldReplies = nil
	for _, hr := range held {
		if n.stepHeld(hr) {
			n.heldReplies = append(n.heldReplies, hr)
			continue
		}
		n.endHeld(hr)
	}
}

// stepHeld builds and sends held reply hr if it can. It reports whether
// hr waits on.
func (n *node) stepHeld(hr *heldReply) bool {
	c, s := n.clientOf(hr.h)
	if c == nil {
		return false
	}
	if hr.st != n.st {
		// A snapshot installed since replaced the store it reads: the
		// call ends as a connection lost would end it, its outcome
		// unknown to the client, never as a refusal.
		n.abortBig(c, s)
		return false
	}
	if hr.fetching {
		return true
	}
	size := sizeBytesField(1, n.hdrSizeAt(hr.hdrRev)) + hr.body
	if !n.msgRoom(c, int64(5+size)) {
		return true
	}
	st := hr.st
	recs := n.kvs[:0]
	defer func() {
		clear(recs)
		n.kvs = recs[:0]
	}()
	away := false
	st.noWait = true
	for _, r := range hr.refs {
		if r.rec != nil {
			recs = append(recs, r.rec)
			continue
		}
		rec, err := st.putAt(r.id, r.mod)
		if errors.Is(err, errAway) {
			away = true
			continue
		}
		if err != nil || rec == nil {
			// Never, as its states are held.
			st.noWait = false
			n.abortBig(c, s)
			return false
		}
		recs = append(recs, rec)
	}
	st.noWait = false
	if away {
		// It gives back the room it took until its values come.
		n.giveRoom(c, false)
		hr.fetching = true
		n.fetchAway()
		return true
	}
	if hr.kind == kTxn {
		for i := range hr.res {
			if x := &hr.res[i]; x.kind == kPut && x.kvs1 > x.kvs0 {
				x.prev = recs[x.kvs0]
			}
		}
	}
	// Its records read again are the bytes they were: its size is.
	size = n.heldSize(hr, recs)
	switch hr.kind {
	case kPut:
		n.writePutReply(c, s, size, hr.hdrRev, firstRec(recs))
	case kDel:
		n.writeDelReply(c, s, size, hr.hdrRev, hr.deleted, recs)
	default:
		n.writeTxnReply(c, s, size, hr.hdrRev, hr.ok, hr.res, hr.r0, hr.r1, recs)
	}
	return false
}

// endHeld lets go of what held reply hr holds.
func (n *node) endHeld(hr *heldReply) {
	if hr.refs != nil || hr.res != nil {
		hr.c.roomWaiters--
	}
	if hr.st == n.st {
		if hr.hold >= 0 {
			hr.st.releaseTrim(hr.hold)
		}
		for _, r := range hr.refs {
			if r.rec == nil {
				hr.st.releaseState(r.id, r.mod)
			}
		}
	}
	hr.hold = -1
	hr.c.growOut(-hr.counted)
	hr.c.heldCounted -= int64(hr.counted)
	hr.counted = 0
	hr.refs, hr.res = nil, nil
}
