package wl

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"go.etcd.io/raft/v3"
)

// node is one replica: everything below runs on the loop's one thread.
type node struct {
	id, clusterID uint64
	members       []member

	st *store
	w  watches

	socks    []any // *client, *peerIn or *peerOut, by index
	gens     []uint16
	dirty    []*sock
	clients  int
	peersOut map[uint64]*peerOut

	rn      *raft.RawNode
	storage *raft.MemoryStorage
	lead    uint64
	term    uint64
	applied uint64
	commit  uint64

	// The log ring: proposals and received entries are written here once,
	// and the WAL, the peers and the apply read them from here.
	ring       []byte
	ringPos    int
	side       []byte // failure branches kept for their streams
	sidePos    int
	rx         []byte // received entries
	rxPos      int
	batchStart int
	batchOps   int
	batchOwn   []handle

	// Linearizable reads wait for a ReadIndex: reads not yet asked about,
	// and the batches that wait for their index or the apply.
	reads       []pendingRead
	readBuf     []byte
	readBatches []*readBatch
	freeBatches []*readBatch
	readSeq     uint64

	wal *wal

	// scratch for replies
	kvs     [][]byte
	txnRes  []opRes
	scratch []byte

	// The batches this node proposed, by sequence: the leader names them
	// in its appends to this node instead of sending them back (#626).
	holding bool // some peer's messages wait behind a held append
	// carryUntil: until when the leader carries the batches it would name
	// for a follower that lacked one.
	carryUntil map[uint64]time.Time
	fwd        []fwdBatch
	fwdPeers   map[uint64][]fwdBatch
	fwdSeq     uint32
}

type fwdBatch struct {
	seq     uint32
	data    []byte
	handles []handle
}

const fwdSlots = 1 << 14

// batchOrigin returns the node and sequence of a batch, if data is one.
func batchOrigin(data []byte) (uint64, uint32, bool) {
	if len(data) < batchHdr || binary.LittleEndian.Uint32(data[12:]) != magic {
		return 0, 0, false
	}
	return uint64(binary.LittleEndian.Uint32(data)), binary.LittleEndian.Uint32(data[4:]), true
}

type member struct {
	id              uint64
	name            string
	peerURL, cliURL string
	peerAddr        string
}

type pendingRead struct {
	h        handle
	kind     uint8
	off, len int
}

type readBatch struct {
	seq   uint64
	index uint64
	known bool
	asked int64 // tick when asked
	reads []pendingRead
	buf   []byte
}

type opRes struct {
	kind    uint8 // kRange, kPut, kDel, kTxn
	kvs0    int
	kvs1    int
	count   int64
	more    bool
	prev    []byte
	deleted int64
	prevKVs bool
}

// Batch layout: [4 origin][4 sequence][4 ops][4 magic], then each op [1 kind]
// [4 length] and the request's protobuf as the client sent it.
const (
	batchHdr = 16
	opHdr    = 5
	magic    = 0x77697265
	ringSize = 256 << 20
)

func (n *node) markDirty(s *sock) {
	if !s.dirty {
		s.dirty = true
		n.dirty = append(n.dirty, s)
	}
}

// client returns the client of a handle and its stream, if both are
// still there.
func (n *node) clientOf(h handle) (*client, *stream) {
	idx, gen, sid := h.parts()
	if idx >= len(n.socks) || n.gens[idx] != gen {
		return nil, nil
	}
	c, ok := n.socks[idx].(*client)
	if !ok || c.dead {
		return nil, nil
	}
	s := c.get(sid)
	if s == nil {
		return nil, nil
	}
	return c, s
}

// request takes one gRPC request message of stream s; msg is only valid
// during the call.
func (n *node) request(c *client, s *stream, msg []byte) error {
	switch s.kind {
	case kRange, kRangeStream:
		var r rangeReq
		if err := parseRange(msg, &r); err != nil {
			return err
		}
		if r.serializable {
			n.serveRange(c, s, &r)
			return nil
		}
		n.queueRead(mkHandle(c, s.id), s.kind, msg)
	case kTxn:
		n.addTxn(mkHandle(c, s.id), s, msg)
	case kPut, kDel, kCompact, kLeaseGrant, kLeaseRevoke:
		n.addOp(mkHandle(c, s.id), s.kind, msg)
	case kLeaseKeepAlive:
		id := int64(0)
		for m := msg; len(m) > 0; {
			num, _, v, _, rest, err := field(m)
			if err != nil {
				return err
			}
			if num == 1 {
				id = int64(v)
			}
			m = rest
		}
		ttl := n.st.leases[id]
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(id)) + sizeVarintField(3, uint64(ttl))
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(id))
		writeVarintNZ(&w, 3, uint64(ttl))
		n.endReply(c, s, &w)
	case kLeaseTTL:
		id := int64(0)
		for m := msg; len(m) > 0; {
			num, _, v, _, rest, err := field(m)
			if err != nil {
				return err
			}
			if num == 1 {
				id = int64(v)
			}
			m = rest
		}
		ttl, ok := n.st.leases[id]
		if !ok {
			ttl = -1
		}
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(id)) + sizeVarintField(3, uint64(ttl)) + sizeVarintField(4, uint64(ttl))
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(id))
		writeVarintNZ(&w, 3, uint64(ttl))
		writeVarintNZ(&w, 4, uint64(ttl))
		n.endReply(c, s, &w)
	case kWatch:
		return n.watchRequest(c, s, msg)
	case kStatus:
		n.replyStatus(c, s)
	case kMemberList:
		n.replyMembers(c, s)
	case kMoveLeader:
		target := uint64(0)
		for m := msg; len(m) > 0; {
			num, _, v, _, rest, err := field(m)
			if err != nil {
				return err
			}
			if num == 1 {
				target = v
			}
			m = rest
		}
		n.rn.TransferLeader(target)
		var w msgW
		n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize()))
		n.writeHdr(&w)
		n.endReply(c, s, &w)
	}
	return nil
}

func writeVarintNZ(w *msgW, num int, v uint64) {
	if v != 0 {
		w.varintField(num, v)
	}
}

// ---- replies

func (n *node) hdrSize() int {
	return sizeVarintField(1, n.clusterID) + sizeVarintField(2, n.id) + sizeVarintField(3, uint64(n.st.rev)) + sizeVarintField(4, n.term)
}

func (n *node) writeHdr(w *msgW) {
	w.lenField(1, n.hdrSize())
	writeVarintNZ(w, 1, n.clusterID)
	writeVarintNZ(w, 2, n.id)
	writeVarintNZ(w, 3, uint64(n.st.rev))
	writeVarintNZ(w, 4, n.term)
}

// beginReply writes the response headers of s and starts its one message
// of size bytes.
func (n *node) beginReply(c *client, s *stream, w *msgW, size int) {
	b := c.frame(9 + len(respHeaders))
	putFrameHeader(b, len(respHeaders), frHeaders, flEndHeaders, s.id)
	copy(b[9:], respHeaders)
	w.begin(&c.sock, s.id, size)
}

// endReply ends the message and the stream with grpc-status 0.
func (n *node) endReply(c *client, s *stream, w *msgW) {
	stats.replies++
	w.end()
	b := c.frame(9 + len(respTrailers))
	putFrameHeader(b, len(respTrailers), frHeaders, flEndHeaders|flEndStream, s.id)
	copy(b[9:], respTrailers)
	c.endStream(s)
	n.markDirty(&c.sock)
}

// replyError ends stream sid with a gRPC error, headers and trailers in
// one block.
func (n *node) replyError(c *client, sid uint32, code int, msg string) {
	blk := append([]byte(nil), respHeaders...)
	blk = append(blk, 0x00, 11)
	blk = append(blk, "grpc-status"...)
	cs := []byte(itoa(code))
	blk = append(blk, byte(len(cs)))
	blk = append(blk, cs...)
	blk = append(blk, 0x00, 12)
	blk = append(blk, "grpc-message"...)
	blk = append(blk, byte(len(msg)))
	blk = append(blk, msg...)
	b := c.frame(9 + len(blk))
	putFrameHeader(b, len(blk), frHeaders, flEndHeaders|flEndStream, sid)
	copy(b[9:], blk)
	if s := c.get(sid); s != nil {
		c.endStream(s)
	}
	n.markDirty(&c.sock)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func (n *node) replyErrorH(h handle, code int, msg string) {
	if c, s := n.clientOf(h); c != nil {
		n.replyError(c, s.id, code, msg)
	}
}

const (
	codeOutOfRange  = 11
	codeUnavailable = 14
	msgCompacted    = "etcdserver: mvcc: required revision has been compacted"
	msgFuture       = "etcdserver: mvcc: required revision is a future revision"
)

// ---- reads

func (n *node) queueRead(h handle, kind uint8, msg []byte) {
	off := len(n.readBuf)
	n.readBuf = append(n.readBuf, msg...)
	n.reads = append(n.reads, pendingRead{h: h, kind: kind, off: off, len: len(msg)})
}

// askReads sends one ReadIndex for the reads that came in this turn.
func (n *node) askReads(now int64) {
	if len(n.reads) == 0 {
		return
	}
	n.readSeq++
	var ctx [8]byte
	binary.BigEndian.PutUint64(ctx[:], n.readSeq)
	var b *readBatch
	if k := len(n.freeBatches); k > 0 {
		b = n.freeBatches[k-1]
		n.freeBatches = n.freeBatches[:k-1]
	} else {
		b = &readBatch{}
	}
	b.seq, b.known, b.asked = n.readSeq, false, now
	b.reads, n.reads = n.reads, b.reads[:0]
	b.buf, n.readBuf = n.readBuf, b.buf[:0]
	n.readBatches = append(n.readBatches, b)
	stats.readIndex++
	n.rn.ReadIndex(ctx[:])
}

// readState records the index of a ReadIndex.
func (n *node) readState(ctx []byte, index uint64) {
	if len(ctx) != 8 {
		return
	}
	seq := binary.BigEndian.Uint64(ctx)
	for _, b := range n.readBatches {
		if b.seq == seq {
			b.index, b.known = index, true
		}
	}
}

// serveReads answers the batches whose index is applied.
func (n *node) serveReads() {
	k := 0
	for i, b := range n.readBatches {
		if b.known && b.index <= n.applied {
			for _, r := range b.reads {
				c, s := n.clientOf(r.h)
				if c == nil {
					continue
				}
				var req rangeReq
				if err := parseRange(b.buf[r.off:r.off+r.len], &req); err != nil {
					n.replyError(c, s.id, 3, "bad request")
					continue
				}
				n.serveRange(c, s, &req)
			}
			n.freeBatches = append(n.freeBatches, b)
			continue
		}
		n.readBatches[k] = n.readBatches[i]
		k++
	}
	n.readBatches = n.readBatches[:k]
}

// retryReads asks again for batches that waited too long, as after a
// leader change that dropped their ReadIndex.
func (n *node) retryReads(now int64) {
	for _, b := range n.readBatches {
		if !b.known && now-b.asked > 5 {
			n.readSeq++
			b.seq, b.asked = n.readSeq, now
			var ctx [8]byte
			binary.BigEndian.PutUint64(ctx[:], b.seq)
			n.rn.ReadIndex(ctx[:])
		}
	}
}

// serveRange answers a Range or RangeStream from the store as it is.
func (n *node) serveRange(c *client, s *stream, r *rangeReq) {
	st := n.st
	if r.rev > 0 && r.rev <= st.compactRev && r.rev < st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgCompacted)
		return
	}
	if r.rev > st.rev {
		n.replyError(c, s.id, codeOutOfRange, msgFuture)
		return
	}
	kvs := n.kvs[:0]
	var count int64
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			if rec := st.at(id, r.rev); rec != nil {
				count = 1
				if !r.countOnly {
					kvs = append(kvs, rec)
				}
			}
		}
	} else {
		end := r.end
		if len(end) == 1 && end[0] == 0 {
			end = nil
		}
		limit := r.limit
		if limit <= 0 {
			limit = math.MaxInt64
		}
		st.tree.scan(r.key, end, func(id uint32) bool {
			rec := st.at(id, r.rev)
			if rec == nil {
				return true
			}
			count++
			if !r.countOnly && int64(len(kvs)) < limit {
				kvs = append(kvs, rec)
			}
			return true
		})
	}
	if r.keysOnly {
		for i, kv := range kvs {
			kvs[i] = stripValue(kv)
		}
	}
	more := int64(len(kvs)) < count && !r.countOnly
	body := sizeBytesField(1, n.hdrSize()) + sizeVarintField(4, uint64(count))
	if more {
		body += 2
	}
	for _, kv := range kvs {
		body += sizeBytesField(2, len(kv))
	}
	size := body
	if s.kind == kRangeStream {
		size = sizeBytesField(1, body)
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	if s.kind == kRangeStream {
		w.lenField(1, body)
	}
	n.writeHdr(&w)
	for _, kv := range kvs {
		w.bytesField(2, kv)
	}
	if more {
		w.varintField(3, 1)
	}
	writeVarintNZ(&w, 4, uint64(count))
	n.endReply(c, s, &w)
	clear(kvs)
	n.kvs = kvs[:0]
}

func stripValue(kv []byte) []byte {
	var out []byte
	for m := kv; len(m) > 0; {
		num, typ, v, p, rest, err := field(m)
		if err != nil {
			return kv
		}
		switch {
		case num == 5:
		case typ == 0:
			out = appendVarintField(out, num, v)
		default:
			out = appendBytesField(out, num, p)
		}
		m = rest
	}
	return out
}

// ---- writes

// addOp appends a write to this turn's batch, in the log ring.
func (n *node) addOp(h handle, kind uint8, msg []byte) {
	need := opHdr + len(msg)
	if n.batchStart < 0 {
		need += batchHdr
	}
	if n.ringPos+need > len(n.ring) {
		// Wrap: the batch so far moves to the start of the ring.
		if n.batchStart >= 0 {
			k := copy(n.ring, n.ring[n.batchStart:n.ringPos])
			n.batchStart, n.ringPos = 0, k
		} else {
			n.ringPos = 0
		}
	}
	if n.batchStart < 0 {
		n.batchStart = n.ringPos
		b := n.ring[n.ringPos : n.ringPos+batchHdr]
		n.fwdSeq++
		binary.LittleEndian.PutUint32(b, uint32(n.id))
		binary.LittleEndian.PutUint32(b[4:], n.fwdSeq)
		binary.LittleEndian.PutUint32(b[12:], magic)
		n.ringPos += batchHdr
		n.batchOps = 0
		n.batchOwn = n.batchOwn[:0]
	}
	b := n.ring[n.ringPos : n.ringPos+opHdr]
	b[0] = kind
	binary.LittleEndian.PutUint32(b[1:], uint32(len(msg)))
	copy(n.ring[n.ringPos+opHdr:], msg)
	n.ringPos += opHdr + len(msg)
	n.batchOps++
	n.batchOwn = append(n.batchOwn, h)
}

// addTxn adds a Txn. A failure branch of reads only changes nothing,
// so only this node, which answers the client, needs it: it stays with
// the stream, and the batch carries the compares and the success branch.
func (n *node) addTxn(h handle, s *stream, msg []byte) {
	failBytes, readsOnly := 0, true
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			readsOnly = false
			break
		}
		if num == 3 {
			failBytes += len(m) - len(rest)
			if onum, _, _, _, _, err := field(p); err != nil || onum != 1 {
				readsOnly = false
			}
		}
		m = rest
	}
	if !readsOnly || failBytes == 0 {
		n.addOp(h, kTxn, msg)
		return
	}
	// Keep the failure branch in the side ring.
	if n.sidePos+failBytes > len(n.side) {
		n.sidePos = 0
	}
	off := n.sidePos
	kept := n.scratch[:0]
	for m := msg; len(m) > 0; {
		_, _, _, _, rest, _ := field(m)
		f := m[:len(m)-len(rest)]
		if f[0] == 3<<3|2 {
			n.sidePos += copy(n.side[n.sidePos:], f)
		} else {
			kept = append(kept, f...)
		}
		m = rest
	}
	s.fail = n.side[off:n.sidePos:n.sidePos]
	n.addOp(h, kTxn, kept)
	n.scratch = kept[:0]
}

// proposeBatch hands this turn's batch to Raft.
func (n *node) proposeBatch() {
	if n.batchStart < 0 {
		return
	}
	data := n.ring[n.batchStart:n.ringPos:n.ringPos]
	binary.LittleEndian.PutUint32(data[8:], uint32(n.batchOps))
	n.batchStart = -1
	// A follower's batch comes back from the leader by name.
	seq := binary.LittleEndian.Uint32(data[4:])
	f := &n.fwd[seq&(fwdSlots-1)]
	f.seq, f.data = seq, data
	f.handles = append(f.handles[:0], n.batchOwn...)
	stats.proposals++
	if n.lead != n.id && n.lead != 0 {
		n.sendBatch(data)
	}
	if err := n.rn.Propose(data); err != nil {
		for _, h := range n.batchOwn {
			n.replyErrorH(h, codeUnavailable, "etcdserver: leader changed")
		}
	}
}

// copyToRing copies received entry data into the ring and returns it
// there.
func (n *node) copyToRing(p []byte) []byte {
	if n.batchStart >= 0 {
		panic("copyToRing inside a batch")
	}
	if n.ringPos+len(p) > len(n.ring) {
		n.ringPos = 0
	}
	d := n.ring[n.ringPos : n.ringPos+len(p) : n.ringPos+len(p)]
	copy(d, p)
	n.ringPos += len(p)
	return d
}

// ---- apply

func le64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

// applyBatch applies one entry's batch; walOff is where its data lies in
// the WAL.
func (n *node) applyBatch(data []byte, walOff int64) {
	if len(data) < batchHdr || binary.LittleEndian.Uint32(data[12:]) != magic {
		return
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
		switch kind {
		case kPut:
			n.applyPut(req, voff, c, s)
		case kDel:
			n.applyDel(req, c, s)
		case kTxn:
			n.applyTxn(req, voff, c, s)
		case kCompact:
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
				var w msgW
				n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize()))
				n.writeHdr(&w)
				n.endReply(c, s, &w)
			}
		case kLeaseGrant:
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
		case kLeaseRevoke:
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
				var w msgW
				n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize()))
				n.writeHdr(&w)
				n.endReply(c, s, &w)
			}
		}
	}
}

func voffOf(req, sub []byte, base int64) int64 {
	return base + int64(cap(req)-cap(sub))
}

func (n *node) applyPut(req []byte, voff int64, c *client, s *stream) {
	var r putReq
	if err := parsePut(req, &r); err != nil {
		if c != nil {
			n.replyError(c, s.id, 3, "bad request")
		}
		return
	}
	st := n.st
	rev := st.rev + 1
	id := st.intern(r.key)
	value := r.value
	vo := voffOf(req, r.value, voff)
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
	prev := st.put(id, value, r.lease, rev, vo)
	st.rev = rev
	n.w.record(st, id, false, rev, st.live(id), prev)
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
}

func (n *node) applyDel(req []byte, c *client, s *stream) {
	var r delReq
	if err := parseDel(req, &r); err != nil {
		return
	}
	st := n.st
	rev := st.rev + 1
	kvs := n.kvs[:0]
	var deleted int64
	del := func(id uint32) {
		if prev := st.del(id, rev); prev != nil {
			deleted++
			n.w.record(st, id, true, rev, nil, prev)
			if r.prevKV {
				kvs = append(kvs, prev)
			}
		}
	}
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			del(id)
		}
	} else {
		end := r.end
		if len(end) == 1 && end[0] == 0 {
			end = nil
		}
		var ids []uint32
		st.tree.scan(r.key, end, func(id uint32) bool { ids = append(ids, id); return true })
		for _, id := range ids {
			del(id)
		}
	}
	if deleted > 0 {
		st.rev = rev
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

func (n *node) applyTxn(req []byte, voff int64, c *client, s *stream) {
	st := n.st
	ok := true
	var cmp compare
	for m := req; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return
		}
		if num == 1 && ok {
			if err := parseCompare(p, &cmp); err != nil {
				return
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
			return
		}
		m = rest
		if num != branch {
			continue
		}
		// p is a RequestOp: one field.
		onum, _, _, op, _, err := field(p)
		if err != nil {
			return
		}
		switch onum {
		case 1:
			var r rangeReq
			if parseRange(op, &r) != nil {
				return
			}
			k0 := len(kvs)
			var count int64
			if len(r.end) == 0 {
				if id, ok := st.find(r.key); ok {
					if rec := st.at(id, r.rev); rec != nil {
						count = 1
						if !r.countOnly {
							kvs = append(kvs, rec)
						}
					}
				}
			} else {
				end := r.end
				if len(end) == 1 && end[0] == 0 {
					end = nil
				}
				limit := r.limit
				if limit <= 0 {
					limit = math.MaxInt64
				}
				st.tree.scan(r.key, end, func(id uint32) bool {
					if rec := st.at(id, r.rev); rec != nil {
						count++
						if !r.countOnly && int64(len(kvs)-k0) < limit {
							kvs = append(kvs, rec)
						}
					}
					return true
				})
			}
			res = append(res, opRes{kind: kRange, kvs0: k0, kvs1: len(kvs), count: count, more: int64(len(kvs)-k0) < count && !r.countOnly})
		case 2:
			var r putReq
			if parsePut(op, &r) != nil {
				return
			}
			id := st.intern(r.key)
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
			prev := st.put(id, value, r.lease, rev, vo)
			changed = true
			n.w.record(st, id, false, rev, st.live(id), prev)
			x := opRes{kind: kPut}
			if r.prevKV {
				x.prev = prev
			}
			res = append(res, x)
		case 3:
			var r delReq
			if parseDel(op, &r) != nil {
				return
			}
			k0 := len(kvs)
			var deleted int64
			del := func(id uint32) {
				if prev := st.del(id, rev); prev != nil {
					deleted++
					changed = true
					n.w.record(st, id, true, rev, nil, prev)
					if r.prevKV {
						kvs = append(kvs, prev)
					}
				}
			}
			if len(r.end) == 0 {
				if id, ok := st.find(r.key); ok {
					del(id)
				}
			} else {
				end := r.end
				if len(end) == 1 && end[0] == 0 {
					end = nil
				}
				var ids []uint32
				st.tree.scan(r.key, end, func(id uint32) bool { ids = append(ids, id); return true })
				for _, id := range ids {
					del(id)
				}
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

// ---- watch

func (n *node) watchRequest(c *client, s *stream, msg []byte) error {
	if !s.watch {
		s.watch = true
		b := c.frame(9 + len(respHeaders))
		putFrameHeader(b, len(respHeaders), frHeaders, flEndHeaders, s.id)
		copy(b[9:], respHeaders)
	}
	num, _, _, p, _, err := field(msg)
	if err != nil {
		return err
	}
	switch num {
	case 1:
		x := watcher{sock: c.idx, gen: c.gen, sid: s.id, id: int64(len(s.wids))}
		var filters []byte
		for m := p; len(m) > 0; {
			fn, typ, v, fp, rest, err := field(m)
			if err != nil {
				return err
			}
			switch fn {
			case 1:
				x.key = bytes.Clone(fp)
			case 2:
				x.end = bytes.Clone(fp)
			case 5:
				if typ == 2 {
					filters = append(filters, fp...)
				} else {
					filters = append(filters, byte(v))
				}
			case 6:
				x.prevKV = v != 0
			case 7:
				if v != 0 {
					x.id = int64(v)
				}
			}
			m = rest
		}
		for _, f := range filters {
			switch f {
			case 0:
				x.noPut = true
			case 1:
				x.noDel = true
			}
		}
		i := n.w.add(x)
		s.wids = append(s.wids, i)
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id)) + 2
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(x.id))
		w.varintField(3, 1)
		w.end()
	case 2:
		var wid int64
		for m := p; len(m) > 0; {
			fn, _, v, _, rest, err := field(m)
			if err != nil {
				return err
			}
			if fn == 1 {
				wid = int64(v)
			}
			m = rest
		}
		for _, i := range s.wids {
			if n.w.ws[i].active && n.w.ws[i].id == wid {
				n.w.remove(i)
			}
		}
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(wid)) + 2
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(wid))
		w.varintField(4, 1)
		w.end()
	case 3:
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, math.MaxUint64)
		w.begin(&c.sock, s.id, size)
		n.writeHdr(&w)
		w.varintField(2, math.MaxUint64)
		w.end()
	}
	n.markDirty(&c.sock)
	return nil
}

func (n *node) closeWatchStream(c *client, s *stream) {
	for _, i := range s.wids {
		if n.w.ws[i].active && n.w.ws[i].sock == c.idx && n.w.ws[i].sid == s.id {
			n.w.remove(i)
		}
	}
	s.wids = s.wids[:0]
}

// flushEvents writes each watcher's events of the apply as one
// WatchResponse.
func (n *node) flushEvents() {
	w := &n.w
	st := n.st
	for _, i := range w.touched {
		x := &w.ws[i]
		x.queued = false
		first := x.first
		x.first, x.last = -1, -1
		if !x.active {
			continue
		}
		c, s := n.clientOf(mkHandleRaw(x.sock, x.gen, x.sid))
		if c == nil {
			continue
		}
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			size += sizeBytesField(11, n.eventSize(x, &w.events[e]))
		}
		var mw msgW
		mw.begin(&c.sock, s.id, size)
		n.writeHdr(&mw)
		writeVarintNZ(&mw, 2, uint64(x.id))
		for e := first; e >= 0; e = w.events[e].next {
			ev := &w.events[e]
			mw.lenField(11, n.eventSize(x, ev))
			if ev.del {
				mw.varintField(1, 1)
				key := st.key(ev.key)
				mw.lenField(2, sizeBytesField(1, len(key))+sizeVarintField(3, uint64(ev.rev)))
				mw.bytesField(1, key)
				mw.varintField(3, uint64(ev.rev))
			} else {
				mw.bytesField(2, ev.kv)
			}
			if x.prevKV && ev.prev != nil {
				mw.bytesField(3, ev.prev)
			}
		}
		mw.end()
		n.markDirty(&c.sock)
	}
	clear(w.events)
	w.events = w.events[:0]
	w.touched = w.touched[:0]
}

func (n *node) eventSize(x *watcher, ev *event) int {
	b := 0
	if ev.del {
		key := n.st.key(ev.key)
		b += 2 + sizeBytesField(2, sizeBytesField(1, len(key))+sizeVarintField(3, uint64(ev.rev)))
	} else {
		b += sizeBytesField(2, len(ev.kv))
	}
	if x.prevKV && ev.prev != nil {
		b += sizeBytesField(3, len(ev.prev))
	}
	return b
}

func mkHandleRaw(idx int, gen uint16, sid uint32) handle {
	return handle(uint64(idx)<<48 | uint64(gen)<<32 | uint64(sid))
}

// ---- status

func (n *node) replyStatus(c *client, s *stream) {
	ver := []byte("3.7.2")
	size := sizeBytesField(1, n.hdrSize()) + sizeBytesField(2, len(ver)) + sizeVarintField(3, 1<<20) +
		sizeVarintField(4, n.lead) + sizeVarintField(5, n.commit) + sizeVarintField(6, n.term) + sizeVarintField(7, n.applied)
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	w.bytesField(2, ver)
	w.varintField(3, 1<<20)
	writeVarintNZ(&w, 4, n.lead)
	writeVarintNZ(&w, 5, n.commit)
	writeVarintNZ(&w, 6, n.term)
	writeVarintNZ(&w, 7, n.applied)
	n.endReply(c, s, &w)
}

func (n *node) replyMembers(c *client, s *stream) {
	var body []byte
	for _, m := range n.members {
		var mb []byte
		mb = appendVarintField(mb, 1, m.id)
		mb = appendBytesField(mb, 2, []byte(m.name))
		mb = appendBytesField(mb, 3, []byte(m.peerURL))
		mb = appendBytesField(mb, 4, []byte(m.cliURL))
		body = appendBytesField(body, 2, mb)
	}
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize())+len(body))
	n.writeHdr(&w)
	w.write(body)
	n.endReply(c, s, &w)
}

var dbgOps, dbgBytes [16]int
