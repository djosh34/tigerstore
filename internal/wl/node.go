package wl

import (
	"encoding/binary"
	"errors"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// node is one replica: everything below runs on the loop's one thread.
type node struct {
	id, clusterID uint64
	members       []member

	st     *store
	w      watches
	leases leaseClock

	stats   counters
	metrics *nodeMetrics

	socks    []any // *client, *peerIn or *peerOut, by index
	gens     []uint16
	dirty    []*sock
	peersOut map[uint64]*peerOut

	rn      *raft.RawNode
	storage *logStorage
	lead    uint64
	term    uint64
	applied uint64
	commit  uint64

	// The rings: proposals and received entries are written once, into
	// ring and rx, and the log, the peers and the apply read them there.
	// side keeps Txns' failure branches for their streams.
	ring, rx, side *ring
	now            time.Time // when the turn began

	// The open batch: its bytes, whether they lie in the ring, its op
	// count and its ops' clients.
	batch       []byte
	batchInRing bool
	batchOps    int
	batchOwn    []handle

	// Linearizable reads wait for a ReadIndex: reads not yet asked about,
	// and the batches that wait for their index or the apply.
	reads       []pendingRead
	readBuf     []byte
	readBatches []*readBatch
	freeBatches []*readBatch
	readSeq     uint64

	wal *wal

	// scratch for replies
	kvs    [][]byte
	txnRes []opRes
	// txnScratch holds a Txn branch's results while it runs, one per
	// nesting depth.
	txnScratch [][]opRes
	scratch    []byte // a Txn's fields that are replicated

	// The batches this node proposed, by sequence: the leader names them
	// in its appends to this node instead of sending them back (#626).
	holding bool // some peer's messages wait behind a held append
	// carryUntil: until when the leader carries the batches it would name
	// for a follower that lacked one.
	carryUntil map[uint64]time.Time
	fwd        []fwdBatch
	fwdPeers   map[uint64][]fwdBatch
	fwdSeq     uint32
	// proposed are the batches proposed and not yet timed out, in order.
	proposed []proposal

	// confState is the membership as of the applied index.
	confState *pb.ConfState
	// Checkpoints: the one being written, if any; the generation and log
	// position of the newest; and their directory.
	ckpt     *ckptRun
	ckptGen  int
	ckptFrom int64
	// ckptCut: the log before it is the newest checkpoint's, which holds
	// what the store names there.
	ckptCut  int64
	ckptName string
	ckptDir  string
	// recvSnap is a snapshot received and not installed yet.
	recvSnap string
	// shell does what takes other goroutines: waking the loop, and
	// sending a snapshot to a follower.
	shell shell

	// goLimit is the memory limit last given Go's runtime.
	goLimit int64
	// bufs are buffers that ended streams gave back (takeBuf).
	bufs [][]byte
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
	// A nested Txn: whether its compares held, and where its operations'
	// results lie.
	ok         bool
	sub0, sub1 int
}

// Batch layout: [4 origin][4 sequence][4 ops][4 magic], then each op [1 kind]
// [4 length] and the request's protobuf as the client sent it.
const (
	batchHdr = 16
	opHdr    = 5
	magic    = 0x77697265
	ringSize = 64 << 20
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
		if err := checkRange(&r, msg); err != nil {
			n.reject(c, s, err)
			return nil
		}
		if r.serializable {
			n.serveRange(c, s, &r)
			return nil
		}
		n.queueRead(mkHandle(c, s.id), s.kind, msg)
	case kTxn:
		if err := checkRequest(kTxn, msg); err != nil {
			n.reject(c, s, err)
			return nil
		}
		n.addTxn(mkHandle(c, s.id), s, msg)
	case kPut, kDel, kCompact, kLeaseGrant, kLeaseRevoke:
		if err := checkRequest(s.kind, msg); err != nil {
			n.reject(c, s, err)
			return nil
		}
		n.addOp(mkHandle(c, s.id), s.kind, msg)
	case kLeaseKeepAlive:
		n.leaseKeepAlive(c, s, msg)
	case kLeaseTTL:
		return n.leaseTimeToLive(c, s, msg)
	case kLeaseLeases:
		n.leaseLeases(c, s)
	case kAlarm:
		// No alarm is ever raised: the answer lists none, whatever it
		// asks.
		n.replyHeader(c, s)
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

// reject answers a request that its check rejected.
func (n *node) reject(c *client, s *stream, err error) {
	if re, ok := errors.AsType[*requestError](err); ok {
		n.replyError(c, s.id, re.code, re.msg)
		return
	}
	n.replyError(c, s.id, codeInvalidArgument, err.Error())
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
	w.begin(c, s, size)
}

// endReply ends the message and the stream with grpc-status 0.
func (n *node) endReply(c *client, s *stream, w *msgW) {
	n.stats.replies++
	n.metrics.count(s.kind, 0)
	w.end()
	n.endStreamOK(c, s)
}

// replyError ends stream sid with a gRPC error, headers and trailers in
// one block.
func (n *node) replyError(c *client, sid uint32, code int, msg string) {
	if s := c.get(sid); s != nil {
		n.metrics.count(s.kind, code)
	}
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
	s := c.get(sid)
	if s != nil && s.holds {
		c.hold(s, nil, blk)
	} else {
		b := c.frame(9 + len(blk))
		putFrameHeader(b, len(blk), frHeaders, flEndHeaders|flEndStream, sid)
		copy(b[9:], blk)
	}
	if s != nil {
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
	codeInvalidArgument = 3
	codeOutOfRange      = 11
	codeInternal        = 13
	codeUnavailable     = 14
	msgCompacted        = "etcdserver: mvcc: required revision has been compacted"
	msgFuture           = "etcdserver: mvcc: required revision is a future revision"
)

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

// openStream sends a streaming call's response headers, once.
func (n *node) openStream(c *client, s *stream) {
	if s.streaming {
		return
	}
	s.streaming = true
	b := c.frame(9 + len(respHeaders))
	putFrameHeader(b, len(respHeaders), frHeaders, flEndHeaders, s.id)
	copy(b[9:], respHeaders)
	n.markDirty(&c.sock)
}

// endStreaming ends a streaming call whose client ended its side, with
// grpc-status 0.
func (n *node) endStreaming(c *client, s *stream) { n.endStreamOK(c, s) }

// endStreamOK ends stream s with grpc-status 0, after anything it holds.
func (n *node) endStreamOK(c *client, s *stream) {
	if s.holds {
		c.hold(s, nil, respTrailers)
	} else {
		b := c.frame(9 + len(respTrailers))
		putFrameHeader(b, len(respTrailers), frHeaders, flEndHeaders|flEndStream, s.id)
		copy(b[9:], respTrailers)
	}
	c.endStream(s)
	n.markDirty(&c.sock)
}
