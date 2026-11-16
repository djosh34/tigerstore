package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/authtoken"
	etcdmetrics "github.com/djosh34/tigerstore/internal/metrics"
)

// node is one replica: everything below runs on the loop's one thread.
type node struct {
	id, clusterID uint64
	// sizes are the node's buffers outside the store, from its budget;
	// connOut is a client connection's share of sizes.clientOut for the
	// clients connected at the last tick.
	sizes   sizes
	connOut int64
	// connIn is a client connection's share of requests sent in part
	// (sizes.connIn), and clients the client connections the node holds;
	// cpool is what they hold of the client buffers (sizes.clientBufs) to
	// read, and cout what waits to be sent to them; active is how many
	// hold buffers to read.
	connIn          int64
	clients, active int
	cpool, cout     int64
	// turnCkpt is where the turn's checkpoint step spent its time.
	turnCkpt ckptTurn
	// waiting are the clients that wait for the client buffers to read,
	// in the order they came (loop.serveWaiting).
	waiting []*client
	// sys is the node's handle on the system calls of sys_os.go.
	sys sysEnv
	// members is the member list as of the applied index, and removed the
	// IDs it no longer holds. self is this node's name and client URLs,
	// which it publishes at publishAt; seed are the members it was told
	// of, which it dials until it has a list.
	members   []member
	removed   map[uint64]bool
	self      member
	publishAt time.Time
	seed      []member
	// memberReqs are the membership changes clients wait on, by the
	// sequence their ConfChange carries.
	memberReqs map[uint64]memberReq
	memberSeq  uint64
	// confHold is what the leader holds back of membership changes while
	// its Raft would drop them (confBlocked): followers' proposals, and
	// its own clients' requests.
	confHold confHold
	// rm removes files off the loop's thread.
	rm *remover
	// stopErr stops the loop at its next turn: the node was removed.
	stopErr error
	// snapWaiters wait for the checkpoint a client's Snapshot asked for;
	// clientSnaps are being sent.
	snapWaiters []handle
	clientSnaps []*clientSnap

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
	fwdSeq     uint64
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
	ckptCut int64
	// ckptNotBefore: the next checkpoint waits, but for a follower, until
	// the loop's time encoding the last is at most 1/ckptLoopShare of the
	// time since it started.
	ckptNotBefore time.Time
	// ckptLog is the log written after the newest checkpoint before the
	// next is taken (Config.CheckpointLog).
	ckptLog  int64
	ckptName string
	// catchup is the applied entries the storage keeps for followers
	// that lag (Config.CatchupEntries).
	catchup uint64
	ckptDir string
	// recvSnap is a snapshot received and not installed yet.
	recvSnap string
	// shell does what takes other goroutines: waking the loop, and
	// sending a snapshot to a follower.
	shell shell

	// goLimit is the memory limit last given Go's runtime.
	goLimit int64
	// bufs are buffers that ended streams gave back (takeBuf).
	bufs [][]byte
	// alarms are the raised alarms, by type and member; noSpace and
	// corrupt say whether one of those types is raised.
	alarms           []alarm
	noSpace, corrupt bool
	// quota is --quota-backend-bytes, -1 for none; noSpaceAsked is the
	// applied index when the node last proposed its NOSPACE alarm.
	quota        int64
	noSpaceAsked uint64
	compactor    compactor
	// downgrade is the minor version a downgrade is on to, 0 for none.
	downgrade int64
	// progressEvery is --experimental-watch-progress-notify-interval,
	// and progressAt when the next notifications go.
	progressEvery time.Duration
	progressAt    time.Time
	// sideSecret marks the side calls' own client, which names the
	// client each call is for (side.go).
	sideSecret string
	// leaseIDs is the suffix of the lease IDs the node gives grants that
	// ask for none (newLeaseID).
	leaseIDs uint64
	// auth is the auth store (auth.go); tokens the simple tokens, logins
	// the Authenticates under way here, jwt what signs and checks JWTs.
	auth       *authStore
	tokens     map[string]*simpleToken
	logins     map[handle]authLogin
	jwt        *authtoken.JWT
	tokenTTL   time.Duration
	bcryptCost int
	// The version gate (format.go): the cluster's format, the epoch its
	// members confirm, and this start's run.
	format      uint8
	formatEpoch uint64
	run         uint64
	// gs is what the loop counts for the metrics; extensive is --metrics
	// extensive. quotaBytes is --quota-backend-bytes.
	gs           loopStats
	membershipAt uint64 // the applied index of the last v2 store snapshot etcd would take
	prom         *etcdmetrics.Set
	appliedOps   []appliedOp
	turnReqs     [kinds]int // requests this turn, by kind, for the slow-turn log
	txnWrote     bool       // the Txn applied last put or deleted
	plantedSubs  int64      // planted bug B09a: the keys the request changed so far
	extensive    bool
	quotaBytes   int64
	// moves are the MoveLeaders that wait for the leader to move.
	moves []moveReq
	// sideCalls end the contexts of the side calls under way; sideReqs
	// are the requests of those whose client has not ended its side.
	sideCalls map[handle]context.CancelFunc
	sideReqs  map[handle][]byte
}

type fwdBatch struct {
	seq     uint64
	data    []byte
	handles []handle
}

const fwdSlots = 1 << 14

// isBatch reports whether an entry's data is a batch.
func isBatch(data []byte) bool {
	return len(data) >= batchHdr && string(data[17:batchHdr]) == magic
}

// batchOrigin returns the node that proposed a batch and its sequence
// there, if data is one.
func batchOrigin(data []byte) (uint64, uint64, bool) {
	if !isBatch(data) {
		return 0, 0, false
	}
	return uint64(binary.LittleEndian.Uint32(data)), binary.LittleEndian.Uint64(data[4:]), true
}

type pendingRead struct {
	h        handle
	kind     uint8
	off, len int
	// done, if set, is called instead of answering a client: a read of
	// the node's own, such as a health check's.
	done func()
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
	rev     int64 // the revision its header names, as etcd's; 0 for a Txn
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

// Batch layout: [4 origin][8 sequence][4 ops][1 format][3 magic], then
// each op [1 kind][4 length] and the request's protobuf as the client
// sent it. A batch's sequence is its origin's run in the high 32 bits and
// a count of the run's batches in the low: an origin never names two
// batches alike, across its restarts too, so a named append never finds
// a batch of another run. The format is the cluster's (format.go) when
// the batch was opened.
const (
	batchHdr = 20
	opHdr    = 5
	magic    = "wl\x06"
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
	if h == noHandle {
		return nil, nil
	}
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
	n.turnReqs[s.kind]++
	if c.holdsTooMuch() {
		n.reject(c, s, errTooManyRequests)
		return nil
	}
	learner := n.isLearner()
	if learner && !learnerServes(s.kind) {
		n.reject(c, s, errLearner)
		return nil
	}
	if sideKind(s.kind) {
		n.sideRequest(c, s, msg)
		return nil
	}
	if authKind(s.kind) {
		return n.authRequest(c, s, msg)
	}
	if n.auth.enabled && s.kind != kWatch && !n.authAllows(c, s, msg) {
		return nil
	}
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
		if s.kind == kRangeStream {
			if err := checkRangeStream(&r); err != nil {
				n.reject(c, s, err)
				return nil
			}
		}
		if learner && !r.serializable {
			n.reject(c, s, errLearner)
			return nil
		}
		if r.serializable {
			n.serveRange(c, s, &r)
			return nil
		}
		n.queueRead(mkHandle(c, s.id), s.kind, msg)
	case kTxn:
		if err := checkRequest(kTxn, msg, c.identityOf(s)); err != nil {
			n.reject(c, s, err)
			return nil
		}
		if !n.quotaLeft(kTxn, msg) {
			n.refuseNoSpace(c, s)
			return nil
		}
		n.addCaller(c, s)
		n.addTxn(mkHandle(c, s.id), s, msg)
	case kPut, kDel, kCompact, kLeaseGrant, kLeaseRevoke:
		if s.kind == kLeaseGrant {
			msg = n.withLeaseID(msg)
		}
		if err := checkRequest(s.kind, msg, c.identityOf(s)); err != nil {
			n.reject(c, s, err)
			return nil
		}
		if !n.quotaLeft(s.kind, msg) {
			n.refuseNoSpace(c, s)
			return nil
		}
		n.addCaller(c, s)
		n.addOp(mkHandle(c, s.id), s.kind, msg)
	case kLeaseKeepAlive:
		n.leaseKeepAlive(c, s, msg)
	case kLeaseTTL:
		return n.leaseTimeToLive(c, s, msg)
	case kLeaseLeases:
		n.leaseLeases(c, s)
	case kAlarm:
		return n.alarmRequest(c, s, msg)
	case kWatch:
		return n.watchRequest(c, s, msg)
	case kStatus:
		n.replyStatus(c, s)
	case kMemberList:
		// A linearizable list waits for the ReadIndex, as a range does.
		if num, _, v, _, _, err := field(msg); err == nil && num == 1 && v != 0 {
			n.queueRead(mkHandle(c, s.id), s.kind, nil)
			return nil
		}
		n.replyMembers(c, s)
	case kMemberAdd, kMemberRemove, kMemberUpdate, kMemberPromote:
		return n.memberRequest(c, s, msg)
	case kSnapshot:
		n.snapshotRequest(c, s)
	case kMoveLeader:
		return n.moveLeader(c, s, msg)
	case kDowngrade:
		return n.downgradeRequest(c, s, msg)
	case kHash, kHashKV:
		return n.hashRequest(c, s, msg)
	case kDefragment:
		// Nothing to defragment: etcd's answer, an empty response.
		n.replyEmpty(c, s)
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

func (n *node) hdrSize() int { return n.hdrSizeAt(n.st.rev) }

func (n *node) writeHdr(w *msgW) { n.writeHdrAt(w, n.st.rev) }

// replyEmpty answers with an empty message, as etcd answers a call whose
// response has no header.
func (n *node) replyEmpty(c *client, s *stream) {
	var w msgW
	n.beginReply(c, s, &w, 0)
	n.endReply(c, s, &w)
}

// hdrSizeAt is the size of a ResponseHeader that names revision rev.
func (n *node) hdrSizeAt(rev int64) int {
	return sizeVarintField(1, n.clusterID) + sizeVarintField(2, n.id) + sizeVarintField(3, uint64(rev)) + sizeVarintField(4, n.term)
}

func (n *node) writeHdrAt(w *msgW, rev int64) {
	w.lenField(1, n.hdrSizeAt(rev))
	writeVarintNZ(w, 1, n.clusterID)
	writeVarintNZ(w, 2, n.id)
	writeVarintNZ(w, 3, uint64(rev))
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
	n.callEnded(s, 0)
	w.end()
	n.endStreamOK(c, s)
}

// replyError ends stream sid with a gRPC error, headers and trailers in
// one block.
func (n *node) replyError(c *client, sid uint32, code int, msg string) {
	if s := c.get(sid); s != nil {
		n.callEnded(s, code)
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
	codeInvalidArgument    = 3
	codeFailedPrecondition = 9
	codeUnknown            = 2
	codeOutOfRange         = 11
	codeUnimplemented      = 12
	codeInternal           = 13
	codeUnavailable        = 14
	msgCompacted           = "etcdserver: mvcc: required revision has been compacted"
	msgFuture              = "etcdserver: mvcc: required revision is a future revision"
)

// ---- status

func (n *node) replyStatus(c *client, s *stream) {
	ver := []byte("3.7.2")
	size := sizeBytesField(1, n.hdrSize()) + sizeBytesField(2, len(ver)) + sizeVarintField(3, 1<<20) +
		sizeVarintField(4, n.lead) + sizeVarintField(5, n.commit) + sizeVarintField(6, n.term) + sizeVarintField(7, n.applied)
	for _, a := range n.alarms {
		size += sizeBytesField(8, len(a.String()))
	}
	size += sizeBytesField(11, len(n.storageVersion())) + sizeVarintField(12, uint64(max(n.quota, 0)))
	if info := n.downgradeInfo(); len(info) > 0 {
		size += sizeBytesField(13, len(info))
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	w.bytesField(2, ver)
	w.varintField(3, 1<<20)
	writeVarintNZ(&w, 4, n.lead)
	writeVarintNZ(&w, 5, n.commit)
	writeVarintNZ(&w, 6, n.term)
	writeVarintNZ(&w, 7, n.applied)
	for _, a := range n.alarms {
		w.bytesField(8, []byte(a.String()))
	}
	w.bytesField(11, []byte(n.storageVersion()))
	writeVarintNZ(&w, 12, uint64(max(n.quota, 0)))
	if info := n.downgradeInfo(); len(info) > 0 {
		w.bytesField(13, info)
	}
	n.endReply(c, s, &w)
}

func (n *node) replyMembers(c *client, s *stream) {
	var body []byte
	for i := range n.members {
		body = appendBytesField(body, 2, appendMember(nil, &n.members[i]))
	}
	// etcd's cluster calls name no revision in their header.
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSizeAt(0))+len(body))
	n.writeHdrAt(&w, 0)
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
