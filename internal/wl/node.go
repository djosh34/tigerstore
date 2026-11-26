package wl

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/authtoken"
	etcdmetrics "github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/planted"
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
	// turnCkpt is where the turn's checkpoint step spent its time, and
	// turnReady where ready() spent it.
	turnCkpt  ckptTurn
	turnReady readyTurn
	// waiting are the clients that wait for the client buffers to read,
	// or for room in them for a request's part, in the order they came
	// (loop.serveWaiting), and waitPass counts serveWaiting's passes.
	// inTurn is the one that may take the buffers up to turnLimit since
	// turnAt, past the read half by the turnRoom it took of the reserve,
	// and by what they took with no check of their room since then
	// (looseGrown past turnLoose; partwait.go).
	waiting               []*client
	waitPass              uint64
	inTurn                *client
	turnRoom, turnLimit   int64
	looseGrown, turnLoose int64
	turnAt                time.Time
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
	// heard are the peers of the cluster that connected to this node
	// though its list lacks them, at the addresses their hellos name: a
	// member added in entries this node has not applied yet. It dials them
	// until its list has them or removed them.
	heard map[uint64]string
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
	// leaseSeen is when this node last saw each lease renewed: its grant
	// or a keepalive applied, or a new leader, which starts every TTL
	// over. A node that does not lead answers a lease's TTL from it.
	leaseSeen map[int64]time.Time
	// plantedRevoked is the TTL of each lease revoked, under planted B36
	// alone (lease.go).
	plantedRevoked map[int64]int64

	stats   counters
	metrics *nodeMetrics
	// putTimings is nil unless large-request diagnostics are enabled.
	putTimings *putTimings

	socks    []any // *client, *peerIn or *peerOut, by index
	gens     []uint16
	dirty    []*sock
	peersOut map[uint64]*peerOut
	// beatsOut are the peers' connections of heartbeats and votes, which
	// an append crossing a slow link does not hold back (beatMark).
	beatsOut map[uint64]*peerOut
	// beatAsked is, by peer, whether its last heartbeat came on its
	// connection of heartbeats: the answer goes back on it (onBeats).
	beatAsked map[uint64]bool

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
	// batchAlarm is the error the alarms up gave the open batch's writes
	// as they came, nil if they allow them: a batch never holds writes of
	// both (alarmApart). batchFlags are its ops' alarmFlags, by batchOwn,
	// taken while an alarm is up, else 0.
	batchAlarm error
	batchFlags []uint8

	// Linearizable reads wait for a ReadIndex: reads not yet asked about,
	// and the batches that wait for their index or the apply.
	reads       []pendingRead
	readBuf     []byte
	readBatches []*readBatch
	freeBatches []*readBatch
	readSeq     uint64
	// Ranges that wait for values read off the loop, and the places of
	// the values fetches read (fetch.go).
	awayReads []awayRead
	fetching  map[int64]bool
	// bigs are the range replies sent in pieces (bigrange.go).
	bigs []*bigRange
	// selKeys is txnRange's scratch (replysize.go).
	selKeys []sortedKey
	// reserved is what the clients hold of the reserve; roomWait are
	// the ranges whose replies wait for room in it, served again once
	// roomFreed (reserve.go).
	reserved int64
	roomWait []awayRead
	// tokenWaits are the clients whose reading waits for the entry of a
	// call's simple token (tokenwait.go).
	tokenWaits []*client
	roomFreed  bool
	// heldReplies are the replies of applied writes that wait for room
	// in the reserve, unbuilt (reserve.go).
	heldReplies []*heldReply
	// bigWatches are the watch responses sent in pieces (bigwatch.go).
	bigWatches []*bigWatch
	// roomShort is set from when a message found the reserve spent until
	// some room comes back: the watches' catch-ups wait for it.
	roomShort bool

	wal *wal

	// scratch for replies
	kvs    [][]byte
	txnRes []opRes
	// txnScratch holds a Txn branch's results while it runs, one per
	// nesting depth.
	txnScratch [][]opRes
	scratch    []byte // a Txn's fields that are replicated

	// votes are this node's requests for votes in a campaign, sent with
	// the responses of the append that makes its new term and its vote
	// for itself durable. Raft with asynchronous storage writes would
	// send them at once: a candidate elected by its peers alone, which
	// lost its term before it was durable, could start again a term
	// lower, win the same term again from the same voters, and lead it
	// with another log (TestWLFaultSeeds seed 19159). As in etcd, whose
	// candidate sends nothing before its hard state is saved.
	votes []*pb.Message

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
	// ckptRoomAt: after a checkpoint found no room on the disk, the next
	// looks again at this time, not at every turn (ckptRetry, #769).
	// ckptShort is set, after a checkpoint that asked for ckptShortNeed
	// bytes the disk did not have, until it has them or the store is
	// smaller than ckptShortSize.
	ckptRoomAt                   time.Time
	ckptShort                    bool
	ckptShortNeed, ckptShortSize int64
	// ckptLog is the log written after the newest checkpoint before the
	// next is taken (Config.CheckpointLog).
	ckptLog  int64
	ckptName string
	// ckptKeys is the most keys a chunk of a checkpoint encodes
	// (Config.CheckpointChunkKeys), 0 for ckptChunkKeys.
	ckptKeys int
	// catchup is the applied entries the storage keeps for followers
	// that lag (Config.CatchupEntries).
	catchup uint64
	ckptDir string
	// recvSnap is a snapshot received and not installed yet.
	recvSnap string
	// shell does what takes other goroutines: waking the loop, and
	// sending a snapshot to a follower.
	shell shell

	// goLimit is the memory limit last given Go's runtime, and goLimits
	// how many times it was given one.
	goLimit  int64
	goLimits int
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
	// maxStreams is --max-concurrent-streams (Config.MaxStreams).
	maxStreams uint32
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
	plantedSkip  uint64     // planted bug B23: the entry a start's replay skips
	silent       bool       // a leader on a stalled disk sends its peers nothing (silent.go)
	extensive    bool
	quotaBytes   int64
	// moves are the MoveLeaders that wait for the leader to move.
	moves []moveReq
	// sideCalls end the contexts of the side calls under way; sideReqs
	// are the requests of those whose client has not ended its side.
	sideCalls map[handle]context.CancelFunc
	sideReqs  map[handle][]byte
	// mustSync are the indexes of the entries past the durable commit
	// whose commit is synced, and recoveredLast the log's last index at
	// the start, through which every entry's is (mustSyncCommit).
	mustSync      []uint64
	recoveredLast uint64
	// ringPledged is the log bytes of the writes taken since Raft last
	// handed the log an append, which the ring's room must hold too
	// (ringTaken).
	ringPledged int64
	// batchPledge is the part of ringPledged that the open batch's writes
	// took, and pledgeEpoch counts the appends Raft handed the log, at
	// each of which ringPledged starts again (storageAppend).
	batchPledge int64
	pledgeEpoch uint64
	// nudgedAt is the apply the empty entry nudgeRing proposed waits for,
	// one past the applied index it was proposed at, and nudgedWhen when.
	nudgedAt   uint64
	nudgedWhen time.Time
	// appendPledged is the log bytes of the appends this follower took
	// that Raft has not handed to the log yet, and appendLast the last
	// index they hold (followerDrops).
	appendPledged  int64
	appendLast     uint64
	appendLastTerm uint64
	// oldPeerWire makes the node act as a release before the refusal frame
	// on the wire (refusal.go): a test's mixed cluster.
	oldPeerWire bool
	// holdCkpt is Config.holdCkpt.
	holdCkpt *atomic.Bool
}

type fwdBatch struct {
	seq     uint64
	data    []byte
	handles []handle
	// pledged is the ring room the batch's writes were taken for
	// (ringRefuses), in pledge epoch pledgeEpoch: released if the batch
	// never comes to the log (releasePledge).
	pledged     int64
	pledgeEpoch uint64
	// flags are the batch's ops' alarmFlags, by handles (batchFlags):
	// once the leader drops the batch, refused answers each write with
	// the error the alarms still up give it.
	flags []uint8
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
	// started is the tick it was first asked: past requestTimeout its
	// reads are told they timed out (retryReads).
	started int64
	reads   []pendingRead
	buf     []byte
	// reask is set while its ask is held, as Raft's answer could be short
	// (askReadIndex): it is asked once it no longer could (askHeld).
	reask bool
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
	if c.holdsTooMuch() && s.kind != kWatch {
		// etcd refuses no watch message so; a watch's events wait as a
		// slow watcher's do.
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
		if r.serializable || planted.On("B01a") {
			// Planted B01a: a linearizable read is served without
			// ReadIndex, from whatever this node applied.
			n.serveRange(c, s, &r, msg)
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
		if n.ringRefuses(c, s, msg) {
			return nil
		}
		n.alarmApart(kTxn, msg)
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
		if f := testAdmit.Load(); f != nil {
			(*f)(n, s.kind, msg)
		}
		if n.ringRefuses(c, s, msg) {
			return nil
		}
		n.alarmApart(s.kind, msg)
		n.addCaller(c, s)
		n.addOp(mkHandle(c, s.id), s.kind, msg)
	case kLeaseKeepAlive:
		n.leaseKeepAlive(c, s, msg)
	case kLeaseTTL:
		// Linearizable, as etcd's: a node that lags does not answer from
		// leases revoked or renewed since.
		n.queueRead(mkHandle(c, s.id), s.kind, msg)
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
func (n *node) hdrSizeAt(rev int64) int { return n.hdrSizeTerm(rev, n.term) }

func (n *node) writeHdrAt(w *msgW, rev int64) { n.writeHdrTerm(w, rev, n.term) }

// hdrSizeTerm is the size of a ResponseHeader that names revision rev and
// term term.
func (n *node) hdrSizeTerm(rev int64, term uint64) int {
	return sizeVarintField(1, n.clusterID) + sizeVarintField(2, n.id) + sizeVarintField(3, uint64(rev)) + sizeVarintField(4, term)
}

func (n *node) writeHdrTerm(w *msgW, rev int64, term uint64) {
	w.lenField(1, n.hdrSizeTerm(rev, term))
	writeVarintNZ(w, 1, n.clusterID)
	writeVarintNZ(w, 2, n.id)
	writeVarintNZ(w, 3, uint64(rev))
	writeVarintNZ(w, 4, term)
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

// replyError ends stream sid with a gRPC error. Before streaming, the
// response headers and trailers share one block; after it, only trailers.
func (n *node) replyError(c *client, sid uint32, code int, msg string) {
	if s := c.get(sid); s != nil && s.watch {
		if s.big {
			// A large response is under way in it: trailers held behind
			// it would go out inside it once the stream ends. It is reset,
			// as a lost connection would end it (#689's re-check 2).
			n.abortBigWatch(c, s)
			n.markDirty(&c.sock)
			return
		}
		// Its watches end with it.
		n.closeWatchStream(c, s)
	}
	if s := c.get(sid); s != nil {
		n.callEnded(s, code)
	}
	s := c.get(sid)
	var blk []byte
	if s == nil || !s.streaming {
		blk = append(blk, respHeaders...)
	}
	// Once response headers went out, only gRPC trailers may follow:
	// a second :status in the trailing block is an HTTP/2 error.
	blk = appendLiteral(blk, "grpc-status", itoa(code))
	blk = appendLiteral(blk, "grpc-message", encodeGrpcMessage(msg))
	if s != nil && (s.holds || s.big) {
		c.hold(s, nil, blk)
	} else {
		b := c.frame(9 + len(blk))
		putFrameHeader(b, len(blk), frHeaders, flEndHeaders|flEndStream, sid)
		copy(b[9:], blk)
	}
	if s != nil {
		n.tracePutReply(s, code)
		c.endStream(s)
	}
	n.markDirty(&c.sock)
}

// maxErrMsg bounds an error's grpc-message, which may hold what a client
// sent (a method's path): its header block stays well within a frame
// and a record.
const maxErrMsg = 1024

// encodeGrpcMessage is msg as grpc-go's encodeGrpcMessage sends it, its bytes
// outside printable ASCII and '%' percent-encoded, cut to maxErrMsg.
func encodeGrpcMessage(msg string) string {
	const hex = "0123456789ABCDEF"
	var b []byte
	for i := 0; i < len(msg) && len(b) < maxErrMsg-3; i++ {
		c := msg[i]
		if c >= ' ' && c <= '~' && c != '%' {
			b = append(b, c)
			continue
		}
		b = append(b, '%', hex[c>>4], hex[c&15])
	}
	return string(b)
}

// appendLiteral appends a header field to an HPACK block as a literal
// without indexing and with a literal name (RFC 7541, 6.2.2).
func appendLiteral(b []byte, name, value string) []byte {
	b = append(b, 0x00)
	b = appendHpackInt(b, uint64(len(name)))
	b = append(b, name...)
	b = appendHpackInt(b, uint64(len(value)))
	return append(b, value...)
}

// appendHpackInt appends v as an HPACK integer of a 7-bit prefix, the
// form of a string's length with no Huffman code (RFC 7541, 5.1).
func appendHpackInt(b []byte, v uint64) []byte {
	if v < 127 {
		return append(b, byte(v))
	}
	b = append(b, 127)
	for v -= 127; v >= 128; v >>= 7 {
		b = append(b, byte(v&127|128))
	}
	return append(b, byte(v))
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
	n.tracePutReply(s, 0)
	c.endStream(s)
	n.markDirty(&c.sock)
}
