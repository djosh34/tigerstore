package wl

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/net/http2/hpack"
)

// The client side: HTTP/2 frames read in place from the opened records,
// gRPC messages handed to the node as slices of them.

const (
	kUnknown uint8 = iota
	kRange
	kRangeStream
	kPut
	kDel
	kTxn
	kCompact
	kLeaseGrant
	kLeaseRevoke
	kLeaseKeepAlive
	kLeaseTTL
	kWatch
	kStatus
	kMemberList
	kMoveLeader
	kLeaseLeases
	kAlarm
	kMemberAdd
	kMemberRemove
	kMemberUpdate
	kMemberPromote
	kSnapshot
	// kMemberAttrs is a member's name and client URLs, which it publishes
	// in a batch; no client sends it.
	kMemberAttrs
	kDefragment
	kHash
	kHashKV
	kDowngrade
	// The Auth service's methods (authapi.go), in authMethods' order.
	kAuthEnable
	kAuthDisable
	kAuthStatus
	kAuthenticate
	kUserAdd
	kUserGet
	kUserList
	kUserDelete
	kUserChangePassword
	kUserGrantRole
	kUserRevokeRole
	kRoleAdd
	kRoleGet
	kRoleList
	kRoleDelete
	kRoleGrantPermission
	kRoleRevokePermission
	// kAuthOp is an auth request in a batch: an InternalRaftRequest.
	// kCaller, while auth is on, names the user and auth revision of the
	// write that follows it in its batch.
	kAuthOp
	kCaller
	// kSide is the first of the side methods' kinds (side.go).
	kSide
	kinds = kSide + maxSideMethods // how many kinds there are
)

var paths = map[string]uint8{
	"/etcdserverpb.KV/Range":               kRange,
	"/etcdserverpb.KV/RangeStream":         kRangeStream,
	"/etcdserverpb.KV/Put":                 kPut,
	"/etcdserverpb.KV/DeleteRange":         kDel,
	"/etcdserverpb.KV/Txn":                 kTxn,
	"/etcdserverpb.KV/Compact":             kCompact,
	"/etcdserverpb.Lease/LeaseGrant":       kLeaseGrant,
	"/etcdserverpb.Lease/LeaseRevoke":      kLeaseRevoke,
	"/etcdserverpb.Lease/LeaseKeepAlive":   kLeaseKeepAlive,
	"/etcdserverpb.Lease/LeaseTimeToLive":  kLeaseTTL,
	"/etcdserverpb.Watch/Watch":            kWatch,
	"/etcdserverpb.Maintenance/Status":     kStatus,
	"/etcdserverpb.Cluster/MemberList":     kMemberList,
	"/etcdserverpb.Maintenance/MoveLeader": kMoveLeader,
	"/etcdserverpb.Lease/LeaseLeases":      kLeaseLeases,
	"/etcdserverpb.Maintenance/Alarm":      kAlarm,
	"/etcdserverpb.Cluster/MemberAdd":      kMemberAdd,
	"/etcdserverpb.Cluster/MemberRemove":   kMemberRemove,
	"/etcdserverpb.Cluster/MemberUpdate":   kMemberUpdate,
	"/etcdserverpb.Cluster/MemberPromote":  kMemberPromote,
	"/etcdserverpb.Maintenance/Snapshot":   kSnapshot,
	"/etcdserverpb.Maintenance/Defragment": kDefragment,
	"/etcdserverpb.Maintenance/Hash":       kHash,
	"/etcdserverpb.Maintenance/HashKV":     kHashKV,
	"/etcdserverpb.Maintenance/Downgrade":  kDowngrade,
}

func init() {
	for kind, m := range authMethods {
		paths[m.path] = kind
	}
}

// stream is one HTTP/2 stream of a client.
type stream struct {
	id   uint32
	kind uint8
	open bool
	gzip bool // the client's messages are gzip'd
	// snapRequested admits only one Snapshot message. snapRetries counts
	// its copies interrupted by log cuts; both start anew with the stream.
	snapRequested bool
	snapRetries   uint8
	// began is the turn of the call's request, for its time; readonly
	// says a Txn has no write, etcd's ReadonlyTxn.
	began    time.Time
	readonly bool
	ended    bool   // the client ended its side
	buf      []byte // a gRPC message across DATA frames
	counted  int64  // what of buf its client counts in partial
	watch    bool
	wids     []int32 // the watchers of a watch stream
	// nextWatch is the next watch ID a create that names none may take.
	nextWatch int64
	fail      []byte // a Txn's failure branch, kept here and not replicated
	win       int64  // the stream's send window
	recvd     int    // DATA taken since the stream's last WINDOW_UPDATE
	holds     bool   // held messages wait for window: the stream's window is in its client's flow
	// big: a watch response is written into it in pieces, and its other
	// messages wait behind it (bigwatch.go).
	big bool
	// streaming: the stream's response headers are out, and it answers
	// each request with a message until the client ends its side.
	streaming bool
	// failAt is fail's absolute position in the side ring, -1 if it is on
	// the heap.
	failAt int64
	// progressDue: a progress request of the watch stream waits for its
	// watches to catch up; progressWaits, also for one from a revision
	// still to come (watches.progressWait).
	progressDue, progressWaits bool
	// token is the call's auth token, from its token header. A side
	// call's (byCaller) is for the client of caller's certificate name.
	token    string
	caller   string
	byCaller bool
}

// streamSlots are a client's streams kept in place; more at once go to a
// map (client.over).
const streamSlots = 1 << 10

// client is one client connection.
type client struct {
	sock
	preface  bool
	slots    []stream
	over     map[uint32]*stream
	dec      *hpack.Decoder
	hdrCache map[string]hdrInfo
	hdrEnc   string // the grpc-encoding of the block being decoded
	hdrPath  string
	hdrToken string // the auth token of the block being decoded
	// hdrCaller is the side caller header of the block (side.go).
	hdrCaller string
	// hdrBad: the block being decoded is not a well-formed request
	// (RFC 9113 §8.3.1), or, as trailers (hdrTrailers), holds a
	// pseudo-header; hdrSeen are the pseudo-headers it named, and
	// hdrRegular says a regular field came.
	hdrBad, hdrTrailers, hdrRegular bool
	hdrSeen                         uint8
	inBlock                         bool
	blockSid                        uint32
	// lastSid is the highest stream the client opened: a stream above it
	// is idle, and a new one must be above it.
	lastSid  uint32
	blockEnd bool
	block    []byte
	unacked  int
	node     *node
	flow     flow
	// identity is what the name of the client's certificate adds to the
	// size of its writes as etcd counts it; cn is that name.
	identity int
	cn       string
	// paused is set while its send buffer is over its share (backedUp):
	// the node reads nothing more of it until a flush brings the buffer
	// under half the share.
	paused bool
	// tokenWait, while not 0, is the entry a call's simple token names,
	// which the node has not applied: it reads no more of the client's
	// frames until it does, or requestTimeout from tokenAt (tokenwait.go).
	tokenWait uint64
	tokenAt   time.Time
	// partial is what its streams hold of requests sent in part, and
	// streams how many are open.
	partial int64
	streams int
	// starved is set while it waits for the client buffers to read
	// (loop.input); busy, the tick it last read.
	starved bool
	busy    int64
	// roomNeed, while not 0, is what the part of stream roomSid's request
	// it stopped reading at adds to the client buffers, which wait for
	// room since roomAt; roomLate, that the part is refused, once it
	// waited requestTimeout (partwait.go). linePass and seenPass are
	// serveWaiting's marks.
	roomNeed           int64
	roomSid            uint32
	roomAt             time.Time
	roomLate           bool
	linePass, seenPass uint64
	// roomHeld is the room of the reserve it holds, and roomWaiters how
	// many of its messages wait for room (reserve.go).
	roomHeld      int64
	roomWaiters   int
	roomWake      bool        // queued in node.roomClients
	coveredRoom   int64       // output quota backed by the client pool, not input reserve
	stagePending  int64       // unpublished wire bytes, including worker quarantine
	outputs       *heldOutput // intrusive, already-funded ordered output owners
	outputSeq     uint32
	controlHeld   int64       // unchanged main watch-control debit, #912
	replyStage    *replyStage // non-yielding ordinary response serialization
	stagePromise  int64       // protected socket/finish capacity
	socketPromise int64       // stagePromise consumed by actual socket backing
	stages        int
	stageOrphan   bool
	fillOwners    int
	// heldCounted is what its held replies and its ranges waiting for
	// room keep: past its share with what waits for window, its new
	// requests are refused, as etcd refuses too many (#689's re-check, B6).
	heldCounted int64
	// bigs counts its streams that a watch response is written into in
	// pieces (stream.big), and bigW is the one large watch response that
	// may write on the connection, from its first message to its end.
	bigs int
	bigW *bigWatch
}

// The client buffers (sizes.clientOut) bound what waits unsent to each
// client connection in two parts, each within the connection's share
// (sizes.connOut); a message that would take it past the share takes
// room of the shared reserve first, or waits (reserve.go):
//   - its send buffer, while the socket does not take it: the node stops
//     reading the connection (paused), and its watches catch up later
//     from the change index, as etcd's slow watchers do;
//   - what its streams hold for want of HTTP/2 window: the node goes on
//     reading, for the client's WINDOW_UPDATEs, but refuses its new
//     requests as etcd refuses too many (errTooManyRequests), and its
//     watches catch up later.

// waiting is what waits in the client's send buffer.
func (c *client) waiting() int64 { return int64(len(c.out) - c.ow) }

// backedUp reports whether the client's send buffer is over its share.
func (c *client) backedUp() bool { return c.waiting() > c.node.connOut }

// holdsTooMuch reports whether the client's streams hold more than its
// share for want of window.
func (c *client) holdsTooMuch() bool { return c.flow.bytes+c.heldCounted > c.node.connOut }

// slow reports whether the client's watches send nothing now.
func (c *client) slow() bool { return c.paused || c.backedUp() || c.holdsTooMuch() }

// errTooManyRequests is etcd's answer to a client it cannot take more
// requests of.
var errTooManyRequests = requestErr(codeResourceExhausted, "etcdserver: too many requests")

// newClient is a client connection of node n, whose buffers count in n's
// client pool; it takes none until it reads (takeBufs), but what its TLS
// handshake read past its end.
func newClient(h *handoff, n *node) *client {
	c := &client{
		fd: h.fd, rd: h.rd, wr: h.wr, plain: h.plain, rec: -1, stats: &n.stats, pool: &n.cpool, outPool: &n.cout, loose: &n.looseGrown, outBase: clientOutBuf, creditWake: &n.fillCreditWake,
		hdrCache: map[string]hdrInfo{}, flow: newFlow(), identity: identityBytes(h.cn), cn: h.cn, node: n,
	}
	c.outHold = &c.stages
	if len(h.left) > 0 {
		// What its handshake read past its end is taken with no check
		// of the buffers' room.
		n.looseGrown += c.bufNeed()
		c.takeBufs()
		if len(h.left) > len(c.in) {
			c.growLoose(len(h.left) - len(c.in))
			c.in = make([]byte, len(h.left))
		}
		c.iw = copy(c.in, h.left)
	}
	c.dec = hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		c.checkField(f)
		switch f.Name {
		case ":path":
			c.hdrPath = f.Value
		case "grpc-encoding":
			c.hdrEnc = f.Value
		case "token", "authorization":
			c.hdrToken = f.Value
		case sideCallerHeader:
			c.hdrCaller = f.Value
		}
	})
	return c
}

// The pseudo-headers of a request (hdrSeen).
const (
	seenMethod = 1 << iota
	seenScheme
	seenPath
	seenAuthority
	seenRequest = seenMethod | seenScheme | seenPath
)

// checkField marks the block being decoded bad (hdrBad) at a field a
// request may not hold, as grpc-go's server refuses it: a pseudo-header
// that is unknown, of a response, twice, after a regular field or in
// trailers; an empty :path; a name in upper case; a field of the
// connection's; te but trailers.
func (c *client) checkField(f hpack.HeaderField) {
	if name, ok := strings.CutPrefix(f.Name, ":"); ok {
		var bit uint8
		switch name {
		case "method":
			bit = seenMethod
		case "scheme":
			bit = seenScheme
		case "path":
			bit = seenPath
			if f.Value == "" {
				c.hdrBad = true
			}
		case "authority":
			bit = seenAuthority
		}
		if bit == 0 || c.hdrTrailers || c.hdrRegular || c.hdrSeen&bit != 0 {
			c.hdrBad = true
		}
		c.hdrSeen |= bit
		return
	}
	c.hdrRegular = true
	for i := range len(f.Name) {
		if 'A' <= f.Name[i] && f.Name[i] <= 'Z' {
			c.hdrBad = true
		}
	}
	switch f.Name {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		c.hdrBad = true
	case "te":
		if f.Value != "trailers" {
			c.hdrBad = true
		}
	}
}

// decodeBlock decodes a whole header block, as trailers or as a request,
// and reports whether it is well formed.
func (c *client) decodeBlock(block []byte, trailers bool) (bool, error) {
	c.hdrBad, c.hdrTrailers, c.hdrRegular, c.hdrSeen = false, trailers, false, 0
	if _, err := c.dec.Write(block); err != nil {
		return false, err
	}
	if err := c.dec.Close(); err != nil {
		return false, err
	}
	return !c.hdrBad && (trailers || c.hdrSeen&seenRequest == seenRequest), nil
}

// A client caches what at most hdrCacheMax header blocks of up to
// hdrCacheBlock bytes said, which an idle client keeps (idleConn).
const (
	hdrCacheMax   = 64
	hdrCacheBlock = 128
)

// maxHeaderBlock is the most a header block sent in CONTINUATION frames
// may take; past it the connection breaks, as Go's HTTP/2 server breaks
// it past its header list limit.
const maxHeaderBlock = 64 << 10

var errHeaderBlock = errors.New("HTTP/2 header block past its limit")

// addBlock adds p to the header block in CONTINUATION frames, starting it
// if first, counted in the client buffers.
func (c *client) addBlock(p []byte, first bool) error {
	if first {
		c.block = c.block[:0]
	}
	if len(c.block)+len(p) > maxHeaderBlock {
		return errHeaderBlock
	}
	before, spare := cap(c.block), c.headerSlack()
	if c.node.cursorFetch && cap(c.block) < maxHeaderBlock {
		b := make([]byte, len(c.block), maxHeaderBlock)
		copy(b, c.block)
		c.block = b
	}
	c.block = append(c.block, p...)
	c.growLoose(cap(c.block) - before)
	if c.in != nil {
		c.node.headerSpare += c.headerSlack() - spare
	}
	return nil
}

func (c *client) headerSlack() int64 {
	if cap(c.block) < maxHeaderBlock {
		return maxHeaderBlock
	}
	return 0
}

// slotBytes are a client's stream slots, taken with its other buffers.
const slotBytes = streamSlots * int(unsafe.Sizeof(stream{}))

// bufNeed is what a client takes from the client buffers to read: its
// read and opened buffers, and its stream slots unless it holds them.
func (c *client) bufNeed() int64 {
	n := int64(clientInBuf + clientPtBuf)
	if c.slots == nil {
		n += int64(slotBytes)
	}
	return n
}

// takeBufs takes the client's read and opened buffers and its stream
// slots.
func (c *client) takeBufs() {
	c.grow(int(c.bufNeed()))
	c.node.active++
	c.node.headerSpare += c.headerSlack()
	c.in, c.pt = make([]byte, clientInBuf), make([]byte, clientPtBuf)
	if c.slots == nil {
		c.slots = make([]stream, streamSlots)
	}
}

// idle reports whether the client holds nothing it read, nothing to send
// and no request in part, so that its buffers may go back.
func (c *client) idle() bool {
	return c.stages == 0 && !c.hasPrivateFill() && c.ir == c.iw && c.pr == c.pw && len(c.out) == c.ow && c.rec < 0 &&
		len(c.flow.held) == 0 && c.partial == 0 && !c.paused && c.tokenWait == 0
}

// giveBufs gives the buffers of an idle client back, and its stream slots
// when no stream is open.
func (c *client) giveBufs() {
	if c.in != nil {
		c.node.active--
		c.node.headerSpare -= c.headerSlack()
	}
	c.grow(-cap(c.in) - cap(c.pt))
	c.shrinkControlCapacity(0)
	c.growOut(-cap(c.out))
	c.in, c.pt, c.out = nil, nil, nil
	c.ir, c.iw, c.pr, c.pw, c.ow = 0, 0, 0, 0, 0
	if c.streams == 0 && c.over == nil {
		c.grow(-slotBytes)
		c.slots = nil
	}
}

// dropAll takes what a closed client held out of the client pool.
func (c *client) dropAll() {
	c.node.dropReplyStage(c)
	if c.in != nil {
		c.node.active--
		c.node.headerSpare -= c.headerSlack()
	}
	c.node.dropClientWaits(c)
	if (c.stages > 0 || c.fillOwners > 0) && !c.stageOrphan {
		c.stageOrphan = true
		c.node.stageOrphans++
	}
	c.dropStageOutput()
	c.node.controlHeld -= c.controlHeld
	c.controlHeld = 0
	c.node.giveRoom(c, true)
	c.grow(-cap(c.in) - cap(c.pt) - int(c.partial) - cap(c.block))
	c.shrinkControlCapacity(0)
	c.growOut(-cap(c.out) - int(c.flow.bytes))
	c.block = nil
	if c.slots != nil {
		c.grow(-slotBytes)
	}
	c.in, c.pt, c.out, c.slots = nil, nil, nil, nil
	c.partial, c.flow.bytes = 0, 0
}

func (c *client) get(id uint32) *stream {
	if c.slots == nil {
		return nil
	}
	s := &c.slots[id>>1&(streamSlots-1)]
	if s.open && s.id == id {
		return s
	}
	if c.over != nil {
		return c.over[id]
	}
	return nil
}

func (c *client) newStream(id uint32, kind uint8) *stream {
	s := &c.slots[id>>1&(streamSlots-1)]
	if s.open {
		if c.over == nil {
			c.over = map[uint32]*stream{}
		}
		s = &stream{}
		c.over[id] = s
	}
	*s = stream{id: id, kind: kind, open: true, buf: s.buf[:0], wids: s.wids[:0], fail: nil, win: c.flow.initial}
	c.streams++
	return s
}

func (c *client) endStream(s *stream) {
	if !s.open {
		return
	}
	if c.node.putTimings != nil {
		c.node.tracePutClosed(s)
	}
	c.partial -= s.counted
	c.grow(-int(s.counted))
	c.streams--
	s.counted = 0
	s.fail = nil // the side ring may take its bytes again
	if s.big {
		s.big = false
		c.bigs--
	}
	c.node.putBuf(s.buf)
	s.buf = nil
	if c.over != nil {
		if o, ok := c.over[s.id]; ok && o == s {
			delete(c.over, s.id)
			if len(c.over) == 0 {
				c.over = nil
			}
		}
	}
	s.open = false
}

var clientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

// defaultMaxStreams is the most streams a client connection may have open
// at once: etcd's default, math.MaxUint32, is more than a connection can
// open in its life, and a lower bound refuses a client's stream before its
// buffers run out instead.
const defaultMaxStreams = 1 << 20

// codeRefusedStream is HTTP/2's REFUSED_STREAM: the server did no work of
// the stream, and the client may try it again.
const codeRefusedStream = 0x7

// windowAck is how much DATA the node takes, on the connection and on
// each stream, before it gives that window back: half of the 1 GiB each
// starts with (client.start).
var windowAck = 1 << 29

// serverStart is the server's SETTINGS (its limit on streams, large stream
// windows) and a large connection window.
func (c *client) start() {
	var p [12]byte
	binary.BigEndian.PutUint16(p[0:], 0x4) // INITIAL_WINDOW_SIZE
	binary.BigEndian.PutUint32(p[2:], 1<<30)
	binary.BigEndian.PutUint16(p[6:], 0x3) // MAX_CONCURRENT_STREAMS
	binary.BigEndian.PutUint32(p[8:], c.node.maxStreams)
	c.writeFrame(frSettings, 0, p[:])
	var w [4]byte
	binary.BigEndian.PutUint32(w[:], 1<<30)
	c.writeFrame(frWindowUpdate, 0, w[:])
}

// h2Error is an HTTP/2 connection error with its code, which the node
// sends its client in a GOAWAY before it closes the connection.
type h2Error uint32

func (e h2Error) Error() string { return fmt.Sprintf("HTTP/2 connection error %#x", uint32(e)) }

// HTTP/2's error codes the node sends.
const (
	errProtocol    h2Error = 0x1
	errFlowControl h2Error = 0x3
	errFrameSize   h2Error = 0x6
)

// goAway sends the client a GOAWAY with the error's code and the last
// stream it opened, and what it can of it before the node closes the
// connection.
func (c *client) goAway(e h2Error) {
	if !c.preface || c.broken {
		return
	}
	var p [8]byte
	binary.BigEndian.PutUint32(p[0:], c.lastSid)
	binary.BigEndian.PutUint32(p[4:], uint32(e))
	c.writeFrame(frGoAway, 0, p[:])
	_, _ = c.flush()
}

// frames reads the frames in pt and hands complete gRPC messages to n.
func (c *client) frames(n *node) error {
	if !c.preface {
		if c.pw-c.pr < len(clientPreface) {
			return nil
		}
		if !bytes.Equal(c.pt[c.pr:c.pr+len(clientPreface)], clientPreface) {
			return errProtocol
		}
		c.pr += len(clientPreface)
		c.preface = true
		c.start()
		n.markDirty(&c.sock)
	}
	for {
		if c.tokenWait != 0 || c.roomNeed != 0 {
			// A call's token waits for its entry, or a request's part
			// for room: the rest waits behind it, in pt and the socket
			// (tokenwait.go, partwait.go).
			return nil
		}
		if c.backedUp() {
			// Its replies wait: the rest of its requests waits too, in
			// pt and the socket, until a flush resumes it (loop.flush).
			c.paused = true
			return nil
		}
		avail := c.pw - c.pr
		if avail < 9 {
			return nil
		}
		h := c.pt[c.pr : c.pr+9]
		l := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
		if l > maxFrame {
			// Past SETTINGS_MAX_FRAME_SIZE, which the node leaves at
			// its initial value.
			return errFrameSize
		}
		if avail < 9+l {
			return nil
		}
		typ, flags := h[3], h[4]
		sid := binary.BigEndian.Uint32(h[5:]) & (1<<31 - 1)
		p := c.pt[c.pr+9 : c.pr+9+l]
		c.pr += 9 + l
		if c.inBlock && typ != frContinuation {
			return errProtocol
		}
		// The frames of a stream on stream 0, or of the connection on a
		// stream, and frames of a stream still idle, break the
		// connection (RFC 9113 §5.1, §6): a stream error the node takes
		// as a connection error, as §5.4 lets it.
		switch typ {
		case frData, frHeaders, frPriority, frRST, frContinuation:
			if sid == 0 {
				return errProtocol
			}
		case frSettings, frPing, frGoAway:
			if sid != 0 {
				return errProtocol
			}
		case frPushPromise:
			return errProtocol
		}
		switch typ {
		case frData:
			if flags&flPadded != 0 {
				if len(p) == 0 || int(p[0]) >= len(p) {
					return errProtocol
				}
				p = p[1 : len(p)-int(p[0])]
			}
			s := c.get(sid)
			if n.putTimings != nil && s != nil && !s.ended {
				n.tracePutHeader(s, p)
			}
			if k := int64(len(p)); s != nil && !s.ended && (c.partial+k > n.connIn || n.cpool+k > n.sizes.clientInBufs()) && c.waitsForRoom(n, s, p) {
				// The frame is read again once the buffers have room.
				if n.putTimings != nil {
					n.tracePutEvent(n.putTimingOf(s), "receive_wait", -1)
				}
				c.pr -= 9 + l
				return nil
			}
			c.unacked += l
			if c.unacked >= windowAck {
				var w [4]byte
				binary.BigEndian.PutUint32(w[:], uint32(c.unacked))
				c.writeFrame(frWindowUpdate, 0, w[:])
				c.unacked = 0
				n.markDirty(&c.sock)
			}
			if s == nil {
				if sid > c.lastSid {
					return errProtocol
				}
				// A stream the node closed, which the client may not
				// know yet.
				continue
			}
			if s.ended {
				return errProtocol // STREAM_CLOSED: its client ended its side
			}
			// The stream's window is given back as its DATA is taken, as
			// the connection's is: a stream that sends past the window
			// it started with, a keepalive or watch stream over hours,
			// would wait for good (#689's area-4 review).
			if s.recvd += l; s.recvd >= windowAck && flags&flEndStream == 0 {
				b := c.frame(9 + 4)
				putFrameHeader(b, 4, frWindowUpdate, 0, sid)
				binary.BigEndian.PutUint32(b[9:], uint32(s.recvd))
				s.recvd = 0
				n.markDirty(&c.sock)
			}
			if err := c.data(n, s, p); err != nil {
				return err
			}
			if flags&flEndStream != 0 {
				s.ended = true
				// A watch stream whose client closed its side goes on,
				// as etcd's does, until the client resets it or goes.
				switch {
				case s.watch:
				case sideKind(s.kind):
					n.sideEnded(c, s)
				case s.streaming && s.kind == kLeaseKeepAlive:
					// A keepalive stream ends with its client's side. A
					// RangeStream or Snapshot, whose client sent all it sends,
					// ends with its last message: one sent over turns
					// (bigrange.go) was cut short.
					n.endStreaming(c, s)
				}
			}
		case frHeaders:
			if flags&flPadded != 0 {
				if len(p) == 0 || int(p[0]) >= len(p) {
					return errProtocol
				}
				p = p[1 : len(p)-int(p[0])]
			}
			if flags&flPriority != 0 {
				if len(p) < 5 || binary.BigEndian.Uint32(p)&(1<<31-1) == sid {
					return errProtocol
				}
				p = p[5:]
			}
			c.blockSid, c.blockEnd = sid, flags&flEndStream != 0
			if flags&flEndHeaders == 0 {
				c.inBlock = true
				if err := c.addBlock(p, true); err != nil {
					return err
				}
				continue
			}
			if err := c.headers(n, p, false); err != nil {
				return err
			}
		case frContinuation:
			if !c.inBlock || sid != c.blockSid {
				return errProtocol
			}
			if err := c.addBlock(p, false); err != nil {
				return err
			}
			if flags&flEndHeaders != 0 {
				c.inBlock = false
				err := c.headers(n, c.block, true)
				// The block goes back with the client's other buffers.
				spare := c.headerSlack()
				c.grow(-cap(c.block))
				c.block = nil
				if c.in != nil {
					c.node.headerSpare += c.headerSlack() - spare
				}
				if err != nil {
					return err
				}
			}
		case frPriority:
			// Priorities mean nothing to the node, but a malformed one
			// still breaks the connection.
			if l != 5 || binary.BigEndian.Uint32(p)&(1<<31-1) == sid {
				return errProtocol
			}
		case frRST:
			if l != 4 || sid > c.lastSid {
				return errProtocol
			}
			c.dropHeld(sid)
			// What it held and waited with goes, and the room of the
			// reserve the dropped bytes took (#689's re-check, B4).
			n.dropWaits(c, sid)
			n.giveRoom(c, false)
			if s := c.get(sid); s != nil {
				if s.watch {
					n.closeWatchStream(c, s)
				}
				if sideKind(s.kind) {
					n.cancelSide(mkHandle(c, sid))
				}
				c.endStream(s)
			}
		case frSettings:
			if flags&flAck != 0 && l != 0 {
				return errProtocol
			}
			if flags&flAck == 0 {
				if err := c.settings(p); err != nil {
					return err
				}
				c.writeFrame(frSettings, flAck, nil)
				n.markDirty(&c.sock)
			}
		case frWindowUpdate:
			if len(p) != 4 {
				return errProtocol
			}
			inc := int64(binary.BigEndian.Uint32(p) & (1<<31 - 1))
			if inc == 0 || sid > c.lastSid {
				return errProtocol
			}
			if c.windowOver(sid, inc) {
				return errFlowControl
			}
			c.windowUpdate(sid, inc)
		case frPing:
			if len(p) != 8 {
				return errProtocol
			}
			if flags&flAck == 0 {
				c.writeFrame(frPing, flAck, p)
				n.markDirty(&c.sock)
			}
		case frGoAway:
			return errClosed
		}
	}
}

// headers takes a whole header block that opens stream sid.
func (c *client) headers(n *node, block []byte, continued bool) error {
	sid := c.blockSid
	if s := c.get(sid); s != nil {
		// Trailers of the client, which end its side: nothing to do but
		// that end. More headers of a stream, or any after its end,
		// break the connection.
		if !c.blockEnd || s.ended {
			return errProtocol
		}
		if ok, err := c.decodeBlock(block, true); err != nil || !ok {
			return cmp.Or[error](err, errProtocol)
		}
		if !keepsTable(block) {
			// The table moved: a cached block may mean other headers now.
			clear(c.hdrCache)
		}
		s.ended = true
		if sideKind(s.kind) {
			n.sideEnded(c, s)
		}
		return nil
	}
	// A new stream of the client's is odd and above its last; a closed
	// one takes no more headers.
	if sid&1 == 0 || sid <= c.lastSid {
		return errProtocol
	}
	c.lastSid = sid
	info, ok := hdrInfo{}, false
	if !continued {
		info, ok = c.hdrCache[string(block)]
	}
	if !ok {
		c.hdrPath, c.hdrEnc, c.hdrToken, c.hdrCaller = "", "", "", ""
		if ok, err := c.decodeBlock(block, false); err != nil || !ok {
			return cmp.Or[error](err, errProtocol)
		}
		info.kind, info.token = paths[c.hdrPath], c.hdrToken
		if name, ok := strings.CutPrefix(c.hdrCaller, n.sideSecret+":"); ok && c.hdrCaller != "" {
			info.caller, info.byCaller = name, true
		}
		switch c.hdrEnc {
		case "", "identity":
		case "gzip":
			info.gzip = true
		default:
			info.badEnc = true
		}
		if info.kind != kUnknown && !info.badEnc && keepsTable(block) {
			if len(c.hdrCache) < hdrCacheMax && len(block) <= hdrCacheBlock {
				c.hdrCache[string(block)] = info
			}
		} else if !keepsTable(block) {
			clear(c.hdrCache)
		}
	}
	if uint32(c.streams) >= n.maxStreams {
		// One past the limit the client was told: refused, as grpc-go
		// refuses it, and the client may send it again.
		var p [4]byte
		binary.BigEndian.PutUint32(p[:], codeRefusedStream)
		b := c.frame(9 + 4)
		putFrameHeader(b, 4, frRST, 0, sid)
		copy(b[9:], p[:])
		n.markDirty(&c.sock)
		return nil
	}
	kind := info.kind
	s := c.newStream(sid, kind)
	s.gzip, s.token = info.gzip, info.token
	s.caller, s.byCaller = info.caller, info.byCaller
	if !info.badEnc {
		// etcd answers a call it cannot decode with no token checked.
		n.waitForToken(c, s)
	}
	if kind != kUnknown {
		n.callStarted(s)
	}
	switch {
	case kind == kUnknown:
		n.replyError(c, sid, codeUnimplemented, unknownMethod(c.hdrPath))
		c.endStream(s)
		return nil
	case info.badEnc:
		n.replyError(c, sid, codeUnimplemented, fmt.Sprintf("grpc: Decompressor is not installed for grpc-encoding %q", c.hdrEnc))
		c.endStream(s)
		return nil
	}
	if c.blockEnd {
		s.ended = true
		if sideKind(kind) {
			n.sideEnded(c, s)
		}
	}
	return nil
}

// hdrInfo is what a header block that opens a stream says: the method's
// kind and the messages' encoding.
type hdrInfo struct {
	kind   uint8
	gzip   bool // gzip, else identity
	badEnc bool // an encoding this server does not know
	token  string
	// caller is the certificate name of the client a side call is for,
	// when byCaller.
	caller   string
	byCaller bool
}

// unknownMethod is grpc-go's answer to a path no service serves.
func unknownMethod(path string) string {
	svc, method, ok := strings.CutLast(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return fmt.Sprintf("malformed method name: %q", path)
	}
	for p := range paths {
		if strings.HasPrefix(p, "/"+svc+"/") {
			return "unknown method " + method + " for service " + svc
		}
	}
	return "unknown service " + svc
}

// inflate is a compressed message of s inflated, or grpc-go's error for
// one it cannot inflate.
func inflate(s *stream, msg []byte) ([]byte, string) {
	if !s.gzip {
		return nil, "grpc: compressed flag set with identity or empty encoding"
	}
	out, err := gunzip(msg)
	if err != nil {
		return out, "grpc: failed to decompress the received message: " + err.Error()
	}
	return out, ""
}

// gunzip inflates a gzip message, at most a request's size and the
// gRPC overhead etcd allows.
func gunzip(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(io.LimitReader(zr, maxRequestBytes+grpcOverhead+1))
	if err != nil {
		return out, err
	}
	if len(out) > maxRequestBytes+grpcOverhead {
		// Retain the bounded prefix only until the caller logs its size:
		// a compressed large put may be refused before full decoding.
		return out, errors.New("the message is larger than the server takes")
	}
	return out, nil
}

// grpcOverhead is what etcd's gRPC server takes over --max-request-bytes.
const grpcOverhead = 512 << 10

// keepsTable reports whether an HPACK block leaves the dynamic table as
// it is: only indexed fields and literals without indexing.
func keepsTable(b []byte) bool {
	for len(b) > 0 {
		first := b[0]
		switch {
		case first&0x80 != 0:
			_, rest, ok := hpackInt(b, 7)
			if !ok {
				return false
			}
			b = rest
		case first&0xc0 == 0x40, first&0xe0 == 0x20:
			return false
		default:
			idx, rest, ok := hpackInt(b, 4)
			if !ok {
				return false
			}
			b = rest
			if idx == 0 {
				if b, ok = hpackStr(b); !ok {
					return false
				}
			}
			if b, ok = hpackStr(b); !ok {
				return false
			}
		}
	}
	return true
}

func hpackInt(b []byte, n uint) (uint64, []byte, bool) {
	if len(b) == 0 {
		return 0, nil, false
	}
	limit := uint64(1)<<n - 1
	v := uint64(b[0]) & limit
	b = b[1:]
	if v < limit {
		return v, b, true
	}
	for shift := uint(0); len(b) > 0 && shift < 63; shift += 7 {
		v += uint64(b[0]&0x7f) << shift
		last := b[0]&0x80 == 0
		b = b[1:]
		if last {
			return v, b, true
		}
	}
	return 0, nil, false
}

func hpackStr(b []byte) ([]byte, bool) {
	n, b, ok := hpackInt(b, 7)
	if !ok || n > uint64(len(b)) {
		return nil, false
	}
	return b[n:], true
}

// data takes the bytes of a DATA frame of stream s.
// A message that spans DATA frames waits in its stream's buffer. Streams
// that end give their buffers back to the node, which keeps a few of them
// for the next, at most sizes.kept of them, each of at most keptBufMax: a
// stream slot that kept its own grew to the largest message it ever had,
// in each of a client's 8,192 slots, and the k8s bench's nodes held
// hundreds of megabytes of them (#602).
const keptBufMax = 256 << 10

// takeBuf returns an empty buffer that a stream gave back, or nil.
func (n *node) takeBuf() []byte {
	k := len(n.bufs)
	if k == 0 {
		return nil
	}
	b := n.bufs[k-1]
	n.bufs[k-1] = nil
	n.bufs = n.bufs[:k-1]
	return b[:0]
}

// putBuf keeps b for another stream, unless the node keeps enough or b
// is large.
func (n *node) putBuf(b []byte) {
	if cap(b) > 0 && cap(b) <= keptBufMax && len(n.bufs) < n.sizes.kept {
		n.bufs = append(n.bufs, b)
	}
}

func (c *client) data(n *node, s *stream, p []byte) error {
	was := s.counted
	c.partial -= s.counted
	c.grow(-int(s.counted))
	s.counted = 0
	if len(s.buf) > 0 {
		s.buf = append(s.buf, p...)
		p = s.buf
	}
	for len(p) >= 5 {
		l := int(binary.BigEndian.Uint32(p[1:5]))
		if n.putTimings != nil {
			n.tracePutStart(s, l)
		}
		if l > maxMsg {
			// As grpc-go's server refuses it, before taking it in.
			n.replyError(c, s.id, codeResourceExhausted, "grpc: received message larger than max ("+itoa(l)+" vs. "+itoa(maxMsg)+")")
			return nil
		}
		if len(p)-5 < l {
			break
		}
		msg := p[5 : 5+l]
		n.gs.calls[s.kind].received++
		n.gs.recvBytes += int64(l)
		if s.began.IsZero() {
			s.began = n.now
		}
		if p[0] != 0 {
			var bad string
			if msg, bad = inflate(s, msg); bad != "" {
				if n.putTimings != nil {
					n.tracePutDecoded(s, l, msg, false)
					n.tracePutEvent(n.putTimingOf(s), "decode_refused", -1)
				}
				n.replyError(c, s.id, codeInternal, bad)
				return nil
			}
		}
		if n.putTimings != nil {
			n.tracePutDecoded(s, l, msg, true)
			n.tracePutEvent(n.putTimingOf(s), "received", -1)
		}
		p = p[5+l:]
		if err := n.request(c, s, msg); errors.Is(err, errProto) {
			// A request it cannot read ends the call, not the
			// connection, as grpc-go answers it.
			if s.watch {
				n.closeWatchStream(c, s)
			}
			n.replyError(c, s.id, codeInternal, "grpc: error unmarshalling request: proto: cannot parse invalid wire-format data")
			return nil
		} else if err != nil {
			return err
		}
		if !s.open {
			p = nil
			break
		}
	}
	if int64(len(p)) > was && (c.partial+int64(len(p)) > n.connIn || !n.roomFor(c, int64(len(p)))) {
		// The connection holds its share of requests sent in part, or
		// the client buffers stayed full for requestTimeout while the
		// part waited for room (partwait.go): this one is refused, as
		// etcd refuses too many requests, and the rest of it dropped.
		n.reject(c, s, errTooManyRequests)
		return nil
	}
	if len(p) > 0 {
		if len(s.buf) > 0 {
			s.buf = append(s.buf[:0], p...) // overlapping copy of the tail is fine
		} else {
			s.buf = append(n.takeBuf(), p...)
		}
	} else {
		s.buf = s.buf[:0]
	}
	if c.get(s.id) == s {
		s.counted = int64(len(s.buf))
		c.partial += s.counted
		c.grow(int(s.counted))
	}
	return nil
}

// Header blocks the server sends: :status 200 and the gRPC content type;
// trailers with grpc-status 0.
var (
	respHeaders  = append([]byte{0x88, 0x0f, 0x10, 16}, "application/grpc"...)
	respTrailers = append(append([]byte{0x00, 11}, "grpc-status"...), 1, '0')
)

// handle packs a client stream into a number that names it in batches and
// queues: socket index, socket generation and stream id.
type handle uint64

// noHandle is an op's handle that no client waits on.
const noHandle = ^handle(0)

func mkHandle(c *client, sid uint32) handle {
	return handle(uint64(c.idx)<<48 | uint64(c.gen)<<32 | uint64(sid))
}

func (h handle) parts() (idx int, gen uint16, sid uint32) {
	return int(h >> 48), uint16(h >> 32), uint32(h)
}
