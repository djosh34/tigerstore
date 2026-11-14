package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/loop"
)

// HTTP/2 limits of one client connection.
const (
	// minStreamWindow is the smallest receive window of a stream; see
	// Server.streamWindow.
	minStreamWindow = 4 << 20
	// connWindow is the receive window of the connection. Data is credited
	// back as it arrives; the stream window bounds what one stream holds.
	connWindow = 16 << 20
	// sendQueue is how many encoded bytes one stream may queue for the
	// writer before its Send waits.
	sendQueue = 4 << 20
	// writeTimeout drops a client that stops reading from its socket.
	writeTimeout = 20 * time.Second
	ioBuffer     = 64 << 10
	// frameHeaderLen is the size of an HTTP/2 frame header.
	frameHeaderLen = 9
)

// conn is one HTTP/2 client connection. The node's event loop owns its
// socket, its input and the stream table's creation (loopconn.go). mu
// guards everything the loop shares with the handlers, which run on
// goroutines of their own.
type conn struct {
	s *Server
	// metrics count this connection's calls for /metrics.
	metrics *connMetrics
	// apiVersion is the client API version of the last call, and
	// apiCounters its client_requests_total counters; guarded by mu.
	apiVersion  string
	apiCounters [2]prometheus.Counter
	// nc is what the connection reads and writes: TLS over the bridge, or
	// the bridge itself without TLS. raw is the socket under them, which
	// any goroutine may close.
	nc     net.Conn
	bridge *loop.Bridge
	// records is nc when the connection speaks TLS 1.3, whose records it
	// seals and opens itself; its Flush seals the record being written.
	records *loop.Records
	raw     net.Conn
	// traffic is what the connection serves: gRPC alone answers HTTP
	// requests as grpc-go does, HTTP alone takes gRPC calls as HTTP.
	traffic Traffic
	// fr writes the control frames; the loop reads frames itself
	// (parseFrame).
	fr *http2.Framer
	// caller sizes this client's writes as etcd would.
	caller core.Caller
	// cluster and member go in every response header.
	cluster, member uint64

	ctx    context.Context // ends when the connection closes
	cancel context.CancelFunc
	once   sync.Once

	// Loop only: the socket's token; the decrypted input from inR to inW,
	// and its phase; ending once the input ended with a GOAWAY, which
	// closes the connection by endBy; the preface's deadline; since when
	// output waits for the socket; and gone once the connection left the
	// loop.
	token    uint64
	in       []byte
	inR, inW int
	// readErr is how the last read ended, kept while frames read before
	// it wait for a turn with room (readFrames).
	readErr   error
	phase     int
	ending    bool
	endBy     time.Time
	prefaceBy time.Time
	waitSince time.Time
	gone      bool
	// outCharged is the output waiting for the socket that counts in
	// Server.outAll (chargeOut).
	outCharged int
	// attached is set once the loop has the connection, from when a close
	// tells it; reading is set while the loop reads the connection, which
	// then writes it anyway; and signaled while a signal waits for the
	// loop.
	attached, reading, signaled atomic.Bool

	// Loop only.
	maxID uint32 // highest client stream ID seen
	// contID is the stream whose header block waits for a CONTINUATION
	// frame, or 0.
	contID uint32
	batch  opBatch
	// arena holds the requests of the calls the node runs (arena.go).
	arena arena
	// deadlines are the calls that end at a deadline (deadline.go).
	deadlines deadlines
	// free are streams of ended calls for the next calls (freeStream).
	free []*stream
	// slabs are the messages requests are decoded into (admit).
	slabs core.Slabs
	// hdec decodes header blocks into hdr, the headers of the block being
	// read, with no allocation for fields from the HPACK tables.
	hdec *hpack.Decoder
	hdr  reqHeaders
	// blocks are the headers of gRPC calls whose block held only indexed
	// fields, by the block's bytes. Such a block does not change the
	// HPACK tables, so while no other block changed them, the same bytes
	// decode to the same headers. A client sends the same block for
	// every call of one method once its fields are in the table; decoding
	// it took 3% of a leader's core with transactions (#506).
	blocks map[string]reqHeaders

	mu        sync.Mutex
	streams   map[uint32]*stream
	ctrl      []ctrlFrame // frames for the writer that are not stream output
	ready     []*stream   // streams with output, in order
	sendWin   int64       // connection send window
	initWin   int64       // the client's initial stream window
	maxFrame  int         // the client's largest frame payload
	closed    bool
	goingAway bool
	// recvCredit is connection bytes received and not yet credited back.
	recvCredit int
	// held is the bytes the connection's calls hold; see maxHeld.
	held int

	// completions are the replies from the node for the writer to encode,
	// and waitingReplies their estimated size.
	completions    atomic.Pointer[completed]
	waitingReplies atomic.Int64
	// atNode is the calls the node has and has not answered.
	atNode map[*stream]struct{}

	// frames counts the frames the loop read, and wrote is set when the
	// loop sends HEADERS or DATA, which forgives the client's pings
	// (keepalive.go). A count, not a time: a clock read per frame took
	// 2% of a leader's core (#506). lastPing and pingStrikes are the
	// loop's.
	frames      atomic.Uint64
	wrote       atomic.Bool
	lastPing    time.Time
	pingStrikes int
}

// ctrlKind is the kind of a frame other than HEADERS and DATA.
type ctrlKind byte

const (
	ctrlSettings ctrlKind = iota
	ctrlSettingsAck
	ctrlPingAck
	ctrlWindowUpdate
	ctrlRST
	ctrlGoAway
	ctrlPing
)

// ctrlFrame is a frame other than HEADERS and DATA.
type ctrlFrame struct {
	kind ctrlKind
	id   uint32 // stream, or last stream for GOAWAY
	val  uint32 // window increment or error code
	ping [8]byte
	// debug is the debug data of a GOAWAY.
	debug string
}

// caller is the client of a connection as etcd's size limit for writes
// sees it: with client certificates required, as --client-cert-auth does,
// the common name of the leaf of the first verified chain, as etcd's
// AuthInfoFromTLS takes it.
func caller(cfg *tls.Config, nc net.Conn) core.Caller {
	tc, ok := nc.(tlsState)
	if !ok || cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		return core.Caller{}
	}
	chains := tc.ConnectionState().VerifiedChains
	if len(chains) == 0 {
		return core.Caller{}
	}
	return core.NewCaller(chains[0][0].Subject.CommonName)
}

// tlsState is a TLS connection: crypto/tls's, or our own records after
// its handshake.
type tlsState interface {
	ConnectionState() tls.ConnectionState
}

// newConn makes a connection that reads and writes nc, over the bridge
// to its socket.
func newConn(s *Server, nc net.Conn, bridge *loop.Bridge) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		s: s, nc: nc, bridge: bridge, raw: bridge.Conn, ctx: ctx, cancel: cancel, metrics: newConnMetrics(),
		cluster: s.cluster, member: s.member,
		streams:  map[uint32]*stream{},
		atNode:   map[*stream]struct{}{},
		sendWin:  65535,
		initWin:  65535,
		maxFrame: 16384,
	}
	c.records, _ = nc.(*loop.Records)
	c.caller = caller(s.cfg.TLS, nc)
	c.fr = http2.NewFramer(frameDst{c}, nil)
	c.hdec = hpack.NewDecoder(4096, c.onField)
	c.hdec.SetMaxStringLength(maxHeaderList)
	c.ctrl = append(c.ctrl,
		ctrlFrame{kind: ctrlSettings},
		ctrlFrame{kind: ctrlWindowUpdate, val: connWindow - 65535})
	return c
}

// close ends the connection and every stream on it. It never writes, so
// it never waits for a client that stopped reading.
func (c *conn) close() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		for _, st := range c.streams {
			st.countEnd(canceled)
			st.reset = true
			c.cancelLocked(st)
			st.out = nil
		}
		c.ready = nil
		c.mu.Unlock()
		c.cancel()
		_ = c.raw.Close()
		// The loop lets go of the connection at its next turn; before it
		// has it, attach sees the close.
		c.signal()
	})
}

// isClosed reports whether the connection was closed.
func (c *conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// goAway tells the client to open no more streams. The loop closes the
// connection once the streams in flight have ended.
func (c *conn) goAway() {
	c.mu.Lock()
	if c.closed || c.goingAway {
		c.mu.Unlock()
		return
	}
	c.goingAway = true
	c.ctrl = append(c.ctrl, ctrlFrame{kind: ctrlGoAway, id: c.maxID})
	c.mu.Unlock()
	c.signal()
}

func (c *conn) lastID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxID
}

// maxCtrl bounds the control frames waiting for the loop to write them,
// as x/net's server does with 10,000; a client that floods pings and does
// not read is dropped. 512 keep the queue, about 24 KiB, inside a
// connection's share of the memory budget (connBytes).
const maxCtrl = 512

func (c *conn) queueCtrl(f ctrlFrame) {
	c.mu.Lock()
	c.addCtrl(f)
	c.mu.Unlock()
	c.signal()
}

// addCtrl queues a control frame. c.mu is held.
func (c *conn) addCtrl(f ctrlFrame) {
	if c.closed {
		return
	}
	if len(c.ctrl) >= maxCtrl {
		// close takes c.mu, so it runs once the caller lets go.
		go c.close()
		return
	}
	c.ctrl = append(c.ctrl, f)
}

func (c *conn) onFrame(f *frame) error {
	switch f.typ {
	case http2.FrameHeaders:
		c.hdr = reqHeaders{id: f.id, ended: f.has(http2.FlagHeadersEndStream)}
		return c.headerFragment(f.data, true, f.has(http2.FlagHeadersEndHeaders))
	case http2.FrameContinuation:
		return c.headerFragment(f.data, false, f.has(http2.FlagContinuationEndHeaders))
	case http2.FrameData:
		return c.onData(f)
	case http2.FrameWindowUpdate:
		return c.onWindowUpdate(f)
	case http2.FrameSettings:
		if f.has(http2.FlagSettingsAck) {
			return nil
		}
		return c.onSettings(f)
	case http2.FramePing:
		if !f.has(http2.FlagPingAck) {
			c.queueCtrl(ctrlFrame{kind: ctrlPingAck, ping: f.ping()})
			return c.checkPing()
		}
	case http2.FrameRSTStream:
		c.resetStream(f.id, 0)
	case http2.FrameGoAway, http2.FramePriority, http2.FramePriorityUpdate, http2.FramePushPromise:
		// A client's GOAWAY and priority signals need nothing, and
		// parseFrame refused PUSH_PROMISE. Unknown frames need nothing
		// either.
	}
	return nil
}

func (c *conn) onSettings(f *frame) error {
	var err error
	c.mu.Lock()
	for i := range f.length / 6 {
		s := f.setting(i)
		if err = s.Valid(); err != nil {
			break
		}
		switch s.ID {
		case http2.SettingInitialWindowSize:
			delta := int64(s.Val) - c.initWin
			c.initWin = int64(s.Val)
			for _, st := range c.streams {
				st.sendWin += delta
			}
		case http2.SettingMaxFrameSize:
			c.maxFrame = int(s.Val)
		case http2.SettingHeaderTableSize, http2.SettingEnablePush, http2.SettingMaxConcurrentStreams,
			http2.SettingMaxHeaderListSize, http2.SettingEnableConnectProtocol, http2.SettingNoRFC7540Priorities:
			// The encoder never indexes, so the header table size does not
			// matter, and no other setting changes what the server sends.
		}
	}
	if err == nil {
		c.addCtrl(ctrlFrame{kind: ctrlSettingsAck})
	}
	c.mu.Unlock()
	c.signal()
	return err
}

func (c *conn) onWindowUpdate(f *frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.id == 0 {
		c.sendWin += int64(f.increment())
		if c.sendWin > 1<<31-1 {
			return http2.ConnectionError(http2.ErrCodeFlowControl)
		}
	} else if st := c.streams[f.id]; st != nil {
		st.sendWin += int64(f.increment())
	}
	c.signal()
	return nil
}

// resetStream drops a stream that the client reset, or that broke a rule
// (code is then the RST_STREAM code to send).
func (c *conn) resetStream(id uint32, code http2.ErrCode) {
	c.mu.Lock()
	st := c.streams[id]
	if st != nil {
		delete(c.streams, id)
		st.countEnd(canceled)
		st.reset = true
		c.disarmDeadline(st)
		// A call the node still runs keeps its request charge and its
		// request until its reply comes and is dropped.
		c.releaseLocked(&st.replyHeld)
		if _, at := c.atNode[st]; !at {
			c.releaseBody(st)
		}
		c.cancelLocked(st)
		st.out = nil
		st.queued = 0
		// An answer in pieces is no longer at the node, but its next piece
		// may wait for room (nextPiece): wake it, so it ends the answer.
		st.wake()
	}
	if code != 0 {
		c.addCtrl(ctrlFrame{kind: ctrlRST, id: id, val: uint32(code)})
	}
	c.mu.Unlock()
	if st != nil && st.cancel != nil {
		st.cancel()
	}
	c.signal()
}

// credit counts n received connection bytes and credits them back in
// batches, unless the connection's calls hold too much.
func (c *conn) credit(n int) {
	c.mu.Lock()
	c.recvCredit += n
	c.creditLocked()
	c.mu.Unlock()
	c.signal()
}

// reqHeaders is what the server reads from a request's header block.
type reqHeaders struct {
	id                        uint32
	ended                     bool // the HEADERS frame had END_STREAM
	method, path, contentType string
	timeout                   time.Duration
	hasTimeout, needLeader    bool
	// gateway: the call carries grpcgateway-accept metadata, for which
	// etcd ignores the client certificate's identity.
	gateway bool
	// token is the call's auth token: etcd's "token" metadata, or its
	// "authorization", without "Bearer " either way.
	token string
	// encoding is the call's grpc-encoding, and apiVersion its first
	// client-api-version.
	encoding      string
	apiVersion    string
	hasAPIVersion bool
	// traceMD is the call's metadata that tracing reads (tracing.go).
	traceMD metadata.MD
	// authority is :authority, and fields the regular fields of an HTTP
	// request; gRPC calls keep none.
	authority string
	fields    []hpack.HeaderField
	size      int
	regular   bool  // a regular field came; pseudo-fields must come first
	pseudo    uint8 // bits of the pseudo-fields seen
	bad       bool  // a field broke HTTP/2's rules
}

// maxHeaderList bounds the decoded size of one header block, as
// SETTINGS_MAX_HEADER_LIST_SIZE would. 32 KiB holds gRPC's headers, an
// auth token and tracing with room to spare, and keeps a header block that
// waits for its CONTINUATION inside a connection's bound (connBytes).
const maxHeaderList = 32 << 10

// headerFragment decodes part of a header block. parseFrame checks that
// CONTINUATION frames follow their HEADERS frame.
func (c *conn) headerFragment(b []byte, first, last bool) error {
	h := &c.hdr
	// Only a block in one HEADERS frame may come from blocks: the last
	// fragment of a longer one continues what the decoder holds.
	whole := first && last
	if whole {
		if known, ok := c.blocks[string(b)]; ok {
			known.id, known.ended = h.id, h.ended
			*h = known
			return c.onHeaders(h)
		}
	}
	// As x/net does since CVE-2023-45288: a block that already broke the
	// rules, or whose fragments could decode past the limit, ends the
	// connection instead of being decoded on.
	if h.bad || len(b) > 2*(maxHeaderList-h.size) {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	if _, err := c.hdec.Write(b); err != nil {
		return http2.ConnectionError(http2.ErrCodeCompression)
	}
	if !last {
		return nil
	}
	if err := c.hdec.Close(); err != nil {
		return http2.ConnectionError(http2.ErrCodeCompression)
	}
	if whole && onlyIndexed(b) {
		if c.cacheable(h, b) {
			if c.blocks == nil {
				c.blocks = make(map[string]reqHeaders)
			}
			known := *h
			known.id, known.ended = 0, false
			c.blocks[string(b)] = known
		}
	} else {
		// The block may have changed the tables, which other blocks'
		// indexes name.
		c.blocks = nil
	}
	if !h.bad {
		return c.onHeaders(h)
	}
	// Malformed trailers on an open stream, or a malformed block on a new
	// stream, reset only that stream; otherwise the stream ID rules hold
	// as for any other block.
	c.mu.Lock()
	open := c.streams[h.id] != nil
	c.mu.Unlock()
	if open {
		c.resetStream(h.id, http2.ErrCodeProtocol)
		return nil
	}
	if h.id%2 == 0 || h.id <= c.maxID {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	c.mu.Lock()
	c.maxID = h.id
	c.mu.Unlock()
	c.resetStream(h.id, http2.ErrCodeProtocol)
	return nil
}

// cacheable reports whether the connection keeps the decoded headers h of
// block b, a block of indexed fields only, for the next time it comes.
func (c *conn) cacheable(h *reqHeaders, b []byte) bool {
	return !h.bad && h.traceMD == nil && h.fields == nil && len(b) <= maxBlockKey && len(c.blocks) < maxBlocks
}

// maxBlocks bounds the header blocks a connection keeps decoded, and
// maxBlockKey the size of each block it keeps. A block of indexed fields
// decodes to strings from the HPACK table, at most 4 KiB, so the blocks
// kept stay under 40 KiB. A client sends one block per method it calls.
const (
	maxBlocks   = 8
	maxBlockKey = 256
)

// onlyIndexed reports whether a header block holds only indexed fields
// (RFC 7541, 6.1): a set top bit, then an integer with a 7-bit prefix.
func onlyIndexed(b []byte) bool {
	for len(b) > 0 {
		if b[0]&0x80 == 0 {
			return false
		}
		more := b[0]&0x7f == 0x7f
		b = b[1:]
		for more {
			if len(b) == 0 {
				return false
			}
			more = b[0]&0x80 != 0
			b = b[1:]
		}
	}
	return true
}

// onField takes one decoded header field. The hpack decoder state must
// stay in step with the client's even for a field that breaks the rules,
// so a bad field only marks the block.
func (c *conn) onField(f hpack.HeaderField) {
	h := &c.hdr
	h.size += int(f.Size())
	if h.size > maxHeaderList || !httpguts.ValidHeaderFieldValue(f.Value) {
		h.bad = true
		return
	}
	if strings.HasPrefix(f.Name, ":") {
		if h.regular {
			h.bad = true
		}
		var bit uint8
		switch f.Name {
		case ":method":
			bit, h.method = 1, f.Value
		case ":path":
			bit, h.path = 2, f.Value
		case ":scheme":
			bit = 4
		case ":authority":
			bit, h.authority = 8, f.Value
		default:
			h.bad = true
		}
		if h.pseudo&bit != 0 {
			// Each pseudo-field comes at most once (RFC 9113 8.3).
			h.bad = true
		}
		h.pseudo |= bit
		return
	}
	h.regular = true
	if !validFieldName(f.Name) {
		h.bad = true
		return
	}
	if isHTTPPath(h.path) {
		h.fields = append(h.fields, f)
	}
	if traceKey(f.Name) {
		if h.traceMD == nil {
			h.traceMD = metadata.MD{}
		}
		h.traceMD.Append(f.Name, f.Value)
	}
	switch f.Name {
	case "content-type":
		h.contentType = f.Value
	case "grpc-timeout":
		h.timeout, h.hasTimeout = parseTimeout(f.Value)
	case "hasleader":
		h.needLeader = h.needLeader || f.Value == "true"
	case "grpcgateway-accept":
		h.gateway = true
	case "grpc-encoding":
		h.encoding = f.Value
	case "client-api-version":
		if !h.hasAPIVersion {
			h.apiVersion, h.hasAPIVersion = f.Value, true
		}
	case "token":
		h.token = strings.TrimPrefix(f.Value, "Bearer ")
	case "authorization":
		if h.token == "" {
			h.token = strings.TrimPrefix(f.Value, "Bearer ")
		}
	}
}

// validFieldName reports whether name is a lowercase HTTP token, as
// HTTP/2 requires of regular field names.
func validFieldName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if ch >= 'A' && ch <= 'Z' || !httpguts.IsTokenRune(rune(ch)) {
			return false
		}
	}
	return true
}

func (c *conn) onHeaders(h *reqHeaders) error {
	id := h.id
	if id%2 == 0 || id <= c.maxID {
		c.mu.Lock()
		st := c.streams[id]
		c.mu.Unlock()
		if st != nil && h.ended && !st.inEOF {
			// Trailers from the client end its side of the stream.
			return c.onEnd(st)
		}
		if st != nil {
			c.resetStream(id, http2.ErrCodeStreamClosed)
			return nil
		}
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	c.mu.Lock()
	c.maxID = id
	refuse := c.goingAway || uint64(len(c.streams)) >= uint64(c.s.maxStreams)
	c.mu.Unlock()
	if refuse {
		c.resetStream(id, http2.ErrCodeRefusedStream)
		return nil
	}
	st := c.newStream(h)
	if st.done.Load() || (st.rpc == nil && st.httpPath == "") {
		// Refused, or gRPC to an unknown method: its error is already
		// queued, and no handler runs.
		if st.cancel != nil {
			st.cancel()
		}
		if h.ended {
			return c.onEnd(st)
		}
		return nil
	}
	if st.rpc != nil && st.rpc.stream != nil {
		go c.runStream(st)
	}
	if st.httpPath != "" {
		go c.runHTTP(st)
	}
	if h.ended {
		return c.onEnd(st)
	}
	return nil
}

func (c *conn) newStream(h *reqHeaders) *stream {
	id := h.id
	grpc := h.method == "POST" && strings.HasPrefix(h.contentType, "application/grpc") && c.traffic != TrafficHTTP
	var rpc *method
	if grpc {
		rpc = methods[h.path]
	}
	st := c.freeStream(rpc)
	*st = stream{c: c, id: id, rpc: rpc, needLeader: h.needLeader, gateway: h.gateway, token: h.token, encoding: h.encoding}
	if !grpc {
		st.httpMethod, st.httpPath = h.method, h.path
		if st.httpPath == "" {
			st.httpPath = "/"
		}
		st.authority, st.httpFields = h.authority, h.fields
		st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)
	}
	// A call that the node runs needs no context of its own: the node
	// answers it within its request timeout, its deadline ends it early,
	// and a reset drops the reply.
	if !grpc || (rpc != nil && rpc.op == nil) {
		st.ctx, st.cancel = context.WithCancel(c.ctx)
	}
	if rpc != nil && rpc.stream != nil {
		st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)
	}
	// The answer to a call that runs no further, if any.
	var early error
	switch {
	case grpc && rpc == nil:
		early = unknownMethod(h.path)
	case grpc && h.encoding != "" && h.encoding != identity && h.encoding != gzipName:
		// grpc-go's answer before it runs a handler.
		early = status.Errorf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", h.encoding)
	case rpc != nil && rpc.learner == learnerNever && c.s.learner():
		// As etcd's interceptors, a learner serves only Status,
		// serializable ranges and snapshots.
		early = rpctypes.ErrGRPCNotSupportedForLearner
	case rpc != nil:
		early = validAPIVersion(h.apiVersion)
	}
	c.mu.Lock()
	st.sendWin = c.initWin
	if c.closed {
		// The connection closed while the reader decoded these headers:
		// the stream is dead from the start and arms no deadline.
		st.reset = true
		c.mu.Unlock()
		return st
	}
	c.streams[id] = st
	if rpc != nil {
		st.calls = &c.metrics.calls[rpc.idx]
		st.countStart()
		st.beginTrace(h)
		if early == nil {
			// etcd's interceptor counts a call it lets through.
			c.clientRequests(rpc.stream != nil, h.apiVersion).Inc()
		}
	}
	c.mu.Unlock()
	if grpc && h.hasTimeout {
		// The deadline ends the call whether or not its request is
		// complete or its handler started.
		c.armDeadline(st, time.Now().Add(h.timeout))
	}
	if early != nil {
		st.finish(early)
	}
	return st
}

// maxFreeStreams bounds the streams a connection keeps for its next
// calls, and streamBytes the size of one (TestStreamSize).
const (
	maxFreeStreams = 128
	streamBytes    = 640
)

// freeStream returns a stream for a call of rpc: for a unary call the
// node runs, one that an earlier such call left (reuseStream). Loop only.
func (c *conn) freeStream(rpc *method) *stream {
	if n := len(c.free); n > 0 && rpc != nil && rpc.op != nil {
		st := c.free[n-1]
		c.free[n-1] = nil
		c.free = c.free[:n-1]
		return st
	}
	return &stream{}
}

// reuseStream keeps an ended stream for the connection's next call, if
// nothing refers to it any more: a unary call that the node ran and
// answered whole, with no context, trace or request of its own left.
// Every other kind of call may still be referred to by a goroutine or the
// node. Its deadline, if it had one, is disarmed. c.mu is held, on the
// loop.
func (c *conn) reuseStream(st *stream) {
	if st.rpc == nil || st.rpc.op == nil || st.cancel != nil || st.trace != nil || st.space != nil || st.chunk != nil || st.due != 0 || len(c.free) >= maxFreeStreams {
		return
	}
	if _, at := c.atNode[st]; at {
		return
	}
	c.free = append(c.free, st)
}

func (c *conn) onData(f *frame) error {
	c.credit(f.length)
	c.mu.Lock()
	st := c.streams[f.id]
	var halfClosed, overflow bool
	if st != nil {
		halfClosed = st.inEOF
		// The stream's window is credited back only as Recv takes
		// messages; a client that sends past it breaks flow control.
		// Padding is never stored, so it is credited back at once.
		st.recvUsed += len(f.data)
		overflow = st.recvUsed > c.s.streamWindow
		if pad := f.length - len(f.data); pad > 0 && !overflow && !halfClosed {
			c.addCtrl(ctrlFrame{kind: ctrlWindowUpdate, id: st.id, val: uint32(pad)})
		}
	}
	c.mu.Unlock()
	switch {
	case st == nil:
		// A stream that is gone or reset; the data is dropped.
		return nil
	case halfClosed:
		c.resetStream(f.id, http2.ErrCodeStreamClosed)
		return nil
	case overflow:
		c.resetStream(f.id, http2.ErrCodeFlowControl)
		return nil
	}
	switch {
	case st.httpPath != "":
		if d := f.data; len(d) > 0 {
			st.deliver(append([]byte(nil), d...), len(d))
		}
	case st.rpc != nil && !st.done.Load():
		if err := st.receive(f.data); err != nil {
			st.finish(err)
		}
	}
	if f.has(http2.FlagDataEndStream) {
		return c.onEnd(st)
	}
	return nil
}

// onEnd handles the end of the client's side of a stream.
func (c *conn) onEnd(st *stream) error {
	c.mu.Lock()
	st.inEOF = true
	if st.ended {
		delete(c.streams, st.id)
	}
	c.mu.Unlock()
	select {
	case st.inSignal <- struct{}{}:
	default:
	}
	switch {
	case st.done.Load(), st.httpPath != "":
	case st.rpc.op != nil:
		c.admit(st)
	case st.rpc.unary != nil:
		go c.runUnary(st)
	}
	return nil
}

// parseTimeout reads a grpc-timeout header: up to 8 digits and a unit.
func parseTimeout(v string) (time.Duration, bool) {
	if len(v) < 2 || len(v) > 9 {
		return 0, false
	}
	var units time.Duration
	switch v[len(v)-1] {
	case 'H':
		units = time.Hour
	case 'M':
		units = time.Minute
	case 'S':
		units = time.Second
	case 'm':
		units = time.Millisecond
	case 'u':
		units = time.Microsecond
	case 'n':
		units = time.Nanosecond
	default:
		return 0, false
	}
	var n int64
	for _, ch := range v[:len(v)-1] {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int64(ch-'0')
	}
	if units == time.Hour && n > int64(1<<63-1)/int64(time.Hour) {
		return 1<<63 - 1, true
	}
	return time.Duration(n) * units, true
}

// The response header blocks are encoded without indexing, so they do not
// depend on the connection's HPACK state and are built once.
var (
	grpcHeaders = headerBlock(
		hpack.HeaderField{Name: ":status", Value: "200"},
		hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
	gzipHeaders = headerBlock(
		hpack.HeaderField{Name: ":status", Value: "200"},
		hpack.HeaderField{Name: "content-type", Value: "application/grpc"},
		hpack.HeaderField{Name: "grpc-encoding", Value: gzipName})
	okTrailers = headerBlock(hpack.HeaderField{Name: "grpc-status", Value: "0"})
)

// headerBlock encodes fields as literals that are never indexed. It
// starts with a dynamic table size update to 0, so it is valid whatever
// table size the client set: a decoder whose limit was lowered expects an
// update in the next block.
func headerBlock(fields ...hpack.HeaderField) []byte {
	var b strings.Builder
	b.WriteByte(0x20)
	e := hpack.NewEncoder(&b)
	for _, f := range fields {
		f.Sensitive = true
		// strings.Builder never fails a write.
		_ = e.WriteField(f)
	}
	return []byte(b.String())
}

// statusBlock is the trailer block of a failed gRPC call; with headers it
// is a whole trailers-only response.
func statusBlock(st *status.Status, headers bool) []byte {
	var fields []hpack.HeaderField
	if headers {
		fields = append(fields,
			hpack.HeaderField{Name: ":status", Value: "200"},
			hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
	}
	msg := encodeGrpcMessage(st.Message())
	// Keep the block well inside the smallest frame a client accepts.
	if len(msg) > 8192 {
		msg = msg[:8192]
	}
	fields = append(fields,
		hpack.HeaderField{Name: "grpc-status", Value: codeString(st.Code())},
		hpack.HeaderField{Name: "grpc-message", Value: msg})
	return headerBlock(fields...)
}

func codeString(c codes.Code) string {
	const digits = "0123456789"
	if c < 10 {
		return digits[c : c+1]
	}
	return string([]byte{digits[c/10], digits[c%10]})
}

// encodeGrpcMessage percent-encodes a grpc-message value as the gRPC
// protocol asks: every byte outside printable ASCII, and '%'.
func encodeGrpcMessage(m string) string {
	const hex = "0123456789ABCDEF"
	var b []byte
	for i := 0; i < len(m); i++ {
		ch := m[i]
		if ch >= ' ' && ch <= '~' && ch != '%' {
			if b != nil {
				b = append(b, ch)
			}
			continue
		}
		if b == nil {
			b = append(make([]byte, 0, len(m)+16), m[:i]...)
		}
		b = append(b, '%', hex[ch>>4], hex[ch&15])
	}
	if b == nil {
		return m
	}
	return string(b)
}

// frameHeader appends an HTTP/2 frame header.
func frameHeader(b []byte, n int, typ http2.FrameType, flags http2.Flags, id uint32) []byte {
	b = append(b, byte(n>>16), byte(n>>8), byte(n), byte(typ), byte(flags))
	return binary.BigEndian.AppendUint32(b, id)
}
