package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
)

// HTTP/2 limits of one client connection.
const (
	// maxStreams is the limit of concurrent streams per connection. One
	// kube-apiserver connection carries many watches; etcd advertises
	// math.MaxUint32.
	maxStreams = 1 << 20
	// streamWindow is the receive window of each stream. It is above the
	// largest request, so a unary request never waits for a window update.
	streamWindow = 4 << 20
	// connWindow is the receive window of the connection. Data is credited
	// back as it arrives; streamWindow bounds what one stream holds.
	connWindow = 16 << 20
	// maxRecvMsg is etcd's request limit plus the room it leaves for gRPC.
	maxRecvMsg = core.MaxRequestBytes + 512*1024
	// sendQueue is how many encoded bytes one stream may queue for the
	// writer before its Send waits.
	sendQueue = 4 << 20
	// writeTimeout drops a client that stops reading from its socket.
	writeTimeout = 20 * time.Second
	ioBuffer     = 64 << 10
)

// conn is one HTTP/2 client connection. The reader goroutine owns the
// Framer's read side and the stream table's creation; the writer goroutine
// owns the Framer's write side. mu guards everything they share with each
// other and with the handlers.
type conn struct {
	s   *Server
	nc  net.Conn
	raw net.Conn // nc without TLS, closed to abort a stuck write
	fr  *http2.Framer
	bw  *bufio.Writer

	ctx    context.Context // ends when the connection closes
	cancel context.CancelFunc
	once   sync.Once
	wake   chan struct{} // the writer has work; capacity 1

	// Reader only.
	maxID      uint32 // highest client stream ID seen
	recvCredit int    // connection bytes received and not yet credited back

	mu        sync.Mutex
	streams   map[uint32]*stream
	ctrl      []ctrlFrame // frames for the writer that are not stream output
	ready     []*stream   // streams with output, in order
	sendWin   int64       // connection send window
	initWin   int64       // the client's initial stream window
	maxFrame  int         // the client's largest frame payload
	closed    bool
	goingAway bool
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
)

// ctrlFrame is a frame other than HEADERS and DATA.
type ctrlFrame struct {
	kind ctrlKind
	id   uint32 // stream, or last stream for GOAWAY
	val  uint32 // window increment or error code
	ping [8]byte
}

func newConn(s *Server, nc, raw net.Conn) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		s: s, nc: nc, raw: raw, ctx: ctx, cancel: cancel,
		bw:       bufio.NewWriterSize(nc, ioBuffer),
		wake:     make(chan struct{}, 1),
		streams:  map[uint32]*stream{},
		sendWin:  65535,
		initWin:  65535,
		maxFrame: 16384,
	}
	c.fr = http2.NewFramer(c.bw, bufio.NewReaderSize(nc, ioBuffer))
	c.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	c.ctrl = append(c.ctrl,
		ctrlFrame{kind: ctrlSettings},
		ctrlFrame{kind: ctrlWindowUpdate, val: connWindow - 65535})
	return c
}

// serve runs the connection until it ends.
func (c *conn) serve() {
	var wg sync.WaitGroup
	wg.Go(func() {
		defer c.close()
		_ = c.writeLoop()
	})
	err := c.readLoop()
	if ce, ok := errors.AsType[http2.ConnectionError](err); ok {
		c.queueCtrl(ctrlFrame{kind: ctrlGoAway, id: c.lastID(), val: uint32(ce)})
		c.mu.Lock()
		c.goingAway = true
		c.mu.Unlock()
		// Let the writer send the GOAWAY, but not wait for long.
		select {
		case <-c.ctx.Done():
		case <-time.After(time.Second):
		}
	}
	c.close()
	wg.Wait()
}

// close ends the connection and every stream on it. It never writes, so
// it never waits for a client that stopped reading.
func (c *conn) close() {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		for _, st := range c.streams {
			st.reset = true
		}
		c.ready = nil
		c.mu.Unlock()
		c.cancel()
		_ = c.raw.Close()
	})
}

// goAway tells the client to open no more streams. The writer closes the
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

func (c *conn) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *conn) queueCtrl(f ctrlFrame) {
	c.mu.Lock()
	if !c.closed {
		c.ctrl = append(c.ctrl, f)
	}
	c.mu.Unlock()
	c.signal()
}

func (c *conn) readLoop() error {
	if err := c.nc.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(c.nc, preface); err != nil {
		return err
	}
	if string(preface) != http2.ClientPreface {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	f, err := c.fr.ReadFrame()
	if err != nil {
		return err
	}
	sf, ok := f.(*http2.SettingsFrame)
	if !ok || sf.IsAck() {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	if err := c.onSettings(sf); err != nil {
		return err
	}
	if err := c.nc.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	for {
		f, err := c.fr.ReadFrame()
		if err != nil {
			if se, ok := errors.AsType[http2.StreamError](err); ok {
				c.resetStream(se.StreamID, se.Code)
				continue
			}
			return err
		}
		if err := c.onFrame(f); err != nil {
			return err
		}
	}
}

func (c *conn) onFrame(f http2.Frame) error {
	switch f := f.(type) {
	case *http2.MetaHeadersFrame:
		return c.onHeaders(f)
	case *http2.DataFrame:
		return c.onData(f)
	case *http2.WindowUpdateFrame:
		return c.onWindowUpdate(f)
	case *http2.SettingsFrame:
		if f.IsAck() {
			return nil
		}
		return c.onSettings(f)
	case *http2.PingFrame:
		if !f.IsAck() {
			c.queueCtrl(ctrlFrame{kind: ctrlPingAck, ping: f.Data})
		}
	case *http2.RSTStreamFrame:
		c.resetStream(f.StreamID, 0)
	case *http2.PushPromiseFrame:
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	// GOAWAY from the client, PRIORITY and unknown frames need nothing.
	return nil
}

func (c *conn) onSettings(f *http2.SettingsFrame) error {
	c.mu.Lock()
	err := f.ForeachSetting(func(s http2.Setting) error {
		if err := s.Valid(); err != nil {
			return err
		}
		// The encoder never indexes, so the header table size does not
		// matter, and no other setting changes what the server sends.
		if s.ID == http2.SettingInitialWindowSize {
			delta := int64(s.Val) - c.initWin
			c.initWin = int64(s.Val)
			for _, st := range c.streams {
				st.sendWin += delta
			}
		}
		if s.ID == http2.SettingMaxFrameSize {
			c.maxFrame = int(s.Val)
		}
		return nil
	})
	if err == nil && !c.closed {
		c.ctrl = append(c.ctrl, ctrlFrame{kind: ctrlSettingsAck})
	}
	c.mu.Unlock()
	c.signal()
	return err
}

func (c *conn) onWindowUpdate(f *http2.WindowUpdateFrame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.StreamID == 0 {
		c.sendWin += int64(f.Increment)
		if c.sendWin > 1<<31-1 {
			return http2.ConnectionError(http2.ErrCodeFlowControl)
		}
	} else if st := c.streams[f.StreamID]; st != nil {
		st.sendWin += int64(f.Increment)
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
		st.reset = true
		st.out = nil
		st.queued = 0
	}
	if code != 0 && !c.closed {
		c.ctrl = append(c.ctrl, ctrlFrame{kind: ctrlRST, id: id, val: uint32(code)})
	}
	c.mu.Unlock()
	if st != nil {
		st.cancel()
	}
	c.signal()
}

// credit counts n received connection bytes and credits them back in
// batches.
func (c *conn) credit(n int) {
	c.recvCredit += n
	if c.recvCredit >= connWindow/4 {
		c.queueCtrl(ctrlFrame{kind: ctrlWindowUpdate, val: uint32(c.recvCredit)})
		c.recvCredit = 0
	}
}

func (c *conn) onHeaders(f *http2.MetaHeadersFrame) error {
	id := f.StreamID
	if id%2 == 0 || id <= c.maxID {
		c.mu.Lock()
		st := c.streams[id]
		c.mu.Unlock()
		if st != nil && f.StreamEnded() {
			// Trailers from the client end its side of the stream.
			return c.onEnd(st)
		}
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	c.mu.Lock()
	c.maxID = id
	refuse := c.goingAway || len(c.streams) >= maxStreams
	c.mu.Unlock()
	if refuse {
		c.resetStream(id, http2.ErrCodeRefusedStream)
		return nil
	}
	st := c.newStream(id, f)
	if st.rpc == nil && st.httpPath == "" {
		// gRPC to an unknown method; its error is already queued.
		if f.StreamEnded() {
			return c.onEnd(st)
		}
		return nil
	}
	if st.rpc != nil && st.rpc.stream != nil {
		go c.runStream(st)
	}
	if f.StreamEnded() {
		return c.onEnd(st)
	}
	return nil
}

func (c *conn) newStream(id uint32, f *http2.MetaHeadersFrame) *stream {
	var timeout time.Duration
	var contentType, method, path string
	needLeader := false
	for _, h := range f.Fields {
		switch h.Name {
		case ":method":
			method = h.Value
		case ":path":
			path = h.Value
		case "content-type":
			contentType = h.Value
		case "grpc-timeout":
			if d, ok := parseTimeout(h.Value); ok {
				timeout = d
			}
		case "hasleader":
			needLeader = needLeader || h.Value == "true"
		}
	}
	st := &stream{c: c, id: id, needLeader: needLeader}
	if timeout > 0 {
		st.ctx, st.cancel = context.WithTimeout(c.ctx, timeout)
	} else {
		st.ctx, st.cancel = context.WithCancel(c.ctx)
	}
	c.mu.Lock()
	st.sendWin = c.initWin
	c.streams[id] = st
	c.mu.Unlock()

	grpc := method == "POST" && strings.HasPrefix(contentType, "application/grpc")
	if !grpc {
		st.httpMethod, st.httpPath = method, path
		if st.httpPath == "" {
			st.httpPath = "/"
		}
		return st
	}
	st.rpc = methods[path]
	switch {
	case st.rpc == nil:
		st.finish(unknownMethod(path))
	case st.rpc.stream != nil:
		st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)
	}
	return st
}

func (c *conn) onData(f *http2.DataFrame) error {
	c.credit(int(f.Length))
	c.mu.Lock()
	st := c.streams[f.StreamID]
	c.mu.Unlock()
	if st == nil {
		// A stream that is gone or reset; the data is dropped.
		return nil
	}
	if st.rpc != nil && !st.done.Load() {
		if err := st.receive(f.Data()); err != nil {
			st.finish(err)
		}
	}
	if f.StreamEnded() {
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
	case st.done.Load():
	case st.httpPath != "":
		go c.runHTTP(st)
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
	okTrailers = headerBlock(hpack.HeaderField{Name: "grpc-status", Value: "0"})
)

// headerBlock encodes fields as literals that are never indexed.
func headerBlock(fields ...hpack.HeaderField) []byte {
	var b strings.Builder
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
