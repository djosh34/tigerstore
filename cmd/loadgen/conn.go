package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/internal/bufconn"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// Our receive windows. Responses are small next to these, so the windows are
// opened wide once and topped up in large steps instead of per frame.
const (
	recvWindow     = 1 << 30
	recvWindowStep = recvWindow / 2
	writeTimeout   = 10 * time.Second
	dialTimeout    = 5 * time.Second
	maxStreamID    = 1<<31 - 1
)

var epoch = time.Now()

// now is the monotonic time in nanoseconds since the process started.
func now() int64 { return int64(time.Since(epoch)) }

// handler receives the responses of one stream. Its methods run on the
// connection's reader goroutine with conn.mu held.
type handler interface {
	// message is called for each gRPC response message. msg is only valid
	// during the call.
	message(st *stream, msg []byte) error
	// done is called once when the stream ends, with the first error of the
	// call or nil. st is reused after done returns.
	done(st *stream, at int64, err error)
}

// stream is one gRPC call.
type stream struct {
	id       uint32
	h        handler
	intended int64  // when the open-loop schedule wanted to send it, ns since epoch
	sent     int64  // when it was written, ns since epoch
	slot     int    // workload data, such as the key index
	buf      []byte // a gRPC message split across DATA frames
	unacked  int64  // received bytes not yet returned in a stream WINDOW_UPDATE
	status   string // HTTP :status of the response headers
	msgs     int    // complete response messages received
	err      error  // the first bad response, reported when the stream ends
	// many allows more than one response message, as a RangeStream sends.
	many bool
	// written is the fewest bytes the call must make durable on each
	// node; read is the key and value bytes its answer carried. Both feed
	// the ceilings of cmd/benchreport.
	written, read int64
}

type outBuf []byte

func (o *outBuf) Write(p []byte) (int, error) {
	*o = append(*o, p...)
	return len(p), nil
}

// conn is one HTTP/2 connection carrying many gRPC streams. One writer
// goroutine starts streams and flushes; one reader goroutine reads frames
// and completes streams.
type conn struct {
	addr string
	nc   net.Conn
	fr   *http2.Framer // writes to out
	br   *bufio.Reader // reads from nc, owned by the reader goroutine
	out  outBuf        // frames not yet written, owned by the writer goroutine

	mu         sync.Mutex
	streams    map[uint32]*stream
	free       []*stream
	nextID     uint32
	sendWin    int64  // connection-level send window
	maxStreams uint32 // the server's SETTINGS_MAX_CONCURRENT_STREAMS
	streamWin  uint32 // the server's SETTINGS_INITIAL_WINDOW_SIZE
	err        error  // set once the connection failed
	closing    bool
	// Control frames the reader wants sent; the writer sends them.
	settingsAck bool
	pings       [][8]byte
	connWinIncr uint32
	streamIncr  []streamIncr

	wake  chan struct{} // tells the writer that a stream ended or a window or control frame changed
	ready chan struct{} // closed on the server's first SETTINGS
	gone  chan struct{} // closed when the reader exits

	// enc encodes the request headers, which a header block indexes the
	// first time it sends each field, as grpc-go's does; blocks holds the
	// block last encoded for each path, which is the same bytes as long as
	// no block added to the table since (gen). Both are guarded by mu.
	enc    *hpack.Encoder
	encBuf bytes.Buffer
	gen    int
	blocks map[string]headerBlock

	// Reader-only state.
	dec          *hpack.Decoder
	recvUnacked  int64
	gotSettings  bool
	hdrStream    uint32
	hdrEnd       bool
	hdrStatus    string
	grpcStatus   string
	grpcMessage  string
	hasGRPCState bool
}

type streamIncr struct {
	id   uint32
	incr uint32
}

// clientTLS, if set, is the TLS configuration of every connection;
// without it connections are plain text (HTTP/2 with prior knowledge).
var clientTLS *tls.Config

func dial(addr string) (*conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	var nc net.Conn
	var err error
	if nc, err = d.Dial("tcp", addr); err != nil {
		return nil, err
	}
	if clientTLS != nil {
		if nc, err = clientHandshake(nc, addr); err != nil {
			return nil, err
		}
	}
	c := &conn{
		addr:       addr,
		nc:         nc,
		streams:    make(map[uint32]*stream),
		nextID:     1,
		sendWin:    65535,
		maxStreams: math.MaxUint32,
		streamWin:  65535,
		wake:       make(chan struct{}, 1),
		ready:      make(chan struct{}),
		gone:       make(chan struct{}),
	}
	// Frames are read through a buffer above TLS too: each read of TLS
	// checks the handshake under a deferred unlock, which costs more than
	// copying a record once more. The framer only writes.
	c.br = bufio.NewReaderSize(nc, 64<<10)
	c.fr = http2.NewFramer(&c.out, nil)
	c.dec = hpack.NewDecoder(4096, c.headerField)

	c.out = append(c.out, http2.ClientPreface...)
	// Writes into c.out cannot fail.
	_ = c.fr.WriteSettings(
		http2.Setting{ID: http2.SettingInitialWindowSize, Val: recvWindow},
		http2.Setting{ID: http2.SettingEnablePush, Val: 0},
	)
	_ = c.fr.WriteWindowUpdate(0, recvWindow-65535)
	if err := c.flush(); err != nil {
		_ = nc.Close()
		return nil, err
	}
	go c.readLoop()

	select {
	case <-c.ready:
	case <-c.gone:
		return nil, c.failure()
	case <-time.After(5 * time.Second):
		c.close()
		return nil, fmt.Errorf("%s: no HTTP/2 SETTINGS from the server", addr)
	}
	if c.streamWin < maxDataFrame+grpcPrefixLen {
		c.close()
		return nil, fmt.Errorf("%s: server stream window %d is smaller than one request", addr, c.streamWin)
	}
	return c, nil
}

// clientHandshake runs TLS over nc, reading its records through a large
// buffer (bufconn).
func clientHandshake(nc net.Conn, addr string) (net.Conn, error) {
	cfg := clientTLS
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Join(err, nc.Close())
		}
		cfg = cfg.Clone()
		cfg.ServerName = host
	}
	tc := tls.Client(bufconn.New(nc, 256<<10), cfg)
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(err, nc.Close())
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		return nil, errors.Join(fmt.Errorf("%s: the server did not negotiate h2", addr), tc.Close())
	}
	return tc, nil
}

func (c *conn) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *conn) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// close shuts the connection. Streams still open, such as watches, end
// without being reported as errors.
func (c *conn) close() {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	_ = c.nc.Close()
	<-c.gone
}

// newStream returns a reset stream. The caller holds c.mu.
func (c *conn) newStream(h handler) *stream {
	var st *stream
	if n := len(c.free); n > 0 {
		st = c.free[n-1]
		c.free = c.free[:n-1]
	} else {
		st = new(stream)
	}
	*st = stream{h: h, buf: st.buf[:0]}
	return st
}

// canStart reports whether a request of dataLen DATA bytes can start now,
// with at most limit streams in flight. The caller holds c.mu.
func (c *conn) canStart(dataLen, limit int) bool {
	return c.err == nil &&
		len(c.streams) < limit &&
		uint32(len(c.streams)) < c.maxStreams &&
		c.sendWin >= int64(dataLen) &&
		int64(c.streamWin) >= int64(dataLen) &&
		c.nextID < maxStreamID
}

// startStream appends the request t on a new stream for st and returns the
// bytes of its DATA frames, for the caller to fill in keys and values
// before the next flush. The caller holds c.mu and has checked canStart.
func (c *conn) startStream(st *stream, t *template) []byte {
	st.id = c.nextID
	c.nextID += 2
	block := c.headerBlock(t.path)
	c.out = appendFrameHeader(c.out, len(block), http2.FrameHeaders, http2.FlagHeadersEndHeaders)
	binary.BigEndian.PutUint32(c.out[len(c.out)-4:], st.id)
	c.out = append(c.out, block...)
	start := len(c.out)
	c.out = append(c.out, t.b...)
	b := c.out[start:]
	for _, off := range t.frames {
		binary.BigEndian.PutUint32(b[off+5:], st.id)
	}
	c.streams[st.id] = st
	c.sendWin -= int64(t.dataLen)
	return b
}

// startMessage appends a call of path on a new stream for st that sends
// the one message m and ends the stream, framed at send time, for requests
// whose size changes from call to call. The caller holds c.mu and has
// checked canStart with messageLen(m).
func (c *conn) startMessage(st *stream, path string, m []byte) {
	st.id = c.nextID
	c.nextID += 2
	block := c.headerBlock(path)
	c.out = appendFrameHeader(c.out, len(block), http2.FrameHeaders, http2.FlagHeadersEndHeaders)
	binary.BigEndian.PutUint32(c.out[len(c.out)-4:], st.id)
	c.out = append(c.out, block...)
	total := messageLen(m)
	for pos := 0; pos < total; {
		end := min(pos+maxDataFrame, total)
		var flags http2.Flags
		if end == total {
			flags = http2.FlagDataEndStream
		}
		c.out = appendFrameHeader(c.out, end-pos, http2.FrameData, flags)
		binary.BigEndian.PutUint32(c.out[len(c.out)-4:], st.id)
		if pos == 0 {
			c.out = binary.BigEndian.AppendUint32(append(c.out, 0), uint32(len(m)))
			pos = grpcPrefixLen
		}
		c.out = append(c.out, m[pos-grpcPrefixLen:end-grpcPrefixLen]...)
		pos = end
	}
	c.streams[st.id] = st
	c.sendWin -= int64(messageLen(m))
}

// messageLen is the DATA bytes of a call that sends m.
func messageLen(m []byte) int { return grpcPrefixLen + len(m) }

// headerBlock is a header block of a path, valid while the table is at
// generation gen.
type headerBlock struct {
	b   []byte
	gen int
}

// headerBlock returns the request headers of a call to path, encoded for
// this connection: fields new to its table are added to it, as grpc-go's
// encoder does, so later calls send one-byte references. Blocks go out in
// the order they are encoded, under c.mu.
func (c *conn) headerBlock(path string) []byte {
	if h, ok := c.blocks[path]; ok && h.gen == c.gen {
		return h.b
	}
	enc := c.encoder()
	c.encBuf.Reset()
	for _, f := range [...]hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: path},
		{Name: ":authority", Value: c.addr},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		// Writing to a bytes.Buffer cannot fail.
		_ = enc.WriteField(f)
	}
	b := bytes.Clone(c.encBuf.Bytes())
	// A block of only indexed fields leaves the table as it is and can go
	// out again; any other adds to it, and the blocks encoded before may
	// no longer hold.
	for _, h := range b {
		if h&0x80 == 0 {
			c.gen++
			return b
		}
	}
	c.blocks[path] = headerBlock{b: b, gen: c.gen}
	return b
}

// encoder returns the connection's header encoder, made on first use.
func (c *conn) encoder() *hpack.Encoder {
	if c.enc == nil {
		c.enc, c.blocks = hpack.NewEncoder(&c.encBuf), map[string]headerBlock{}
	}
	return c.enc
}

// appendControl appends the control frames the reader asked for. The caller
// holds c.mu.
func (c *conn) appendControl() {
	// Writes into c.out cannot fail.
	if c.settingsAck {
		_ = c.fr.WriteSettingsAck()
		c.settingsAck = false
	}
	for _, p := range c.pings {
		_ = c.fr.WritePing(true, p)
	}
	c.pings = c.pings[:0]
	if c.connWinIncr > 0 {
		_ = c.fr.WriteWindowUpdate(0, c.connWinIncr)
		c.connWinIncr = 0
	}
	for _, s := range c.streamIncr {
		_ = c.fr.WriteWindowUpdate(s.id, s.incr)
	}
	c.streamIncr = c.streamIncr[:0]
}

// flush writes the pending frames. Only the writer goroutine calls it.
func (c *conn) flush() error {
	if len(c.out) == 0 {
		return nil
	}
	if err := c.nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	_, err := c.nc.Write(c.out)
	c.out = c.out[:0]
	if err != nil {
		return fmt.Errorf("%s: write: %w", c.addr, err)
	}
	return nil
}

func (c *conn) readLoop() {
	defer close(c.gone)
	err := c.read()
	at := now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return
	}
	if c.err == nil {
		c.err = fmt.Errorf("%s: %w", c.addr, err)
	}
	for id, st := range c.streams {
		delete(c.streams, id)
		st.h.done(st, at, c.err)
	}
	c.signal()
}

// HTTP/2 frame flags (RFC 9113, 6).
const (
	flagEndStream  = 0x1
	flagAck        = 0x1
	flagEndHeaders = 0x4
	flagPadded     = 0x8
	flagPriority   = 0x20
)

var errFrame = errors.New("malformed HTTP/2 frame")

// read reads frames in place from c.br: a frame's payload is a slice of the
// buffer until the next frame, which saves copying every byte the server
// sends once more. The server's frames are at most 16 KiB, the HTTP/2
// default, as the generator never raises SETTINGS_MAX_FRAME_SIZE.
func (c *conn) read() error {
	size := 0
	for {
		if _, err := c.br.Discard(size); err != nil {
			return err
		}
		head, err := c.br.Peek(9)
		if err != nil {
			return err
		}
		n := int(head[0])<<16 | int(head[1])<<8 | int(head[2])
		typ, flags := http2.FrameType(head[3]), head[4]
		id := binary.BigEndian.Uint32(head[5:9]) & (1<<31 - 1)
		if 9+n > c.br.Size() {
			return fmt.Errorf("a %v frame of %d bytes, past the largest frame", typ, n)
		}
		frame, err := c.br.Peek(9 + n)
		if err != nil {
			return err
		}
		size = 9 + n
		p := frame[9:]
		//exhaustive:ignore // other frames need no answer
		switch typ {
		case http2.FrameData:
			data, ok := unpad(p, flags)
			if !ok {
				return errFrame
			}
			err = c.onData(id, n, data, flags&flagEndStream != 0)
		case http2.FrameHeaders:
			frag, ok := unpad(p, flags)
			if ok && flags&flagPriority != 0 {
				ok = len(frag) >= 5
				frag = frag[min(5, len(frag)):]
			}
			if !ok {
				return errFrame
			}
			c.hdrStream = id
			c.hdrEnd = flags&flagEndStream != 0
			c.hdrStatus, c.grpcStatus, c.grpcMessage, c.hasGRPCState = "", "", "", false
			err = c.onHeaderBlock(frag, flags&flagEndHeaders != 0)
		case http2.FrameContinuation:
			if id != c.hdrStream {
				return errors.New("CONTINUATION on the wrong stream")
			}
			err = c.onHeaderBlock(p, flags&flagEndHeaders != 0)
		case http2.FrameRSTStream:
			if len(p) != 4 {
				return errFrame
			}
			c.endStream(id, fmt.Errorf("stream reset: %v", http2.ErrCode(binary.BigEndian.Uint32(p))))
		case http2.FrameSettings:
			if flags&flagAck == 0 {
				err = c.onSettings(p)
			}
		case http2.FrameWindowUpdate:
			if len(p) != 4 {
				return errFrame
			}
			if id == 0 {
				c.mu.Lock()
				c.sendWin += int64(binary.BigEndian.Uint32(p) & (1<<31 - 1))
				c.mu.Unlock()
				c.signal()
			}
		case http2.FramePing:
			if len(p) != 8 {
				return errFrame
			}
			if flags&flagAck == 0 {
				c.mu.Lock()
				c.pings = append(c.pings, [8]byte(p))
				c.mu.Unlock()
				c.signal()
			}
		case http2.FrameGoAway:
			if len(p) < 8 {
				return errFrame
			}
			return fmt.Errorf("GOAWAY %v: %q", http2.ErrCode(binary.BigEndian.Uint32(p[4:8])), p[8:])
		}
		if err != nil {
			return err
		}
	}
}

// unpad returns the payload of a DATA or HEADERS frame without its
// padding, and false if the padding does not fit.
func unpad(p []byte, flags byte) ([]byte, bool) {
	if flags&flagPadded == 0 {
		return p, true
	}
	if len(p) == 0 || int(p[0]) >= len(p) {
		return nil, false
	}
	return p[1 : len(p)-int(p[0])], true
}

// onSettings applies the server's settings, 6 bytes each.
func (c *conn) onSettings(p []byte) error {
	if len(p)%6 != 0 {
		return errFrame
	}
	c.mu.Lock()
	var err error
	for ; len(p) > 0 && err == nil; p = p[6:] {
		v := binary.BigEndian.Uint32(p[2:6])
		//exhaustive:ignore // the default case handles the rest
		switch http2.SettingID(binary.BigEndian.Uint16(p[:2])) {
		case http2.SettingMaxConcurrentStreams:
			c.maxStreams = v
		case http2.SettingInitialWindowSize:
			c.streamWin = v
		case http2.SettingHeaderTableSize:
			c.encoder().SetMaxDynamicTableSizeLimit(v)
			c.gen++
		case http2.SettingMaxFrameSize:
			if v < maxDataFrame {
				err = fmt.Errorf("server max frame size %d is below the HTTP/2 minimum", v)
			}
		default:
		}
	}
	c.settingsAck = true
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if !c.gotSettings {
		c.gotSettings = true
		close(c.ready)
	}
	c.signal()
	return nil
}

// onData takes a DATA frame of stream id: length bytes against the flow
// control windows, of which data are the message bytes.
func (c *conn) onData(id uint32, length int, data []byte, end bool) error {
	n := int64(length)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recvUnacked += n
	if c.recvUnacked >= recvWindowStep {
		c.connWinIncr += uint32(c.recvUnacked)
		c.recvUnacked = 0
		c.signal()
	}
	st := c.streams[id]
	if st == nil {
		return nil
	}
	st.unacked += n
	if st.unacked >= recvWindowStep {
		c.streamIncr = append(c.streamIncr, streamIncr{st.id, uint32(st.unacked)})
		st.unacked = 0
		c.signal()
	}
	// A bad response is reported when the stream ends, because until then
	// it still counts against the server's stream limit.
	if st.err == nil {
		st.err = c.messages(st, data)
	}
	if end {
		c.finish(st, errors.New("stream ended without trailers"))
	}
	return nil
}

// messages hands each complete gRPC message in p to the stream's handler and
// keeps a trailing partial message for the next DATA frame.
func (c *conn) messages(st *stream, p []byte) error {
	if len(st.buf) > 0 {
		st.buf = append(st.buf, p...)
		p = st.buf
	}
	for len(p) >= grpcPrefixLen {
		if p[0] != 0 {
			return errors.New("compressed gRPC message")
		}
		n := binary.BigEndian.Uint32(p[1:grpcPrefixLen])
		if uint64(len(p)-grpcPrefixLen) < uint64(n) {
			break
		}
		st.msgs++
		if err := st.h.message(st, p[grpcPrefixLen:grpcPrefixLen+int(n)]); err != nil {
			return err
		}
		p = p[grpcPrefixLen+int(n):]
	}
	st.buf = append(st.buf[:0], p...)
	return nil
}

func (c *conn) onHeaderBlock(frag []byte, end bool) error {
	if _, err := c.dec.Write(frag); err != nil {
		return err
	}
	if !end {
		return nil
	}
	if err := c.dec.Close(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.streams[c.hdrStream]
	if st == nil {
		return nil
	}
	if !c.hdrEnd {
		st.status = c.hdrStatus
		return nil
	}
	// Trailers, or a trailers-only response that carries :status as well.
	status := st.status
	if c.hdrStatus != "" {
		status = c.hdrStatus
	}
	var err error
	switch {
	case status != "200":
		err = fmt.Errorf("HTTP status %q", status)
	case !c.hasGRPCState:
		err = errors.New("no grpc-status in trailers")
	case c.grpcStatus != "0":
		err = grpcError(c.grpcStatus, c.grpcMessage)
	case len(st.buf) > 0:
		err = errors.New("truncated gRPC message")
	}
	c.finish(st, err)
	return nil
}

func grpcError(code, message string) error {
	n, err := strconv.Atoi(code)
	if err != nil {
		return fmt.Errorf("grpc-status %q: %s", code, message)
	}
	return fmt.Errorf("grpc %s: %s", grpcCodeName(n), message)
}

func grpcCodeName(n int) string {
	names := []string{
		"OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound",
		"AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition", "Aborted",
		"OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated",
	}
	if n >= 0 && n < len(names) {
		return names[n]
	}
	return "code " + strconv.Itoa(n)
}

func (c *conn) headerField(f hpack.HeaderField) {
	switch f.Name {
	case ":status":
		c.hdrStatus = f.Value
	case "grpc-status":
		c.grpcStatus = f.Value
		c.hasGRPCState = true
	case "grpc-message":
		c.grpcMessage = f.Value
	}
}

func (c *conn) endStream(id uint32, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.streams[id]; st != nil {
		c.finish(st, err)
	}
}

// finish ends st with its first error. The caller holds c.mu.
func (c *conn) finish(st *stream, err error) {
	if st.err != nil {
		err = st.err
	}
	delete(c.streams, st.id)
	st.h.done(st, now(), err)
	c.free = append(c.free, st)
	c.signal()
}
