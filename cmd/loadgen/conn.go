package main

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// Our receive windows. Responses are small next to these, so the windows are
// opened wide once and topped up in large steps instead of per frame.
const (
	recvWindow     = 1 << 30
	recvWindowStep = recvWindow / 2
	writeTimeout   = 10 * time.Second
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
	fr   *http2.Framer // reads from nc, writes to out
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
	d := &net.Dialer{Timeout: 5 * time.Second}
	var nc net.Conn
	var err error
	if clientTLS != nil {
		var tc *tls.Conn
		tc, err = tls.DialWithDialer(d, "tcp", addr, clientTLS)
		if err == nil && tc.ConnectionState().NegotiatedProtocol != "h2" {
			err = errors.Join(fmt.Errorf("%s: the server did not negotiate h2", addr), tc.Close())
		}
		nc = tc
	} else {
		nc, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
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
	c.fr = http2.NewFramer(&c.out, bufio.NewReaderSize(nc, 64<<10))
	c.fr.SetReuseFrames()
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

// canStart reports whether a request of t can start now. The caller holds
// c.mu.
func (c *conn) canStart(t *template, limit int) bool {
	return c.err == nil &&
		len(c.streams) < limit &&
		uint32(len(c.streams)) < c.maxStreams &&
		c.sendWin >= int64(t.dataLen) &&
		c.nextID < maxStreamID
}

// startStream appends the request t on a new stream for st and returns its
// bytes, for the caller to fill in keys and values before the next flush.
// The caller holds c.mu and has checked canStart.
func (c *conn) startStream(st *stream, t *template) []byte {
	st.id = c.nextID
	c.nextID += 2
	start := len(c.out)
	c.out = append(c.out, t.b...)
	b := c.out[start:]
	binary.BigEndian.PutUint32(b[5:], st.id)
	binary.BigEndian.PutUint32(b[t.data+5:], st.id)
	c.streams[st.id] = st
	c.sendWin -= int64(t.dataLen)
	return b
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

func (c *conn) read() error {
	for {
		f, err := c.fr.ReadFrame()
		if err != nil {
			return err
		}
		switch f := f.(type) {
		case *http2.DataFrame:
			err = c.onData(f)
		case *http2.HeadersFrame:
			c.hdrStream = f.StreamID
			c.hdrEnd = f.StreamEnded()
			c.hdrStatus, c.grpcStatus, c.grpcMessage, c.hasGRPCState = "", "", "", false
			err = c.onHeaderBlock(f.HeaderBlockFragment(), f.HeadersEnded())
		case *http2.ContinuationFrame:
			if f.StreamID != c.hdrStream {
				return errors.New("CONTINUATION on the wrong stream")
			}
			err = c.onHeaderBlock(f.HeaderBlockFragment(), f.HeadersEnded())
		case *http2.RSTStreamFrame:
			c.endStream(f.StreamID, fmt.Errorf("stream reset: %v", f.ErrCode))
		case *http2.SettingsFrame:
			err = c.onSettings(f)
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				c.mu.Lock()
				c.sendWin += int64(f.Increment)
				c.mu.Unlock()
				c.signal()
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				c.mu.Lock()
				c.pings = append(c.pings, f.Data)
				c.mu.Unlock()
				c.signal()
			}
		case *http2.GoAwayFrame:
			return fmt.Errorf("GOAWAY %v: %q", f.ErrCode, f.DebugData())
		}
		if err != nil {
			return err
		}
	}
}

func (c *conn) onSettings(f *http2.SettingsFrame) error {
	if f.IsAck() {
		return nil
	}
	c.mu.Lock()
	err := f.ForeachSetting(func(s http2.Setting) error {
		//exhaustive:ignore // the default case handles the rest
		switch s.ID {
		case http2.SettingMaxConcurrentStreams:
			c.maxStreams = s.Val
		case http2.SettingInitialWindowSize:
			c.streamWin = s.Val
		case http2.SettingMaxFrameSize:
			if s.Val < maxDataFrame {
				return fmt.Errorf("server max frame size %d is below the HTTP/2 minimum", s.Val)
			}
		default:
		}
		return nil
	})
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

func (c *conn) onData(f *http2.DataFrame) error {
	n := int64(f.Length)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recvUnacked += n
	if c.recvUnacked >= recvWindowStep {
		c.connWinIncr += uint32(c.recvUnacked)
		c.recvUnacked = 0
		c.signal()
	}
	st := c.streams[f.StreamID]
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
		st.err = c.messages(st, f.Data())
	}
	if f.StreamEnded() {
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
