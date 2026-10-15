package main

// Raw HTTP/2 over TLS, driven by exactly one goroutine: it writes requests,
// reads frames and answers control frames. No locks.

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"io"
	"syscall"
	"os"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const (
	recvWindow = 1 << 30
	recvStep   = 1 << 28
	maxFrame   = 16384
	fData      = 0
	fHeaders   = 1
	fRST       = 3
	fSettings  = 4
	fPing      = 6
	fGoAway    = 7
	fWinUpd    = 8
	fCont      = 9
	flEnd      = 0x1 // END_STREAM, also ACK on SETTINGS/PING
	flEndHdr   = 0x4
	flPadded   = 0x8
	flPrio     = 0x20
)

type h2 struct {
	tc   *tls.Conn
	raw  *rawConn
	rbuf []byte
	r, w int
	out  []byte

	sendWin     int64
	peerInitWin int64
	recvUnacked int64
	gotSettings bool

	dec              *hpack.Decoder
	dStatus, dGrpc   string
	dMsg             string
	dHasGrpc         bool
	okHdr, okTrailer []byte // learned blocks that touch no HPACK dynamic table
}

var clientTLS *tls.Config

func dialH2(addr string) (*h2, error) {
	d := &net.Dialer{Timeout: 5 * time.Second}
	nc, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	sc, err := nc.(*net.TCPConn).SyscallConn()
	if err != nil {
		return nil, err
	}
	rw := &rawConn{Conn: nc, rc: sc}
	cfg := clientTLS.Clone()
	host, _, _ := net.SplitHostPort(addr)
	cfg.ServerName = host
	tc := tls.Client(rw, cfg)
	if err := tc.Handshake(); err != nil {
		return nil, err
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		tc.Close()
		return nil, errors.New("no h2")
	}
	c := &h2{tc: tc, raw: rw, rbuf: make([]byte, 512<<10), sendWin: 65535, peerInitWin: 65535}
	c.dec = hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		switch f.Name {
		case ":status":
			c.dStatus = f.Value
		case "grpc-status":
			c.dGrpc = f.Value
			c.dHasGrpc = true
		case "grpc-message":
			c.dMsg = f.Value
		}
	})
	c.out = append(c.out, http2.ClientPreface...)
	c.out = appendFH(c.out, 12, fSettings, 0, 0)
	c.out = binary.BigEndian.AppendUint16(c.out, uint16(http2.SettingInitialWindowSize))
	c.out = binary.BigEndian.AppendUint32(c.out, recvWindow)
	c.out = binary.BigEndian.AppendUint16(c.out, uint16(http2.SettingEnablePush))
	c.out = binary.BigEndian.AppendUint32(c.out, 0)
	c.out = appendWinUpd(c.out, 0, recvWindow-65535)
	if err := c.flush(); err != nil {
		return nil, err
	}
	tc.SetReadDeadline(time.Now().Add(5 * time.Second))
	for !c.gotSettings {
		typ, flags, sid, p, ok, err := c.frame()
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := c.fill(); err != nil {
				return nil, err
			}
			continue
		}
		if _, err := c.control(typ, flags, sid, p); err != nil {
			return nil, err
		}
	}
	tc.SetReadDeadline(time.Time{})
	return c, c.flush()
}

func appendFH(b []byte, n int, typ, flags byte, sid uint32) []byte {
	return append(b, byte(n>>16), byte(n>>8), byte(n), typ, flags, byte(sid>>24), byte(sid>>16), byte(sid>>8), byte(sid))
}

func appendWinUpd(b []byte, sid uint32, inc uint32) []byte {
	b = appendFH(b, 4, fWinUpd, 0, sid)
	return binary.BigEndian.AppendUint32(b, inc)
}

func (c *h2) flush() error {
	if len(c.out) == 0 {
		return nil
	}
	if debugFrames {
		dumpFrames(c.out)
	}
	_, err := c.tc.Write(c.out)
	c.out = c.out[:0]
	return err
}

// fill reads once more from the connection.
func (c *h2) fill() error {
	if c.r > 0 && (c.r == c.w || len(c.rbuf)-c.w < 64<<10) {
		c.w = copy(c.rbuf, c.rbuf[c.r:c.w])
		c.r = 0
	}
	n, err := c.tc.Read(c.rbuf[c.w:])
	c.w += n
	if n > 0 {
		return nil
	}
	return err
}

// tryFill reads only what is already there, without blocking. got is
// false if nothing was.
func (c *h2) tryFill() (got bool, err error) {
	if c.r > 0 && (c.r == c.w || len(c.rbuf)-c.w < 64<<10) {
		c.w = copy(c.rbuf, c.rbuf[c.r:c.w])
		c.r = 0
	}
	c.raw.nonblock = true
	n, err := c.tc.Read(c.rbuf[c.w:])
	c.raw.nonblock = false
	c.w += n
	if n > 0 {
		return true, nil
	}
	if err == errWouldBlock {
		return false, nil
	}
	return false, err
}

// rawConn lets one Read be non-blocking: it returns errWouldBlock, which
// crypto/tls treats as temporary, instead of waiting for data.
type rawConn struct {
	net.Conn
	rc       syscall.RawConn
	nonblock bool
}

type wouldBlock struct{}

func (wouldBlock) Error() string   { return "would block" }
func (wouldBlock) Timeout() bool   { return true }
func (wouldBlock) Temporary() bool { return true }

var errWouldBlock error = wouldBlock{}

func (r *rawConn) Read(b []byte) (int, error) {
	if !r.nonblock {
		return r.Conn.Read(b)
	}
	var n int
	var err error
	if e := r.rc.Read(func(fd uintptr) bool {
		n, err = syscall.Read(int(fd), b)
		return true
	}); e != nil {
		return 0, e
	}
	if err == syscall.EAGAIN {
		return 0, errWouldBlock
	}
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// frame returns the next complete frame in the read buffer, ok=false if
// none. The payload is valid until the next fill.
func (c *h2) frame() (typ, flags byte, sid uint32, p []byte, ok bool, err error) {
	b := c.rbuf[c.r:c.w]
	if len(b) < 9 {
		return
	}
	n := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
	if n > maxFrame {
		err = fmt.Errorf("frame of %d bytes above our max frame size", n)
		return
	}
	if len(b) < 9+n {
		return
	}
	typ, flags = b[3], b[4]
	sid = binary.BigEndian.Uint32(b[5:9]) & 0x7fffffff
	p = b[9 : 9+n]
	c.r += 9 + n
	ok = true
	if debugFrames {
		fmt.Fprintf(os.Stderr, "recv: type %d flags %x sid %d len %d\n", typ, flags, sid, n)
	}
	return
}

// control handles connection-level frames. handled=false means a stream
// frame (DATA, HEADERS, RST_STREAM) for the caller.
func (c *h2) control(typ, flags byte, sid uint32, p []byte) (handled bool, err error) {
	switch typ {
	case fSettings:
		if flags&flEnd != 0 {
			return true, nil
		}
		if len(p)%6 != 0 {
			return true, errors.New("bad SETTINGS")
		}
		for i := 0; i < len(p); i += 6 {
			id := http2.SettingID(binary.BigEndian.Uint16(p[i:]))
			v := binary.BigEndian.Uint32(p[i+2:])
			switch id {
			case http2.SettingInitialWindowSize:
				c.peerInitWin = int64(v)
			case http2.SettingMaxFrameSize:
				if v < maxFrame {
					return true, errors.New("bad max frame size")
				}
			}
		}
		c.out = appendFH(c.out, 0, fSettings, flEnd, 0)
		c.gotSettings = true
		return true, nil
	case fPing:
		if flags&flEnd == 0 && len(p) == 8 {
			c.out = appendFH(c.out, 8, fPing, flEnd, 0)
			c.out = append(c.out, p...)
		}
		return true, nil
	case fWinUpd:
		if len(p) != 4 {
			return true, errors.New("bad WINDOW_UPDATE")
		}
		if sid == 0 {
			c.sendWin += int64(binary.BigEndian.Uint32(p) & 0x7fffffff)
		}
		// Stream windows: each unary request is far below the initial
		// stream window and a stream carries one request.
		return true, nil
	case fGoAway:
		return true, fmt.Errorf("GOAWAY %x", p)
	case fCont:
		return true, errors.New("CONTINUATION not supported")
	case fData, fHeaders, fRST:
		return false, nil
	}
	return true, nil // unknown frame types are ignored
}

// dataPayload strips padding and accounts the connection receive window.
func (c *h2) dataPayload(flags byte, p []byte) ([]byte, error) {
	c.recvUnacked += int64(len(p))
	if c.recvUnacked >= recvStep {
		c.out = appendWinUpd(c.out, 0, uint32(c.recvUnacked))
		c.recvUnacked = 0
	}
	if flags&flPadded != 0 {
		if len(p) < 1 || int(p[0]) > len(p)-1 {
			return nil, errors.New("bad padding")
		}
		p = p[1 : len(p)-int(p[0])]
	}
	return p, nil
}

func headerBlock(flags byte, p []byte) ([]byte, error) {
	if flags&flEndHdr == 0 {
		return nil, errors.New("CONTINUATION not supported")
	}
	if flags&flPadded != 0 {
		if len(p) < 1 || int(p[0]) > len(p)-1 {
			return nil, errors.New("bad padding")
		}
		p = p[1 : len(p)-int(p[0])]
	}
	if flags&flPrio != 0 {
		if len(p) < 5 {
			return nil, errors.New("bad priority")
		}
		p = p[5:]
	}
	return p, nil
}

// checkBlock checks response headers (:status 200) or trailers (grpc-status
// 0). A block equal to one already decoded and found good, and which uses
// only the static table, means the same again and skips the decoder.
func (c *h2) checkBlock(b []byte, trailers bool) error {
	if trailers {
		if c.okTrailer != nil && bytes.Equal(b, c.okTrailer) {
			return nil
		}
	} else if c.okHdr != nil && bytes.Equal(b, c.okHdr) {
		return nil
	}
	c.dStatus, c.dGrpc, c.dMsg, c.dHasGrpc = "", "", "", false
	if _, err := c.dec.Write(b); err != nil {
		return err
	}
	if err := c.dec.Close(); err != nil {
		return err
	}
	if trailers {
		if c.dStatus != "" && c.dStatus != "200" {
			return fmt.Errorf("HTTP status %q", c.dStatus)
		}
		if !c.dHasGrpc {
			return errors.New("no grpc-status")
		}
		if c.dGrpc != "0" {
			return fmt.Errorf("grpc-status %s: %s", c.dGrpc, c.dMsg)
		}
		if c.dStatus == "" && staticOnly(b) {
			c.okTrailer = bytes.Clone(b)
		}
		return nil
	}
	if c.dStatus != "200" {
		return fmt.Errorf("HTTP status %q", c.dStatus)
	}
	if c.dHasGrpc {
		return errors.New("grpc-status in response headers")
	}
	if staticOnly(b) {
		c.okHdr = bytes.Clone(b)
	}
	return nil
}

// staticOnly reports whether an HPACK block neither reads nor writes the
// dynamic table, so its meaning does not depend on decoder state.
func staticOnly(b []byte) bool {
	for len(b) > 0 {
		c := b[0]
		switch {
		case c&0x80 != 0:
			idx, n := hpackInt(b, 7)
			if n < 0 || idx == 0 || idx > 61 {
				return false
			}
			b = b[n:]
		case c&0xc0 == 0x40, c&0xe0 == 0x20:
			return false
		default: // literal without indexing / never indexed
			idx, n := hpackInt(b, 4)
			if n < 0 || idx > 61 {
				return false
			}
			b = b[n:]
			if idx == 0 {
				if b = hpackSkipString(b); b == nil {
					return false
				}
			}
			if b = hpackSkipString(b); b == nil {
				return false
			}
		}
	}
	return true
}

func hpackInt(b []byte, prefix uint) (uint64, int) {
	if len(b) == 0 {
		return 0, -1
	}
	max := uint64(1)<<prefix - 1
	v := uint64(b[0]) & max
	if v < max {
		return v, 1
	}
	var m uint
	for i := 1; i < len(b) && i < 10; i++ {
		v += uint64(b[i]&0x7f) << m
		m += 7
		if b[i]&0x80 == 0 {
			return v, i + 1
		}
	}
	return 0, -1
}

func hpackSkipString(b []byte) []byte {
	l, n := hpackInt(b, 7)
	if n < 0 || uint64(len(b)-n) < l {
		return nil
	}
	r := b[n+int(l):]
	if r == nil {
		r = []byte{}
	}
	return r
}

// reqHeaders is a HEADERS block for a gRPC call, all fields never indexed.
func reqHeaders(authority, path string) []byte {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for _, f := range [][2]string{
		{":method", "POST"}, {":scheme", "http"}, {":path", path}, {":authority", authority},
		{"content-type", "application/grpc"}, {"te", "trailers"},
	} {
		_ = enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1], Sensitive: true})
	}
	return buf.Bytes()
}
