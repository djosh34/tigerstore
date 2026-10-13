package server

import (
	"net"
	"os"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/credentials"

	"github.com/djosh34/tigerstore/internal/expclock"
)

// This wrapper observes plaintext HTTP/2 after TLS, only in an explicitly
// requested diagnostic run. It records sizes and selected control fields,
// never header values, message bodies, certificates or GOAWAY debug data.
func pre47TLSCredentials(c credentials.TransportCredentials) credentials.TransportCredentials {
	if os.Getenv("PRE47_H2_TRACE") != "1" {
		return c
	}
	return &pre47H2Credentials{TransportCredentials: c}
}

type pre47H2Credentials struct {
	credentials.TransportCredentials
}

var pre47H2NextConn atomic.Uint64

func (c *pre47H2Credentials) Clone() credentials.TransportCredentials {
	return &pre47H2Credentials{TransportCredentials: c.TransportCredentials.Clone()}
}

func (c *pre47H2Credentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return conn, info, err
	}
	id := pre47H2NextConn.Add(1)
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		expclock.Event("h2_conn_port", id, 0, uint64(addr.Port), 0)
	}
	return &pre47H2Conn{
		Conn: conn,
		in:   pre47H2Parser{connID: id, inbound: true, prefaceLeft: 24, emit: expclock.Event},
		out:  pre47H2Parser{connID: id, emit: expclock.Event},
	}, info, nil
}

type pre47H2Conn struct {
	net.Conn
	readMu, writeMu sync.Mutex
	in, out         pre47H2Parser
}

func (c *pre47H2Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	n, err := c.Conn.Read(p)
	c.in.feed(p[:n])
	return n, err
}

func (c *pre47H2Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.Conn.Write(p)
	// Parse only bytes the underlying connection accepted. A caller's retry
	// after a short write supplies the remainder, not a second full frame.
	c.out.feed(p[:n])
	return n, err
}

// The parser stores at most nine header bytes and eight control-field bytes.
// The 24-byte client preface is skipped without retaining it. Frame lengths
// are 24-bit counters, so even an invalid giant frame cannot grow memory.
// emit's fields are connection ID, stream ID, value and HTTP/2 flags.
// DATA uses full frame payload length, including any padding. GOAWAY uses
// its last-stream ID and error code; debug data is discarded. SETTINGS emits
// one event for each requested setting, not a raw payload or ACK event.
// Events occur after a complete DATA/control frame has passed Read/Write,
// except SETTINGS fields, which emit when their six bytes are available.
// A Write event means TLS accepted plaintext, not peer receipt or disk ack.
// Parser state advances outside capture too, for captures starting mid-frame.
// Each direction has its own parser and lock.
type pre47H2Parser struct {
	connID      uint64
	inbound     bool
	prefaceLeft int
	header      [9]byte
	headerN     int
	remaining   uint32
	length      uint32
	streamID    uint32
	frameType   byte
	flags       byte
	field       [8]byte
	fieldN      int
	emit        func(string, uint64, uint64, uint64, uint64)
}

func (p *pre47H2Parser) event(in, out string, stream, value uint32) {
	kind := out
	if p.inbound {
		kind = in
	}
	p.emit(kind, p.connID, uint64(stream), uint64(value), uint64(p.flags))
}

func (p *pre47H2Parser) feed(b []byte) {
	if p.prefaceLeft > 0 {
		n := min(p.prefaceLeft, len(b))
		p.prefaceLeft -= n
		b = b[n:]
	}
	for len(b) > 0 {
		if p.headerN < len(p.header) {
			n := copy(p.header[p.headerN:], b)
			p.headerN += n
			b = b[n:]
			if p.headerN < len(p.header) {
				return
			}
			p.length = uint32(p.header[0])<<16 | uint32(p.header[1])<<8 | uint32(p.header[2])
			p.remaining = p.length
			p.frameType, p.flags = p.header[3], p.header[4]
			p.streamID = pre47H2Uint32(p.header[5:]) & 0x7fffffff
			p.fieldN = 0
			if p.remaining == 0 {
				p.finish()
				p.headerN = 0
				continue
			}
		}
		n := min(len(b), int(p.remaining))
		part := b[:n]
		switch p.frameType {
		case 4: // SETTINGS consists of six-byte fields.
			for len(part) > 0 {
				k := copy(p.field[p.fieldN:6], part)
				p.fieldN += k
				part = part[k:]
				if p.fieldN == 6 {
					p.setting()
					p.fieldN = 0
				}
			}
		case 3, 7, 8: // RST_STREAM, GOAWAY, WINDOW_UPDATE.
			p.fieldN += copy(p.field[p.fieldN:], part)
		}
		p.remaining -= uint32(n)
		b = b[n:]
		if p.remaining == 0 {
			p.finish()
			p.headerN = 0
		}
	}
}

func (p *pre47H2Parser) setting() {
	if p.flags&1 != 0 { // SETTINGS ACK has no settings.
		return
	}
	id := uint16(p.field[0])<<8 | uint16(p.field[1])
	value := pre47H2Uint32(p.field[2:6])
	switch id {
	case 3:
		p.event("h2_in_settings_streams", "h2_out_settings_streams", p.streamID, value)
	case 4:
		p.event("h2_in_settings_window", "h2_out_settings_window", p.streamID, value)
	case 5:
		p.event("h2_in_settings_frame", "h2_out_settings_frame", p.streamID, value)
	}
}

func (p *pre47H2Parser) finish() {
	switch p.frameType {
	case 0:
		p.event("h2_in_data", "h2_out_data", p.streamID, p.length)
	case 3:
		if p.length == 4 && p.fieldN >= 4 {
			p.event("h2_in_rst", "h2_out_rst", p.streamID, pre47H2Uint32(p.field[:4]))
		}
	case 7:
		if p.length >= 8 && p.fieldN == 8 {
			p.event("h2_in_goaway", "h2_out_goaway", pre47H2Uint32(p.field[:4])&0x7fffffff, pre47H2Uint32(p.field[4:]))
		}
	case 8:
		if p.length == 4 && p.fieldN >= 4 {
			p.event("h2_in_window_update", "h2_out_window_update", p.streamID, pre47H2Uint32(p.field[:4])&0x7fffffff)
		}
	}
}

func pre47H2Uint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
