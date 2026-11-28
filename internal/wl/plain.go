package wl

import (
	"bytes"
	"context"
	"net"
	"time"
)

// Clients of a listener without TLS (an http or unix URL), as etcd serves
// them: HTTP/2 with prior knowledge (h2c), which gRPC clients speak, on
// the loop, and HTTP/1.1 to the HTTP endpoints, through net/http. The
// accept goroutine reads the start of the connection and tells them apart
// by HTTP/2's connection preface, as etcd's cmux does.

// h2Preface is HTTP/2's client connection preface (RFC 9113, 3.4).
const h2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// plainTimeout bounds the wait for a client's first bytes, as the TLS
// handshake's does.
const plainTimeout = 10 * time.Second

// plainHandoff reads the start of nc, a client of a listener without TLS:
// a client that begins with the preface is handed to the loop with what
// was read; any other is served by net/http, the bytes read first. The
// wait for the first bytes ends once ctx is done.
func plainHandoff(ctx context.Context, sys sysEnv, nc net.Conn) (*handoff, error) {
	if err := nc.SetReadDeadline(time.Now().Add(plainTimeout)); err != nil {
		_ = nc.Close()
		return nil, err
	}
	defer context.AfterFunc(ctx, func() { _ = nc.SetReadDeadline(time.Now()) })()
	var b []byte
	buf := make([]byte, len(h2Preface))
	for len(b) < len(h2Preface) && bytes.HasPrefix([]byte(h2Preface), b) {
		n, err := nc.Read(buf[:len(h2Preface)-len(b)])
		b = append(b, buf[:n]...)
		if err != nil {
			_ = nc.Close()
			return nil, err
		}
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		_ = nc.Close()
		return nil, err
	}
	if string(b) != h2Preface {
		return &handoff{kind: kindClient, h1: &replayConn{Conn: nc, first: b}}, nil
	}
	fd, err := sys.connFD(nc)
	if err != nil {
		return nil, err
	}
	return &handoff{fd: fd, kind: kindClient, alpn: "h2", plain: true, left: b}, nil
}

// replayConn is a connection whose first bytes were read already: its
// reads give them first.
type replayConn struct {
	net.Conn
	first []byte
}

func (c *replayConn) Read(p []byte) (int, error) {
	if len(c.first) > 0 {
		n := copy(p, c.first)
		c.first = c.first[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}
