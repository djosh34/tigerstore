package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/internal/loop"
)

// tconn is a peer connection's TLS: crypto/tls's handshake over a bridge,
// and after it the loop's own records (loop.Handshake), which a message
// connection keeps when the loop takes it. It is what the transport
// tracks from the dial or the accept on, so that a close before the
// handshake is done closes the raw connection. The records lock their own
// write side, as crypto/tls does: a call or a fetch may be closed from
// another goroutine while it writes.
type tconn struct {
	br     *loop.Bridge
	cfg    *tls.Config
	server bool
	// mu guards rec, which the handshake sets.
	mu  sync.Mutex
	rec *loop.Records
}

var _ net.Conn = (*tconn)(nil)

func newTConn(br *loop.Bridge, cfg *tls.Config, server bool) *tconn {
	return &tconn{br: br, cfg: cfg, server: server}
}

// errNotTLS13 is the error of a peer that negotiated a version below TLS
// 1.3, which every node's transport requires.
var errNotTLS13 = errors.New("the peer did not speak TLS 1.3")

// HandshakeContext runs the TLS handshake, once.
func (c *tconn) HandshakeContext(ctx context.Context) error {
	c.mu.Lock()
	done := c.rec != nil
	c.mu.Unlock()
	if done {
		return nil
	}
	r, _, err := loop.Handshake(ctx, c.br, c.cfg, c.server)
	if errors.Is(err, loop.ErrNotTLS13) {
		return errNotTLS13
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.rec = r
	c.mu.Unlock()
	return nil
}

// errNoHandshake is the error of a read or write before the handshake,
// which the transport always runs first.
var errNoHandshake = errors.New("peer: a TLS connection used before its handshake")

// records is the record layer, once the handshake is done.
func (c *tconn) records() (*loop.Records, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rec == nil {
		return nil, errNoHandshake
	}
	return c.rec, nil
}

// ConnectionState is the handshake's connection state, the zero one
// before it is done.
func (c *tconn) ConnectionState() tls.ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rec == nil {
		return tls.ConnectionState{}
	}
	return c.rec.ConnectionState()
}

func (c *tconn) Read(p []byte) (int, error) {
	r, err := c.records()
	if err != nil {
		return 0, err
	}
	return r.Read(p)
}

func (c *tconn) Write(p []byte) (int, error) {
	r, err := c.records()
	if err != nil {
		return 0, err
	}
	return r.Write(p)
}

// Close sends close_notify once the handshake is done, as crypto/tls's
// close does, and closes the connection.
func (c *tconn) Close() error {
	r, err := c.records()
	if err != nil {
		return c.br.Close()
	}
	return r.Close()
}

func (c *tconn) LocalAddr() net.Addr                { return c.br.LocalAddr() }
func (c *tconn) RemoteAddr() net.Addr               { return c.br.RemoteAddr() }
func (c *tconn) SetDeadline(t time.Time) error      { return c.br.SetDeadline(t) }
func (c *tconn) SetReadDeadline(t time.Time) error  { return c.br.SetReadDeadline(t) }
func (c *tconn) SetWriteDeadline(t time.Time) error { return c.br.SetWriteDeadline(t) }
