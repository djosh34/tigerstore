package peer

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net"
)

// recordConn is the raw connection below outbound TLS. Handshake writes
// pass through. After the handshake, the sender alone owns bw and copies
// encrypted records into it; TLS reuses its record scratch storage.
// Close only closes the socket, so it can interrupt a blocked Flush without
// touching bw. This adapter is not for connections with a TLS reader after
// the handshake: a reader can generate outbound TLS protocol messages.
type recordConn struct {
	net.Conn
	bw *bufio.Writer
}

func (c *recordConn) Write(p []byte) (int, error) {
	if c.bw == nil {
		return c.Conn.Write(p)
	}
	return c.bw.Write(p)
}

// outboundConn sends plaintext through TLS, then buffers its ciphertext.
// The sender is the only writer and flusher. Closing from any goroutine
// aborts raw TCP instead of writing a TLS close_notify into the buffer.
type outboundConn struct {
	net.Conn
	records *recordConn
}

func (c *outboundConn) Close() error { return c.records.Conn.Close() }

// flush writes every record left by the plaintext writer's Flush. Its
// failure ends the connection even though TLS already accepted the bytes.
func (c *outboundConn) flush() error { return c.records.bw.Flush() }

// dialOutbound gives TCP dialing and TLS negotiation one timeout budget.
func dialOutbound(ctx context.Context, addr string, cfg *tls.Config) (*outboundConn, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	raw, err := new(net.Dialer).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return handshakeOutbound(ctx, raw, cfg)
}

func handshakeOutbound(ctx context.Context, raw net.Conn, cfg *tls.Config) (*outboundConn, error) {
	records := &recordConn{Conn: raw}
	tc := tls.Client(records, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(err, raw.Close())
	}
	// Until here, even TLS's final handshake flight went straight to TCP.
	// Each directed connection adds only this fixed ciphertext buffer.
	records.bw = bufio.NewWriterSize(raw, ioBuffer)
	return &outboundConn{Conn: tc, records: records}, nil
}
