package server

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func clientTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return nil
	}
	cfg = cfg.Clone()
	cfg.NextProtos = []string{"h2", "http/1.1"}
	return cfg
}

func (s *Server) serveTLS(ln net.Listener) error {
	clients := &clientListener{Listener: ln, cfg: s.http.TLSConfig, conns: make(map[*clientConn]struct{})}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return clients.Close()
	}
	s.clients = clients
	s.mu.Unlock()

	m := cmux.New(clients)
	m.SetReadTimeout(10 * time.Second)
	// gRPC clients can wait for server SETTINGS before sending HEADERS.
	// Inspect content-type, not just the preface, to keep HTTP/2 GETs working.
	grpcMatcher := cmux.HTTP2MatchHeaderFieldPrefixSendSettings("content-type", "application/grpc")
	grpcl := m.MatchWithWriters(func(w io.Writer, r io.Reader) bool {
		c := w.(*clientConn)
		return grpcMatcher(settingsWriter{conn: c}, r)
	})
	httpl := &httpTLSListener{Listener: m.Match(cmux.Any())}
	results := make(chan error, 3)
	go func() { results <- s.g.Serve(grpcl) }()
	go func() { results <- s.http.Serve(httpl) }()
	go func() { results <- m.Serve() }()

	err := serveError(<-results)
	stopErr := s.Stop()
	m.Close()
	return errors.Join(err, stopErr, serveError(<-results), serveError(<-results))
}

func serveError(err error) error {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) ||
		errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, cmux.ErrListenerClosed) ||
		errors.Is(err, cmux.ErrServerClosed) {
		return nil
	}
	return err
}

// clientListener tracks accepted sockets so shutdown also interrupts sniffing
// and TLS handshakes. cmux otherwise waits for its read timeout on these sockets.
type clientListener struct {
	net.Listener
	cfg    *tls.Config
	mu     sync.Mutex
	closed bool
	conns  map[*clientConn]struct{}
}

func (l *clientListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c := &clientConn{Conn: tls.Server(raw, l.cfg), owner: l}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		_ = raw.Close()
		return nil, net.ErrClosed
	}
	l.conns[c] = struct{}{}
	return c, nil
}

func (l *clientListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	conns := make([]*clientConn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.mu.Unlock()
	err := serveError(l.Listener.Close())
	for _, c := range conns {
		_ = c.Close()
	}
	return err
}

type clientConn struct {
	*tls.Conn
	owner    *clientListener
	settings int // empty SETTINGS sent by the matcher, before routing
}

func (c *clientConn) Close() error {
	// Stop is abrupt, like grpc.Stop. Close the socket without waiting to
	// write TLS close_notify to a client that may not be reading.
	err := c.Conn.NetConn().Close()
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return err
}

func muxTLSState(conn net.Conn) (tls.ConnectionState, error) {
	muxConn, ok := conn.(*cmux.MuxConn)
	if !ok {
		return tls.ConnectionState{}, errors.New("native TLS: expected cmux connection")
	}
	c, ok := muxConn.Conn.(*clientConn)
	if !ok {
		return tls.ConnectionState{}, errors.New("native TLS: expected client TLS connection")
	}
	state := c.ConnectionState()
	if !state.HandshakeComplete {
		return tls.ConnectionState{}, errors.New("native TLS: handshake incomplete")
	}
	return state, nil
}

// completedTLSCredentials keeps peer.AuthInfo available to gRPC handlers.
// Authentication and certificate verification happened while cmux read TLS.
type completedTLSCredentials struct {
	credentials.TransportCredentials
}

func (c completedTLSCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	state, err := muxTLSState(conn)
	if err != nil {
		return nil, nil, err
	}
	return conn, credentials.TLSInfo{
		State:          state,
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
	}, nil
}

func (c completedTLSCredentials) Clone() credentials.TransportCredentials {
	return completedTLSCredentials{c.TransportCredentials.Clone()}
}

// Go 1.27 net/http recognizes ConnectionState on wrapped TLS connections.
// Keep cmux's buffered bytes while restoring ALPN handling and Request.TLS.
type httpTLSListener struct{ net.Listener }

func (l *httpTLSListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		state, err := muxTLSState(conn)
		if err != nil {
			// A rejected or timed-out TLS handshake falls through cmux's
			// catch-all matcher. Drop that client, not the HTTP listener.
			_ = conn.Close()
			continue
		}
		var reader io.Reader = conn
		if state.NegotiatedProtocol == "h2" {
			c := conn.(*cmux.MuxConn).Conn.(*clientConn)
			// Replay the preface unchanged, then remove ACKs for cmux's
			// SETTINGS. net/http rejects ACKs for SETTINGS it did not send.
			reader = io.MultiReader(io.LimitReader(conn, 24), &settingsAckReader{conn: conn, remaining: c.settings})
		}
		return &httpTLSConn{Conn: conn, state: state, reader: reader}, nil
	}
}

type httpTLSConn struct {
	net.Conn
	state  tls.ConnectionState
	reader io.Reader
}

func (c *httpTLSConn) Read(p []byte) (int, error)           { return c.reader.Read(p) }
func (c *httpTLSConn) ConnectionState() tls.ConnectionState { return c.state }

// The cmux matcher writes one empty SETTINGS frame per client SETTINGS.
// Count successful writes so unusual clients that send several are covered.
type settingsWriter struct{ conn *clientConn }

func (w settingsWriter) Write(p []byte) (int, error) {
	n, err := w.conn.Write(p)
	if err == nil && n == len(p) && bytes.Equal(p, []byte{0, 0, 0, 4, 0, 0, 0, 0, 0}) {
		w.conn.settings++
	}
	return n, err
}

type settingsAckReader struct {
	conn      io.Reader
	remaining int
	frame     io.Reader
}

func (r *settingsAckReader) Read(p []byte) (int, error) {
	for {
		if r.frame != nil {
			n, err := r.frame.Read(p)
			if err != io.EOF {
				return n, err
			}
			r.frame = nil
			if n > 0 {
				return n, nil
			}
		}
		if r.remaining == 0 {
			return r.conn.Read(p)
		}
		var header [9]byte
		if _, err := io.ReadFull(r.conn, header[:]); err != nil {
			return 0, err
		}
		length := int64(header[0])<<16 | int64(header[1])<<8 | int64(header[2])
		if length == 0 && header[3] == 4 && header[4] == 1 && binary.BigEndian.Uint32(header[5:]) == 0 {
			r.remaining--
			continue
		}
		r.frame = io.MultiReader(bytes.NewReader(header[:]), io.LimitReader(r.conn, length))
	}
}
