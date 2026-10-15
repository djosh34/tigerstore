package server

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/http2/hpack"
)

// HTTP requests on an HTTP/2 stream run Server.ServeHTTP as soon as their
// headers arrive, with the stream's DATA as the request body and the
// handler's output as the response, so that the gateway's streams (Watch,
// LeaseKeepAlive) run both ways. HTTP/1.1 clients get net/http's server,
// as in etcd.

// isHTTPPath reports whether a request path is not a gRPC method: a gRPC
// method's first segment names a package and has a dot.
func isHTTPPath(path string) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return !strings.Contains(first, ".")
}

// runHTTP serves an HTTP request on an HTTP/2 stream.
func (c *conn) runHTTP(st *stream) {
	defer st.cancel()
	st.done.Store(true)
	w := &h2Writer{st: st, header: http.Header{}, head: st.httpMethod == http.MethodHead}
	u, err := url.ParseRequestURI(st.httpPath)
	if err != nil {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		w.finish()
		return
	}
	r := &http.Request{
		Method: st.httpMethod, URL: u, RequestURI: st.httpPath,
		Proto: "HTTP/2.0", ProtoMajor: 2, Header: http.Header{}, Host: st.authority,
		Body: &h2Body{st: st}, ContentLength: -1, RemoteAddr: c.nc.RemoteAddr().String(),
	}
	for _, f := range st.httpFields {
		r.Header.Add(f.Name, f.Value)
	}
	if n, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err == nil {
		r.ContentLength = n
	}
	if tc, ok := c.nc.(*tls.Conn); ok {
		cs := tc.ConnectionState()
		r.TLS = &cs
	}
	c.s.ServeHTTP(w, r.WithContext(st.ctx))
	w.finish()
}

// h2Body is a request body from a stream's DATA frames.
type h2Body struct {
	st  *stream
	buf []byte
}

func (b *h2Body) Read(p []byte) (int, error) {
	for len(b.buf) == 0 {
		next, err := b.st.next()
		if err != nil {
			return 0, err
		}
		b.buf = next
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

func (b *h2Body) Close() error { return nil }

// h2Writer is an http.ResponseWriter on a stream.
type h2Writer struct {
	st     *stream
	header http.Header
	wrote  bool
	head   bool
	err    error
}

func (w *h2Writer) Header() http.Header { return w.header }

func (w *h2Writer) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	fields := []hpack.HeaderField{{Name: ":status", Value: strconv.Itoa(code)}}
	for k, vs := range w.header {
		name := strings.ToLower(k)
		switch name {
		case "connection", "keep-alive", "transfer-encoding", "upgrade", "proxy-connection":
			// Connection-specific fields have no place in HTTP/2.
			continue
		}
		for _, v := range vs {
			fields = append(fields, hpack.HeaderField{Name: name, Value: v})
		}
	}
	w.err = w.st.pushHTTPItem(outItem{kind: outHeaders, b: headerBlock(fields...)}, false)
}

func (w *h2Writer) Write(p []byte) (int, error) {
	if !w.wrote {
		if w.header.Get("Content-Type") == "" {
			w.header.Set("Content-Type", http.DetectContentType(p))
		}
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.head || len(p) == 0 {
		return len(p), nil
	}
	if w.err = w.st.pushHTTPItem(outItem{kind: outData, b: append([]byte(nil), p...)}, true); w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

// Flush is a no-op: the connection's writer sends what is queued.
func (w *h2Writer) Flush() {}

// finish ends the response.
func (w *h2Writer) finish() {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	_ = w.st.pushHTTPItem(outItem{kind: outData, end: true}, false)
}

// pushHTTPItem queues part of an HTTP response. With wait, it waits while
// the stream has more than sendQueue bytes queued.
func (st *stream) pushHTTPItem(it outItem, wait bool) error {
	c := st.c
	for {
		c.mu.Lock()
		if st.reset || c.closed {
			c.mu.Unlock()
			return errStreamGone
		}
		if wait && st.queued > 0 && st.queued+len(it.b) > sendQueue {
			c.mu.Unlock()
			select {
			case <-st.space:
				continue
			case <-st.ctx.Done():
				return st.ctx.Err()
			}
		}
		st.out = append(st.out, it)
		st.queued += len(it.b)
		c.list(st)
		c.mu.Unlock()
		c.signal()
		return nil
	}
}

// http1Conn is a TLS client that did not offer h2, served by net/http's
// HTTP/1.1 server, as etcd serves it.
type http1Conn struct {
	s    *Server
	nc   net.Conn
	raw  net.Conn
	once sync.Once
}

func (h *http1Conn) goAway() { h.close() }

func (h *http1Conn) close() { h.once.Do(func() { _ = h.raw.Close() }) }

// serve serves the connection until it closes.
func (h *http1Conn) serve() {
	done := make(chan struct{})
	var once sync.Once
	srv := &http.Server{
		Handler: h.s, IdleTimeout: http1Idle, ReadHeaderTimeout: http1Idle,
		ConnState: func(_ net.Conn, st http.ConnState) {
			if st == http.StateClosed || st == http.StateHijacked {
				once.Do(func() { close(done) })
			}
		},
	}
	ln := &oneConn{c: h.nc}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, errOneConn) {
		h.close()
		return
	}
	<-done
}

// errOneConn ends Serve on a oneConn after its connection.
var errOneConn = errors.New("server: the one connection was served")

// oneConn is a listener of one connection.
type oneConn struct {
	c    net.Conn
	used bool
}

func (l *oneConn) Accept() (net.Conn, error) {
	if l.used {
		return nil, errOneConn
	}
	l.used = true
	return l.c, nil
}

func (l *oneConn) Close() error   { return nil }
func (l *oneConn) Addr() net.Addr { return l.c.LocalAddr() }
