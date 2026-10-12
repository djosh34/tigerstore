package main

import (
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer is a plaintext HTTP/2 server that answers like a gRPC server,
// with small limits so the client's stream and flow-control accounting is
// exercised. Paths:
//
//	/ok      a PutResponse in two DATA frames
//	/error   a trailers-only FailedPrecondition response
//	/garbage a response that is not protobuf
//	/reset   a reset stream
type fakeServer struct {
	addr      string
	inflight  atomic.Int64
	maxSeen   atomic.Int64
	slowReads time.Duration
}

const fakeMaxStreams = 8

// startFake starts a fake server whose handlers wait slowReads before
// reading a request, and that pings a connection idle for 20ms and closes
// it if the ping is not answered within pingTimeout.
func startFake(t *testing.T, slowReads, pingTimeout time.Duration) *fakeServer {
	t.Helper()
	fs := &fakeServer{slowReads: slowReads}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs.addr = ln.Addr().String()
	srv := &http.Server{
		Handler:   fs,
		Protocols: new(http.Protocols),
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          fakeMaxStreams,
			MaxReceiveBufferPerConnection: 64 << 10,
			SendPingTimeout:               20 * time.Millisecond,
			PingTimeout:                   pingTimeout,
		},
		ReadHeaderTimeout: 5 * time.Second,
	}
	srv.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return fs
}

func (fs *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := fs.inflight.Add(1)
	defer fs.inflight.Add(-1)
	for {
		m := fs.maxSeen.Load()
		if n <= m || fs.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(fs.slowReads)
	if _, err := io.ReadAll(r.Body); err != nil {
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/grpc")
	switch r.URL.Path {
	case "/ok", "/etcdserverpb.KV/Put":
		h.Set("Trailer", "Grpc-Status")
		hdr := newMsg()
		hdr.varint(headerRevision, 42)
		resp := newMsg()
		resp.sub(respHeader, hdr)
		b := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(resp.b)))
		b = append(b, resp.b...)
		_, _ = w.Write(b[:3])
		w.(http.Flusher).Flush()
		_, _ = w.Write(b[3:])
		h.Set("Grpc-Status", "0")
	case "/empty":
		h.Set("Trailer", "Grpc-Status")
		h.Set("Grpc-Status", "0")
	case "/double":
		h.Set("Trailer", "Grpc-Status")
		hdr := newMsg()
		hdr.varint(headerRevision, 42)
		resp := newMsg()
		resp.sub(respHeader, hdr)
		b := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(resp.b)))
		b = append(b, resp.b...)
		_, _ = w.Write(append(b, b...))
		h.Set("Grpc-Status", "0")
	case "/error":
		h.Set("Grpc-Status", "9")
		h.Set("Grpc-Message", "boom")
	case "/garbage":
		h.Set("Trailer", "Grpc-Status")
		_, _ = w.Write([]byte{0, 0, 0, 0, 2, 0xff, 0xff})
		h.Set("Grpc-Status", "0")
	case "/reset":
		//nolint:forbidigo // net/http resets the stream when a handler panics with ErrAbortHandler
		panic(http.ErrAbortHandler)
	}
}

// fakeRun sends put requests to path on one connection for d and returns
// the worker's stats.
func fakeRun(t *testing.T, fs *fakeServer, path string, valueSize int, rate float64, d time.Duration) *stats {
	t.Helper()
	c, err := dial(fs.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	ks := makeKeys(100)
	tpl, err := newTemplate(c.addr, path, putRequest(ks.size, valueSize), true)
	if err != nil {
		t.Fatal(err)
	}
	b := base{tpl: tpl, ks: ks, vals: *makeValues(valueSize, 1), rng: wk.rng, stats: &wk.stats, limit: 64}
	for k := range ks.n {
		b.own = append(b.own, k)
	}
	start := now()
	wk.stats.winStart, wk.stats.winStop = start, start+int64(d)
	if err := wk.run(&putLoad{base: b}, phase{rate: rate, start: start, stop: start + int64(d), drain: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	return &wk.stats
}

func TestFakeServerOK(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	s := fakeRun(t, fs, "/ok", 1024, 0, 300*time.Millisecond)
	if len(s.errs) > 0 || s.ops == 0 {
		t.Fatalf("ops %d, errors %v", s.ops, s.errs)
	}
	if m := fs.maxSeen.Load(); m > fakeMaxStreams {
		t.Fatalf("server saw %d concurrent streams, limit %d", m, fakeMaxStreams)
	}
}

// TestFakeServerFlowControl sends far more than the server's 64 KiB
// connection window while its handlers read slowly, with server pings that
// must be answered. Exceeding the window or missing a ping kills the
// connection.
func TestFakeServerFlowControl(t *testing.T) {
	fs := startFake(t, 2*time.Millisecond, time.Second)
	s := fakeRun(t, fs, "/ok", 10240, 0, 500*time.Millisecond)
	if len(s.errs) > 0 || s.ops < 100 {
		t.Fatalf("ops %d, errors %v", s.ops, s.errs)
	}
}

func TestFakeServerOpenLoop(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	s := fakeRun(t, fs, "/ok", 1024, 2000, 500*time.Millisecond)
	if len(s.errs) > 0 || s.ops < 700 || s.ops > 1300 {
		t.Fatalf("ops %d at 2000/s for 0.5s, errors %v", s.ops, s.errs)
	}
}

func TestFakeServerErrors(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	for path, want := range map[string]string{
		"/error":   "grpc FailedPrecondition: boom",
		"/garbage": "malformed protobuf response",
		"/reset":   "stream reset",
	} {
		s := fakeRun(t, fs, path, 1024, 0, 100*time.Millisecond)
		if s.ops != 0 || len(s.errs) != 1 {
			t.Fatalf("%s: ops %d, errors %v", path, s.ops, s.errs)
		}
		for e := range s.errs {
			if !strings.Contains(e, want) {
				t.Errorf("%s: error %q, want %q", path, e, want)
			}
		}
	}
}

// TestFakeServerPings sends so slowly that the server pings between
// requests, and closes the connection if a ping goes unanswered for 50ms.
func TestFakeServerPings(t *testing.T) {
	fs := startFake(t, 0, 50*time.Millisecond)
	s := fakeRun(t, fs, "/ok", 1024, 10, time.Second)
	if len(s.errs) > 0 || s.ops < 3 {
		t.Fatalf("ops %d, errors %v", s.ops, s.errs)
	}
}
