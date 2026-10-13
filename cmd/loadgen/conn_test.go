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
	// rev is the last revision answered; every write gets the next one.
	rev atomic.Int64
	// repeat answers every write with revision 42 instead.
	repeat bool
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
		rev := fs.rev.Add(1)
		if fs.repeat {
			rev = 42
		}
		hdr.varint(headerRevision, uint64(rev))
		resp := newMsg()
		resp.sub(respHeader, hdr)
		b := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(resp.b)))
		b = append(b, resp.b...)
		_, _ = w.Write(b[:3])
		w.(http.Flusher).Flush()
		_, _ = w.Write(b[3:])
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
	// Everything is built before the dial, as the generator does: nothing
	// answers the server's pings until wk.run starts, and building the
	// value pool takes over 50 ms under -race (#102).
	ks := makeKeys(100)
	tpl, err := newTemplate(fs.addr, path, putRequest(ks.size, valueSize), true)
	if err != nil {
		t.Fatal(err)
	}
	vals := makeValues(valueSize, 1)
	writes := newRevisionSet()
	c, err := dial(fs.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	b := base{tpl: tpl, ks: ks, vals: *vals, rng: wk.rng, stats: &wk.stats, limit: 64, writes: writes}
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
// requests, and closes the connection if a ping goes unanswered.
func TestFakeServerPings(t *testing.T) {
	fs := startFake(t, 0, 50*time.Millisecond)
	s := fakeRun(t, fs, "/ok", 1024, 10, time.Second)
	if len(s.errs) > 0 || s.ops < 3 {
		t.Fatalf("ops %d, errors %v", s.ops, s.errs)
	}
}

// TestRepeatedRevisionFails answers every write with the same revision:
// the run must report it.
func TestRepeatedRevisionFails(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	fs.repeat = true
	s := fakeRun(t, fs, "/ok", 64, 0, 100*time.Millisecond)
	if s.errs["revision 42 returned for two writes"] == 0 {
		t.Fatalf("repeated revisions were not caught: errors %v", s.errs)
	}
}

// TestLateCompletionsInCohort answers slowly, so the requests of the end
// of the window complete after it: their latency still counts, and so
// does every request that arrived in the window.
func TestLateCompletionsInCohort(t *testing.T) {
	fs := startFake(t, 200*time.Millisecond, time.Second)
	s := fakeRun(t, fs, "/ok", 64, 200, 300*time.Millisecond)
	if len(s.errs) > 0 || s.cohort == 0 || s.cohortFailed != 0 {
		t.Fatalf("cohort %d, failed %d, errors %v", s.cohort, s.cohortFailed, s.errs)
	}
	if s.resp.n != uint64(s.cohort) {
		t.Fatalf("%d latencies for a cohort of %d", s.resp.n, s.cohort)
	}
	// With 200 ms answers, the last 200 ms of arrivals complete after the
	// window, so the window counts fewer completions than its cohort.
	// 200 arrivals a second for 0.3 s: about 60, all in the cohort.
	if s.cohort < 50 {
		t.Fatalf("cohort %d, want about 60: arrivals left out", s.cohort)
	}
	if s.ops >= s.cohort {
		t.Fatalf("%d completions in the window, cohort %d: late completions were not in the cohort", s.ops, s.cohort)
	}
}

// TestFailedCPUSampleIsInvalid runs against a server whose pid does not
// exist: the run must fail and its result be invalid, not report zeros.
func TestFailedCPUSampleIsInvalid(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	res, err := run(config{
		endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 10, conns: 1, streams: 4,
		warmup: 50 * time.Millisecond, duration: 100 * time.Millisecond, pids: []int{1 << 30}, seed: 1,
	})
	if err == nil || res == nil || res.Valid {
		t.Fatalf("a run without server CPU: error %v, result %+v", err, res)
	}
}

// TestResponseShapes feeds the CAS and Range handlers answers that look
// right on the surface: each must be refused.
func TestResponseShapes(t *testing.T) {
	header := func(rev uint64) *msg {
		h := newMsg()
		h.varint(headerRevision, rev)
		return h
	}
	ks := makeKeys(1)
	b := base{ks: ks, writes: newRevisionSet()}
	st := &stream{slot: 0}

	cas := newCASLoad(b, make([]int64, 1))
	noOps := newMsg()
	noOps.sub(respHeader, header(5))
	noOps.varint(txnSucceeded, 1)
	if err := cas.message(st, noOps.b); err == nil {
		t.Fatal("a CAS answer without a put response was taken")
	}
	put := newMsg()
	put.sub(respHeader, header(6))
	op := newMsg()
	op.sub(opResponsePut, put)
	ok := newMsg()
	ok.sub(respHeader, header(6))
	ok.varint(txnSucceeded, 1)
	ok.sub(txnResponses, op)
	if err := cas.message(st, ok.b); err != nil {
		t.Fatalf("a good CAS answer: %v", err)
	}

	want := []byte("the value that was put")
	rng := &rangeLoad{base: b, valueSize: len(want), want: [][]byte{want}}
	answer := func(value []byte) []byte {
		kv := newMsg()
		kv.bytes(kvKey, ks.key(0))
		kv.bytes(kvValue, value)
		r := newMsg()
		r.sub(respHeader, header(7))
		r.sub(rangeKVs, kv)
		r.varint(rangeCount, 1)
		return r.b
	}
	if err := rng.message(st, answer(make([]byte, len(want)))); err == nil {
		t.Fatal("a range answer with zeros of the right size was taken")
	}
	if err := rng.message(st, answer(want)); err != nil {
		t.Fatalf("a good range answer: %v", err)
	}
}
