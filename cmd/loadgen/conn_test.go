package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// fakeServer is a plaintext HTTP/2 server that answers like a gRPC server,
// with small limits so the client's stream and flow-control accounting is
// exercised. Paths:
//
//	/ok      a PutResponse in two DATA frames
//	/error   a trailers-only FailedPrecondition response
//	/reset   a reset stream
type fakeServer struct {
	addr      string
	inflight  atomic.Int64
	maxSeen   atomic.Int64
	slowReads time.Duration
	// rev is the last revision answered; every write gets the next one.
	rev atomic.Int64
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
	tpl, err := newTemplate(path, putRequest(ks.size, valueSize), true)
	if err != nil {
		t.Fatal(err)
	}
	vals := makeValues(valueSize, 1)
	c, err := dial(fs.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	b := base{tpl: tpl, ks: ks, vals: *vals, rng: wk.rng, stats: &wk.stats}
	for k := range ks.n {
		b.own = append(b.own, k)
	}
	start := now()
	wk.stats.winStart, wk.stats.winStop = start, start+int64(d)
	if err := wk.run(&putLoad{base: b}, phase{rate: rate, start: start, stop: start + int64(d), drain: 5 * time.Second, limit: 64}); err != nil {
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
		"/error": "grpc FailedPrecondition: boom",
		"/reset": "stream reset",
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

// TestAnswerSizes reads a Txn's outcome and a Range's key and value bytes
// from real etcd answers, and refuses a CAS that did not succeed.
func TestAnswerSizes(t *testing.T) {
	txn, err := proto.Marshal(&pb.TxnResponse{
		Header: &pb.ResponseHeader{Revision: 6}, Succeeded: true,
		Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev, ok, err := txnOutcome(txn); err != nil || !ok || rev != 6 {
		t.Fatalf("txnOutcome: %d %v %v", rev, ok, err)
	}
	cas := newCASLoad(base{ks: makeKeys(1)}, make([]int64, 1))
	if err := cas.message(&stream{}, txn); err != nil || cas.revs[0] != 6 {
		t.Fatalf("a good CAS answer: %v, revision %d", err, cas.revs[0])
	}
	failed, err := proto.Marshal(&pb.TxnResponse{Header: &pb.ResponseHeader{Revision: 7}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cas.message(&stream{}, failed); !errors.Is(err, errCASFailed) {
		t.Fatalf("a failed CAS: %v", err)
	}
	if _, _, err := txnOutcome(nil); err == nil {
		t.Fatal("an answer without a header was taken")
	}

	r, err := proto.Marshal(&pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 7}, Count: 9, More: true, Kvs: []*mvccpb.KeyValue{
		{Key: []byte("abc"), Value: make([]byte, 100), ModRevision: 5, CreateRevision: 2, Version: 3},
		{Key: []byte("de"), Value: make([]byte, 7), Lease: 4},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if n, kvs, err := rangeBytes(r); err != nil || n != 3+100+2+7 || kvs != 2 {
		t.Fatalf("rangeBytes: %d, %v", n, err)
	}
	st := &stream{}
	if err := (&rangeLoad{}).message(st, r); err != nil || st.read != 112 {
		t.Fatalf("range read %d bytes: %v", st.read, err)
	}
}

// TestLargeMessageFrames sends a message larger than a DATA frame and reads
// it back as an HTTP/2 server would.
func TestLargeMessageFrames(t *testing.T) {
	c := &conn{addr: "h:1", streams: newStreamTable(), nextID: 1, sendWin: 1 << 30, streamWin: 1 << 30, maxStreams: 100}
	m := make([]byte, 40000)
	for i := range m {
		m[i] = byte(i)
	}
	if !c.canStart(messageLen(m), 1) {
		t.Fatal("cannot start")
	}
	st := c.newStream(nil)
	c.startMessage(st, "/p", m)
	if c.sendWin != 1<<30-int64(messageLen(m)) {
		t.Fatalf("send window %d", c.sendWin)
	}
	fr := http2.NewFramer(nil, bytes.NewReader(c.out))
	var got []byte
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if f.Header().StreamID != st.id {
			t.Fatalf("frame on stream %d", f.Header().StreamID)
		}
		if d, ok := f.(*http2.DataFrame); ok {
			if d.Length > maxDataFrame {
				t.Fatalf("DATA frame of %d bytes", d.Length)
			}
			got = append(got, d.Data()...)
			if d.StreamEnded() {
				break
			}
		}
	}
	if !bytes.Equal(got[:grpcPrefixLen], []byte{0, 0, 0, 0x9c, 0x40}) || !bytes.Equal(got[grpcPrefixLen:], m) {
		t.Fatal("the frames do not carry the message")
	}
}

// TestReadHeaderBlockSequence checks that the frame reader takes a header
// block only as HTTP/2 allows it: a frame between HEADERS and its
// CONTINUATION, a CONTINUATION of another stream or of no block, and a
// frame past the largest size end the connection (#555).
func TestReadHeaderBlockSequence(t *testing.T) {
	type done struct {
		ended bool
		err   error
	}
	for name, write := range map[string]func(fr *http2.Framer) error{
		"DATA inside a header block": func(fr *http2.Framer) error {
			return errors.Join(
				fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte{0x88}}),
				fr.WriteData(1, false, []byte{0, 0, 0, 0, 0}),
				fr.WriteContinuation(1, true, nil))
		},
		"CONTINUATION of another stream": func(fr *http2.Framer) error {
			return errors.Join(
				fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte{0x88}}),
				fr.WriteContinuation(3, true, nil))
		},
		"CONTINUATION of no block": func(fr *http2.Framer) error {
			return fr.WriteContinuation(1, true, nil)
		},
		"a frame past the largest": func(fr *http2.Framer) error {
			return fr.WriteData(1, false, make([]byte, maxDataFrame+1))
		},
	} {
		var wire bytes.Buffer
		if err := write(http2.NewFramer(&wire, nil)); err != nil {
			t.Fatal(err)
		}
		var d done
		h := handlerFunc(func(err error) { d = done{true, err} })
		c := &conn{br: bufio.NewReaderSize(bytes.NewReader(wire.Bytes()), 64<<10), streams: newStreamTable(), wake: make(chan struct{}, 1)}
		c.streams.put(&stream{id: 1, h: h})
		c.dec = hpack.NewDecoder(4096, c.headerField)
		if err := c.read(); !errors.Is(err, errFrame) {
			t.Errorf("%s: read ended with %v, stream done %+v", name, err, d)
		}
	}
}

// handlerFunc is a handler that takes no messages and reports how its
// stream ended.
type handlerFunc func(error)

func (handlerFunc) message(*stream, []byte) error        { return nil }
func (h handlerFunc) done(_ *stream, _ int64, err error) { h(err) }

// TestTableEffect tells header blocks that add to HPACK's dynamic table
// from those that only use it, and from tigerstore's, which empty it first
// and add nothing: the encoder indexes new fields the first time it sends
// them and refers to them after.
func TestTableEffect(t *testing.T) {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	block := func(fields ...hpack.HeaderField) []byte {
		buf.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		return slices.Clone(buf.Bytes())
	}
	trailers := []hpack.HeaderField{{Name: "grpc-status", Value: "0"}, {Name: "grpc-message", Value: ""}}
	for _, c := range []struct {
		name  string
		block []byte
		want  effect
	}{
		{"the first block adds its fields", block(trailers...), changes},
		{"the same fields again refer to the table", block(trailers...), keeps},
		{"a never-indexed field leaves the table alone", block(hpack.HeaderField{Name: "x-secret", Value: "1", Sensitive: true}), keeps},
		{"tigerstore's trailers empty the table and add nothing", append([]byte{0x20}, block(hpack.HeaderField{Name: "grpc-status", Value: "0", Sensitive: true})...), empties},
		{"an emptied table has no entry 62 to name", []byte{0x20, 0x80 | 62}, changes},
		{"a size update after a field", []byte{0x88, 0x20}, changes},
		{"a size update to more than 0", []byte{0x3f, 0x01}, changes},
		{"a block cut short", []byte{0xff}, changes},
	} {
		if got := tableEffect(c.block); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

// TestStreamTable finds open streams by ID, and skips an ID whose slot an
// older stream, such as a watch, still holds.
func TestStreamTable(t *testing.T) {
	c := &conn{streams: newStreamTable(), nextID: 1}
	watch := &stream{id: c.newID()}
	c.streams.put(watch)
	c.nextID = 1 + 2*streamSlots // the watch's slot again
	id := c.newID()
	if id != 3+2*streamSlots {
		t.Fatalf("new stream %d, want %d", id, 3+2*streamSlots)
	}
	st := &stream{id: id}
	c.streams.put(st)
	if c.streams.get(1) != watch || c.streams.get(id) != st || c.streams.get(1+2*streamSlots) != nil || c.streams.n != 2 {
		t.Fatal("lookups or count wrong")
	}
	c.streams.remove(st)
	if c.streams.get(id) != nil || c.streams.n != 1 {
		t.Fatal("removed stream still there")
	}
}
