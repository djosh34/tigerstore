package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvcc "go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

func TestLeanCASWire(t *testing.T) {
	for _, key := range [][]byte{[]byte("/registry/pods/ns-000/pod-00000000"), bytes.Repeat([]byte("k"), 126)} {
		value := bytes.Repeat([]byte("v"), 1024)
		templates, err := leanTemplates([][]byte{key}, [][]byte{value}, "cas")
		if err != nil {
			t.Fatal(err)
		}
		scratch := make([]byte, 0, 2048)
		for _, rev := range []int64{1, 127, 128, 16383, 16384, 1<<21 - 1, 1 << 21, 1 << 28, 1 << 35, 1 << 42, 1 << 49, 1 << 56, math.MaxInt64} {
			scratch, err = templates[0].casWire(scratch, rev)
			if err != nil {
				t.Fatal(err)
			}
			if int(binary.BigEndian.Uint32(scratch[1:5])) != len(scratch)-5 {
				t.Fatal("bad gRPC length")
			}
			var got pb.TxnRequest
			if err := proto.Unmarshal(scratch[5:], &got); err != nil {
				t.Fatal(err)
			}
			want := casRequest(key, value, rev)
			if !proto.Equal(&got, want) {
				t.Fatalf("revision %d: bad request %v", rev, &got)
			}
			canonical, err := proto.Marshal(want)
			if err != nil || !bytes.Equal(canonical, scratch[5:]) {
				t.Fatalf("noncanonical encoding at %d", rev)
			}
		}
		if _, err := templates[0].casWire(scratch, 0); err == nil {
			t.Fatal("accepted absent revision")
		}
		if _, err := templates[0].casWire(scratch, -1); err == nil {
			t.Fatal("accepted negative revision")
		}
		if allocs := testing.AllocsPerRun(10, func() { scratch, _ = templates[0].casWire(scratch, 16384) }); allocs != 0 {
			t.Fatalf("CAS encoding allocated: %v", allocs)
		}
	}
}

func testWire(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return grpcWire(b)
}

func TestLeanResponseValidation(t *testing.T) {
	h := &pb.ResponseHeader{Revision: 43, RaftTerm: 2}
	put := &pb.PutResponse{Header: h}
	passed := &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: put}}}}
	failed := &pb.TxnResponse{Header: h, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: h, Count: 1, Kvs: []*mvcc.KeyValue{{Key: []byte("k"), Value: []byte("v"), ModRevision: 42}}}}}}}
	for _, tc := range []struct {
		op     string
		m      proto.Message
		passed bool
		rev    int64
	}{{"put", put, false, 0}, {"cas", passed, true, 43}, {"cas", failed, false, 42}} {
		got, pass, rev, err := leanResponse(tc.op, testWire(t, tc.m), []byte("k"), []byte("v"))
		if err != nil || got.RaftTerm != 2 || pass != tc.passed || rev != tc.rev {
			t.Fatalf("%s: %v %v %d %v", tc.op, got, pass, rev, err)
		}
	}
	for _, mutate := range []func(*pb.TxnResponse){
		func(r *pb.TxnResponse) { r.Header = nil },
		func(r *pb.TxnResponse) { r.Header.RaftTerm = 0 },
		func(r *pb.TxnResponse) { r.Responses = nil },
		func(r *pb.TxnResponse) { r.Responses[0] = nil },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Header = nil },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Header.RaftTerm++ },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().More = true },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Count = 0 },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Kvs[0].Key = []byte("bad") },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Kvs[0].Value = []byte("bad") },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Kvs[0].ModRevision = 0 },
		func(r *pb.TxnResponse) { r.Responses[0].GetResponseRange().Kvs[0].ModRevision = 44 },
	} {
		r := proto.Clone(failed).(*pb.TxnResponse)
		mutate(r)
		if _, _, _, err := leanResponse("cas", testWire(t, r), []byte("k"), []byte("v")); err == nil {
			t.Fatal("accepted bad CAS response")
		}
	}
	for _, m := range []*pb.PutResponse{{}, {Header: &pb.ResponseHeader{Revision: 1}}, {Header: &pb.ResponseHeader{RaftTerm: 1}}, {Header: h, PrevKv: &mvcc.KeyValue{}}} {
		if _, _, _, err := leanResponse("put", testWire(t, m), nil, nil); err == nil {
			t.Fatal("accepted bad Put")
		}
	}
	for _, wire := range [][]byte{nil, {0, 0, 0, 0, 1}, {1, 0, 0, 0, 0}, append(testWire(t, put), 0), grpcWire([]byte{0xff})} {
		if _, _, _, err := leanResponse("put", wire, nil, nil); err == nil {
			t.Fatal("accepted bad framing/protobuf")
		}
	}
}

func TestLeanResponseHeaders(t *testing.T) {
	initial := []hpack.HeaderField{{Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}}
	p := &leanPending{}
	if err := p.responseHeaders(initial, false); err != nil {
		t.Fatal(err)
	}
	if err := p.responseHeaders([]hpack.HeaderField{{Name: "grpc-status", Value: "0"}}, true); err != nil {
		t.Fatal(err)
	}
	for _, fields := range [][]hpack.HeaderField{
		{{Name: ":status", Value: "500"}, {Name: "content-type", Value: "application/grpc"}},
		{{Name: ":status", Value: "200"}},
		{{Name: ":status", Value: "200"}, {Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}},
	} {
		if err := (&leanPending{}).responseHeaders(fields, false); err == nil {
			t.Fatal("accepted bad HTTP headers")
		}
	}
	for _, fields := range [][]hpack.HeaderField{nil, {{Name: "grpc-status", Value: "13"}}, {{Name: "grpc-status", Value: "0"}, {Name: "grpc-status", Value: "0"}}} {
		p := &leanPending{}
		_ = p.responseHeaders(initial, false)
		if err := p.responseHeaders(fields, true); err == nil {
			t.Fatal("accepted missing/bad/duplicate trailers")
		}
	}
	p = &leanPending{}
	_ = p.responseHeaders(initial, false)
	if err := p.responseHeaders([]hpack.HeaderField{{Name: "grpc-status", Value: "0"}}, false); err == nil {
		t.Fatal("accepted nonterminal status")
	}
}

func TestLeanHPACKResize(t *testing.T) {
	l := &leanClient{fields: []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "https"}, {Name: ":path", Value: "/etcdserverpb.KV/Txn"}, {Name: ":authority", Value: "fixture"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}, {Name: "user-agent", Value: "bench75-lean/1"}}}
	l.encoder = hpack.NewEncoder(&l.headerBuf)
	l.f = http2.NewFramer(io.Discard, nil)
	decoder := hpack.NewDecoder(4096, nil)
	pending := map[uint32]*leanPending{}
	free := []*leanPending{}
	r := leanResult{}
	for _, size := range []uint32{4096, 128, 0, 4096} {
		if err := l.handleEvent(leanEvent{settings: []http2.Setting{{ID: http2.SettingHeaderTableSize, Val: size}}}, pending, &free, &r); err != nil {
			t.Fatal(err)
		}
		if !l.needHeaders {
			t.Fatal("resize did not invalidate cached headers")
		}
		if err := l.prepareHeaders(); err != nil {
			t.Fatal(err)
		}
		for _, block := range [][]byte{l.firstHeaders, l.headers, l.headers, l.headers} {
			fields, err := decoder.DecodeFull(block)
			if err != nil || len(fields) != len(l.fields) {
				t.Fatalf("table %d: %v %v", size, fields, err)
			}
			for i := range fields {
				if fields[i].Name != l.fields[i].Name || fields[i].Value != l.fields[i].Value {
					t.Fatal("HPACK state desynchronized")
				}
			}
		}
	}
}

func TestLeanFlowControlEvents(t *testing.T) {
	var out bytes.Buffer
	l := &leanClient{window: 100, initial: 65535, next: 101}
	l.f = http2.NewFramer(&out, nil)
	p := &leanPending{window: 10, gotHeaders: true}
	pending := map[uint32]*leanPending{1: p}
	free := []*leanPending{}
	r := leanResult{}
	// Accumulate padded DATA without emitting per-frame credit. Even after
	// nearly 64KiB, neither connection nor stream credit is replenished yet.
	for _, flow := range []uint32{100, 16384, 16384, 16384, 16283} {
		if err := l.handleEvent(leanEvent{id: 1, data: make([]byte, flow-1), flow: flow}, pending, &free, &r); err != nil {
			t.Fatal(err)
		}
		if out.Len() != 0 {
			t.Fatal("receive credit was not coalesced")
		}
	}
	if l.received != leanReceiveCredit-1 || p.received != leanReceiveCredit-1 {
		t.Fatal("lost padded DATA credit")
	}
	if err := l.handleEvent(leanEvent{id: 1, flow: 1}, pending, &free, &r); err != nil {
		t.Fatal(err)
	}
	if l.received != 0 || p.received != 0 {
		t.Fatal("threshold credit was not reset")
	}
	f := http2.NewFramer(io.Discard, &out)
	for _, id := range []uint32{0, 1} {
		frame, err := f.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		w, ok := frame.(*http2.WindowUpdateFrame)
		if !ok || w.StreamID != id || w.Increment != leanReceiveCredit {
			t.Fatalf("bad receive credit %v", frame)
		}
	}
	if err := l.handleEvent(leanEvent{settings: []http2.Setting{{ID: http2.SettingInitialWindowSize, Val: 0}, {ID: http2.SettingMaxConcurrentStreams, Val: 2}}}, pending, &free, &r); err != nil {
		t.Fatal(err)
	}
	if p.window != 10-65535 || l.maxConcurrent != 2 {
		t.Fatal("SETTINGS failed to adjust active stream windows")
	}
	if err := l.handleEvent(leanEvent{id: 1, delta: 65535}, pending, &free, &r); err != nil || p.window != 10 {
		t.Fatal("lost stream WINDOW_UPDATE")
	}
	if err := l.handleEvent(leanEvent{id: 99, delta: 100}, pending, &free, &r); err != nil {
		t.Fatal("rejected legal late window update")
	}
	if err := l.handleEvent(leanEvent{id: 103, delta: 100}, pending, &free, &r); err == nil {
		t.Fatal("accepted idle stream WINDOW_UPDATE")
	}
	if err := l.handleEvent(leanEvent{id: 1, end: true}, pending, &free, &r); err == nil {
		t.Fatal("accepted DATA END_STREAM without status trailers")
	}
}

func TestLeanUnaryCreditCoalescing(t *testing.T) {
	var out bytes.Buffer
	l := &leanClient{op: "put", keys: [][]byte{[]byte("k")}, values: [][]byte{[]byte("v")}, received: leanReceiveCredit - 30}
	l.f = http2.NewFramer(&out, nil)
	wire := testWire(t, &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 43, RaftTerm: 1}})
	if len(wire) > 20 {
		t.Fatal("fixture does not fit padded DATA")
	}
	free := []*leanPending{}
	r := leanResult{terms: map[uint64]int{}}
	for _, id := range []uint32{1, 3, 5} {
		pending := map[uint32]*leanPending{id: {gotHeaders: true}}
		if err := l.handleEvent(leanEvent{id: id, data: wire, flow: 20}, pending, &free, &r); err != nil {
			t.Fatal(err)
		}
		if err := l.handleEvent(leanEvent{id: id, headers: []hpack.HeaderField{{Name: "grpc-status", Value: "0"}}, end: true}, pending, &free, &r); err != nil {
			t.Fatal(err)
		}
	}
	if l.received != 20 || len(r.latencies) != 3 {
		t.Fatal("connection credit did not persist across unary streams")
	}
	f := http2.NewFramer(io.Discard, &out)
	frame, err := f.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	w, ok := frame.(*http2.WindowUpdateFrame)
	if !ok || w.StreamID != 0 || w.Increment != leanReceiveCredit+10 || out.Len() != 0 {
		t.Fatalf("expected only one coalesced connection update, got %v (%d extra bytes)", frame, out.Len())
	}
}

type leanDeadlineConn struct {
	net.Conn
	deadlines []time.Time
	err       error
}

func (c *leanDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	c.deadlines = append(c.deadlines, deadline)
	return c.err
}

func TestLeanWriteDeadlineRefresh(t *testing.T) {
	conn := &leanDeadlineConn{}
	l := &leanClient{conn: conn}
	now := time.Unix(1000, 0)
	if err := l.refreshWriteDeadline(now); err != nil {
		t.Fatal(err)
	}
	if len(conn.deadlines) != 1 || !l.writeDeadline.Equal(now.Add(leanTimeout)) {
		t.Fatal("initial deadline not cached")
	}
	for _, offset := range []time.Duration{0, time.Millisecond, time.Second, leanTimeout/2 - time.Nanosecond} {
		if err := l.refreshWriteDeadline(now.Add(offset)); err != nil {
			t.Fatal(err)
		}
		if len(conn.deadlines) != 1 {
			t.Fatal("renewed deadline with more than half its lifetime remaining")
		}
	}
	if err := l.refreshWriteDeadline(now.Add(leanTimeout / 2)); err != nil {
		t.Fatal(err)
	}
	if len(conn.deadlines) != 2 || !l.writeDeadline.Equal(now.Add(leanTimeout+leanTimeout/2)) {
		t.Fatal("did not renew at half-time boundary")
	}
	cached := l.writeDeadline
	conn.err = fmt.Errorf("deadline failure")
	if err := l.refreshWriteDeadline(now.Add(30 * time.Second)); err == nil || !l.writeDeadline.Equal(cached) {
		t.Fatal("lost deadline error or cached a failed renewal")
	}
	conn.err = nil
	if err := l.refreshWriteDeadline(now.Add(30 * time.Second)); err != nil || !l.writeDeadline.Equal(now.Add(40*time.Second)) {
		t.Fatal("did not renew after idle accounting interval")
	}
}

func TestLeanSlotDistribution(t *testing.T) {
	for _, workers := range []int{1, 5, 17, 64, 256} {
		counts := leanSlotCounts(workers, 16)
		if len(counts) != min(workers, 16) {
			t.Fatal("incorrect active connections")
		}
		sum, smallest, largest := 0, math.MaxInt, 0
		for _, n := range counts {
			sum += n
			smallest = min(smallest, n)
			largest = max(largest, n)
		}
		if sum != workers || smallest < 1 || largest-smallest > 1 {
			t.Fatalf("%d workers distributed as %v", workers, counts)
		}
		if workers == 64 && largest != 4 {
			t.Fatal("64 workers must be four outstanding per connection")
		}
		if workers == 256 && largest != 16 {
			t.Fatal("256 workers must be 16 outstanding per connection")
		}
	}
}

func TestLeanGlobalClaims(t *testing.T) {
	p := &leanPhase{total: 103}
	var wg sync.WaitGroup
	seen := make(chan uint64, 103)
	for range 16 {
		wg.Go(func() {
			for {
				i, ok := p.claim()
				if !ok {
					return
				}
				seen <- i
			}
		})
	}
	wg.Wait()
	close(seen)
	ids := map[uint64]bool{}
	for i := range seen {
		if ids[i] || i >= 103 {
			t.Fatal("duplicate/out-of-bounds claim")
		}
		ids[i] = true
	}
	if len(ids) != 103 || !p.exhausted() {
		t.Fatal("not exactly global N")
	}
	p = &leanPhase{deadline: time.Now().Add(-time.Second)}
	if _, ok := p.claim(); ok || !p.exhausted() {
		t.Fatal("admitted past duration")
	}
}

// net.Pipe, not a listening server. Reader and writer are separate to avoid
// full-duplex pipe deadlocks. The peer uses tiny stream windows and caps two
// concurrent streams, so request fragmentation and flow-control are exercised.
type leanFixture struct {
	conn                       net.Conn
	writes                     chan func(*http2.Framer) error
	errors                     chan error
	done                       chan struct{}
	requests                   [][]byte
	maxActive, active, windows int
	warm                       bool
	batch                      []uint32
	bodies                     map[uint32][]byte
	op                         string
	keys, values               [][]byte
	expected                   int
}

func newLeanFixture(conn net.Conn, op string, keys, values [][]byte, expected int, tableSize uint32) *leanFixture {
	s := &leanFixture{conn: conn, writes: make(chan func(*http2.Framer) error, 64), errors: make(chan error, 2), done: make(chan struct{}), bodies: map[uint32][]byte{}, op: op, keys: keys, values: values, expected: expected, warm: true}
	go func() {
		f := http2.NewFramer(conn, nil)
		for write := range s.writes {
			if err := write(f); err != nil {
				s.errors <- err
				return
			}
		}
	}()
	go s.read(tableSize)
	return s
}

func (s *leanFixture) read(tableSize uint32) {
	defer close(s.done)
	defer close(s.writes)
	fail := func(err error) { s.errors <- err }
	rd := bufio.NewReader(s.conn)
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(rd, preface); err != nil {
		fail(err)
		return
	}
	if string(preface) != http2.ClientPreface {
		fail(fmt.Errorf("bad preface"))
		return
	}
	f := http2.NewFramer(io.Discard, rd)
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	s.writes <- func(f *http2.Framer) error {
		return f.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 64}, http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 2}, http2.Setting{ID: http2.SettingHeaderTableSize, Val: tableSize})
	}
	for {
		frame, err := f.ReadFrame()
		if err != nil {
			if len(s.requests) < s.expected {
				fail(err)
			}
			return
		}
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			if !v.IsAck() {
				s.writes <- func(f *http2.Framer) error { return f.WriteSettingsAck() }
			}
		case *http2.WindowUpdateFrame:
			s.windows++
		case *http2.MetaHeadersFrame:
			path := "/etcdserverpb.KV/Put"
			if s.op == "cas" {
				path = "/etcdserverpb.KV/Txn"
			}
			got := map[string]string{}
			for _, h := range v.Fields {
				got[h.Name] = h.Value
			}
			if got[":method"] != "POST" || got[":path"] != path || got["content-type"] != "application/grpc" || got["te"] != "trailers" {
				fail(fmt.Errorf("bad request headers: %v", got))
				return
			}
			s.active++
			s.maxActive = max(s.active, s.maxActive)
			if s.active > 2 {
				fail(fmt.Errorf("MAX_CONCURRENT_STREAMS ignored"))
				return
			}
		case *http2.DataFrame:
			id, n := v.StreamID, v.Header().Length
			if n > 64 {
				fail(fmt.Errorf("stream window exceeded: %d", n))
				return
			}
			s.bodies[id] = append(s.bodies[id], v.Data()...)
			if !v.StreamEnded() {
				s.writes <- func(f *http2.Framer) error { return f.WriteWindowUpdate(id, n) }
				continue
			}
			s.requests = append(s.requests, bytes.Clone(s.bodies[id]))
			s.batch = append(s.batch, id)
			if s.warm || len(s.batch) == 2 || len(s.requests) == s.expected {
				for _, id := range s.batch {
					body, err := s.response(s.bodies[id])
					if err != nil {
						fail(err)
						return
					}
					delete(s.bodies, id)
					s.active--
					s.writes <- func(f *http2.Framer) error {
						var h bytes.Buffer
						enc := hpack.NewEncoder(&h)
						for _, field := range []hpack.HeaderField{{Name: ":status", Value: "200"}, {Name: "content-type", Value: "application/grpc"}} {
							if err := enc.WriteField(field); err != nil {
								return err
							}
						}
						if err := f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: h.Bytes(), EndHeaders: true}); err != nil {
							return err
						}
						if err := f.WriteData(id, false, body); err != nil {
							return err
						}
						h.Reset()
						if err := enc.WriteField(hpack.HeaderField{Name: "grpc-status", Value: "0"}); err != nil {
							return err
						}
						return f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: h.Bytes(), EndHeaders: true, EndStream: true})
					}
				}
				s.batch = nil
				s.warm = false
			}
		}
	}
}

func (s *leanFixture) response(wire []byte) ([]byte, error) {
	if len(wire) < 5 || int(binary.BigEndian.Uint32(wire[1:5])) != len(wire)-5 {
		return nil, fmt.Errorf("bad request envelope")
	}
	h := &pb.ResponseHeader{Revision: int64(43 + len(s.requests)), RaftTerm: 2}
	var response proto.Message = &pb.PutResponse{Header: h}
	if s.op == "put" {
		var req pb.PutRequest
		if err := proto.Unmarshal(wire[5:], &req); err != nil {
			return nil, err
		}
		if len(req.Value) != 1024 {
			return nil, fmt.Errorf("value changed")
		}
	} else {
		var req pb.TxnRequest
		if err := proto.Unmarshal(wire[5:], &req); err != nil {
			return nil, err
		}
		if len(req.Compare) != 1 || req.Compare[0].GetModRevision() <= 0 || len(req.Success) != 1 || len(req.Failure) != 1 {
			return nil, fmt.Errorf("bad CAS request")
		}
		canonical, err := proto.Marshal(&req)
		if err != nil || !bytes.Equal(canonical, wire[5:]) {
			return nil, fmt.Errorf("noncanonical CAS request")
		}
		if len(s.requests)%2 == 1 {
			response = &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: h}}}}}
		} else {
			p := req.Success[0].GetRequestPut()
			response = &pb.TxnResponse{Header: h, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Header: h, Count: 1, Kvs: []*mvcc.KeyValue{{Key: p.Key, Value: p.Value, ModRevision: 42}}}}}}}
		}
	}
	b, err := proto.Marshal(response)
	return grpcWire(b), err
}

func TestLeanInMemoryHTTP2(t *testing.T) {
	for _, tc := range []struct {
		op        string
		tableSize uint32
	}{{"put", 4096}, {"cas", 4096}, {"cas", 0}} {
		t.Run(fmt.Sprintf("%s/table%d", tc.op, tc.tableSize), func(t *testing.T) {
			op := tc.op
			keys, values := make([][]byte, 4096), make([][]byte, 4096)
			revisions := make([]atomic.Int64, len(keys))
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%08d", i%64, i))
				values[i] = bytes.Repeat([]byte{byte(i)}, 1024)
				revisions[i].Store(42)
			}
			templates, err := leanTemplates(keys, values, op)
			if err != nil {
				t.Fatal(err)
			}
			client, peer := net.Pipe()
			fixture := newLeanFixture(peer, op, keys, values, 8, tc.tableSize) // one warm + seven measured
			l, err := newLeanConnection(client, "fixture", op, 4, templates, keys, values, revisions)
			if err != nil {
				peer.Close()
				t.Fatal(err)
			}
			defer func() { l.close(); peer.Close(); <-l.done; <-fixture.done }()
			start := make(chan struct{})
			close(start)
			l.commands <- leanCommand{phase: &leanPhase{start: start, warm: true, total: 1}}
			select {
			case r := <-l.results:
				if len(r.errs) != 0 || len(r.latencies) != 1 {
					t.Fatalf("warm: %+v", r)
				}
			case err := <-fixture.errors:
				t.Fatal(err)
			case <-time.After(2 * time.Second):
				t.Fatal("warm stalled")
			}
			if !l.settingsSeen || !l.settingsAck {
				t.Fatal("warm preceded settings handshake")
			}
			warmReceived := l.received
			if warmReceived == 0 || l.writeDeadline.IsZero() {
				t.Fatal("warm state was not retained")
			}
			// Stale stream credit must be reset before these slots are reused.
			for _, p := range l.free {
				p.received = leanReceiveCredit - 1
			}
			l.commands <- leanCommand{phase: &leanPhase{start: start, total: 7}}
			select {
			case r := <-l.results:
				if len(r.errs) != 0 || len(r.latencies) != 7 || r.terms[2] != 7 {
					t.Fatalf("measured: %+v", r)
				}
				if op == "cas" && (r.passes == 0 || r.failures == 0 || r.passes+r.failures != 7) {
					t.Fatal("CAS outcomes not counted")
				}
			case err := <-fixture.errors:
				t.Fatal(err)
			case <-time.After(2 * time.Second):
				t.Fatal("measured stalled")
			}
			l.close()
			peer.Close()
			<-fixture.done
			if len(fixture.requests) != 8 || fixture.maxActive != 2 || fixture.windows != 1 || l.received <= warmReceived {
				t.Fatalf("count/concurrency/window mismatch: %d/%d/%d", len(fixture.requests), fixture.maxActive, fixture.windows)
			}
			expectedKeys := map[string]int{}
			for i := range 7 {
				expectedKeys[string(keys[(uint64(i)*2654435761)%4096])]++
			}
			for _, wire := range fixture.requests[1:] {
				var got []byte
				if op == "put" {
					var req pb.PutRequest
					if err := proto.Unmarshal(wire[5:], &req); err != nil {
						t.Fatal(err)
					}
					got = req.Key
				} else {
					var req pb.TxnRequest
					if err := proto.Unmarshal(wire[5:], &req); err != nil {
						t.Fatal(err)
					}
					got = req.Compare[0].Key
				}
				if expectedKeys[string(got)] == 0 {
					t.Fatalf("global permutation replaced: unexpected %q", got)
				}
				expectedKeys[string(got)]--
			}
		})
	}
}
