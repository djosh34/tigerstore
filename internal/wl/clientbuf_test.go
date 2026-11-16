package wl

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// A client that reads its watch slowly holds the node's memory within the
// client buffers (#696's failure on a client connection): while its socket
// takes nothing, its send buffer stays within the connection's share,
// and the node reads nothing more of it. Once it reads, its watch catches
// up and gets every event, in order. Without the bound, the node's send
// buffer to it took every event written.
func TestASlowWatcherStaysWithinItsShare(t *testing.T) {
	const budget = 256 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	cli := tc.client(t)

	// The slow client: a watch of /slow/ on node 1, read by no one.
	conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
	startSlowWatch(t, conn, "/slow/")
	time.Sleep(500 * time.Millisecond) // the watch is created

	const puts, size = 768, 64 << 10 // 48 MiB of events
	val := string(bytes.Repeat([]byte("v"), size))
	for i := range puts {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := cli.Put(ctx, fmt.Sprintf("/slow/%04d", i), val)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second) // node 1 applied them

	s := seeNode(loopOf(1))
	z := sizeFor(budget)
	// Every client within its share, one reply over it (a watch response
	// of up to the request limit), and a send buffer's slack of a record.
	limit := z.clientBufs() + int64(s.clients)*maxMsg + 4*outBuf
	t.Logf("clients %d, share %d MiB; send buffers %d MiB, held for windows %d MiB; limit %d MiB",
		s.clients, s.connOut>>20, s.m.sockOut>>20, s.m.clientHeld>>20, limit>>20)
	if s.m.sockOut+s.m.clientHeld > limit {
		t.Fatalf("the send buffers hold %d MiB for %d MiB of events to a client that reads nothing; the bound is %d MiB",
			(s.m.sockOut+s.m.clientHeld)>>20, puts*size>>20, limit>>20)
	}

	// The client reads: its watch gets every put, in order.
	revs := readWatch(t, conn, puts)
	for i := 1; i < len(revs); i++ {
		if revs[i] != revs[i-1]+1 {
			t.Fatalf("event %d at revision %d after %d", i, revs[i], revs[i-1])
		}
	}
}

// Many idle client connections hold no buffers: 2,000 at the bench's
// 3 GiB, each of which sent its preface and settings, give their buffers
// back once idle, and the client pool holds next to nothing.
func TestManyIdleClientsHoldNoBuffers(t *testing.T) {
	const budget, clients = 3 << 30, 2000
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	for range clients {
		h2Start(t, rawConn(t, tc.endpoints[0], tc.clientTLS()))
	}
	time.Sleep(time.Second) // a few ticks: every client idle since the last
	s := seeNode(loopOf(1))
	z := sizeFor(budget)
	t.Logf("%d connections: the node holds %d clients, %d with buffers, %d KiB of the client pool's %d MiB; sockets in %d KiB out %d KiB",
		clients, s.clients, s.active, s.cpool>>10, z.clientPool>>20, s.m.sockIn>>10, s.m.sockOut>>10)
	if s.clients < clients {
		t.Fatalf("the node holds %d of %d client connections", s.clients, clients)
	}
	// Each idle client holds idleConn at most outside the pool
	// (TestAnIdleClientHoldsNoBuffers); in it, nothing.
	if s.active != 0 || s.cpool != 0 || s.cout != 0 {
		t.Fatalf("%d idle clients hold buffers, %d KiB of the client pool and %d KiB to send", s.active, s.cpool>>10, s.cout>>10)
	}
}

// An idle client connection holds no buffer and at most idleConn: its
// client, HPACK decoder with its table full, header cache full, and flow
// state.
func TestAnIdleClientHoldsNoBuffers(t *testing.T) {
	n, _ := testNode(t)
	const k = 1000
	cs := make([]*client, k)
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	// Each at its fullest: an HPACK table of 4,096 bytes, and the
	// header cache full of blocks as long as it takes.
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for i := range 64 {
		if err := enc.WriteField(hpack.HeaderField{Name: fmt.Sprintf("x-%02d", i), Value: strings.Repeat("v", 64)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range cs {
		c := newClient(&handoff{fd: -1}, n)
		if _, err := c.dec.Write(hb.Bytes()); err != nil {
			t.Fatal(err)
		}
		for j := range hdrCacheMax {
			c.hdrCache[fmt.Sprintf("%0*d", hdrCacheBlock, j)] = hdrInfo{}
		}
		cs[i] = c
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)
	per := int64(m1.HeapAlloc-m0.HeapAlloc) / k
	t.Logf("an idle client holds %d bytes", per)
	if per > idleConn || n.cpool != 0 {
		t.Fatalf("an idle client holds %d bytes, past %d, and %d of the pool", per, idleConn, n.cpool)
	}
	runtime.KeepAlive(cs)
}

// Past maxClients the node closes a new client connection at once: a
// backstop, which at 256 MiB takes 256.
func TestTheNodeClosesClientsPastItsBackstop(t *testing.T) {
	const budget = 256 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	most := sizeFor(budget).maxClients()
	var conns []*tls.Conn
	for range most + 20 {
		c := rawConn(t, tc.endpoints[0], tc.clientTLS())
		h2Start(t, c)
		conns = append(conns, c)
	}
	time.Sleep(500 * time.Millisecond)
	s := seeNode(loopOf(1))
	closed := 0
	for _, c := range conns {
		if err := c.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 64<<10)
		for {
			if _, err := c.Read(b); err != nil {
				if ne, ok := errors.AsType[net.Error](err); !ok || !ne.Timeout() {
					closed++
				}
				break
			}
		}
	}
	t.Logf("%d connections: the node holds %d clients (at most %d), refused %d, closed %d", len(conns), s.clients, most, s.refused, closed)
	if s.clients > most || s.refused == 0 || closed < len(conns)-most {
		t.Fatalf("%d connections: the node holds %d clients, at most %d, refused %d and closed %d", len(conns), s.clients, most, s.refused, closed)
	}
}

// Requests sent in part stay within the connection's share: a client
// that starts many large Puts and sends half of each gets the ones past
// its share refused with etcd's "too many requests", and one that says
// it is larger than the request limit is refused before it comes in.
func TestHalfSentRequestsStayWithinTheShare(t *testing.T) {
	const budget = 256 << 20
	loopOf := watchLoops(t)
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	conn := rawConn(t, tc.endpoints[0], tc.clientTLS())
	fr := h2Start(t, conn)
	const calls, half = 40, 512 << 10
	part := make([]byte, half)
	send := func(sid uint32, size int, p []byte) {
		h2Call(t, fr, sid, "/etcdserverpb.KV/Put")
		hdr := make([]byte, 5)
		binary.BigEndian.PutUint32(hdr[1:], uint32(size))
		if err := fr.WriteData(sid, false, hdr); err != nil {
			t.Fatal(err)
		}
		for len(p) > 0 {
			k := min(len(p), 16<<10)
			if err := fr.WriteData(sid, false, p[:k]); err != nil {
				t.Fatal(err)
			}
			p = p[k:]
		}
	}
	send(1, maxMsg+1, nil)
	for i := range calls {
		send(uint32(3+2*i), 1<<20, part)
	}
	time.Sleep(time.Second)
	s := seeNode(loopOf(1))

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	rd := http2.NewFramer(io.Discard, conn)
	rd.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	tooMany, tooLarge := 0, 0
	for {
		f, err := rd.ReadFrame()
		if err != nil {
			break
		}
		mh, ok := f.(*http2.MetaHeadersFrame)
		if !ok {
			continue
		}
		switch msg := grpcMessage(mh); {
		case strings.Contains(msg, "too many requests"):
			tooMany++
		case strings.Contains(msg, "larger than max"):
			tooLarge++
		}
	}
	t.Logf("%d Puts half sent: %d refused as too many, %d as too large; %d KiB held in part, the share %d KiB",
		calls, tooMany, tooLarge, s.m.clientPartial>>10, s.connIn>>10)
	if s.m.clientPartial > s.connIn || s.m.clientPartial == 0 {
		t.Fatalf("the node holds %d KiB of requests sent in part, its share is %d KiB", s.m.clientPartial>>10, s.connIn>>10)
	}
	if tooMany == 0 || tooLarge != 1 {
		t.Fatalf("%d refused as too many requests, %d as larger than the limit; want some, and one", tooMany, tooLarge)
	}
}

// grpcMessage is the grpc-message field of h, if any.
func grpcMessage(h *http2.MetaHeadersFrame) string {
	for _, f := range h.Fields {
		if f.Name == "grpc-message" {
			return f.Value
		}
	}
	return ""
}

// watchLoops has the nodes the test starts tell it their loops, and
// returns each by its node's ID.
func watchLoops(t *testing.T) func(id uint64) *loop {
	var mu sync.Mutex
	loops := map[uint64]*loop{}
	testLoops = func(id uint64, l *loop) {
		mu.Lock()
		defer mu.Unlock()
		loops[id] = l
	}
	t.Cleanup(func() { testLoops = nil })
	return func(id uint64) *loop {
		mu.Lock()
		defer mu.Unlock()
		return loops[id]
	}
}

// nodeSeen is what a node's loop showed of its client connections.
type nodeSeen struct {
	m               memStats
	connOut, connIn int64
	clients, active int
	refused         uint64
	cpool, cout     int64
}

// seeNode asks l's loop for its memory and its clients' shares.
func seeNode(l *loop) nodeSeen {
	got := make(chan nodeSeen, 1)
	l.post(func() error {
		got <- nodeSeen{l.n.memStats(), l.n.connOut, l.n.connIn, l.n.clients, l.n.active, l.n.stats.clientsRefused, l.n.cpool, l.n.cout}
		return nil
	})
	return <-got
}

// rawConn dials a client connection to addr that speaks HTTP/2, with a
// small receive buffer so that the kernel holds little for it.
func rawConn(t *testing.T, addr string, cfg *tls.Config) *tls.Conn {
	t.Helper()
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 64<<10)
		}); err != nil {
			return err
		}
		return serr
	}}
	cfg.NextProtos = []string{"h2"}
	conn, err := tls.DialWithDialer(&d, "tcp", addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// h2Start opens HTTP/2 on conn with windows as large as it allows, so
// that only its socket holds the node back.
func h2Start(t *testing.T, conn net.Conn) *http2.Framer {
	t.Helper()
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1<<31 - 1}); err != nil {
		t.Fatal(err)
	}
	if err := fr.WriteWindowUpdate(0, 1<<31-1-65535); err != nil {
		t.Fatal(err)
	}
	return fr
}

// h2Call starts a gRPC call of path on stream sid.
func h2Call(t *testing.T, fr *http2.Framer, sid uint32, path string) {
	t.Helper()
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, f := range [][2]string{
		{":method", "POST"},
		{":scheme", "https"},
		{":path", path},
		{":authority", "localhost"},
		{"content-type", "application/grpc"},
		{"te", "trailers"},
	} {
		if err := enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: sid, BlockFragment: hb.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}
}

// startSlowWatch opens a gRPC Watch of prefix on conn, by hand.
func startSlowWatch(t *testing.T, conn net.Conn, prefix string) {
	t.Helper()
	fr := h2Start(t, conn)
	h2Call(t, fr, 1, "/etcdserverpb.Watch/Watch")
	req, err := proto.Marshal(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 5, 5+len(req))
	binary.BigEndian.PutUint32(msg[1:], uint32(len(req)))
	if err := fr.WriteData(1, false, append(msg, req...)); err != nil {
		t.Fatal(err)
	}
}

// readWatch reads conn's watch responses until they hold n events, and
// returns the events' revisions.
func readWatch(t *testing.T, conn net.Conn, n int) []int64 {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(io.Discard, conn)
	fr.MaxHeaderListSize = 1 << 20
	var data []byte
	var revs []int64
	for len(revs) < n {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("after %d events: %v", len(revs), err)
		}
		df, ok := f.(*http2.DataFrame)
		if !ok {
			continue
		}
		data = append(data, df.Data()...)
		for len(data) >= 5 {
			k := int(binary.BigEndian.Uint32(data[1:]))
			if len(data) < 5+k {
				break
			}
			var resp pb.WatchResponse
			if err := proto.Unmarshal(data[5:5+k], &resp); err != nil {
				t.Fatal(err)
			}
			if resp.Canceled || resp.CompactRevision != 0 {
				t.Fatalf("the watch was canceled after %d events: %v", len(revs), &resp)
			}
			for _, ev := range resp.Events {
				revs = append(revs, ev.Kv.ModRevision)
			}
			data = data[5+k:]
		}
	}
	return revs
}
