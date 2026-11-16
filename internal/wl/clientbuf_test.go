package wl

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	var mu sync.Mutex
	loops := map[uint64]*loop{}
	testLoops = func(id uint64, l *loop) {
		mu.Lock()
		defer mu.Unlock()
		loops[id] = l
	}
	t.Cleanup(func() { testLoops = nil })
	tc := newTestClusterWith(t, func(tc *testCluster) { tc.budget = budget })
	cli := tc.client(t)

	// The slow client: a watch of /slow/ on node 1, read by no one, with
	// a small receive buffer so that the kernel holds little for it.
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 64<<10)
		}); err != nil {
			return err
		}
		return serr
	}}
	cfg := tc.clientTLS()
	cfg.NextProtos = []string{"h2"}
	conn, err := tls.DialWithDialer(&d, "tcp", tc.endpoints[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
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

	mu.Lock()
	l := loops[1]
	mu.Unlock()
	type seen struct {
		m       memStats
		connOut int64
		clients int
	}
	got := make(chan seen, 1)
	l.post(func() error {
		k := 0
		for _, x := range l.n.socks {
			if _, ok := x.(*client); ok {
				k++
			}
		}
		got <- seen{l.n.memStats(), l.n.connOut, k}
		return nil
	})
	s := <-got
	z := sizeFor(budget)
	// Every client within its share, one reply over it (a watch response
	// of up to the request limit), and a send buffer's slack of a record.
	limit := z.clientOut + int64(s.clients)*(maxRequestBytes+outBuf)
	t.Logf("clients %d, share %d MiB; send buffers %d MiB, held for windows %d MiB; limit %d MiB",
		s.clients, s.connOut>>20, s.m.sockOut>>20, s.m.clientHeld>>20, limit>>20)
	if s.m.sockOut+s.m.clientHeld > limit {
		t.Fatalf("the client buffers hold %d MiB for %d MiB of events to a client that reads nothing; the bound is %d MiB",
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

// startSlowWatch opens a gRPC Watch of prefix on conn, by hand: the
// client's windows are as large as HTTP/2 allows, so that only its socket
// holds the node back.
func startSlowWatch(t *testing.T, conn net.Conn, prefix string) {
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
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, f := range [][2]string{
		{":method", "POST"},
		{":scheme", "https"},
		{":path", "/etcdserverpb.Watch/Watch"},
		{":authority", "localhost"},
		{"content-type", "application/grpc"},
		{"te", "trailers"},
	} {
		if err := enc.WriteField(hpack.HeaderField{Name: f[0], Value: f[1]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}
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
