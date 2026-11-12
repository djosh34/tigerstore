package peer

import (
	"crypto/tls"
	"encoding/binary"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// stuck is a socket that takes nothing, as a peer's that stalled.
type stuck struct{}

func (stuck) Read([]byte) (int, error)  { return 0, loop.ErrWouldBlock }
func (stuck) Write([]byte) (int, error) { return 0, loop.ErrWouldBlock }
func (stuck) Close() error              { return nil }

// TestStalledLaneKeepsItsMessages flushes a lane into a socket that takes
// nothing. A flush must leave later messages on the lane instead of
// encoding them behind the first batch, and the lane must count that
// batch until the socket took it: so its limit bounds everything it holds,
// queued or encoded, and a full lane drops what comes.
func TestStalledLaneKeepsItsMessages(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueFor("node", 2, nil, []string{"peer"})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer a.Close() //nolint:errcheck // closing a test pipe
	defer b.Close() //nolint:errcheck // closing a test pipe
	srv := tls.Server(b, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	go func() { _ = srv.Handshake() }()
	br := loop.NewBridge(a, ioBuffer)
	tc := tls.Client(br, &tls.Config{RootCAs: ca.Pool, ServerName: "peer", MinVersion: tls.VersionTLS13})
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	br.Attach(stuck{}, nil, nil)
	set, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var dropped int
	tr := &TCP{cfg: Config{Unreachable: func(uint64) { dropped++ }, Metrics: set}, loop: loop.New(loop.NewChanPoller())}
	s := &sender{t: tr, id: 2, counters: tr.countersOf(2)}
	s.ctrl.max, s.data.max = controlQueue, dataQueue
	oc := &outConn{s: s, l: &s.data, tc: tc, br: br, v: maxVersion, down: make(chan error, 1)}
	s.data.out = oc
	s.send(msg(1, strings.Repeat("x", 8<<20)))
	oc.Flush()
	held := br.Pending()
	if held < 8<<20 || s.data.bytes < 8<<20 {
		t.Fatalf("after the first flush %d bytes wait below the lane, which counts %d", held, s.data.bytes)
	}
	big := strings.Repeat("x", 1<<20)
	for i := range uint64(63) {
		s.send(msg(2+i, big))
		oc.Flush()
	}
	if br.Pending() != held {
		t.Fatalf("%d bytes wait below the lane, %d before: later messages left it", br.Pending(), held)
	}
	if dropped == 0 || s.data.bytes > maxQueued {
		t.Fatalf("the lane counts %d bytes with %d drops; its limit is %d", s.data.bytes, dropped, maxQueued)
	}
}

// handingRaw is a raw connection whose close first runs during, as a
// hand-over that wins the race with a close of an old list does.
type handingRaw struct {
	net.Conn
	during func()
}

func (h *handingRaw) Close() error {
	h.during()
	return h.Conn.Close()
}

// TestCloseRacesHandOver closes a TLS connection from an old list while
// it goes to the loop: the close found no loop handle, and the hand-over
// came before the raw close ended. The loop must still hear of the close,
// since a socket closed off the loop sends epoll no event.
func TestCloseRacesHandOver(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck // closing a test pipe
	l := loop.New(loop.NewChanPoller())
	tr := &TCP{loop: l, conns: map[net.Conn]struct{}{}, inbound: map[net.Conn]inbound{}, handed: map[net.Conn]*owned{}}
	raw := &handingRaw{Conn: a}
	tc := tls.Client(loop.NewBridge(raw, callBuffer), &tls.Config{MinVersion: tls.VersionTLS13})
	tr.conns[tc] = struct{}{}
	stopped := false
	raw.during = func() {
		o := tr.handOver(tc)
		o.stop = func() { stopped = true }
	}
	tr.closeAll([]net.Conn{tc})
	l.Dispatch(false)
	if !stopped {
		t.Fatal("the loop did not hear of the close of a connection it took meanwhile")
	}
}

// TestBusyNodeHoldsData reads a heartbeat, an append and a heartbeat from
// one connection while the node takes no data messages: the first
// heartbeat steps, the append waits with the second heartbeat behind it,
// so the order holds, and both go once the node takes data again.
func TestBusyNodeHoldsData(t *testing.T) {
	var got []raftpb.MessageType
	busy := true
	tr := &TCP{cfg: Config{
		Deliver: func(m *raftpb.Message) { got = append(got, m.GetType()) },
		Busy:    func() bool { return busy },
	}, loop: loop.New(loop.NewChanPoller())}
	var frames []byte
	for _, typ := range []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgApp, raftpb.MsgHeartbeat} {
		b, err := proto.Marshal(&raftpb.Message{Type: typ.Enum(), From: new(uint64(2)), To: new(uint64(1))})
		if err != nil {
			t.Fatal(err)
		}
		frames = binary.BigEndian.AppendUint32(frames, uint32(len(b)))
		frames = append(frames, b...)
	}
	cs := &counters{received: prometheus.NewCounter(prometheus.CounterOpts{Name: "received"}), heartbeatReceived: prometheus.NewGauge(prometheus.GaugeOpts{Name: "heartbeat"})}
	ic := &inConn{t: tr, from: 2, v: maxVersion, cs: cs, buf: frames, w: len(frames)}
	if err := ic.frames(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != raftpb.MsgHeartbeat || ic.held == nil {
		t.Fatalf("while busy: delivered %v, holding %v", got, ic.held != nil)
	}
	busy = false
	m := ic.held
	ic.held = nil
	tr.cfg.Deliver(m)
	if err := ic.frames(); err != nil {
		t.Fatal(err)
	}
	want := []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgApp, raftpb.MsgHeartbeat}
	if !slices.Equal(got, want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
}
