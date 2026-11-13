package peer

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

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
	tc, _, err := loop.Handshake(context.Background(), br, &tls.Config{RootCAs: ca.Pool, ServerName: "peer", MinVersion: tls.VersionTLS13}, false)
	if err != nil {
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
	tc := newTConn(loop.NewBridge(raw, callBuffer), &tls.Config{MinVersion: tls.VersionTLS13}, false)
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

// TestBusyNodeHoldsData runs an inbound connection on a loop while the
// node takes no data messages: of a heartbeat, an append and a heartbeat,
// the first heartbeat steps at once, and the append waits, with the second
// heartbeat behind it, through turns that hold the node's other sockets.
// Once the node takes data again, both go at the loop's next turn that
// takes sockets, in order, with no new bytes from the peer to wake it.
func TestBusyNodeHoldsData(t *testing.T) {
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
	br := loop.NewBridge(a, ioBuffer)
	cli := tls.Client(b, &tls.Config{RootCAs: ca.Pool, ServerName: "peer", MinVersion: tls.VersionTLS13})
	go func() { _ = cli.Handshake() }()
	srv, _, err := loop.Handshake(context.Background(), br, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}, true)
	if err != nil {
		t.Fatal(err)
	}
	var got []raftpb.MessageType
	busy := true
	l := loop.New(loop.NewChanPoller())
	tr := &TCP{cfg: Config{
		Deliver: func(m *raftpb.Message) { got = append(got, m.GetType()) },
		Busy:    func() bool { return busy },
	}, loop: l}
	cs := &counters{received: prometheus.NewCounter(prometheus.CounterOpts{Name: "received"}), heartbeatReceived: prometheus.NewGauge(prometheus.GaugeOpts{Name: "heartbeat"})}
	ic := &inConn{t: tr, tc: srv, br: br, from: 2, v: maxVersion, cs: cs, buf: make([]byte, ioBuffer)}
	sock, token, err := l.Attach(a, ic)
	if err != nil {
		t.Fatal(err)
	}
	br.Attach(sock, nil, nil)
	ic.token = token
	var frames []byte
	for _, typ := range []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgApp, raftpb.MsgHeartbeat} {
		m, err := proto.Marshal(&raftpb.Message{Type: typ.Enum(), From: new(uint64(2)), To: new(uint64(1))})
		if err != nil {
			t.Fatal(err)
		}
		frames = binary.BigEndian.AppendUint32(frames, uint32(len(m)))
		frames = append(frames, m...)
	}
	go func() { _, _ = cli.Write(frames) }()
	// turn takes one turn of the loop as a node's committer does, with the
	// node's sockets held while it is busy.
	turn := func() {
		if err := l.Wait(50 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		l.Dispatch(!busy)
		l.Flush()
	}
	for deadline := time.Now().Add(10 * time.Second); ic.held == nil; turn() {
		if time.Now().After(deadline) {
			t.Fatalf("the append never came; delivered %v", got)
		}
	}
	for range 5 {
		turn()
	}
	if !slices.Equal(got, []raftpb.MessageType{raftpb.MsgHeartbeat}) {
		t.Fatalf("while busy: delivered %v, want the first heartbeat only", got)
	}
	// The held connection asks for the next turn, so the node, once it
	// takes data again, does not wait for anything else to wake it.
	if !l.Holding() {
		t.Fatal("the loop holds nothing for the node's next turn")
	}
	busy = false
	turn()
	want := []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgApp, raftpb.MsgHeartbeat}
	if !slices.Equal(got, want) {
		t.Fatalf("after the node took data again: delivered %v, want %v", got, want)
	}
}
