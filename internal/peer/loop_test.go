package peer

import (
	"crypto/tls"
	"net"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/loop"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/testcert"
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
	br.Attach(stuck{})
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
