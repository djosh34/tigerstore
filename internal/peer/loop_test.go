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

// TestStalledLaneKeepsItsMessages flushes a lane twice into a socket that
// takes nothing: the second flush must leave its message on the lane,
// where the lane's limits count it, instead of encoding it behind the
// first, which would grow what waits below the lane without a bound.
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
	tr := &TCP{cfg: Config{Unreachable: func(uint64) {}, Metrics: set}, loop: loop.New(loop.NewChanPoller())}
	s := &sender{t: tr, id: 2, counters: tr.countersOf(2)}
	s.ctrl.max, s.data.max = controlQueue, dataQueue
	oc := &outConn{s: s, l: &s.data, tc: tc, br: br, v: maxVersion, down: make(chan error, 1)}
	s.data.out = oc
	big := strings.Repeat("x", 1<<20)
	s.send(msg(1, big))
	oc.Flush()
	held := br.Pending()
	if held < 1<<20 || s.data.bytes != 0 {
		t.Fatalf("after the first flush %d bytes wait below the lane and it charges %d", held, s.data.bytes)
	}
	s.send(msg(2, big))
	oc.Flush()
	if br.Pending() != held || s.data.bytes < 1<<20 {
		t.Fatalf("after the second flush %d bytes wait below the lane (%d before) and it charges %d", br.Pending(), held, s.data.bytes)
	}
}
