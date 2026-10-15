package peer

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// listen listens on addr, or on a free port if addr is empty. The tests
// hand the open listener to the transport, so no other process can take
// the port in between; a port that a closed transport just released can
// be busy for a moment, so it retries for a while.
func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// listeners listens on a free port for each ID and returns the addresses
// and the listeners.
func listeners(t *testing.T, ids ...uint64) (map[uint64]string, map[uint64]net.Listener) {
	t.Helper()
	addrs, lns := map[uint64]string{}, map[uint64]net.Listener{}
	for _, id := range ids {
		lns[id] = listen(t, "")
		addrs[id] = lns[id].Addr().String()
	}
	return addrs, lns
}

type end struct {
	t           *TCP
	got         chan *raftpb.Message
	unreachable atomic.Int64
}

// start starts node id on ln, or on a new listener on its address if ln
// is nil.
func start(t *testing.T, id uint64, ln net.Listener, addrs map[uint64]string, cert tls.Certificate, pool *x509.CertPool) *end {
	t.Helper()
	return startWith(t, id, ln, addrs, cert, pool, func(*Config) {})
}

// startWith is start with set applied to the config.
func startWith(t *testing.T, id uint64, ln net.Listener, addrs map[uint64]string, cert tls.Certificate, pool *x509.CertPool, set func(*Config)) *end {
	t.Helper()
	e := &end{got: make(chan *raftpb.Message, 1000)}
	peers := map[uint64]string{}
	for pid, a := range addrs {
		if pid != id {
			peers[pid] = a
		}
	}
	if ln == nil {
		ln = listen(t, addrs[id])
	}
	cfg := Config{
		ID: id, Listeners: []net.Listener{ln}, Peers: peers, Certificate: cert, CA: pool,
		Deliver:     func(m *raftpb.Message) { e.got <- m },
		Unreachable: func(uint64) { e.unreachable.Add(1) },
		Logf:        t.Logf,
	}
	set(&cfg)
	tr, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.t = tr
	return e
}

// msg is a message with one entry from node 1 to node 2.
func msg(index uint64, data string) *raftpb.Message {
	return &raftpb.Message{
		Type: raftpb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Index: new(index),
		Entries: []*raftpb.Entry{{Index: new(index), Data: []byte(data)}},
	}
}

func receive(t *testing.T, e *end, n int) []*raftpb.Message {
	t.Helper()
	var out []*raftpb.Message
	timeout := time.After(10 * time.Second)
	for len(out) < n {
		select {
		case m := <-e.got:
			out = append(out, m)
		case <-timeout:
			t.Fatalf("got %d of %d messages", len(out), n)
		}
	}
	return out
}

func TestSendInOrderAndReconnect(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := map[uint64]tls.Certificate{}
	for id := uint64(1); id <= 2; id++ {
		c, _, _, err := ca.Issue("node", int64(id+1))
		if err != nil {
			t.Fatal(err)
		}
		certs[id] = c
	}
	addrs, lns := listeners(t, 1, 2)
	a := start(t, 1, lns[1], addrs, certs[1], ca.Pool)
	b := start(t, 2, lns[2], addrs, certs[2], ca.Pool)
	defer func() {
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
	}()

	for i := uint64(1); i <= 200; i++ {
		a.t.Send(msg(i, "x"))
	}
	for i, m := range receive(t, b, 200) {
		if m.GetIndex() != uint64(i+1) || m.GetFrom() != 1 || string(m.Entries[0].Data) != "x" {
			t.Fatalf("message %d = %v", i, m)
		}
	}
	// Both ends count the same frames, and node 1 has an active
	// connection to node 2.
	sent := testutil.ToFloat64(a.t.cfg.Metrics.PeerSent.WithLabelValues("2"))
	received := testutil.ToFloat64(b.t.cfg.Metrics.PeerReceived.WithLabelValues("1"))
	if sent == 0 || sent != received {
		t.Fatalf("node 1 sent %g bytes to node 2, which received %g", sent, received)
	}
	if active := testutil.ToFloat64(a.t.cfg.Metrics.ActivePeers.WithLabelValues("1", "2")); active != 1 {
		t.Fatalf("etcd_network_active_peers is %g", active)
	}

	// Node 2 goes away: sends are dropped and reported, never blocking.
	if err := b.t.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for a.unreachable.Load() == 0 && time.Now().Before(deadline) {
		a.t.Send(msg(1, "lost"))
		time.Sleep(10 * time.Millisecond)
	}
	if a.unreachable.Load() == 0 {
		t.Fatal("no unreachable report while the peer was down")
	}
	if n := testutil.ToFloat64(a.t.cfg.Metrics.DisconnectedPeers.WithLabelValues("1", "2")); n == 0 {
		t.Fatal("etcd_network_disconnected_peers_total did not count the lost peer")
	}

	// Node 2 comes back on the same address and gets new messages.
	b = start(t, 2, nil, addrs, certs[2], ca.Pool)
	defer func() {
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	}()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a.t.Send(msg(999, "back"))
		select {
		case m := <-b.got:
			if string(m.Entries[0].Data) == "back" {
				return
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("no delivery after the peer came back")
}

func TestRejectsOtherCA(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	other, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	good, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	bad, _, _, err := other.Issue("node", 3)
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 2)
	a := start(t, 1, lns[1], addrs, bad, ca.Pool)
	b := start(t, 2, lns[2], addrs, good, ca.Pool)
	defer func() {
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	}()
	for range 20 {
		a.t.Send(msg(1, "x"))
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case m := <-b.got:
		t.Fatalf("a node with a certificate from another CA delivered %v", m)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestQueueByType(t *testing.T) {
	cases := []struct {
		m    *raftpb.Message
		data bool
	}{
		{&raftpb.Message{Type: raftpb.MsgApp.Enum()}, true}, // an empty append updates commit
		{msg(1, "x"), true},
		{&raftpb.Message{Type: raftpb.MsgSnap.Enum()}, true},
		{&raftpb.Message{Type: raftpb.MsgProp.Enum(), Entries: []*raftpb.Entry{{Data: make([]byte, 4<<20)}}}, true},
		{&raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum()}, false},
		{&raftpb.Message{Type: raftpb.MsgAppResp.Enum()}, false},
		{&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum()}, false},
		{&raftpb.Message{Type: raftpb.MsgReadIndex.Enum(), Entries: []*raftpb.Entry{{Data: []byte("ctx")}}}, false},
		{&raftpb.Message{Type: raftpb.MsgReadIndexResp.Enum(), Entries: []*raftpb.Entry{{Data: []byte("ctx")}}}, false},
		{&raftpb.Message{Type: raftpb.MsgVote.Enum()}, false},
	}
	for _, c := range cases {
		if got := isData(c.m); got != c.data {
			t.Errorf("%s with %d entries: data queue = %v, want %v", c.m.GetType(), len(c.m.Entries), got, c.data)
		}
	}
}

func TestControlQueueIsCapped(t *testing.T) {
	var dropped atomic.Int64
	set, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := &TCP{cfg: Config{Unreachable: func(uint64) { dropped.Add(1) }, Metrics: set}}
	s := &sender{t: tr, id: 2, ctrl: make(chan queued, controlQueue), data: make(chan queued, dataQueue), counters: tr.countersOf(2)}
	big := make([]byte, 1<<20)
	for range 100 {
		s.send(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), Context: big})
	}
	if dropped.Load() == 0 || s.ctrlBytes.Load() > maxQueued {
		t.Fatalf("queued %d bytes of heartbeats with %d drops; the cap is %d", s.ctrlBytes.Load(), dropped.Load(), maxQueued)
	}
	// Each drop is a send failure to the peer, as etcd counts them.
	if got := testutil.ToFloat64(set.PeerSentFailures.WithLabelValues("2")); got != float64(dropped.Load()) {
		t.Fatalf("etcd_network_peer_sent_failures_total is %g after %d drops", got, dropped.Load())
	}
}
