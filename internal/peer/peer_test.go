package peer

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/testcert"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type end struct {
	t           *TCP
	got         chan *raftpb.Message
	unreachable atomic.Int64
}

func start(t *testing.T, id uint64, addrs map[uint64]string, cert tls.Certificate, pool *x509.CertPool) *end {
	t.Helper()
	e := &end{got: make(chan *raftpb.Message, 1000)}
	peers := map[uint64]string{}
	for pid, a := range addrs {
		if pid != id {
			peers[pid] = a
		}
	}
	tr, err := Start(Config{
		ID: id, Listen: addrs[id], Peers: peers, Certificate: cert, CA: pool,
		Deliver:     func(m *raftpb.Message) { e.got <- m },
		Unreachable: func(uint64) { e.unreachable.Add(1) },
		Logf:        t.Logf,
	})
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
	addrs := map[uint64]string{1: freeAddr(t), 2: freeAddr(t)}
	a := start(t, 1, addrs, certs[1], ca.Pool)
	b := start(t, 2, addrs, certs[2], ca.Pool)
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

	// Node 2 comes back on the same address and gets new messages.
	b = start(t, 2, addrs, certs[2], ca.Pool)
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
	addrs := map[uint64]string{1: freeAddr(t), 2: freeAddr(t)}
	a := start(t, 1, addrs, bad, ca.Pool)
	b := start(t, 2, addrs, good, ca.Pool)
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
	tr := &TCP{cfg: Config{Unreachable: func(uint64) { dropped.Add(1) }}}
	s := &sender{t: tr, id: 2, ctrl: make(chan *raftpb.Message, controlQueue), data: make(chan *raftpb.Message, dataQueue)}
	big := make([]byte, 1<<20)
	for range 100 {
		s.send(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), Context: big})
	}
	if dropped.Load() == 0 || s.ctrlBytes.Load() > maxQueued {
		t.Fatalf("queued %d bytes of heartbeats with %d drops; the cap is %d", s.ctrlBytes.Load(), dropped.Load(), maxQueued)
	}
}
