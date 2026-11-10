package peer

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// holdConn holds the first write of more than holdAbove bytes on any of
// its dialer's connections until release is closed, as a slow link holds
// a large append.
type holdConn struct {
	net.Conn
	h *hold
}

type hold struct {
	once    sync.Once
	held    chan struct{}
	release chan struct{}
}

const holdAbove = 8 << 10

func (c *holdConn) Write(p []byte) (int, error) {
	if len(p) > holdAbove {
		first := false
		c.h.once.Do(func() { first = true; close(c.h.held) })
		if first {
			<-c.h.release
		}
	}
	return c.Conn.Write(p)
}

// TestControlPassesHeldData is #553: while an append is on its way to a
// peer, its heartbeats and votes still arrive. With one connection a
// peer's control messages waited behind the whole append, and its
// followers' election timeouts ran out.
func TestControlPassesHeldData(t *testing.T) {
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
	h := &hold{held: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	release := func() { released.Do(func() { close(h.release) }) }
	t.Cleanup(release)
	addrs, lns := listeners(t, 1, 2)
	a := startWith(t, 1, lns[1], addrs, certs[1], ca.Pool, func(c *Config) {
		c.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
			raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			return &holdConn{Conn: raw, h: h}, nil
		}
	})
	b := start(t, 2, lns[2], addrs, certs[2], ca.Pool)
	defer func() {
		release()
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	}()

	const size = 128 << 10
	a.t.Send(msg(1, strings.Repeat("x", size)))
	select {
	case <-h.held:
	case <-time.After(10 * time.Second):
		t.Fatal("the append never reached the connection")
	}
	from, to, term := uint64(1), uint64(2), uint64(3)
	a.t.Send(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: &term})
	a.t.Send(&raftpb.Message{Type: raftpb.MsgVote.Enum(), From: &from, To: &to, Term: &term})
	deadline := time.After(time.Second)
	for _, want := range []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgVote} {
		select {
		case m := <-b.got:
			if m.GetType() != want {
				t.Fatalf("got a %s while the append is held, want a %s", m.GetType(), want)
			}
		case <-deadline:
			t.Fatalf("no %s within a second while the append is held", want)
		}
	}
	release()
	if m := receive(t, b, 1)[0]; m.GetType() != raftpb.MsgApp || len(m.Entries[0].Data) != size {
		t.Fatalf("after the release: a %s with %d entries", m.GetType(), len(m.Entries))
	}
}

// TestLostLanesCountThePeerOnce is #563: node 2 closes node 1's two lane
// connections one at a time, while node 1 dials no more. With one lane
// down the peer is still connected and not lost; with both down it is
// disconnected and counted lost once.
func TestLostLanesCountThePeerOnce(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 2)
	var dials atomic.Int64
	a := startWith(t, 1, lns[1], addrs, cert, ca.Pool, func(c *Config) {
		c.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
			if dials.Add(1) > 2 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
	})
	b := start(t, 2, lns[2], addrs, cert, ca.Pool)
	defer func() {
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	}()
	s, _ := a.t.peer(2)
	lanesUp := func(want int) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			s.upMu.Lock()
			up := s.up
			s.upMu.Unlock()
			if up == want {
				return
			}
		}
		t.Fatalf("%d lanes never were up", want)
	}
	lanesUp(2)
	// Node 1 counts a lane up once it reads node 2's answer to its
	// preface; node 2 answers before it holds the connection in inbound,
	// so it may hold it a moment later (#606).
	var conns []net.Conn
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		conns = conns[:0]
		b.t.mu.Lock()
		for c, in := range b.t.inbound {
			if in.id == 1 {
				conns = append(conns, c)
			}
		}
		b.t.mu.Unlock()
		if len(conns) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node 2 holds %d connections from node 1, want 2", len(conns))
		}
	}
	for i, c := range conns {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		lanesUp(1 - i)
		// The lane counts itself down before it dials again.
		for deadline := time.Now().Add(5 * time.Second); dials.Load() < int64(3+i) && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		active := testutil.ToFloat64(a.t.cfg.Metrics.ActivePeers.WithLabelValues("1", "2"))
		lost := testutil.ToFloat64(a.t.cfg.Metrics.DisconnectedPeers.WithLabelValues("1", "2"))
		if active != float64(1-i) || lost != float64(i) {
			t.Errorf("with %d of 2 lanes closed: etcd_network_active_peers %g and etcd_network_disconnected_peers_total %g, want %d and %d", i+1, active, lost, 1-i, i)
		}
	}
}
