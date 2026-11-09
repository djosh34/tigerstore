package peer

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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
