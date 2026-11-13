package peer

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/loop"
)

// Message connections run on the node's event loop (Config.Loop). Their
// dial or accept and their handshake run on goroutines, as do snapshot
// fetches and calls. Once a message connection is up, the loop owns it:
// it reads an inbound connection's frames and hands their messages to
// Deliver in the same turn, and it writes the frames that a turn's Sends
// queued on an outbound one, with one write for all of them.

// listener accepts raw connections under a bridge, so that a message
// connection's TLS records can move to the loop after the handshake.
type listener struct {
	net.Listener
	cfg *tls.Config
}

func (l listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newTConn(loop.NewBridge(c, callBuffer), l.cfg, true), nil
}

// bridgeOf is the bridge under a connection that dial or listener made.
func bridgeOf(c net.Conn) *loop.Bridge {
	return c.(*tconn).br
}

// owned is a message connection that the loop owns, as the transport
// tracks it once the handshake is done. A close from another goroutine
// closes the raw connection, since the TLS connection's close would write
// into the bridge, and tells the loop, which ends the connection's handler
// there: an epoll socket that another goroutine closes sends no event.
type owned struct {
	net.Conn
	t  *TCP
	tc net.Conn
	// stop ends the connection's handler; loop only, nil until it has one.
	stop func()
}

func (o *owned) Close() error {
	err := o.Conn.Close()
	o.t.loop.Post(o.ended)
	return err
}

// ended runs on the loop for a close from another goroutine.
func (o *owned) ended() {
	if o.stop != nil {
		o.stop()
	}
}

// handOver moves a tracked TLS connection to the loop's ownership, before
// its hand-over is posted, so that from then on nothing but the loop
// writes it, nor closes it but by its raw connection.
func (t *TCP) handOver(c net.Conn) *owned {
	o := &owned{Conn: bridgeOf(c).Conn, t: t, tc: c}
	t.mu.Lock()
	t.handed[c] = o
	delete(t.conns, c)
	t.conns[o] = struct{}{}
	if in, ok := t.inbound[c]; ok {
		delete(t.inbound, c)
		t.inbound[o] = in
	}
	t.mu.Unlock()
	return o
}

// release stops tracking a connection the loop owned, and closes it. It
// runs on the loop.
func (t *TCP) release(o *owned) {
	t.mu.Lock()
	delete(t.conns, o)
	delete(t.inbound, o)
	delete(t.handed, o.tc)
	t.mu.Unlock()
	if err := o.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logf("peer: close: %v", err)
	}
}

// inConn is an inbound message connection on the loop.
type inConn struct {
	t     *TCP
	tc    *loop.Records
	br    *loop.Bridge
	own   *owned
	token uint64
	from  uint64
	v     byte
	cs    *counters
	// buf holds the bytes read and not yet framed, from r to w, and held
	// a data message that waits for the node to take one (Ready).
	buf    []byte
	r, w   int
	held   *raftpb.Message
	closed bool
}

// attachIn hands an inbound message connection, which handOver gave the
// loop as o, to the loop. It runs on the loop.
func (t *TCP) attachIn(c *loop.Records, o *owned, from uint64, v byte) {
	br := c.NetConn()
	ic := &inConn{t: t, tc: c, br: br, own: o, from: from, v: v, cs: t.countersOf(from), buf: make([]byte, ioBuffer)}
	sock, token, err := t.loop.Attach(br.Conn, ic)
	if err != nil {
		t.logf("peer: receive from %d: %v", from, err)
		t.release(o)
		return
	}
	br.Attach(sock, nil, nil)
	ic.token = token
	o.stop = ic.close
}

// Ready reads what arrived and delivers every whole frame. While the node
// takes no data messages (Config.Busy), the connection stops at the next
// one, which it keeps, and reads nothing more until the node takes it:
// the peer's control lane goes on, and its data lane waits in TCP.
func (c *inConn) Ready() {
	if c.held != nil {
		if c.busy() {
			return
		}
		m := c.held
		c.held = nil
		c.t.cfg.Deliver(m)
	}
	for !c.closed && c.held == nil {
		if c.w == len(c.buf) {
			c.compact(len(c.buf))
		}
		n, err := c.tc.Read(c.buf[c.w:])
		c.w += n
		if ferr := c.frames(); ferr != nil {
			c.fail(ferr)
			return
		}
		if errors.Is(err, loop.ErrWouldBlock) {
			return
		}
		if err != nil {
			c.fail(err)
			return
		}
	}
}

// frames delivers the whole frames in the buffer. A message of held
// entries on a connection of a version that does not carry them is refused
// and counted: a sender never sends one there (outConn.Flush).
func (c *inConn) frames() error {
	t := c.t
	for c.w-c.r >= 4 {
		n := int(binary.BigEndian.Uint32(c.buf[c.r:]))
		if n > maxFrame {
			return fmt.Errorf("frame of %d bytes from peer %d is over the limit", n, c.from)
		}
		if c.w-c.r < 4+n {
			c.compact(4 + n)
			return nil
		}
		// Unmarshal copies bytes fields, so the buffer is reused: the
		// entries' data, which the index keeps slices of once they apply,
		// is the message's own.
		m := &raftpb.Message{}
		if err := proto.Unmarshal(c.buf[c.r+4:c.r+4+n], m); err != nil {
			return err
		}
		c.r += 4 + n
		if m.GetFrom() != c.from {
			return fmt.Errorf("peer %d sent a message from %d", c.from, m.GetFrom())
		}
		c.cs.received.Add(float64(4 + n))
		if m.GetType() == raftpb.MsgHeartbeat {
			c.cs.heartbeatReceived.SetToCurrentTime()
		}
		if c.v < heldVersion && core.HeldForm(m) {
			t.cfg.Metrics.PeerHeldRefused.Inc()
			t.logf("peer: refused a %s of held entries from %d on a connection of version %d", m.GetType(), c.from, c.v)
			continue
		}
		if isData(m) && c.busy() {
			c.held = m
			return nil
		}
		t.cfg.Deliver(m)
	}
	if c.r == c.w {
		c.r, c.w = 0, 0
	}
	return nil
}

// compact moves the unframed bytes to the front of the buffer and makes
// room for a frame of need bytes. A buffer grown for a large frame shrinks
// back once it is empty.
func (c *inConn) compact(need int) {
	if c.r == c.w && len(c.buf) > ioBuffer && need <= ioBuffer {
		c.buf, c.r, c.w = make([]byte, ioBuffer), 0, 0
		return
	}
	if need > len(c.buf) {
		buf := make([]byte, need)
		c.w = copy(buf, c.buf[c.r:c.w])
		c.buf, c.r = buf, 0
		return
	}
	if c.r > 0 {
		c.w = copy(c.buf, c.buf[c.r:c.w])
		c.r = 0
	}
}

// busy reports whether the node takes no data messages now, and if so,
// lists the connection to be read again at the loop's next turn that
// takes sockets.
func (c *inConn) busy() bool {
	if c.t.cfg.Busy == nil || !c.t.cfg.Busy() {
		return false
	}
	c.t.loop.Again(c.token)
	return true
}

// Urgent marks an inbound peer connection as one the loop reads even while
// its owner holds its other sockets (loop.Urgent): heartbeats, votes and
// their answers keep coming while the node's disk is behind.
func (c *inConn) Urgent() {}

func (c *inConn) fail(err error) {
	c.close()
	if !errors.Is(err, io.EOF) && c.t.ctx.Err() == nil {
		c.cs.receiveFailures.Inc()
		c.t.logf("peer: receive from %d: %v", c.from, err)
	}
}

func (c *inConn) close() {
	if c.closed {
		return
	}
	c.closed = true
	c.t.loop.Forget(c.token)
	c.t.release(c.own)
	// The socket closes too: a pump's goroutines end with it.
	_ = c.br.Close()
}

// Stopped closes the connection once the node stopped.
func (c *inConn) Stopped() { c.close() }

// outConn is a lane's connection on the loop: it writes the lane's queue
// and reads only to see the connection end, since the peer writes nothing
// on it.
type outConn struct {
	s     *sender
	l     *lane
	tc    *loop.Records
	br    *loop.Bridge
	own   *owned
	token uint64
	v     byte
	buf   []byte
	// sending is the lane charge of the messages encoded and not yet all
	// sent: the lane keeps counting them until the socket took them.
	sending int64
	// since is when the output that waits for the socket last moved; zero
	// while none waits.
	since  time.Time
	down   chan<- error
	closed bool
}

// attach hands a lane's new connection, which handOver gave the loop as
// o, to the loop, which writes the lane from now on and reports on down
// how it ended. It runs on the loop.
func (s *sender) attach(l *lane, c *loop.Records, o *owned, v byte, down chan<- error) {
	br := c.NetConn()
	oc := &outConn{s: s, l: l, tc: c, br: br, own: o, v: v, down: down}
	sock, token, err := s.t.loop.Attach(br.Conn, oc)
	if err != nil {
		s.t.release(o)
		down <- err
		return
	}
	br.Attach(sock, nil, nil)
	br.KeepOut(keepOut)
	o.stop = func() { oc.fail(net.ErrClosed) }
	oc.token = token
	l.out = oc
	s.t.loop.Dirty(oc)
}

// Ready sees whether the connection ended, and writes what waits.
func (c *outConn) Ready() {
	if c.closed {
		return
	}
	var b [1]byte
	n, err := c.tc.Read(b[:])
	switch {
	case n > 0:
		c.fail(fmt.Errorf("peer %d wrote on a message connection", c.s.id))
		return
	case err != nil && !errors.Is(err, loop.ErrWouldBlock):
		c.fail(err)
		return
	}
	c.Flush()
}

// Flush writes the lane's queued messages, as far as the socket takes
// them. Frames gather into one TLS write, whose records the bridge sends
// with one system call. Bytes already encoded go first, no message leaves
// the lane while the socket holds some back, and the lane counts a batch
// until the socket took all of it, so the lane's limits bound the whole
// path to the socket.
// Output that waits for the socket longer than the write deadline fails
// the connection, so a stalled peer is dropped and reported.
func (c *outConn) Flush() {
	s, l := c.s, c.l
	for !c.closed {
		if before := c.br.Pending(); before > 0 {
			// Seals the record being written first.
			if err := c.tc.Flush(); err != nil {
				c.fail(err)
				return
			}
			if pending := c.br.Pending(); pending > 0 {
				c.wait(before, pending)
				return
			}
		}
		c.since = time.Time{}
		c.br.SendBy(time.Time{})
		l.bytes -= c.sending
		c.sending = 0
		if l.head == len(l.q) {
			if cap(c.buf) > ioBuffer {
				c.buf = nil
			}
			return
		}
		c.buf = c.buf[:0]
		for l.head < len(l.q) && len(c.buf) < ioBuffer {
			q := l.q[l.head]
			l.q[l.head] = queued{}
			l.head++
			c.sending += size(q.m)
			s.waited(l, q)
			var err error
			if c.buf, err = s.frame(c.buf, q.m, c.v); err != nil {
				c.fail(err)
				return
			}
		}
		if l.head == len(l.q) {
			clear(l.q)
			l.q, l.head = l.q[:0], 0
		}
		if len(c.buf) > 0 {
			if _, err := c.tc.Write(c.buf); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

// wait notes output that waits for the socket, and fails the connection
// once it has not moved for the write deadline.
func (c *outConn) wait(before, pending int) {
	now := time.Now()
	if c.since.IsZero() || pending < before {
		c.since = now
	}
	limit, ok := c.s.t.writeLimit(pending)
	if !ok {
		return
	}
	// The socket's own deadline fails the send the loop tries then.
	at := c.since.Add(limit)
	c.br.SendBy(at)
	if now.Before(at) {
		c.s.t.loop.After(c, at)
		return
	}
	c.fail(fmt.Errorf("write of %d bytes: %w", pending, os.ErrDeadlineExceeded))
}

// frame appends m's frame to b: its length and the message. A message of
// held entries for a peer of an older version, or one over the frame limit,
// is left out.
func (s *sender) frame(b []byte, m *raftpb.Message, v byte) ([]byte, error) {
	if v < heldVersion && core.HeldForm(m) {
		// Queued while the link spoke a newer version: an older peer
		// would take a named entry for its data. Raft sends again what
		// the follower misses, in full now (HeldEntries).
		return b, nil
	}
	n := proto.Size(m)
	if n > maxFrame {
		// The receiver would refuse it; Raft sends again in smaller pieces
		// after the unreachable report.
		s.t.logf("peer: dropping a %s of %d bytes to %d, over the frame limit", m.GetType(), n, s.id)
		s.t.cfg.Unreachable(s.id)
		return b, nil
	}
	start := len(b)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)
	if err != nil {
		return b[:start], err
	}
	s.counters.sent.Add(float64(len(b) - start))
	if m.GetType() == raftpb.MsgHeartbeat {
		s.counters.heartbeatSent.SetToCurrentTime()
	}
	return b, nil
}

// fail closes the connection, drops what its lane holds and reports the
// error to the lane's goroutine, which dials again.
func (c *outConn) fail(err error) {
	if c.closed {
		return
	}
	c.closed = true
	if c.l.out == c {
		c.l.out = nil
	}
	c.s.t.loop.Forget(c.token)
	c.s.t.release(c.own)
	_ = c.br.Close()
	c.s.reportWaits(c.l)
	c.s.drop(c.l)
	c.down <- err
}

// Stopped closes the connection once the node stopped.
func (c *outConn) Stopped() { c.fail(net.ErrClosed) }
