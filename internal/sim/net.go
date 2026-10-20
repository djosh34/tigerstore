package sim

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// network carries the nodes' peer connections, and the connections of
// the clients that use etcd's Go client (v3client.go): in-memory streams
// over the links, with each link's delays, stalls and faults. The peer
// transports and the gRPC servers speak TLS over it, with one certificate
// for every node. The clients run on a host of their own, whose index is
// the number of nodes, with a link to each node and one back.
type network struct {
	ca   *x509.CertPool
	cert tls.Certificate
	// listeners are the listening processes by address, and conns the
	// open ends of connections. The Sim's lock guards both, and every
	// connection's state.
	listeners map[string]*listener
	conns     map[*conn]bool
}

// hosts are the names the nodes listen on.
var hosts = []string{"node1", "node2", "node3", "member", "updated-member"}

func (nw *network) init() error {
	ca, err := testcert.NewCA()
	if err != nil {
		return err
	}
	if nw.cert, _, _, err = ca.IssueFor("tigerstore-sim", 2, nil, hosts); err != nil {
		return err
	}
	nw.ca = ca.Pool
	nw.listeners, nw.conns = map[string]*listener{}, map[*conn]bool{}
	return nil
}

// peerURL is the peer URL of node i of the first three.
func peerURL(i int) string { return "https://" + hosts[i] + ":2380" }

func peerAddr(i int) string { return hostPort(peerURL(i)) }

// hostPort is the host:port of a peer URL.
func hostPort(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return u
	}
	return p.Host
}

// errReset is what a connection's ends get when a fault breaks it.
var errReset = errors.New("sim: connection reset")

// errRefused is what a dial gets when no process listens at the address
// or the link to it is down.
var errRefused = errors.New("sim: connection refused")

// dialTimeout is how long a dial waits on a half-open link before it
// gives up, as the peer transport's dialer does.
const dialTimeout = 3 * Second

// listen makes p listen on addrs. The lock is held.
func (nw *network) listen(s *Sim, p *proc, addrs []string) *listener {
	l := &listener{s: s, p: p, addrs: addrs, accept: make(chan net.Conn, 64), closed: make(chan struct{})}
	for _, a := range addrs {
		nw.listeners[a] = l
	}
	return l
}

// dial connects p, or a client if p is nil, to the process listening at
// addr, after a round trip on the links between them.
func (nw *network) dial(ctx context.Context, s *Sim, p *proc, addr string) (net.Conn, error) {
	s.mu.Lock()
	l := nw.listeners[addr]
	if p != nil && p.dead() || l == nil {
		s.mu.Unlock()
		return nil, errRefused
	}
	from, to := len(s.links), l.p.n.idx
	if p != nil {
		from = p.n.idx
	}
	timeout := s.now() + dialTimeout
	for {
		out, back := *s.link(from, to), *s.link(to, from)
		now := s.now()
		wait := out.base + back.base
		until := max(out.halfOpenUntil, back.halfOpenUntil)
		halfOpen := until > now
		if halfOpen {
			// What it sends vanishes until the link ends its half-open
			// state, at its end or at healing; the dial times out first
			// or goes on then.
			if now >= timeout {
				s.mu.Unlock()
				return nil, os.ErrDeadlineExceeded
			}
			wait = min(timeout, until) - now
			if !s.healed {
				// At least 1 ns, so that healing, due now, comes first.
				wait = max(min(wait, s.healAt-now), 1)
			}
		}
		s.mu.Unlock()
		if !out.up || !back.up {
			return nil, errRefused
		}
		t := time.NewTimer(time.Duration(wait))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		}
		s.mu.Lock()
		if !halfOpen {
			break
		}
	}
	defer s.mu.Unlock()
	out, back := *s.link(from, to), *s.link(to, from)
	if p != nil && p.dead() || l.p.dead() || nw.listeners[addr] != l || !out.up || !back.up {
		return nil, errRefused
	}
	host := "client"
	if p != nil {
		host = hosts[from]
	}
	a, b := &conn{s: s, p: p, from: from, to: to, local: host, remote: addr}, &conn{s: s, p: l.p, from: to, to: from, local: addr, remote: host}
	a.in, a.out = newPipe(), newPipe()
	b.in, b.out, a.other, b.other = a.out, a.in, b, a
	select {
	case l.accept <- b:
	default:
		return nil, errRefused
	}
	nw.conns[a], nw.conns[b] = true, true
	return a, nil
}

// cut closes n's listeners and resets every connection of n, as a crash
// does. The lock is held.
func (nw *network) cut(n *node) {
	for _, a := range n.addrs {
		if l := nw.listeners[a]; l != nil {
			l.shut()
		}
	}
	for c := range nw.conns {
		if c.from == n.idx || c.to == n.idx {
			nw.reset(c)
		}
	}
}

// resetBetween resets every connection between nodes i and j. The lock is
// held.
func (nw *network) resetBetween(i, j int) {
	for c := range nw.conns {
		if c.from == i && c.to == j || c.from == j && c.to == i {
			nw.reset(c)
		}
	}
}

// queued returns the pipes from node index from to the node with Raft ID
// to that hold bytes their reader has not taken yet. Bytes written just
// now are among them, as a link takes time.
func (nw *network) queued(s *Sim, from int, to uint64) []*pipe {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*pipe
	for c := range nw.conns {
		if c.from == from && s.nodes[c.to].id == to && len(c.out.chunks) > 0 {
			out = append(out, c.out)
		}
	}
	return out
}

// reset breaks both ends of c's connection. The lock is held.
func (nw *network) reset(c *conn) {
	for _, e := range []*conn{c, c.other} {
		e.in.fail(errReset)
		delete(nw.conns, e)
	}
}

// listener is a process's listening addresses.
type listener struct {
	s      *Sim
	p      *proc
	addrs  []string
	accept chan net.Conn
	closed chan struct{}
	done   bool
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *listener) Close() error {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	l.shut()
	return nil
}

// shut stops the listener. The lock is held.
func (l *listener) shut() {
	if l.done {
		return
	}
	l.done = true
	close(l.closed)
	for _, a := range l.addrs {
		if l.s.net.listeners[a] == l {
			delete(l.s.net.listeners, a)
		}
	}
}

func (l *listener) Addr() net.Addr { return addr(l.addrs[0]) }

type addr string

func (a addr) Network() string { return "sim" }
func (a addr) String() string  { return string(a) }

// sendBuffer is how many bytes a connection holds in each direction that
// its reader has not taken yet, on their way or arrived, like TCP's send
// and receive buffers together. A write past it waits for room, or its
// write deadline. It is small, so that the little traffic of a run fills
// it: a stalled link holds about 3 KiB within the write timeout.
const sendBuffer = 2 << 10

// pipe is one direction of a connection: the bytes written, each write
// with the time it arrives.
type pipe struct {
	chunks []chunk
	// size is how many bytes chunks holds.
	size int
	// last is when the last write arrives; eof is set once the writer
	// closed, and err once the connection broke. wake is closed and
	// replaced when any of them changes.
	last int64
	eof  bool
	err  error
	wake chan struct{}
}

type chunk struct {
	at int64
	b  []byte
}

func newPipe() *pipe { return &pipe{wake: make(chan struct{})} }

func (p *pipe) signal() {
	close(p.wake)
	p.wake = make(chan struct{})
}

func (p *pipe) fail(err error) {
	if p.err == nil {
		p.err = err
	}
	p.signal()
}

// link is the directed link from host from to host to: between two nodes,
// or between a node and the clients' host. The lock is held.
func (s *Sim) link(from, to int) *link {
	switch c := len(s.links); {
	case from == c:
		return &s.clientLinks[to][0]
	case to == c:
		return &s.clientLinks[from][1]
	}
	return &s.links[from][to]
}

// conn is one end of a connection, held by process p of node from, or
// by a client if p is nil, from the clients' host. A
// write goes on its way at once while the connection's buffer has room
// (sendBuffer), and waits for room otherwise, as TCP's does: each arrives
// after its link's delay and faults, in order, and not before a stall of
// the link ends. A half-open link swallows them.
type conn struct {
	s             *Sim
	p             *proc
	from, to      int
	local, remote string
	in, out       *pipe
	other         *conn
	closed        bool
	// deadline and writeDeadline are the read and write deadlines.
	deadline, writeDeadline time.Time
}

func (c *conn) Read(b []byte) (int, error) {
	s := c.s
	for {
		s.mu.Lock()
		if c.closed {
			s.mu.Unlock()
			return 0, net.ErrClosed
		}
		in := c.in
		if in.err != nil {
			s.mu.Unlock()
			return 0, in.err
		}
		now := s.now()
		wait := int64(math.MaxInt64)
		switch {
		case c.p != nil && c.p.n.pausedUntil > now:
			// A paused process reads nothing.
			wait = c.p.n.pausedUntil - now
		case len(in.chunks) > 0:
			at := max(in.chunks[0].at, s.link(c.to, c.from).stallUntil)
			if at <= now {
				k := copy(b, in.chunks[0].b)
				if in.chunks[0].b = in.chunks[0].b[k:]; len(in.chunks[0].b) == 0 {
					in.chunks = slices.Delete(in.chunks, 0, 1)
				}
				// A writer may wait for the room.
				in.size -= k
				in.signal()
				s.mu.Unlock()
				return k, nil
			}
			wait = at - now
		case in.eof:
			s.mu.Unlock()
			return 0, io.EOF
		}
		if !c.deadline.IsZero() {
			left := int64(time.Until(c.deadline))
			if left <= 0 {
				s.mu.Unlock()
				return 0, os.ErrDeadlineExceeded
			}
			wait = min(wait, left)
		}
		wake := in.wake
		s.mu.Unlock()
		t := time.NewTimer(time.Duration(min(wait, math.MaxInt64/2)))
		select {
		case <-t.C:
		case <-wake:
		}
		t.Stop()
	}
}

func (c *conn) Write(b []byte) (int, error) {
	s := c.s
	n, blocked := 0, false
	for {
		s.mu.Lock()
		switch {
		case c.closed:
			s.mu.Unlock()
			return n, net.ErrClosed
		case c.in.err != nil:
			s.mu.Unlock()
			return n, c.in.err
		case c.other.closed:
			s.mu.Unlock()
			return n, errReset
		}
		now := s.now()
		if s.link(c.from, c.to).halfOpenUntil > now {
			s.mu.Unlock()
			return len(b), nil
		}
		if room := sendBuffer - c.out.size; room > 0 {
			k := min(room, len(b)-n)
			c.send(b[n : n+k])
			if n += k; n == len(b) {
				s.mu.Unlock()
				return n, nil
			}
		}
		if !blocked {
			blocked = true
			s.stats.BlockedWrites++
		}
		wait := int64(math.MaxInt64)
		if !c.writeDeadline.IsZero() {
			if wait = int64(time.Until(c.writeDeadline)); wait <= 0 {
				s.stats.WriteTimeouts++
				s.mu.Unlock()
				return n, os.ErrDeadlineExceeded
			}
		}
		wake := c.out.wake
		s.mu.Unlock()
		t := time.NewTimer(time.Duration(min(wait, math.MaxInt64/2)))
		select {
		case <-t.C:
		case <-wake:
		}
		t.Stop()
	}
}

// send puts b on its way: it arrives after the link's delay and the
// fault on the link, if any, in order. The lock is held.
func (c *conn) send(b []byte) {
	s := c.s
	l := s.link(c.from, c.to)
	b = slices.Clone(b)
	at := s.now() + s.delay(l)
	if l.netemUntil > s.now() {
		b, at = s.netem(l, b, at, c.out.last)
	}
	at = max(at, c.out.last)
	c.out.last = at
	c.out.chunks = append(c.out.chunks, chunk{at: at, b: b})
	c.out.size += len(b)
	c.out.signal()
}

func (c *conn) Close() error {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.closed = true
	c.out.eof = true
	c.out.signal()
	c.in.signal()
	delete(s.net.conns, c)
	return nil
}

func (c *conn) LocalAddr() net.Addr  { return addr(c.local) }
func (c *conn) RemoteAddr() net.Addr { return addr(c.remote) }

func (c *conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.deadline = t
	c.in.signal()
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.writeDeadline = t
	c.out.signal()
	return nil
}

// delay draws the delay of one write on a link. The lock is held.
func (s *Sim) delay(l *link) int64 {
	if s.healed {
		return 20*Microsecond + s.rng.Int64N(200*Microsecond)
	}
	if s.cfg.Faults.MaxDelay > 0 && s.rng.IntN(10) == 0 {
		return s.rng.Int64N(s.cfg.Faults.MaxDelay) + 1
	}
	return l.base + s.rng.Int64N(l.jitter+1)
}
