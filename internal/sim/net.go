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
	// signer is the CA that issued cert, for the certificate an operator
	// rotates to (Config.CertExpiresAt); hosts are the names cert holds,
	// and expires when it expires.
	signer  *testcert.CA
	hosts   []string
	expires time.Time
	// valid is how long its certificates are valid: a day, or certValid
	// for an aged image's run (Config.longCerts).
	valid time.Duration
	// listeners are the listening processes by address, and conns the
	// open ends of connections. The Sim's lock guards both, and every
	// connection's state.
	listeners map[string]*listener
	conns     map[*conn]bool
}

// hosts are the names the nodes listen on: the first three voters', the
// member the membership work adds, at its two URLs, then the fourth and
// fifth voters of a cluster of five (Config.Nodes).
var hosts = []string{"node1", "node2", "node3", "member", "updated-member", "node4", "node5"}

// certHosts are the hosts the certificate of a run of voters names: a
// cluster of three or fewer has the certificate it had before clusters of
// five, so its runs keep their bytes on the wire.
func certHosts(voters int) []string {
	if voters > 3 {
		return hosts
	}
	return hosts[:5]
}

// voterHost is the host of voter i.
func voterHost(i int) string {
	if i < 3 {
		return hosts[i]
	}
	return hosts[i+2]
}

// certValid is how long an aged image's certificates are valid: longer
// than its run, its forks and the years between them span (aged_test.go,
// Config.longCerts). Other runs keep certificates of a day: from 2050 on,
// a certificate's dates take more bytes (GeneralizedTime), every
// handshake with them, and every run of a seed would go another way.
const certValid = 100 * 365 * 24 * time.Hour

// hostName is the host of node i: a voter's, or the added member's.
func (s *Sim) hostName(i int) string {
	if i == s.cfg.voters() {
		return hosts[3]
	}
	return voterHost(i)
}

// init sets up an empty network with the CA and certificate of certs, or
// new ones for a cluster of voters if certs is nil, valid for a day or,
// if long, for certValid: whose certificate expires after expires, or
// when they do if it is 0.
func (nw *network) init(certs *network, voters int, expires time.Duration, long bool) error {
	if certs != nil {
		nw.ca, nw.cert, nw.signer, nw.hosts, nw.expires, nw.valid = certs.ca, certs.cert, certs.signer, certs.hosts, certs.expires, certs.valid
	} else {
		nw.valid = 24 * time.Hour
		if long {
			nw.valid = certValid
		}
		ca, err := testcert.NewCAValid(nw.valid)
		if err != nil {
			return err
		}
		if expires == 0 {
			expires = nw.valid
		}
		nw.signer, nw.hosts, nw.expires = ca, certHosts(voters), time.Now().Add(expires)
		if nw.cert, _, _, err = ca.IssueUntil("tigerstore-sim", 2, nil, nw.hosts, nw.expires); err != nil {
			return err
		}
		nw.ca = ca.Pool
	}
	nw.listeners, nw.conns = map[string]*listener{}, map[*conn]bool{}
	return nil
}

// peerURL is the peer URL of voter i.
func peerURL(i int) string { return "https://" + voterHost(i) + ":2380" }

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
		host = s.hostName(from)
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
		if c.from == from && c.to < len(s.nodes) && s.nodes[c.to].id == to && len(c.out.chunks) > 0 {
			out = append(out, c.out)
		}
	}
	return out
}

// resetDamaged resets the connections between hosts i and j that a link
// fault damaged, as chaos heals such a fault with RST. The lock is held.
func (nw *network) resetDamaged(i, j int) {
	for c := range nw.conns {
		if c.damaged && (c.from == i && c.to == j || c.from == j && c.to == i) {
			nw.reset(c)
		}
	}
}

// reset breaks both ends of c's connection. The lock is held.
func (nw *network) reset(c *conn) {
	for _, e := range []*conn{c, c.other} {
		e.in.fail(errReset)
		delete(nw.conns, e)
		e.changed()
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
// it: a stalled link holds about 3 KiB within the write timeout. A run
// with longer links (Config.LinkDelay) holds more on its way, as TCP's
// window grows with the round trip (Sim.sendBuffer).
const sendBuffer = 2 << 10

// sendBuffer is a connection's buffer in this run: sendBuffer, and on
// links of LinkDelay what keeps their bytes per second those of
// DefaultConfig's links, whose delays are about 1 ms on average. At
// 10 ms, 2 KiB let a follower catch up at about 200 KiB a second, and
// the heartbeats queued behind its appends for over a second (#761).
func (s *Sim) sendBuffer() int {
	return sendBuffer * int(max(s.cfg.LinkDelay/Millisecond, 1))
}

// pipe is one direction of a connection: the bytes written, each write
// with the time it arrives.
type pipe struct {
	chunks []chunk
	// size is how many bytes chunks holds.
	size int
	// last is when the last write arrives; eof is set once the writer
	// closed, and err once the connection broke.
	last int64
	eof  bool
	err  error
	// readWake, once the reader waits on it, is closed when bytes arrive,
	// the writer closes, the connection breaks or the read deadline
	// moves; roomWake, once a writer waits on it, when the reader makes
	// room, the connection breaks or the write deadline moves. Apart, a
	// write deadline set before each write wakes no reader.
	readWake, roomWake chan struct{}
}

type chunk struct {
	at int64
	b  []byte
	// damaged is set when a link fault damaged the chunk's bytes.
	damaged bool
}

func newPipe() *pipe { return &pipe{} }

// waiter returns the channel that the next signal of *wake closes. The
// lock is held.
func waiter(wake *chan struct{}) chan struct{} {
	if *wake == nil {
		*wake = make(chan struct{})
	}
	return *wake
}

// signal wakes whoever waits on *wake. The lock is held.
func signal(wake *chan struct{}) {
	if *wake != nil {
		close(*wake)
		*wake = nil
	}
}

// take returns the channel that would wake whoever waits on *wake, nil
// if none, for the caller to close once it let go of the lock: under
// gosim a close yields, and a close with the lock held makes every
// goroutine that runs meanwhile wait for the lock. The lock is held.
func take(wake *chan struct{}) chan struct{} {
	c := *wake
	*wake = nil
	return c
}

// wakeUp closes c, if it is a channel take gave. The lock is not held.
func wakeUp(c chan struct{}) {
	if c != nil {
		close(c)
	}
}

// signalBoth wakes p's reader and writers. The lock is held.
func (p *pipe) signalBoth() {
	signal(&p.readWake)
	signal(&p.roomWake)
}

// waitFor waits until wake is closed or, if wait is not math.MaxInt64,
// wait nanoseconds passed, on the timer *t, which it makes once and then
// reuses. Its callers look again after any wake, so one that comes late
// from an earlier wait does no harm.
func waitFor(t **time.Timer, wait int64, wake <-chan struct{}) {
	if wait == math.MaxInt64 {
		<-wake
		return
	}
	if *t == nil {
		*t = time.NewTimer(time.Duration(wait))
	} else {
		(*t).Reset(time.Duration(wait))
	}
	select {
	case <-(*t).C:
	case <-wake:
		(*t).Stop()
	}
}

func (p *pipe) fail(err error) {
	if p.err == nil {
		p.err = err
	}
	p.signalBoth()
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

// conn is one end of a connection, held by process p of node from, or by
// a client if p is nil, from the clients' host. A write goes on its way at
// once while the connection's buffer has room (sendBuffer), and waits for
// room otherwise, as TCP's does: each arrives after its link's delay and
// faults, in order, and not before a stall of the link ends. A half-open
// link swallows them.
type conn struct {
	s             *Sim
	p             *proc
	from, to      int
	local, remote string
	in, out       *pipe
	other         *conn
	closed        bool
	// damaged is set once a link fault damaged a byte the connection
	// carried (flipped, dropped, duplicated); the fault's end resets it.
	damaged bool
	// deadline and writeDeadline are the read and write deadlines.
	deadline, writeDeadline time.Time
	// readTimer and writeTimer wait out a read and a write (waitFor),
	// one caller at a time: reading and writing are set while one waits
	// on them, and a call that waits beside it makes a timer of its own,
	// as net.Conn allows calls at once.
	readTimer, writeTimer *time.Timer
	reading, writing      bool
	// notify, if set, is told when a read or write that would wait may
	// not any more (simos.Conn's OnChange); notifyAt tells it at the time
	// bytes on their way arrive or a pause or stall ends.
	notify   func()
	notifyAt *time.Timer
}

// errWouldWait is readNow's and writeNow's answer when the call would
// wait.
var errWouldWait = errors.New("sim: the call would wait")

// readNow reads what has arrived as Read does, without waiting: when it
// would wait it returns errWouldWait, and in wait how long until it may
// not, math.MaxInt64 if no time tells. room is the writer to wake once
// the lock is let go (take). The lock is held.
func (c *conn) readNow(b []byte) (k int, wait int64, room chan struct{}, err error) {
	s := c.s
	if c.closed {
		return 0, 0, nil, net.ErrClosed
	}
	in := c.in
	if in.err != nil {
		return 0, 0, nil, in.err
	}
	now := s.now()
	wait = int64(math.MaxInt64)
	switch {
	case c.p != nil && c.p.n.pausedUntil > now:
		// A paused process reads nothing.
		wait = c.p.n.pausedUntil - now
	case len(in.chunks) > 0:
		at := max(in.chunks[0].at, s.link(c.to, c.from).stallUntil)
		if at <= now {
			damaged := in.chunks[0].damaged
			k := copy(b, in.chunks[0].b)
			if in.chunks[0].b = in.chunks[0].b[k:]; len(in.chunks[0].b) == 0 {
				in.chunks = slices.Delete(in.chunks, 0, 1)
			}
			// A writer may wait for the room.
			in.size -= k
			room := take(&in.roomWake)
			c.other.changed()
			if damaged {
				// TLS refuses a damaged record and ends the connection,
				// so it ends here, as the reader gets the damage. Its
				// alert would wait for the write lock of a writer that
				// waits for room, and under the race detector that wait
				// on a mutex stops the synctest clock for good (#439).
				s.net.reset(c)
			}
			return k, 0, room, nil
		}
		wait = at - now
	case in.eof:
		return 0, 0, nil, io.EOF
	}
	return 0, wait, nil, errWouldWait
}

func (c *conn) Read(b []byte) (int, error) {
	s := c.s
	mine := false
	for {
		s.mu.Lock()
		if mine {
			c.reading, mine = false, false
		}
		k, wait, room, err := c.readNow(b)
		if !errors.Is(err, errWouldWait) {
			s.mu.Unlock()
			wakeUp(room)
			return k, err
		}
		in := c.in
		if !c.deadline.IsZero() {
			left := int64(time.Until(c.deadline))
			if left <= 0 {
				s.mu.Unlock()
				return 0, os.ErrDeadlineExceeded
			}
			wait = min(wait, left)
		}
		wake := waiter(&in.readWake)
		t := new(*time.Timer)
		if !c.reading {
			t, c.reading, mine = &c.readTimer, true, true
		}
		s.mu.Unlock()
		waitFor(t, wait, wake)
	}
}

func (c *conn) Write(b []byte) (int, error) {
	s := c.s
	n, blocked, mine := 0, false, false
	for {
		s.mu.Lock()
		if mine {
			c.writing, mine = false, false
		}
		switch {
		case c.closed:
			s.mu.Unlock()
			return n, net.ErrClosed
		case c.in.err != nil:
			s.mu.Unlock()
			return n, c.in.err
		case c.other.closed:
			// The peer answers with RST, as TCP does, and the reset also
			// fails the reads of bytes still on their way. It comes sooner
			// than TCP's would: a stall of the link back holds those bytes
			// and would hold the RST too. But a reader held behind the
			// stall makes gRPC, which retires a transport only once its
			// reader saw the end, try the closing transport again and
			// again, and that busy loop keeps the virtual clock from ever
			// reaching the stall's end (#369). So a stall of a stream whose
			// sender closed ends once the other end writes.
			s.net.reset(c)
			s.mu.Unlock()
			return n, errReset
		}
		if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
			s.mu.Unlock()
			return n, os.ErrDeadlineExceeded
		}
		if s.link(c.from, c.to).halfOpenUntil > s.now() {
			s.mu.Unlock()
			return len(b), nil
		}
		if room := c.s.sendBuffer() - c.out.size; room > 0 {
			k := min(room, len(b)-n)
			reader := c.send(b[n : n+k])
			if n += k; n == len(b) {
				s.mu.Unlock()
				wakeUp(reader)
				return n, nil
			}
			// The reader gets what fit before the writer waits for room,
			// which it looks for again.
			s.mu.Unlock()
			wakeUp(reader)
			continue
		}
		if !blocked {
			blocked = true
			s.stats.BlockedWrites++
		}
		wait := int64(math.MaxInt64)
		if !c.writeDeadline.IsZero() {
			wait = max(int64(time.Until(c.writeDeadline)), 1)
		}
		wake := waiter(&c.out.roomWake)
		t := new(*time.Timer)
		if !c.writing {
			t, c.writing, mine = &c.writeTimer, true, true
		}
		s.mu.Unlock()
		waitFor(t, wait, wake)
	}
}

// send puts b on its way: it arrives after the link's delay and the
// fault on the link, if any, in order. It returns the reader's wake, for
// the caller to close after the lock (take). The lock is held.
func (c *conn) send(b []byte) chan struct{} {
	s := c.s
	l := s.link(c.from, c.to)
	b = slices.Clone(b)
	at := s.now()
	if c.from != c.to {
		// Loopback has no delay.
		at += s.delay(l)
	}
	damaged := false
	if l.netemUntil > s.now() {
		b, at, damaged = s.netem(l, b, at, c.out.last)
		c.damaged = c.damaged || damaged
	}
	at = max(at, c.out.last)
	c.out.last = at
	c.out.chunks = append(c.out.chunks, chunk{at: at, b: b, damaged: damaged})
	c.out.size += len(b)
	c.other.changed()
	return take(&c.out.readWake)
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
	c.out.signalBoth()
	c.in.signalBoth()
	c.changed()
	c.other.changed()
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
	c.deadline = t
	reader := take(&c.in.readWake)
	c.s.mu.Unlock()
	wakeUp(reader)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.s.mu.Lock()
	c.writeDeadline = t
	writer := take(&c.out.roomWake)
	c.s.mu.Unlock()
	wakeUp(writer)
	return nil
}

// delay draws the delay of one write on a link. The lock is held.
func (s *Sim) delay(l *link) int64 {
	if s.healed && s.cfg.LinkDelay == 0 {
		return 20*Microsecond + s.rng.Int64N(200*Microsecond)
	}
	if !s.healed && s.cfg.Faults.MaxDelay > 0 && s.rng.IntN(10) == 0 {
		return s.rng.Int64N(s.cfg.Faults.MaxDelay) + 1
	}
	return l.base + s.rng.Int64N(l.jitter+1)
}
