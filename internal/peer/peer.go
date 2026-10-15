// Package peer carries Raft messages between nodes: framed TLS 1.3 over
// TCP, one connection per directed pair of nodes.
//
// Both ends verify the other's certificate against the peer CA and, when
// set, the allowed CommonNames or hostnames. Both ends also offer the
// protocol versions they speak in TLS ALPN, as "tigerstore-peer/<version>".
// After the TLS handshake, the dialing node writes a 12-byte preface: the
// magic "TS", the lowest and the highest version it speaks, and its Raft
// ID as 8 bytes. The accepting node replies with 1 byte: the highest
// version both speak, which must be the one ALPN chose, or 0 if there is
// none, after which it closes. Then each frame is a 4-byte length and a
// protobuf raftpb.Message.
//
// A follower fetches a snapshot on a connection of its own: its preface
// carries its Raft ID with the top bit set. After the reply it writes the
// first and the last index it needs, 8 bytes each, and reads the snapshot
// stream until the other end closes.
package peer

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/planted"
)

// Transport sends Raft messages to peers. Send never blocks: when a peer's
// queue is full or its connection is down, the message is dropped and the
// peer is reported unreachable, and Raft sends again.
type Transport interface {
	Send(m *raftpb.Message)
	// Fetch asks peer id for the snapshot stream of the entries first to
	// last. The caller reads it to the end and closes it.
	Fetch(ctx context.Context, id, first, last uint64) (io.ReadCloser, error)
	Close() error
}

// Config sets up the TCP transport.
type Config struct {
	// ID is this node's Raft ID.
	ID uint64
	// Listen is the host:port to accept peers on.
	Listen string
	// Listener, if set, is used instead of listening on Listen. Tests
	// pass one they hold open, so no other process can take its port.
	Listener net.Listener
	// Peers maps every other node's Raft ID to its host:port.
	Peers map[uint64]string
	// Certificate is this node's peer certificate and CA the pool that
	// verifies other nodes' certificates. The transport requires TLS 1.3
	// and verifies both ends.
	Certificate tls.Certificate
	CA          *x509.CertPool
	// AllowedCNs and AllowedHostnames are etcd's --peer-cert-allowed-cn
	// and --peer-cert-allowed-hostname: when one is set, the other node's
	// certificate must also have one of these CommonNames, or be valid
	// for one of these hostnames. At most one of them may be set.
	AllowedCNs       []string
	AllowedHostnames []string
	// Deliver gets every message received from a peer. It may block,
	// which slows down that peer's connection.
	Deliver func(*raftpb.Message)
	// Unreachable is called when a message to a peer was dropped.
	Unreachable func(id uint64)
	// Serve writes to w the snapshot stream that peer id asked for, the
	// entries first to last.
	Serve func(id, first, last uint64, w io.Writer) error
	// Logf logs connection errors.
	Logf func(format string, args ...any)

	// minVersion and maxVersion override the protocol versions this node
	// speaks, for tests. Zero means the package's.
	minVersion, maxVersion byte
}

const (
	// minVersion and maxVersion are the protocol versions this binary
	// speaks. Version 0 stands for the unversioned protocol from before
	// version 1, which this binary does not speak. A release that raises
	// maxVersion keeps minVersion at or below the previous release's
	// maxVersion, so that nodes can be upgraded one at a time.
	minVersion = 1
	maxVersion = 1
)

// magic starts the preface.
var magic = [2]byte{'T', 'S'}

// alpn is the TLS ALPN name of protocol version v.
func alpn(v byte) string { return fmt.Sprintf("tigerstore-peer/%d", v) }

// alpns lists the ALPN names of the versions lo to hi, highest first. The
// acceptor takes the first of its names that the dialer offers, so both
// ends land on the highest version they share.
func alpns(lo, hi byte) []string {
	var names []string
	for v := int(hi); v >= int(lo); v-- {
		names = append(names, alpn(byte(v)))
	}
	return names
}

// pick returns the highest version in both the dialer's range dmin to
// dmax and the acceptor's amin to amax, or 0 if the ranges do not meet.
func pick(dmin, dmax, amin, amax byte) byte {
	v := min(dmax, amax)
	if v < max(dmin, amin) {
		return 0
	}
	return v
}

const (
	// maxFrame caps one received message. Raft messages are capped at 4
	// MiB of entries, plus headers.
	maxFrame = 64 << 20
	// maxQueued caps the estimated bytes in each of a peer's two queues.
	maxQueued = 64 << 20
	// controlQueue and dataQueue cap the queued messages per peer.
	controlQueue = 4096
	dataQueue    = 4096

	writeTimeout = 5 * time.Second
	// slowQueue is how long a message may wait in its queue before the
	// wait is logged: a real node's tick. Waits are logged at most once a
	// second per peer.
	slowQueue   = 100 * time.Millisecond
	dialTimeout = 3 * time.Second
	ioBuffer    = 256 << 10
	// handshakeLogEvery is how often a failed handshake with one peer is
	// logged. A dialer retries every second.
	handshakeLogEvery = time.Minute
)

// TCP is the transport over TLS on TCP.
type TCP struct {
	cfg     Config
	verify  func([][]byte, [][]*x509.Certificate) error
	ln      net.Listener
	senders map[uint64]*sender
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	// inbound are the message connections from peers, with the peer's ID
	// and the connection's protocol version.
	inbound map[net.Conn]inbound

	// logged holds when a failed handshake was last logged, by peer.
	logMu  sync.Mutex
	logged map[string]time.Time
}

// Start listens for peers and starts one writer per peer.
func Start(cfg Config) (*TCP, error) {
	if cfg.minVersion == 0 {
		cfg.minVersion, cfg.maxVersion = minVersion, maxVersion
	}
	verify, err := verifyPeer(cfg.AllowedCNs, cfg.AllowedHostnames)
	if err != nil {
		return nil, err
	}
	raw := cfg.Listener
	if raw == nil {
		if raw, err = net.Listen("tcp", cfg.Listen); err != nil {
			return nil, err
		}
	}
	ln := tls.NewListener(raw, &tls.Config{
		Certificates: []tls.Certificate{cfg.Certificate},
		ClientCAs:    cfg.CA,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpns(cfg.minVersion, cfg.maxVersion),
		// The CA check runs first; verify only narrows it.
		VerifyPeerCertificate: verify,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCP{
		cfg:     cfg,
		verify:  verify,
		ln:      ln,
		senders: map[uint64]*sender{},
		ctx:     ctx,
		cancel:  cancel,
		conns:   map[net.Conn]struct{}{},
		inbound: map[net.Conn]inbound{},
		logged:  map[string]time.Time{},
	}
	for id, addr := range cfg.Peers {
		s := &sender{
			t:    t,
			id:   id,
			addr: addr,
			ctrl: make(chan queued, controlQueue),
			data: make(chan queued, dataQueue),
		}
		t.senders[id] = s
		t.wg.Go(s.run)
	}
	t.wg.Go(t.accept)
	return t, nil
}

// Addr is the address the transport listens on.
func (t *TCP) Addr() net.Addr { return t.ln.Addr() }

func (t *TCP) logf(format string, args ...any) {
	if t.cfg.Logf != nil {
		t.cfg.Logf(format, args...)
	}
}

// logHandshake logs a failed handshake with a peer, unless one with the
// same peer was logged within handshakeLogEvery. The dialer names the
// peer by its ID. The acceptor does not know the ID before the preface,
// so it names the peer by its host.
func (t *TCP) logHandshake(peer string, err error) {
	now := time.Now()
	t.logMu.Lock()
	if at, ok := t.logged[peer]; ok && now.Sub(at) < handshakeLogEvery {
		t.logMu.Unlock()
		return
	}
	for p, at := range t.logged {
		if now.Sub(at) >= handshakeLogEvery {
			delete(t.logged, p)
		}
	}
	t.logged[peer] = now
	t.logMu.Unlock()
	t.logf("peer: handshake %s failed, this node speaks versions %d-%d: %v",
		peer, t.cfg.minVersion, t.cfg.maxVersion, err)
}

// inbound is a message connection from a peer.
type inbound struct {
	id      uint64
	version byte
}

// Versions are the protocol versions of a transport: the range it speaks,
// and per peer the version of the connection to it and of the one from
// it, 0 while there is none.
type Versions struct {
	Min, Max byte
	Out, In  map[uint64]byte
}

// Versions returns the transport's protocol versions now.
func (t *TCP) Versions() Versions {
	v := Versions{Min: t.cfg.minVersion, Max: t.cfg.maxVersion, Out: map[uint64]byte{}, In: map[uint64]byte{}}
	for id, s := range t.senders {
		v.Out[id] = byte(s.version.Load())
		v.In[id] = 0
	}
	t.mu.Lock()
	for _, in := range t.inbound {
		v.In[in.id] = max(v.In[in.id], in.version)
	}
	t.mu.Unlock()
	return v
}

// Send queues a message for its peer.
func (t *TCP) Send(m *raftpb.Message) {
	s, ok := t.senders[m.GetTo()]
	if !ok {
		return
	}
	s.send(m)
}

// Close stops the transport and waits for its goroutines.
func (t *TCP) Close() error {
	t.cancel()
	err := t.ln.Close()
	t.mu.Lock()
	for c := range t.conns {
		// The other end may have reset the connection already; that is
		// not a failure of Close.
		if cerr := c.Close(); cerr != nil {
			t.logf("peer: close: %v", cerr)
		}
	}
	t.mu.Unlock()
	t.wg.Wait()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (t *TCP) track(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx.Err() != nil {
		return false
	}
	t.conns[c] = struct{}{}
	return true
}

func (t *TCP) untrack(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	delete(t.inbound, c)
	t.mu.Unlock()
	if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logf("peer: close: %v", err)
	}
}

func (t *TCP) accept() {
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() == nil {
				t.logf("peer: accept: %v", err)
			}
			return
		}
		if !t.track(c) {
			if err := c.Close(); err != nil {
				t.logf("peer: close: %v", err)
			}
			return
		}
		t.wg.Go(func() {
			defer t.untrack(c)
			if err := t.receive(c); err != nil && t.ctx.Err() == nil {
				t.logf("peer: receive from %s: %v", c.RemoteAddr(), err)
			}
		})
	}
}

// receive reads frames from one inbound connection until it fails. It
// returns the error that ended it, nil after a snapshot was served or
// after a failed handshake, which it logs itself.
func (t *TCP) receive(c net.Conn) error {
	if err := c.SetReadDeadline(time.Now().Add(dialTimeout)); err != nil {
		return err
	}
	r := bufio.NewReaderSize(c, ioBuffer)
	from, fetch, v, err := t.handshake(c, r)
	if err != nil {
		if t.ctx.Err() == nil {
			t.logHandshake("from "+remoteHost(c), err)
		}
		return nil
	}
	if fetch {
		return t.serve(c, r, from)
	}
	t.mu.Lock()
	t.inbound[c] = inbound{id: from, version: v}
	t.mu.Unlock()
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	var hdr [4]byte
	var buf []byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n > maxFrame {
			return fmt.Errorf("frame of %d bytes from peer %d is over the limit", n, from)
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		// buf is reused for the next frame. That is safe because
		// Unmarshal copies bytes fields: the entries' data, which the
		// index keeps slices of once they apply, is the message's own.
		m := &raftpb.Message{}
		if err := proto.Unmarshal(buf, m); err != nil {
			return err
		}
		if m.GetFrom() != from {
			return fmt.Errorf("peer %d sent a message from %d", from, m.GetFrom())
		}
		t.cfg.Deliver(m)
	}
}

// handshake checks the ALPN version and the preface of an inbound
// connection and writes the reply. It returns the dialer's Raft ID,
// whether the connection fetches a snapshot, and the protocol version.
func (t *TCP) handshake(c net.Conn, r io.Reader) (id uint64, fetch bool, v byte, err error) {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return 0, false, 0, fmt.Errorf("a %T is not a TLS connection", c)
	}
	if err := tc.Handshake(); err != nil {
		return 0, false, 0, err
	}
	name := tc.ConnectionState().NegotiatedProtocol
	if name == "" {
		return 0, false, 0, errors.New("the dialer offers no version in ALPN; it may be a node from before version 1")
	}
	var p [12]byte
	if _, err := io.ReadFull(r, p[:]); err != nil {
		return 0, false, 0, err
	}
	if p[0] != magic[0] || p[1] != magic[1] {
		return 0, false, 0, fmt.Errorf("the preface starts with %#x, not the magic \"TS\"", p[:2])
	}
	id = binary.BigEndian.Uint64(p[4:])
	fetch = id&fetchBit != 0
	id &^= fetchBit
	if _, ok := t.senders[id]; !ok {
		return 0, false, 0, fmt.Errorf("unknown peer %d", id)
	}
	v = pick(p[2], p[3], t.cfg.minVersion, t.cfg.maxVersion)
	switch {
	case v == 0:
		err = fmt.Errorf("peer %d speaks versions %d-%d", id, p[2], p[3])
	case name != alpn(v):
		err = fmt.Errorf("peer %d chose %s in ALPN, but its preface picks version %d", id, name, v)
		v = 0
	}
	if werr := c.SetWriteDeadline(writeDeadline()); werr != nil {
		return 0, false, 0, errors.Join(err, werr)
	}
	if _, werr := c.Write([]byte{v}); werr != nil {
		return 0, false, 0, errors.Join(err, werr)
	}
	return id, fetch, v, err
}

// remoteHost is the host of c's remote address. Unlike the port, it stays
// the same when a peer dials again.
func remoteHost(c net.Conn) string {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP.String()
	}
	return c.RemoteAddr().String()
}

// preface is what a dialer writes after the TLS handshake: the magic, the
// lowest and the highest version it speaks, and id.
func (t *TCP) preface(id uint64) []byte {
	p := make([]byte, 12)
	copy(p, magic[:])
	p[2], p[3] = t.cfg.minVersion, t.cfg.maxVersion
	binary.BigEndian.PutUint64(p[4:], id)
	return p
}

// fetchBit marks the dialer's ID on a connection that fetches a
// snapshot.
const fetchBit = 1 << 63

// serve answers a snapshot fetch, then closes the connection.
func (t *TCP) serve(c net.Conn, r io.Reader, from uint64) error {
	var span [16]byte
	if _, err := io.ReadFull(r, span[:]); err != nil {
		return err
	}
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	if t.cfg.Serve == nil {
		return errors.New("this node serves no snapshots")
	}
	w := &deadlineWriter{c: c}
	if err := t.cfg.Serve(from, binary.BigEndian.Uint64(span[:]), binary.BigEndian.Uint64(span[8:]), w); err != nil {
		return fmt.Errorf("snapshot for %d: %w", from, err)
	}
	return nil
}

// deadlineWriter fails a write that has not finished within writeTimeout.
type deadlineWriter struct{ c net.Conn }

func (w *deadlineWriter) Write(p []byte) (int, error) {
	if err := w.c.SetWriteDeadline(writeDeadline()); err != nil {
		return 0, err
	}
	return w.c.Write(p)
}

// Fetch opens a connection to peer id that fetches a snapshot stream.
func (t *TCP) Fetch(ctx context.Context, id, first, last uint64) (io.ReadCloser, error) {
	s, ok := t.senders[id]
	if !ok {
		return nil, fmt.Errorf("peer: unknown peer %d", id)
	}
	c, _, err := s.dial(ctx, t.cfg.ID|fetchBit)
	if err != nil {
		return nil, err
	}
	var span [16]byte
	binary.BigEndian.PutUint64(span[:], first)
	binary.BigEndian.PutUint64(span[8:], last)
	if err := c.SetWriteDeadline(writeDeadline()); err != nil {
		t.untrack(c)
		return nil, err
	}
	if _, err := c.Write(span[:]); err != nil {
		t.untrack(c)
		return nil, err
	}
	return &fetchConn{t: t, c: c}, nil
}

// fetchConn reads a snapshot stream; a read that waits longer than
// writeTimeout fails, so a stalled peer does not hold the follower.
type fetchConn struct {
	t *TCP
	c net.Conn
}

func (f *fetchConn) Read(p []byte) (int, error) {
	if err := f.c.SetReadDeadline(time.Now().Add(writeTimeout)); err != nil {
		return 0, err
	}
	return f.c.Read(p)
}

func (f *fetchConn) Close() error {
	f.t.untrack(f.c)
	return nil
}

// sender owns the connection to one peer.
type sender struct {
	t    *TCP
	id   uint64
	addr string
	ctrl chan queued
	data chan queued
	// ctrlBytes and dataBytes are the estimated bytes in each queue.
	ctrlBytes, dataBytes atomic.Int64
	// version is the protocol version of the connection to the peer, 0
	// while there is none.
	version atomic.Uint32
	// Writer only: messages that waited longer than slowQueue since the
	// last report, the longest wait among them, the last one's type, and
	// the report's time.
	slow       int
	slowest    time.Duration
	last       raftpb.MessageType
	slowReport time.Time
}

// queued is a message in a sender's queue and when it was queued.
type queued struct {
	m  *raftpb.Message
	at time.Time
}

// size estimates the bytes a message keeps queued: its entries, context
// and snapshot.
func size(m *raftpb.Message) int64 {
	n := int64(64 + len(m.Context))
	for _, e := range m.Entries {
		n += int64(len(e.Data)) + 32
	}
	if m.Snapshot != nil {
		n += int64(len(m.Snapshot.Data))
	}
	return n
}

// isData reports whether a message goes on the data queue. Appends and
// snapshots do, empty appends included, so that they stay in order with
// each other, and so do proposals that a follower forwards to the leader,
// which carry whole write batches. Everything else (heartbeats, votes,
// ReadIndex and their responses) goes ahead of them.
func isData(m *raftpb.Message) bool {
	t := m.GetType()
	return t == raftpb.MsgApp || t == raftpb.MsgSnap || t == raftpb.MsgProp
}

func (s *sender) send(m *raftpb.Message) {
	q, budget := s.ctrl, &s.ctrlBytes
	if isData(m) {
		q, budget = s.data, &s.dataBytes
	}
	n := size(m)
	if budget.Add(n) > maxQueued {
		budget.Add(-n)
		s.t.cfg.Unreachable(s.id)
		return
	}
	select {
	case q <- queued{m, time.Now()}:
	default:
		budget.Add(-n)
		s.t.cfg.Unreachable(s.id)
	}
}

// next returns the next message, control messages first, and whether the
// queues hold more right now.
func (s *sender) next(ctx context.Context) (*raftpb.Message, bool) {
	var q queued
	select {
	case q = <-s.ctrl:
		s.ctrlBytes.Add(-size(q.m))
	default:
		select {
		case q = <-s.ctrl:
			s.ctrlBytes.Add(-size(q.m))
		case q = <-s.data:
			s.dataBytes.Add(-size(q.m))
		case <-ctx.Done():
			return nil, false
		}
	}
	s.waited(q)
	return q.m, true
}

// waited logs messages that waited long in their queue, at most once a
// second, with the longest wait, so that a stall on a link can be told
// apart from a slow disk or a slow committer.
func (s *sender) waited(q queued) {
	d := time.Since(q.at)
	if d <= slowQueue {
		return
	}
	s.slow++
	if d > s.slowest {
		s.slowest = d
	}
	s.last = q.m.GetType()
	if time.Since(s.slowReport) >= time.Second {
		s.reportWaits()
	}
}

// reportWaits logs the long waits counted since the last report. The
// writer also calls it when a connection ends, so the waits right before
// a failure are not lost.
func (s *sender) reportWaits() {
	if s.slow == 0 {
		return
	}
	s.t.logf("peer: %d messages to %d waited over %v in their queue, the longest %v; the last a %s",
		s.slow, s.id, slowQueue, s.slowest, s.last)
	s.slow, s.slowest, s.slowReport = 0, 0, time.Now()
}

func (s *sender) pending() bool { return len(s.ctrl) > 0 || len(s.data) > 0 }

// drop empties the queues after a failed connection.
func (s *sender) drop() {
	for {
		select {
		case q := <-s.ctrl:
			s.ctrlBytes.Add(-size(q.m))
		case q := <-s.data:
			s.dataBytes.Add(-size(q.m))
		default:
			s.t.cfg.Unreachable(s.id)
			return
		}
	}
}

func (s *sender) run() {
	ctx := s.t.ctx
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		c, v, err := s.dial(ctx, s.t.cfg.ID)
		if err != nil {
			s.drop()
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			backoff = min(2*backoff, time.Second)
			continue
		}
		backoff = 50 * time.Millisecond
		s.version.Store(uint32(v))
		err = s.write(ctx, c)
		s.version.Store(0)
		s.reportWaits()
		if err != nil && ctx.Err() == nil {
			s.t.logf("peer: send to %d: %v", s.id, err)
			s.drop()
		}
		s.t.untrack(c)
	}
}

// dial connects to the peer and runs the handshake, with id in the
// preface, and returns the connection and its protocol version. A failed
// TCP dial is not logged, since that is how a peer that is down looks. A
// failed handshake is.
func (s *sender) dial(ctx context.Context, id uint64) (net.Conn, byte, error) {
	host, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return nil, 0, err
	}
	raw, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, 0, err
	}
	c := tls.Client(raw, &tls.Config{
		Certificates: []tls.Certificate{s.t.cfg.Certificate},
		RootCAs:      s.t.cfg.CA,
		ServerName:   host,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpns(s.t.cfg.minVersion, s.t.cfg.maxVersion),
		// As in etcd, a dialing node checks the allowed names too.
		VerifyPeerCertificate: s.t.verify,
	})
	if !s.t.track(c) {
		return nil, 0, errors.Join(net.ErrClosed, c.Close())
	}
	v, err := s.handshake(ctx, c, id)
	if err != nil {
		if ctx.Err() == nil {
			s.t.logHandshake(fmt.Sprintf("with %d", s.id), err)
		}
		s.t.untrack(c)
		return nil, 0, err
	}
	return c, v, nil
}

// handshake runs the TLS handshake, writes the preface with id and checks
// the reply against the version ALPN chose. It returns that version.
func (s *sender) handshake(ctx context.Context, c *tls.Conn, id uint64) (byte, error) {
	if err := c.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
		return 0, err
	}
	if err := c.HandshakeContext(ctx); err != nil {
		return 0, err
	}
	name := c.ConnectionState().NegotiatedProtocol
	if name == "" {
		return 0, errors.New("the peer chose no version in ALPN; it may be a node from before version 1")
	}
	if _, err := c.Write(s.t.preface(id)); err != nil {
		return 0, err
	}
	var reply [1]byte
	if _, err := io.ReadFull(c, reply[:]); err != nil {
		return 0, err
	}
	v := reply[0]
	switch {
	case v == 0:
		return 0, errors.New("the peer refused the preface with reply 0")
	case name != alpn(v):
		return 0, fmt.Errorf("the peer replied version %d, but chose %s in ALPN", v, name)
	}
	return v, c.SetDeadline(time.Time{})
}

// write sends frames until the connection fails or the transport closes.
func (s *sender) write(ctx context.Context, c net.Conn) error {
	w := bufio.NewWriterSize(c, ioBuffer)
	var hdr [4]byte
	var buf []byte
	for {
		if !s.pending() {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		m, ok := s.next(ctx)
		if !ok {
			return nil
		}
		if n := proto.Size(m); n > maxFrame {
			// The receiver would refuse it; Raft sends again in smaller
			// pieces after the unreachable report.
			s.t.logf("peer: dropping a %s of %d bytes to %d, over the frame limit", m.GetType(), n, s.id)
			s.t.cfg.Unreachable(s.id)
			continue
		}
		var err error
		buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
		if err := c.SetWriteDeadline(writeDeadline()); err != nil {
			return err
		}
		if _, err := w.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
}

// writeDeadline is when a write that has not finished fails the
// connection, so that a stalled peer is dropped and reported.
func writeDeadline() time.Time {
	if planted.On("B14") {
		// Planted: no deadline; a stalled peer holds the writer.
		return time.Time{}
	}
	return time.Now().Add(writeTimeout)
}
