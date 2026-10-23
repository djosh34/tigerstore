// Package peer carries Raft messages between nodes: framed TLS 1.3 over
// TCP, one connection per directed pair of nodes.
//
// Both ends verify the other's certificate against the peer CA and, when
// set, the allowed CommonNames or hostnames. Both ends also offer the
// protocol versions they speak in TLS ALPN, as "tigerstore-peer/<version>".
// After the TLS handshake, the dialing node writes a preface: the magic
// "TS", the lowest and the highest version it speaks, its Raft ID as 8
// bytes, and the host:port it advertises for peers, after a 1-byte length.
// The accepting node replies with 1 byte: the highest version both speak,
// which must be the one ALPN chose, or 0 if there is none, after which it
// closes. Then each frame is a 4-byte length and a protobuf
// raftpb.Message. The accepting node writes nothing more on that
// connection; the dialer reads it only to see it close.
//
// As etcd's peers take messages from any member they have not removed, a
// node takes a message connection from a member it does not know yet,
// such as one added in entries it has not applied, and sends its replies
// to the address in the preface (addRemote), from where it also fetches
// the snapshots that member sends as leader. A member behind on
// membership can so vote for a newer one and catch up from it (#250).
//
// A follower fetches a snapshot on a connection of its own: its preface
// carries its Raft ID with the top bit set. After the reply it writes the
// first and the last index it needs, 8 bytes each, and reads the snapshot
// stream until the other end closes. A node asks another a question (Call)
// on a connection of its own too: its preface carries its Raft ID, or 0
// for a node that has none yet, with the second bit from the top set; then it writes the request, and the
// other end writes the answer, each a 4-byte length and the bytes. An
// answer starts with 0 and the answer's bytes, or with 1, a 4-byte gRPC
// code and the error's message.
package peer

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/metrics"
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
	// Call sends peer id a request, which its Config.Answer answers, and
	// returns the answer.
	Call(ctx context.Context, id uint64, req []byte) ([]byte, error)
	// SetPeers sets the peers by Raft ID and host:port, and the IDs the
	// cluster removed, which it tells that they were.
	SetPeers(peers map[uint64]string, removed []uint64)
	// ActiveSince is since when this node has had a connection to peer
	// id, the zero time while it has none.
	ActiveSince(id uint64) time.Time
	Close() error
}

// Config sets up the TCP transport.
type Config struct {
	// ID is this node's Raft ID.
	ID uint64
	// Listeners accept peers, those of etcd's --listen-peer-urls.
	Listeners []net.Listener
	// Peers maps every other node's Raft ID to its host:port. Addr is this
	// node's own, which its prefaces carry; SetPeers follows this node's
	// entry in its peers, as when the member's peer URL changes.
	Peers map[uint64]string
	Addr  string
	// Certificate is this node's peer certificate and CA the pool that
	// verifies other nodes' certificates. The transport requires TLS 1.3
	// and verifies both ends.
	Certificate tls.Certificate
	CA          *x509.CertPool
	// GetCertificate, if set, gives the certificate for each handshake
	// instead of Certificate, and GetClientCertificate, if set, the one
	// this node dials with, as etcd reads its certificate files again
	// for every handshake and dials with --peer-client-cert-file.
	GetCertificate, GetClientCertificate func() (*tls.Certificate, error)
	// CRLFile is etcd's --peer-crl-file: a peer that dials with a
	// certificate it revokes is refused.
	CRLFile string
	// CheckClientSAN is etcd's check, unless
	// --peer-skip-client-san-verification, that the certificate of a
	// peer that dials in is valid for its address: by an IP SAN, or else
	// by a DNS SAN that a reverse lookup of the address gives.
	CheckClientSAN bool
	// InsecureSkipVerify takes any certificate of the other node, as
	// etcd's --peer-auto-tls does with its self-signed ones; CA is not
	// used then.
	InsecureSkipVerify bool
	// WriteTimeout is etcd's --raft-write-timeout: how long a write to a
	// peer may take before the connection is dropped. ReadTimeout is its
	// --raft-read-timeout: how long a snapshot fetch waits for the next
	// bytes and a peer for an inbound connection's preface. 0 means
	// etcd's 5 s.
	WriteTimeout, ReadTimeout time.Duration
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
	// Removed is called when a peer answers that the cluster removed this
	// node.
	Removed func()
	// Answer answers a request that peer id sent with Call. An error that
	// is a gRPC status reaches the caller with its code and message.
	Answer func(id uint64, req []byte) ([]byte, error)
	// Dial, if set, connects to a peer instead of a TCP dial; the
	// simulator's network uses it. The TLS handshake and the protocol run
	// on what it returns.
	Dial func(ctx context.Context, addr string) (net.Conn, error)
	// Logf logs connection errors.
	Logf func(format string, args ...any)
	// Metrics count the traffic with peers; nil gives the transport a
	// set of its own.
	Metrics *metrics.Set

	// minVersion and maxVersion override the protocol versions this node
	// speaks, for tests. Zero means the package's.
	minVersion, maxVersion byte
}

const (
	// minVersion and maxVersion are the protocol versions this binary
	// speaks. Version 0 stands for the unversioned protocol from before
	// version 1. Version 2 added the dialer's address to the preface
	// (#250); no release spoke version 1, so this binary speaks neither. A
	// release that raises maxVersion keeps minVersion at or below the
	// previous release's maxVersion, so that nodes can be upgraded one at
	// a time.
	minVersion = 2
	maxVersion = 2
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

	// defaultTimeout is etcd's --raft-read-timeout and
	// --raft-write-timeout.
	defaultTimeout = 5 * time.Second
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
	cfg        Config
	clientCert func() (*tls.Certificate, error)
	verify     func([][]byte, [][]*x509.Certificate) error
	lns        []net.Listener
	// senders are the peers by Raft ID, replaced whole by SetPeers;
	// peersMu orders the replacements.
	senders atomic.Pointer[map[uint64]*sender]
	removed atomic.Pointer[map[uint64]bool]
	peersMu sync.Mutex
	// remotes are the senders to members this node does not know yet but
	// that sent it messages (addRemote), until SetPeers makes them peers or
	// removes them. self is the address in this node's prefaces.
	remoteMu sync.Mutex
	remotes  map[uint64]*sender
	self     atomic.Pointer[string]
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

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
	if cfg.Metrics == nil {
		if cfg.Metrics, err = metrics.New(nil); err != nil {
			return nil, err
		}
	}
	if len(cfg.Listeners) == 0 {
		return nil, errors.New("peer: no listener")
	}
	serverCert, clientCert := cfg.certificates()
	scfg := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return serverCert() },
		ClientCAs:      cfg.CA,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS13,
		NextProtos:     alpns(cfg.minVersion, cfg.maxVersion),
		// The CA check runs first; verify only narrows it.
		VerifyPeerCertificate: verify,
	}
	if cfg.InsecureSkipVerify {
		scfg.ClientAuth = tls.RequireAnyClientCert
	}
	if cfg.CRLFile != "" {
		scfg.VerifyConnection = func(cs tls.ConnectionState) error { return checkCRL(cfg.CRLFile, cs.PeerCertificates) }
	}
	var lns []net.Listener
	for _, raw := range cfg.Listeners {
		lns = append(lns, tls.NewListener(raw, scfg))
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCP{
		cfg:        cfg,
		clientCert: clientCert,
		verify:     verify,
		lns:        lns,
		ctx:        ctx,
		cancel:     cancel,
		conns:      map[net.Conn]struct{}{},
		inbound:    map[net.Conn]inbound{},
		logged:     map[string]time.Time{},
		remotes:    map[uint64]*sender{},
	}
	t.self.Store(&cfg.Addr)
	t.senders.Store(&map[uint64]*sender{})
	t.SetPeers(cfg.Peers, nil)
	for _, ln := range lns {
		t.wg.Go(func() { t.accept(ln) })
	}
	return t, nil
}

// ActivePeers is the number of peers this node has a connection to, as
// etcd's transport counts them.
func (t *TCP) ActivePeers() int {
	n := 0
	for _, s := range *t.senders.Load() {
		if s.version.Load() != 0 {
			n++
		}
	}
	return n
}

// ActiveSince is since when this node has had a connection to peer id,
// the zero time while it has none, as etcd's transport reports it.
func (t *TCP) ActiveSince(id uint64) time.Time {
	s, ok := t.peer(id)
	if !ok {
		return time.Time{}
	}
	if ns := s.since.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// peer returns the sender of peer id.
func (t *TCP) peer(id uint64) (*sender, bool) {
	s, ok := (*t.senders.Load())[id]
	return s, ok
}

// SetPeers makes peers the nodes the transport sends to and accepts
// connections from, by Raft ID and host:port. A peer that is new or has
// a new address gets a writer; one that is gone loses its writer. A
// removed node loses its connections to this node, and when it connects
// again it hears that it was removed.
func (t *TCP) SetPeers(peers map[uint64]string, removed []uint64) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	gone := map[uint64]bool{}
	for _, id := range removed {
		gone[id] = true
	}
	t.removed.Store(&gone)
	if addr, ok := peers[t.cfg.ID]; ok {
		t.self.Store(&addr)
	}
	old := *t.senders.Load()
	next := make(map[uint64]*sender, len(peers))
	for id, addr := range peers {
		if id == t.cfg.ID {
			continue
		}
		if s, ok := old[id]; ok && s.addr == addr {
			next[id] = s
			continue
		}
		next[id] = t.newSender(id, addr)
	}
	for id, s := range old {
		if next[id] != s {
			s.cancel()
		}
	}
	t.senders.Store(&next)
	t.remoteMu.Lock()
	for id, s := range t.remotes {
		if _, ok := next[id]; ok || gone[id] {
			s.cancel()
			delete(t.remotes, id)
		}
	}
	t.remoteMu.Unlock()
	t.mu.Lock()
	var drop []net.Conn
	for c, in := range t.inbound {
		if gone[in.id] {
			drop = append(drop, c)
		}
	}
	t.mu.Unlock()
	t.closeAll(drop)
}

// newSender starts a writer to peer id at addr.
func (t *TCP) newSender(id uint64, addr string) *sender {
	ctx, cancel := context.WithCancel(t.ctx)
	s := &sender{
		t:        t,
		id:       id,
		addr:     addr,
		ctx:      ctx,
		cancel:   cancel,
		ctrl:     make(chan queued, controlQueue),
		data:     make(chan queued, dataQueue),
		counters: t.countersOf(id),
	}
	t.wg.Go(s.run)
	return s
}

// addRemote gives replies to member id, which this node does not know
// yet, a writer to addr, as etcd's addRemoteFromRequest does. A new
// address replaces the old writer.
func (t *TCP) addRemote(id uint64, addr string) {
	t.remoteMu.Lock()
	defer t.remoteMu.Unlock()
	if s, ok := t.remotes[id]; ok {
		if s.addr == addr {
			return
		}
		s.cancel()
	}
	t.remotes[id] = t.newSender(id, addr)
}

// Addr is the address the transport listens on.
func (t *TCP) Addr() net.Addr { return t.lns[0].Addr() }

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
	for id, s := range *t.senders.Load() {
		v.Out[id] = byte(s.version.Load())
		v.In[id] = 0
	}
	t.mu.Lock()
	for _, in := range t.inbound {
		if _, ok := v.Out[in.id]; ok {
			v.In[in.id] = max(v.In[in.id], in.version)
		}
	}
	t.mu.Unlock()
	return v
}

// to returns the writer to peer id, or to a member this node does not
// know yet that sent it messages (addRemote).
func (t *TCP) to(id uint64) (*sender, bool) {
	if s, ok := t.peer(id); ok {
		return s, true
	}
	t.remoteMu.Lock()
	defer t.remoteMu.Unlock()
	s, ok := t.remotes[id]
	return s, ok
}

// Send queues a message for its peer, or for a member this node does not
// know yet that sent it messages (addRemote).
func (t *TCP) Send(m *raftpb.Message) {
	if s, ok := t.to(m.GetTo()); ok {
		s.send(m)
	}
}

// Close stops the transport and waits for its goroutines.
func (t *TCP) Close() error {
	t.cancel()
	var err error
	for _, ln := range t.lns {
		if cerr := ln.Close(); !errors.Is(cerr, net.ErrClosed) {
			err = errors.Join(err, cerr)
		}
	}
	t.mu.Lock()
	conns := slices.Collect(maps.Keys(t.conns))
	t.mu.Unlock()
	t.closeAll(conns)
	t.wg.Wait()
	return err
}

// closeAll closes connections taken from t.conns or t.inbound, without
// t.mu: TLS's Close sends close_notify, which waits up to 5 s for a peer
// that does not read, and the connections' goroutines need t.mu to end
// (untrack). The other end may have reset a connection already, or its
// goroutine closed it; neither is a failure of the caller, so the first
// is only logged.
func (t *TCP) closeAll(conns []net.Conn) {
	for _, c := range conns {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.logf("peer: close: %v", err)
		}
	}
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

func (t *TCP) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
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
	if err := c.SetReadDeadline(time.Now().Add(t.readTimeout())); err != nil {
		return err
	}
	r := bufio.NewReaderSize(c, ioBuffer)
	from, mode, v, err := t.handshake(c, r)
	if err != nil {
		if t.ctx.Err() == nil {
			t.logHandshake("from "+remoteHost(c), err)
		}
		return nil
	}
	switch mode {
	case fetchBit:
		return t.serve(c, r, from)
	case callBit:
		return t.answer(c, r, from)
	case 0:
	default:
		return fmt.Errorf("peer %d asks for a fetch and a call at once", from)
	}
	t.mu.Lock()
	t.inbound[c] = inbound{id: from, version: v}
	t.mu.Unlock()
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	cs := t.countersOf(from)
	// frames ends only with an error.
	err = t.frames(r, from, cs)
	if !errors.Is(err, io.EOF) && t.ctx.Err() == nil {
		cs.receiveFailures.Inc()
	}
	return err
}

// frames reads and delivers the messages of an inbound connection until
// it fails.
func (t *TCP) frames(r io.Reader, from uint64, cs *counters) error {
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
		cs.received.Add(float64(len(hdr) + len(buf)))
		if m.GetType() == raftpb.MsgHeartbeat {
			cs.heartbeatReceived.SetToCurrentTime()
		}
		t.cfg.Deliver(m)
	}
}

// handshake checks the ALPN version and the preface of an inbound
// connection and writes the reply. It returns the dialer's Raft ID,
// the connection's mode (fetchBit, callBit or 0 for messages), and the
// protocol version.
func (t *TCP) handshake(c net.Conn, r io.Reader) (id, mode uint64, v byte, err error) {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return 0, 0, 0, fmt.Errorf("a %T is not a TLS connection", c)
	}
	if err := tc.Handshake(); err != nil {
		return 0, 0, 0, err
	}
	if certs := tc.ConnectionState().PeerCertificates; t.cfg.CheckClientSAN && len(certs) > 0 {
		ctx, cancel := context.WithTimeout(t.ctx, t.readTimeout())
		err := CheckCertSAN(ctx, certs[0], c.RemoteAddr().String())
		cancel()
		if err != nil {
			return 0, 0, 0, err
		}
	}
	name := tc.ConnectionState().NegotiatedProtocol
	if name == "" {
		return 0, 0, 0, errors.New("the dialer offers no version in ALPN; it may be a node from before version 1")
	}
	var p [13]byte
	if _, err := io.ReadFull(r, p[:]); err != nil {
		return 0, 0, 0, err
	}
	if p[0] != magic[0] || p[1] != magic[1] {
		return 0, 0, 0, fmt.Errorf("the preface starts with %#x, not the magic \"TS\"", p[:2])
	}
	addr := make([]byte, p[12])
	if _, err := io.ReadFull(r, addr); err != nil {
		return 0, 0, 0, err
	}
	id = binary.BigEndian.Uint64(p[4:12])
	mode = id & (fetchBit | callBit)
	id &^= fetchBit | callBit
	if _, ok := t.peer(id); !ok {
		_, _, aerr := net.SplitHostPort(string(addr))
		switch {
		case (*t.removed.Load())[id]:
			// As etcd's peers answer a removed member, so that it stops.
			err = c.SetWriteDeadline(t.writeDeadline())
			if err == nil {
				_, err = c.Write([]byte{removedReply})
			}
			return 0, 0, 0, errors.Join(fmt.Errorf("peer %d was removed from the cluster", id), err)
		case id == 0 && mode == callBit:
			// A node that has no ID yet asks to join.
		case id != 0 && id != t.cfg.ID && mode == 0 && aerr == nil:
			t.addRemote(id, string(addr))
		default:
			return 0, 0, 0, fmt.Errorf("unknown peer %d at %q", id, addr)
		}
	}
	v = pick(p[2], p[3], t.cfg.minVersion, t.cfg.maxVersion)
	switch {
	case v == 0:
		err = fmt.Errorf("peer %d speaks versions %d-%d", id, p[2], p[3])
	case name != alpn(v):
		err = fmt.Errorf("peer %d chose %s in ALPN, but its preface picks version %d", id, name, v)
		v = 0
	}
	if werr := c.SetWriteDeadline(t.writeDeadline()); werr != nil {
		return 0, 0, 0, errors.Join(err, werr)
	}
	if _, werr := c.Write([]byte{v}); werr != nil {
		return 0, 0, 0, errors.Join(err, werr)
	}
	return id, mode, v, err
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
// lowest and the highest version it speaks, id, and the length and bytes
// of the address it advertises. An address over 255 bytes is left out.
func (t *TCP) preface(id uint64) []byte {
	p := binary.BigEndian.AppendUint64([]byte{magic[0], magic[1], t.cfg.minVersion, t.cfg.maxVersion}, id)
	if addr := *t.self.Load(); len(addr) <= 255 {
		return append(append(p, byte(len(addr))), addr...)
	}
	return append(p, 0)
}

// removedReply is the acceptor's reply to a node the cluster removed, in
// place of a version.
const removedReply = 0xff

// errRemoved is what a node hears when it dials a peer after the cluster
// removed it.
var errRemoved = errors.New("peer: the member has been permanently removed from the cluster")

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
	w := &deadlineWriter{t: t, c: c}
	if err := t.cfg.Serve(from, binary.BigEndian.Uint64(span[:]), binary.BigEndian.Uint64(span[8:]), w); err != nil {
		return fmt.Errorf("snapshot for %d: %w", from, err)
	}
	return nil
}

// deadlineWriter fails a write that has not finished within the write
// timeout.
type deadlineWriter struct {
	t *TCP
	c net.Conn
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	if err := w.c.SetWriteDeadline(w.t.writeDeadline()); err != nil {
		return 0, err
	}
	return w.c.Write(p)
}

// Fetch opens a connection to peer id that fetches a snapshot stream.
func (t *TCP) Fetch(ctx context.Context, id, first, last uint64) (io.ReadCloser, error) {
	// A leader this node does not know yet sends it a snapshot when it is
	// behind on membership, as after a cold restart (#250).
	s, ok := t.to(id)
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
	if err := c.SetWriteDeadline(t.writeDeadline()); err != nil {
		t.untrack(c)
		return nil, err
	}
	if _, err := c.Write(span[:]); err != nil {
		t.untrack(c)
		return nil, err
	}
	return &fetchConn{t: t, c: c}, nil
}

// fetchConn reads a snapshot stream; a read that waits longer than the
// read timeout fails, so a stalled peer does not hold the follower.
type fetchConn struct {
	t *TCP
	c net.Conn
}

func (f *fetchConn) Read(p []byte) (int, error) {
	if err := f.c.SetReadDeadline(time.Now().Add(f.t.readTimeout())); err != nil {
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
	// ctx ends when the transport closes or the peer leaves.
	ctx    context.Context
	cancel context.CancelFunc
	ctrl   chan queued
	data   chan queued
	// ctrlBytes and dataBytes are the estimated bytes in each queue.
	ctrlBytes, dataBytes atomic.Int64
	// version is the protocol version of the connection to the peer, 0
	// while there is none, and since when it is up in Unix nanoseconds.
	version  atomic.Uint32
	since    atomic.Int64
	counters *counters
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
		s.counters.sendFailures.Inc()
		s.t.cfg.Unreachable(s.id)
		return
	}
	select {
	case q <- queued{m, time.Now()}:
	default:
		budget.Add(-n)
		s.counters.sendFailures.Inc()
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
			s.counters.sendFailures.Inc()
		case q := <-s.data:
			s.dataBytes.Add(-size(q.m))
			s.counters.sendFailures.Inc()
		default:
			s.t.cfg.Unreachable(s.id)
			return
		}
	}
}

func (s *sender) run() {
	ctx := s.ctx
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
		s.since.Store(time.Now().UnixNano())
		s.counters.active.Inc()
		err = s.connected(ctx, c)
		s.counters.active.Dec()
		s.version.Store(0)
		s.since.Store(0)
		s.reportWaits()
		if err != nil && ctx.Err() == nil {
			s.counters.disconnected.Inc()
			s.t.logf("peer: send to %d: %v", s.id, err)
			s.drop()
		}
	}
}

// connected writes to c until it fails, the transport closes or the peer
// closes it, then closes it. The peer writes nothing after the handshake,
// so a read ends only when the connection does: a node with nothing to
// send still sees the peer close it, as when the cluster removed this
// node, and dials again to hear why (#263).
func (s *sender) connected(ctx context.Context, c net.Conn) error {
	ctx, stop := context.WithCancel(ctx)
	var rerr error
	read := make(chan struct{})
	go func() {
		defer close(read)
		defer stop()
		var b [1]byte
		if _, err := c.Read(b[:]); err == nil {
			rerr = fmt.Errorf("peer %d wrote on a message connection", s.id)
		} else {
			rerr = err
		}
	}()
	err := s.write(ctx, c)
	stop()
	s.t.untrack(c)
	<-read
	if err == nil && s.ctx.Err() == nil {
		err = rerr
	}
	return err
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
	var raw net.Conn
	if s.t.cfg.Dial != nil {
		raw, err = s.t.cfg.Dial(ctx, s.addr)
	} else {
		raw, err = (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", s.addr)
	}
	if err != nil {
		return nil, 0, err
	}
	c := tls.Client(raw, &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return s.t.clientCert() },
		RootCAs:              s.t.cfg.CA,
		InsecureSkipVerify:   s.t.cfg.InsecureSkipVerify, //nolint:gosec // etcd's --peer-auto-tls

		ServerName: host,
		MinVersion: tls.VersionTLS13,
		NextProtos: alpns(s.t.cfg.minVersion, s.t.cfg.maxVersion),
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
		if errors.Is(err, errRemoved) && s.t.cfg.Removed != nil {
			// Messages and snapshot fetches alike: the node stops.
			s.t.cfg.Removed()
		}
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
	case v == removedReply:
		return 0, errRemoved
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
		n := proto.Size(m)
		if n > maxFrame {
			// The receiver would refuse it; Raft sends again in smaller
			// pieces after the unreachable report.
			s.t.logf("peer: dropping a %s of %d bytes to %d, over the frame limit", m.GetType(), n, s.id)
			s.t.cfg.Unreachable(s.id)
			continue
		}
		// The length and the message go out in one write, so a frame
		// larger than the buffer skips the copy into it; the size is
		// known from Size.
		buf = binary.BigEndian.AppendUint32(buf[:0], uint32(n))
		var err error
		buf, err = proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(buf, m)
		if err != nil {
			return err
		}
		if err := c.SetWriteDeadline(s.t.writeDeadline()); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
		s.counters.sent.Add(float64(len(buf)))
		if m.GetType() == raftpb.MsgHeartbeat {
			s.counters.heartbeatSent.SetToCurrentTime()
		}
	}
}

// writeDeadline is when a write that has not finished fails the
// connection, so that a stalled peer is dropped and reported.
func (t *TCP) writeDeadline() time.Time {
	if planted.On("B14") {
		// Planted: no deadline; a stalled peer holds the writer.
		return time.Time{}
	}
	return time.Now().Add(cmp.Or(t.cfg.WriteTimeout, defaultTimeout))
}

func (t *TCP) readTimeout() time.Duration { return cmp.Or(t.cfg.ReadTimeout, defaultTimeout) }
