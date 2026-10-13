// Package peer carries Raft messages between nodes: framed TLS 1.3 over
// TCP, one connection per directed pair of nodes.
//
// After the TLS handshake, the dialing node writes its Raft ID as 8 bytes.
// Then each frame is a 4-byte length and a protobuf raftpb.Message. Both
// ends verify the other's certificate against the peer CA.
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

	"github.com/djosh34/tigerstore/internal/expclock"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Transport sends Raft messages to peers. Send never blocks: when a peer's
// queue is full or its connection is down, the message is dropped and the
// peer is reported unreachable, and Raft sends again.
type Transport interface {
	Send(m *raftpb.Message)
	Close() error
}

// Config sets up the TCP transport.
type Config struct {
	// ID is this node's Raft ID.
	ID uint64
	// Listen is the host:port to accept peers on.
	Listen string
	// Peers maps every other node's Raft ID to its host:port.
	Peers map[uint64]string
	// Certificate is this node's peer certificate and CA the pool that
	// verifies other nodes' certificates. The transport requires TLS 1.3
	// and verifies both ends.
	Certificate tls.Certificate
	CA          *x509.CertPool
	// Deliver gets every message received from a peer. It may block,
	// which slows down that peer's connection.
	Deliver func(*raftpb.Message)
	// Unreachable is called when a message to a peer was dropped.
	Unreachable func(id uint64)
	// Logf logs connection errors.
	Logf func(format string, args ...any)
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
	dialTimeout  = 3 * time.Second
	ioBuffer     = 256 << 10
)

// TCP is the transport over TLS on TCP.
type TCP struct {
	cfg     Config
	ln      net.Listener
	senders map[uint64]*sender
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// Start listens for peers and starts one writer per peer.
func Start(cfg Config) (*TCP, error) {
	ln, err := tls.Listen("tcp", cfg.Listen, &tls.Config{
		Certificates: []tls.Certificate{cfg.Certificate},
		ClientCAs:    cfg.CA,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCP{
		cfg:     cfg,
		ln:      ln,
		senders: map[uint64]*sender{},
		ctx:     ctx,
		cancel:  cancel,
		conns:   map[net.Conn]struct{}{},
	}
	for id, addr := range cfg.Peers {
		s := &sender{
			t:    t,
			id:   id,
			addr: addr,
			ctrl: make(chan *raftpb.Message, controlQueue),
			data: make(chan *raftpb.Message, dataQueue),
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
			if err := t.receive(c); t.ctx.Err() == nil {
				t.logf("peer: receive from %s: %v", c.RemoteAddr(), err)
			}
		})
	}
}

// receive reads frames from one inbound connection until it fails. It
// always returns the error that ended it.
func (t *TCP) receive(c net.Conn) error {
	if err := c.SetReadDeadline(time.Now().Add(dialTimeout)); err != nil {
		return err
	}
	r := bufio.NewReaderSize(c, ioBuffer)
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	from := binary.BigEndian.Uint64(hdr[:])
	if _, ok := t.senders[from]; !ok {
		return fmt.Errorf("unknown peer %d", from)
	}
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	var buf []byte
	for {
		if _, err := io.ReadFull(r, hdr[:4]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:4])
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

// sender owns the connection to one peer.
type sender struct {
	t    *TCP
	id   uint64
	addr string
	ctrl chan *raftpb.Message
	data chan *raftpb.Message
	// ctrlBytes and dataBytes are the estimated bytes in each queue.
	ctrlBytes, dataBytes atomic.Int64
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
	case q <- m:
	default:
		budget.Add(-n)
		s.t.cfg.Unreachable(s.id)
	}
}

// next returns the next message, control messages first, and whether the
// queues hold more right now.
func (s *sender) next(ctx context.Context) (*raftpb.Message, bool) {
	select {
	case m := <-s.ctrl:
		s.ctrlBytes.Add(-size(m))
		return m, true
	default:
	}
	select {
	case m := <-s.ctrl:
		s.ctrlBytes.Add(-size(m))
		return m, true
	case m := <-s.data:
		s.dataBytes.Add(-size(m))
		return m, true
	case <-ctx.Done():
		return nil, false
	}
}

func (s *sender) pending() bool { return len(s.ctrl) > 0 || len(s.data) > 0 }

// drop empties the queues after a failed connection.
func (s *sender) drop() {
	for {
		select {
		case m := <-s.ctrl:
			s.ctrlBytes.Add(-size(m))
		case m := <-s.data:
			s.dataBytes.Add(-size(m))
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
		c, err := s.dial(ctx)
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
		if err := s.write(ctx, c); err != nil && ctx.Err() == nil {
			s.t.logf("peer: send to %d: %v", s.id, err)
			s.drop()
		}
		s.t.untrack(c)
	}
}

func (s *sender) dial(ctx context.Context) (net.Conn, error) {
	host, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return nil, err
	}
	d := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: dialTimeout},
		Config: &tls.Config{
			Certificates: []tls.Certificate{s.t.cfg.Certificate},
			RootCAs:      s.t.cfg.CA,
			ServerName:   host,
			MinVersion:   tls.VersionTLS13,
		},
	}
	c, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, err
	}
	if !s.t.track(c) {
		return nil, errors.Join(net.ErrClosed, c.Close())
	}
	return c, nil
}

// write sends frames until the connection fails or the transport closes.
func (s *sender) write(ctx context.Context, c net.Conn) error {
	w := bufio.NewWriterSize(c, ioBuffer)
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], s.t.cfg.ID)
	if err := c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	var buf []byte
	for {
		if !s.pending() {
			expclock.Event("peer_flush_begin", s.id, uint64(w.Buffered()), 0, 0)
			if err := w.Flush(); err != nil {
				return err
			}
			expclock.Event("peer_flush_end", s.id, 0, 0, 0)
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
		expclock.Event("peer_write", uint64(m.GetType()), s.id, m.GetIndex(), m.GetCommit())
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(buf)))
		if err := c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return err
		}
		if _, err := w.Write(hdr[:4]); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
}
