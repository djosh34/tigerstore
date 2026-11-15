package wl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	mainnode "github.com/djosh34/tigerstore/internal/node"
	"go.etcd.io/raft/v3"
	"golang.org/x/sys/unix"
)

type peerOut struct {
	sock
	peer uint64
}

type peerIn struct {
	sock
	hello bool
	from  uint64
	held  [][]byte // messages that wait behind an append naming a batch not here yet
	// heldSince is when the first of held came.
	heldSince time.Time
}

// loop owns the epoll instance and the node.
type loop struct {
	n    *node
	epfd int
	hoFD int // handoffs wake the loop through it

	ctx      context.Context
	mu       sync.Mutex
	stopped  bool
	handoffs []*handoff
	redials  map[uint64]chan struct{}

	free    []int
	tick    int64
	logged  int64
	last    counters
	lastWAL walStats
	lastAt  time.Time
	logger  *log.Logger
	next    time.Time
}

const (
	evHandoff = -1
	evFsync   = -2
	tickEvery = 100 * time.Millisecond
)

// Main runs a node with etcd's command line.
func Main(args []string) int {
	fs := flag.NewFlagSet("tigerstore", flag.ContinueOnError)
	name := fs.String("name", "default", "")
	dataDir := fs.String("data-dir", "", "")
	listenClient := fs.String("listen-client-urls", "", "")
	advClient := fs.String("advertise-client-urls", "", "")
	listenPeer := fs.String("listen-peer-urls", "", "")
	fs.String("initial-advertise-peer-urls", "", "")
	initial := fs.String("initial-cluster", "", "")
	fs.String("initial-cluster-token", "", "")
	fs.String("initial-cluster-state", "", "")
	certFile := fs.String("cert-file", "", "")
	keyFile := fs.String("key-file", "", "")
	trusted := fs.String("trusted-ca-file", "", "")
	fs.Bool("client-cert-auth", false, "")
	peerCert := fs.String("peer-cert-file", "", "")
	peerKey := fs.String("peer-key-file", "", "")
	peerTrusted := fs.String("peer-trusted-ca-file", "", "")
	fs.Bool("peer-client-cert-auth", false, "")
	fs.Int64("quota-backend-bytes", 0, "")
	budgetFlag := fs.Int64("memory-budget-bytes", 0, "")
	fs.String("log-level", "", "")
	fs.Bool("enable-pprof", false, "")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	logger := log.New(os.Stderr, "tigerstore-wl ", log.LstdFlags|log.Lmicroseconds)
	budget, from, err := mainnode.MemoryBudget(os.DirFS("/"), *budgetFlag)
	if err != nil {
		logger.Printf("error: %v", err)
		return 2
	}
	logger.Printf("memory budget %d MiB, from %s", budget>>20, from)
	memBudget = budget
	// The node runs until it fails.
	err = run(context.Background(), logger, *name, *dataDir, *listenClient, *advClient, *listenPeer, *initial,
		*certFile, *keyFile, *trusted, *peerCert, *peerKey, *peerTrusted)
	logger.Printf("error: %v", err)
	return 1
}

func loadTLS(cert, key, ca string, server bool) (*tls.Config, error) {
	c, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cfg := &tls.Config{Certificates: []tls.Certificate{c}, MinVersion: tls.VersionTLS13}
	if server {
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func hostOf(u string) (string, error) {
	p, err := url.Parse(strings.Split(u, ",")[0])
	if err != nil {
		return "", err
	}
	return p.Host, nil
}

// memBudget is the memory the node may use.
var memBudget int64

// sideSize is the side ring's: the failure branches of Txns.
const sideSize = 16 << 20

// errStopped is run's error once its context is done.
var errStopped = errors.New("the node was stopped")

// run runs a node until it fails or ctx is done.
func run(ctx context.Context, logger *log.Logger, name, dataDir, listenClient, advClient, listenPeer, initial,
	certFile, keyFile, trusted, peerCert, peerKey, peerTrusted string,
) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	cliTLS, err := loadTLS(certFile, keyFile, trusted, true)
	if err != nil {
		return err
	}
	cliTLS.NextProtos = []string{"h2", "http/1.1"}
	// A client that offers HTTP/1.1, such as curl or the bench's metrics
	// scraper, gets it; gRPC clients offer only h2.
	h1 := cliTLS.Clone()
	h1.NextProtos = []string{"http/1.1"}
	cliTLS.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		if slices.Contains(hi.SupportedProtos, "http/1.1") {
			return h1, nil
		}
		return nil, nil
	}
	peerSrvTLS, err := loadTLS(peerCert, peerKey, peerTrusted, true)
	if err != nil {
		return err
	}
	peerCliTLS, err := loadTLS(peerCert, peerKey, peerTrusted, false)
	if err != nil {
		return err
	}
	n := &node{clusterID: 0x7769726531, peersOut: map[uint64]*peerOut{}, metrics: &nodeMetrics{}}
	var peers []raft.Peer
	for i, part := range strings.Split(initial, ",") {
		nm, u, ok := strings.Cut(part, "=")
		if !ok {
			return fmt.Errorf("bad --initial-cluster %q", initial)
		}
		addr, err := hostOf(u)
		if err != nil {
			return err
		}
		m := member{id: uint64(i + 1), name: nm, peerURL: u, peerAddr: addr}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("bad peer URL %q: %w", u, err)
		}
		pn, err := strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("bad peer URL %q: %w", u, err)
		}
		m.cliURL = fmt.Sprintf("https://%s:%d", host, pn-1)
		if nm == name {
			n.id = m.id
			m.cliURL = advClient
		}
		n.members = append(n.members, m)
		peers = append(peers, raft.Peer{ID: m.id})
	}
	if n.id == 0 {
		return fmt.Errorf("--name %q is not in --initial-cluster", name)
	}
	if n.st, err = newStore(); err != nil {
		return err
	}
	n.fwd = make([]fwdBatch, fwdSlots)
	n.fwdPeers = map[uint64][]fwdBatch{}
	n.carryUntil = map[uint64]time.Time{}
	if n.ring, err = newRing(ringSize); err != nil {
		return err
	}
	if n.rx, err = newRing(ringSize); err != nil {
		return err
	}
	if n.side, err = newRing(sideSize); err != nil {
		return err
	}
	// Raft's storage takes what the log holds; Raft then hands out the
	// committed entries again, and the apply builds the store from them.
	n.storage = newLogStorage(nil)
	if n.wal, err = openWAL(filepath.Join(dataDir, "wal"), n.storage.restore); err != nil {
		return err
	}
	n.storage.log = n.wal
	n.st.log = n.wal
	n.st.setBudget(memBudget, 2*ringSize+sideSize+stageSize+storageHeld)
	n.term, n.commit = n.storage.hs.GetTerm(), n.storage.hs.GetCommit()
	n.rn, err = raft.NewRawNode(raftConfig(n.id, n.storage))
	if err != nil {
		return err
	}
	if n.storage.lastIndex() == 0 {
		if err := n.rn.Bootstrap(peers); err != nil {
			return err
		}
	} else {
		logger.Printf("recovered the log: entries to %d, hard state term %d commit %d", n.storage.lastIndex(), n.term, n.commit)
	}

	l := &loop{n: n, redials: map[uint64]chan struct{}{}, logger: logger, lastAt: time.Now()}
	if l.epfd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		return err
	}
	if l.hoFD, err = unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK); err != nil {
		return err
	}
	if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, l.hoFD, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: evHandoff}); err != nil {
		return err
	}
	if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, n.wal.wakeFD, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: evFsync}); err != nil {
		return err
	}

	cliAddr, err := hostOf(listenClient)
	if err != nil {
		return err
	}
	peerAddr, err := hostOf(listenPeer)
	if err != nil {
		return err
	}
	cl, err := net.Listen("tcp", cliAddr)
	if err != nil {
		return err
	}
	pl, err := net.Listen("tcp", peerAddr)
	if err != nil {
		return err
	}
	l.ctx = ctx
	defer l.shut(cl, pl)
	go func() {
		<-ctx.Done()
		l.mu.Lock()
		defer l.mu.Unlock()
		l.wake()
	}()
	go l.accept(cl, cliTLS, kindClient, logger)
	go l.accept(pl, peerSrvTLS, kindPeerIn, logger)
	for _, m := range n.members {
		if m.id == n.id {
			continue
		}
		ch := make(chan struct{}, 1)
		l.redials[m.id] = ch
		go l.dial(m, peerCliTLS, ch)
	}
	logger.Printf("node %s (id %d) up: clients %s, peers %s", name, n.id, cliAddr, peerAddr)
	return l.run()
}

func (l *loop) accept(ln net.Listener, cfg *tls.Config, kind uint8, logger *log.Logger) {
	for {
		nc, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			logger.Printf("accept: %v", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		go func() {
			h, err := handshake(nc, cfg, true)
			if err != nil {
				return
			}
			h.kind = kind
			if kind == kindClient && h.alpn != "h2" {
				serveHTTP1(h, l.n.metrics)
				return
			}
			l.handOver(h)
		}()
	}
}

func (l *loop) dial(m member, cfg *tls.Config, redial chan struct{}) {
	c := cfg.Clone()
	host, _, _ := net.SplitHostPort(m.peerAddr)
	c.ServerName = host
	pause := func() bool {
		select {
		case <-l.ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
			return true
		}
	}
	for {
		nc, err := net.DialTimeout("tcp", m.peerAddr, time.Second)
		if err != nil {
			if !pause() {
				return
			}
			continue
		}
		h, err := handshake(nc, c, false)
		if err != nil {
			if !pause() {
				return
			}
			continue
		}
		h.kind, h.peer = kindPeerOut, m.id
		l.handOver(h)
		select {
		case <-redial:
		case <-l.ctx.Done():
			return
		}
		if !pause() {
			return
		}
	}
}

func (l *loop) handOver(h *handoff) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		_ = unix.Close(h.fd)
		return
	}
	l.handoffs = append(l.handoffs, h)
	l.wake()
	l.mu.Unlock()
}

// wake wakes the loop through its handoff eventfd, unless it stopped and
// closed it. The caller holds l.mu.
func (l *loop) wake() {
	if l.stopped {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = unix.Write(l.hoFD, b[:])
}

// shut closes what a stopped loop had open: its listeners, sockets, log
// and file descriptors.
func (l *loop) shut(listeners ...net.Listener) {
	for _, ln := range listeners {
		_ = ln.Close()
	}
	l.mu.Lock()
	l.stopped = true
	hs := l.handoffs
	l.handoffs = nil
	l.mu.Unlock()
	for _, h := range hs {
		_ = unix.Close(h.fd)
	}
	for idx, x := range l.n.socks {
		if x != nil {
			l.closeSock(idx)
		}
	}
	l.n.wal.close()
	_ = unix.Close(l.epfd)
	_ = unix.Close(l.hoFD)
}

func (l *loop) takeHandoffs() {
	var b [8]byte
	_, _ = unix.Read(l.hoFD, b[:])
	l.mu.Lock()
	hs := l.handoffs
	l.handoffs = nil
	l.mu.Unlock()
	n := l.n
	for _, h := range hs {
		idx := l.slot()
		var s *sock
		switch h.kind {
		case kindClient:
			c := newClient(h, &n.stats)
			c.node = n
			n.socks[idx] = c
			s = &c.sock
		case kindPeerIn:
			p := &peerIn{sock: *newSock(h, &n.stats)}
			n.socks[idx] = p
			s = &p.sock
		case kindPeerOut:
			p := &peerOut{sock: *newSock(h, &n.stats), peer: h.peer}
			p.sent = &n.metrics.peerSent
			n.socks[idx] = p
			s = &p.sock
			if old := n.peersOut[h.peer]; old != nil && !old.dead {
				l.closeSock(old.idx)
			}
			n.peersOut[h.peer] = p
			var hello [8]byte
			binary.LittleEndian.PutUint64(hello[:], n.id)
			s.put(hello[:])
			n.markDirty(s)
		}
		s.idx, s.gen = idx, n.gens[idx]
		if err := unix.EpollCtl(l.epfd, unix.EPOLL_CTL_ADD, h.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(idx)}); err != nil {
			l.closeSock(idx)
			continue
		}
		// Bytes read with the handshake wait in in already.
		if s.iw > 0 {
			l.input(idx)
		}
	}
}

func (l *loop) slot() int {
	n := l.n
	if k := len(l.free); k > 0 {
		idx := l.free[k-1]
		l.free = l.free[:k-1]
		return idx
	}
	n.socks = append(n.socks, nil)
	n.gens = append(n.gens, 1)
	return len(n.socks) - 1
}

func sockOf(x any) *sock {
	switch v := x.(type) {
	case *client:
		return &v.sock
	case *peerIn:
		return &v.sock
	case *peerOut:
		return &v.sock
	}
	return nil
}

func (l *loop) closeSock(idx int) {
	n := l.n
	x := n.socks[idx]
	s := sockOf(x)
	if s == nil || s.dead {
		return
	}
	s.dead = true
	_ = unix.Close(s.fd)
	switch v := x.(type) {
	case *client:
		for i := range v.slots {
			st := &v.slots[i]
			if st.open && st.watch {
				n.closeWatchStream(v, st)
			}
			st.fail = nil
		}
		for _, st := range v.over {
			if st.watch {
				n.closeWatchStream(v, st)
			}
			st.fail = nil
		}
	case *peerOut:
		if n.peersOut[v.peer] == v {
			delete(n.peersOut, v.peer)
			n.rn.ReportUnreachable(v.peer)
			select {
			case l.redials[v.peer] <- struct{}{}:
			default:
			}
		}
	}
	n.socks[idx] = nil
	n.gens[idx]++
	l.free = append(l.free, idx)
}

// input reads what socket idx has and handles it.
func (l *loop) input(idx int) {
	n := l.n
	x := n.socks[idx]
	s := sockOf(x)
	if s == nil || s.dead {
		return
	}
	got, err := s.readSock()
	if err == nil && (got || s.iw > s.ir) {
		err = s.open()
	}
	if err == nil {
		switch v := x.(type) {
		case *client:
			err = v.frames(n)
		case *peerIn:
			err = n.peerFrames(v)
		case *peerOut:
			// Nothing comes back on an outgoing peer connection.
			s.pr = s.pw
		}
	}
	if err != nil {
		l.closeSock(idx)
	}
}

// run turns the loop until the node fails.
func (l *loop) run() error {
	runtime.LockOSThread()
	n := l.n
	events := make([]unix.EpollEvent, 256)
	l.next = time.Now().Add(tickEvery)
	if n.id == 1 {
		_ = n.rn.Campaign()
	}
	for {
		// Round up: a wait of less than a millisecond must not spin.
		timeout := 0
		wake := l.next
		if n.wal.deferred {
			if t := n.wal.lastKick.Add(syncGap); t.Before(wake) {
				wake = t
			}
		}
		if d := time.Until(wake); d > 0 {
			timeout = int((d + time.Millisecond - 1) / time.Millisecond)
		}
		k, err := unix.EpollWait(l.epfd, events, timeout)
		if err != nil && !errors.Is(err, unix.EINTR) {
			return err
		}
		n.now = time.Now()
		for i := range k {
			ev := events[i]
			switch ev.Fd {
			case evHandoff:
				if l.ctx.Err() != nil {
					return errStopped
				}
				l.takeHandoffs()
			case evFsync:
				if err := n.fsyncDone(); err != nil {
					return err
				}
			default:
				idx := int(ev.Fd)
				if ev.Events&(unix.EPOLLIN|unix.EPOLLRDHUP|unix.EPOLLHUP|unix.EPOLLERR) != 0 {
					l.input(idx)
				}
				if ev.Events&unix.EPOLLOUT != 0 {
					if s := sockOf(n.socks[idx]); s != nil && !s.dead {
						n.markDirty(s)
					}
				}
			}
		}
		if now := time.Now(); !now.Before(l.next) {
			l.next = now.Add(tickEvery)
			l.tick++
			n.rn.Tick()
			n.retryReads(l.tick)
			n.expireProposals()
			if n.leading() {
				n.expireLeases()
			}
		}
		if n.holding {
			n.retryHeld()
		}
		if n.wal.deferred {
			n.wal.kick()
		}
		n.proposeBatch(n.now)
		n.askReads(l.tick)
		t1, ops := time.Now(), n.stats.opsByKind
		if err := n.ready(); err != nil {
			return err
		}
		t2 := time.Now()
		if err := n.syncWatchers(); err != nil {
			return err
		}
		t3 := time.Now()
		l.flush()
		t4 := time.Now()
		if !n.rn.HasReady() {
			// Raft holds no message now that could point into a ring.
			n.sweepRings(n.now)
		}
		if t5 := time.Now(); t5.Sub(n.now) > slowTurn {
			l.logSlowTurn(n.now, t1, t2, t3, t4, t5, ops)
		}
		n.stats.turns++
		if l.tick%50 == 0 && l.tick != l.logged {
			l.logged = l.tick
			l.logStats()
		}
	}
}

func (l *loop) flush() {
	n := l.n
	for _, s := range n.dirty {
		s.dirty = false
		if s.dead {
			continue
		}
		ok, err := s.flush()
		if err != nil {
			l.closeSock(s.idx)
			continue
		}
		if !ok && !s.wantOut {
			s.wantOut = true
			_ = unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, s.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP | unix.EPOLLOUT, Fd: int32(s.idx)})
		} else if ok && s.wantOut {
			s.wantOut = false
			_ = unix.EpollCtl(l.epfd, unix.EPOLL_CTL_MOD, s.fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(s.idx)})
		}
	}
	n.dirty = n.dirty[:0]
}

// serveHTTP1 answers /metrics and /debug/pprof/profile over HTTP/1.1 on
// the record layer, in the handshake's goroutine.
func serveHTTP1(h *handoff, m *nodeMetrics) {
	defer func() { _ = unix.Close(h.fd) }()
	_ = unix.SetNonblock(h.fd, false)
	s := newSock(h, &counters{})
	var req []byte
	for {
		if err := s.open(); err != nil {
			return
		}
		req = append(req, s.pt[s.pr:s.pw]...)
		s.pr = s.pw
		if bytes.Contains(req, []byte("\r\n\r\n")) {
			break
		}
		if _, err := s.readSock(); err != nil {
			return
		}
	}
	line, _, _ := strings.Cut(string(req), "\r\n")
	f := strings.Fields(line)
	path := ""
	if len(f) >= 2 {
		path = f[1]
	}
	var body []byte
	ctype := "text/plain; version=0.0.4"
	switch {
	case strings.HasPrefix(path, "/debug/pprof/profile"):
		secs := 10
		if u, err := url.Parse(path); err == nil {
			if v := u.Query().Get("seconds"); v != "" {
				if s, err := strconv.Atoi(v); err == nil && s > 0 {
					secs = s
				}
			}
		}
		var buf bytes.Buffer
		profMu.Lock()
		if err := pprof.StartCPUProfile(&buf); err == nil {
			time.Sleep(time.Duration(secs) * time.Second)
			pprof.StopCPUProfile()
		}
		profMu.Unlock()
		body = buf.Bytes()
		ctype = "application/octet-stream"
	default:
		body = m.text()
	}
	resp := fmt.Appendf(nil, "HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", ctype, len(body))
	s.put(append(resp, body...))
	for {
		ok, err := s.flush()
		if err != nil || ok {
			return
		}
	}
}

var profMu sync.Mutex

// counters are the loop's own, for its log: per 5 s, how many turns,
// socket reads and writes, WAL writes and fsyncs, replies and Raft work.
type counters struct {
	turns, reads, writes, replies, proposals, readIndex, peerMsgs uint64

	// What the batches carried, and how the peers named or carried them.
	opsByKind, bytesByKind [16]int
	named, held            int
	carried, carriedBytes  [32]int
}

// slowTurn is a turn long enough to log, so that a stall shows where
// its time went.
const slowTurn = 200 * time.Millisecond

// logSlowTurn logs a slow turn's phases and the ops it applied, by kind;
// ops are the counts before the turn's Ready.
func (l *loop) logSlowTurn(start, input, ready, sync, flush, end time.Time, ops [16]int) {
	var applied [16]int
	for k := range applied {
		applied[k] = l.n.stats.opsByKind[k] - ops[k]
	}
	l.logger.Printf("slow turn %v: input and proposals %v, ready and apply %v, watch catch-up %v, flush %v, sweep %v; applied by kind %v; keys %d, watchers %d, catching up %d, held flows %d",
		end.Sub(start), input.Sub(start), ready.Sub(input), sync.Sub(ready), flush.Sub(sync), end.Sub(flush), applied,
		l.n.st.nkeys, len(l.n.w.ws), len(l.n.w.unsynced), l.heldFlows())
}

// heldFlows counts the messages the clients hold for want of window.
func (l *loop) heldFlows() int {
	k := 0
	for _, x := range l.n.socks {
		if c, ok := x.(*client); ok {
			k += len(c.flow.held)
		}
	}
	return k
}

func (l *loop) logStats() {
	d, now := l.n.stats, time.Now()
	ws := l.n.wal.stats
	p := l.last
	secs := now.Sub(l.lastAt).Seconds()
	r := float64(d.replies - p.replies)
	if r == 0 {
		r = 1
	}
	pw := l.lastWAL
	l.logger.Printf("stats %.1fs: replies/s %.0f turns/s %.0f | per reply: turns %.3f reads %.3f writes %.3f walWrites %.3f fsyncs %.4f proposals %.3f readIndex %.3f peerMsgs %.3f walBytes %.1f | keys %d rev %d slab %d MB",
		secs, r/secs, float64(d.turns-p.turns)/secs, float64(d.turns-p.turns)/r, float64(d.reads-p.reads)/r, float64(d.writes-p.writes)/r,
		float64(ws.writes-pw.writes)/r, float64(ws.syncs-pw.syncs)/r, float64(d.proposals-p.proposals)/r, float64(d.readIndex-p.readIndex)/r, float64(d.peerMsgs-p.peerMsgs)/r, float64(ws.bytes-pw.bytes)/r,
		l.n.st.nkeys, l.n.st.rev, l.n.st.slab.top>>20)
	l.lastWAL = ws
	st := l.n.st
	l.logger.Printf("budget: slab used %d MB limit %d MB evictions %d loads %d; stage waits %d",
		st.slab.used>>20, st.limit>>20, st.evictions, st.loads, l.n.wal.stats.waits)
	n := l.n
	l.logger.Printf("rings: spans %d/%d/%d, on the heap %d/%d/%d; storage let go %d, read back %d",
		n.ring.stats.spans, n.rx.stats.spans, n.side.stats.spans, n.ring.stats.heap, n.rx.stats.heap, n.side.stats.heap,
		n.storage.stats.letGo, n.storage.stats.reads)
	l.logger.Printf("rings: sweeps %d; oldest held: %s / %s / %s", n.ring.stats.sweeps, n.ring.held(), n.rx.held(), n.side.held())
	l.last, l.lastAt = d, now
	l.logger.Printf("ops by kind %v bytes %v named %d carried %v bytes %v held %d", d.opsByKind, d.bytesByKind, d.named, d.carried, d.carriedBytes, d.held)
}
