package wl

import (
	"bytes"
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
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"golang.org/x/sys/unix"
)

type peerOut struct {
	sock
	peer   uint64
	redial chan struct{}
}

type peerIn struct {
	sock
	hello bool
	from  uint64
}

// loop owns the epoll instance and the node.
type loop struct {
	n    *node
	epfd int
	hoFD int // handoffs wake the loop through it

	mu       sync.Mutex
	handoffs []*handoff
	redials  map[uint64]chan struct{}

	free []int
	tick int64
	next time.Time
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
	fs.Int64("memory-budget-bytes", 0, "")
	fs.String("log-level", "", "")
	fs.Bool("enable-pprof", false, "")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	logger := log.New(os.Stderr, "tigerstore-wl ", log.LstdFlags|log.Lmicroseconds)
	if err := run(logger, *name, *dataDir, *listenClient, *advClient, *listenPeer, *initial,
		*certFile, *keyFile, *trusted, *peerCert, *peerKey, *peerTrusted); err != nil {
		logger.Printf("error: %v", err)
		return 1
	}
	return 0
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

func run(logger *log.Logger, name, dataDir, listenClient, advClient, listenPeer, initial,
	certFile, keyFile, trusted, peerCert, peerKey, peerTrusted string) error {
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
		for _, p := range hi.SupportedProtos {
			if p == "http/1.1" {
				return h1, nil
			}
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
	n := &node{clusterID: 0x7769726531, batchStart: -1, peersOut: map[uint64]*peerOut{}}
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
		host, port, _ := net.SplitHostPort(addr)
		var pn int
		fmt.Sscan(port, &pn)
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
	n.st = newStore()
	n.ring = mmapBytes(ringSize)
	n.rx = mmapBytes(ringSize)
	n.wal, err = openWAL(filepath.Join(dataDir, "wal"))
	if err != nil {
		return err
	}
	if fd, err := unix.Open(filepath.Join(dataDir, "wal"), unix.O_RDONLY|unix.O_CLOEXEC, 0); err == nil {
		n.st.walFD = fd
	}
	n.storage = raft.NewMemoryStorage()
	n.rn, err = raft.NewRawNode(raftConfig(n.id, n.storage))
	if err != nil {
		return err
	}
	if err := n.rn.Bootstrap(peers); err != nil {
		return err
	}

	l := &loop{n: n, redials: map[uint64]chan struct{}{}}
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
	l.run()
	return nil
}

func (l *loop) accept(ln net.Listener, cfg *tls.Config, kind uint8, logger *log.Logger) {
	for {
		nc, err := ln.Accept()
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
				serveHTTP1(h)
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
	for {
		nc, err := net.DialTimeout("tcp", m.peerAddr, time.Second)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		h, err := handshake(nc, c, false)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		h.kind, h.peer = kindPeerOut, m.id
		l.handOver(h)
		<-redial
		time.Sleep(100 * time.Millisecond)
	}
}

func (l *loop) handOver(h *handoff) {
	l.mu.Lock()
	l.handoffs = append(l.handoffs, h)
	l.mu.Unlock()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = unix.Write(l.hoFD, b[:])
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
			c := newClient(h)
			n.socks[idx] = c
			s = &c.sock
		case kindPeerIn:
			p := &peerIn{sock: *newSock(h)}
			n.socks[idx] = p
			s = &p.sock
		case kindPeerOut:
			p := &peerOut{sock: *newSock(h), peer: h.peer}
			p.countSent = true
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
		}
		for _, st := range v.over {
			if st.watch {
				n.closeWatchStream(v, st)
			}
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

func (l *loop) run() {
	runtime.LockOSThread()
	n := l.n
	events := make([]unix.EpollEvent, 256)
	l.next = time.Now().Add(tickEvery)
	if n.id == 1 {
		_ = n.rn.Campaign()
	}
	for {
		timeout := int(time.Until(l.next) / time.Millisecond)
		if timeout < 0 {
			timeout = 0
		}
		k, err := unix.EpollWait(l.epfd, events, timeout)
		if err != nil && !errors.Is(err, unix.EINTR) {
			panic(err)
		}
		for i := 0; i < k; i++ {
			ev := events[i]
			switch ev.Fd {
			case evHandoff:
				l.takeHandoffs()
			case evFsync:
				n.fsyncDone()
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
		}
		n.proposeBatch()
		n.askReads(l.tick)
		n.ready()
		l.flush()
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
func serveHTTP1(h *handoff) {
	defer unix.Close(h.fd)
	_ = unix.SetNonblock(h.fd, false)
	s := newSock(h)
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
				fmt.Sscan(v, &secs)
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
		body = fmt.Appendf(nil, "# TYPE etcd_server_is_leader gauge\netcd_server_is_leader %d\n# TYPE etcd_network_peer_sent_bytes_total counter\netcd_network_peer_sent_bytes_total{To=\"all\"} %d\n",
			isLeader.Load(), peerSentBytes.Load())
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
