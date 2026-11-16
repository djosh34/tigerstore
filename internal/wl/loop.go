package wl

import (
	"cmp"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	etcdmetrics "github.com/djosh34/tigerstore/internal/metrics"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/sys/unix"
)

type peerOut struct {
	sock
	peer uint64
}

type peerIn struct {
	sock
	from uint64
	held [][]byte // messages that wait behind an append naming a batch not here yet
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
	// posts are what other goroutines hand the loop to run.
	posts   []func() error
	dials   map[uint64]*dialer // by member, while it is one
	peerTLS *tls.Config        // to dial peers
	http1   *http1             // serves the clients that chose HTTP/1.1
	self    *clientv3.Client   // the node's client of itself, for the gateway and side calls
	selfTLS *tls.Config        // its TLS configuration
	// metricsSrvs serve --listen-metrics-urls.
	metricsSrvs []*http.Server
	side        *sideServers

	free []int
	tick int64
	// leaveBy is when a leader that stops gives up handing its
	// leadership to a peer; zero while the node runs.
	leaveBy time.Time
	logged  int64
	last    counters
	lastWAL walStats
	lastAt  time.Time
	lastCPU cpuStats
	// tickStall is what stallStats read at the last tick, which a slow
	// turn logs the change from.
	tickStall stallStats
	logger    *log.Logger
	// turnPosts and turnHandoffs count the posts run and connections
	// taken this turn, for the slow-turn log.
	turnPosts, turnHandoffs int
	next                    time.Time
}

const (
	evHandoff = -1
	evFsync   = -2
	tickEvery = 100 * time.Millisecond
)

// errStopped is Run's error once its context is done.
var errStopped = errors.New("the node was stopped")

// Run runs a node until it fails, or until ctx is done, when it returns
// nil.
func Run(ctx context.Context, cfg Config) error {
	if err := run(ctx, cfg); !errors.Is(err, errStopped) {
		return err
	}
	return nil
}

// run runs a node until it fails or ctx is done.
func run(ctx context.Context, cfg Config) error {
	if err := cfg.check(); err != nil {
		return err
	}
	sys, err := newSysEnv(&cfg)
	if err != nil {
		return err
	}
	logger := cfg.Logger
	if err := sys.mkdirAll(cfg.DataDir); err != nil {
		return err
	}
	if err := checkNotMain(sys, cfg.DataDir); err != nil {
		return err
	}
	run, err := nextRun(sys, cfg.DataDir)
	if err != nil {
		return err
	}
	cliTLS := cfg.ClientTLS.Clone()
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
	n := &node{
		sys: sys, id: cfg.ID, clusterID: cfg.ClusterID, peersOut: map[uint64]*peerOut{}, metrics: &nodeMetrics{},
		ckptDir: filepath.Join(cfg.DataDir, "ckpt"), ckptLog: cmp.Or(cfg.CheckpointLog, ckptMin), catchup: cmp.Or(cfg.CatchupEntries, storageWindow), confState: &pb.ConfState{},
		self:   member{id: cfg.ID, name: cfg.Name, clientURLs: cfg.ClientURLs},
		format: baseFormat, auth: newAuthStore(), tokens: map[string]*simpleToken{}, logins: map[handle]authLogin{},
	}
	n.startRun(run)
	if n.sideSecret, err = newSideSecret(); err != nil {
		return err
	}
	// A new cluster's log starts with a ConfChange for each member, which
	// the apply makes the member list. Until it has the list, a node dials
	// the members it was told of: a node that joins, and one that starts
	// again before its cluster committed those ConfChanges, which would
	// otherwise answer its leader never (seed 10161 of TestWLFaultSeeds).
	var peers []raft.Peer
	for _, m := range cfg.Members {
		mem, err := m.member()
		if err != nil {
			return err
		}
		peers = append(peers, raft.Peer{ID: m.ID, Context: ccContext(0, 0, &mem)})
		n.seed = append(n.seed, mem)
	}
	if n.st, err = newStore(); err != nil {
		return err
	}
	n.fwd = make([]fwdBatch, fwdSlots)
	n.fwdPeers = map[uint64][]fwdBatch{}
	n.sideCalls = map[handle]context.CancelFunc{}
	n.sideReqs = map[handle][]byte{}
	n.carryUntil = map[uint64]time.Time{}
	n.sizes = sizeFor(cfg.MemoryBudget)
	if n.ring, err = newRing(n.sizes.ring); err != nil {
		return err
	}
	if n.rx, err = newRing(n.sizes.ring); err != nil {
		return err
	}
	// The side ring holds the failure branches of Txns.
	if n.side, err = newRing(n.sizes.side); err != nil {
		return err
	}
	// The store starts from the newest checkpoint, and Raft's storage
	// takes what the log holds after it; Raft then hands out the committed
	// entries again, and the apply builds the rest of the store from them.
	n.storage = newLogStorage(nil)
	n.storage.heldMax = n.sizes.held
	from, err := n.loadNewest()
	if err != nil {
		return err
	}
	if n.wal, err = openWAL(n.sys, filepath.Join(cfg.DataDir, "wal"), n.sizes.stage, n.ckptCut, from, n.storage.restore); err != nil {
		return err
	}
	n.storage.log = n.wal
	n.st.log = n.wal
	n.wal.durableHS = n.storage.hs
	n.st.setBudget(cfg.MemoryBudget, n.sizes.mapped())
	n.limitGo()
	// The hard state the checkpoint's pointer kept may be older than the
	// checkpoint: what it holds is committed.
	if hs := n.storage.hs; hs.GetCommit() < n.storage.snapIndex {
		hs.Commit = new(n.storage.snapIndex)
	}
	n.term, n.commit = n.storage.hs.GetTerm(), n.storage.hs.GetCommit()
	n.simRecovered()
	n.rn, err = raft.NewRawNode(raftConfig(n.id, n.storage, n.sizes, len(cfg.Members)))
	if err != nil {
		return err
	}
	if n.storage.lastIndex() == 0 && n.storage.snap == nil && !cfg.Join {
		if err := n.rn.Bootstrap(peers); err != nil {
			return err
		}
	} else {
		logger.Printf("recovered the log: entries to %d, hard state term %d commit %d", n.storage.lastIndex(), n.term, n.commit)
	}

	l := &loop{n: n, dials: map[uint64]*dialer{}, logger: logger, lastAt: time.Now(), peerTLS: cfg.PeerClientTLS}
	n.shell = l
	if l.epfd, err = n.sys.epollCreate(); err != nil {
		return err
	}
	if l.hoFD, err = n.sys.eventfd(true); err != nil {
		return err
	}
	if err := n.sys.epollCtl(l.epfd, unix.EPOLL_CTL_ADD, l.hoFD, unix.EPOLLIN, evHandoff); err != nil {
		return err
	}
	if err := n.sys.epollCtl(l.epfd, unix.EPOLL_CTL_ADD, n.wal.wakeFD, unix.EPOLLIN, evFsync); err != nil {
		return err
	}
	l.ctx = ctx
	l.selfTLS = selfTLS(cfg.ClientTLS)
	if l.self, err = selfClient(n.sys, cfg.ClientListeners[0].Addr().String(), l.selfTLS); err != nil {
		return err
	}
	l.side = newSideServers(l.self)
	set := cfg.Metrics
	if set == nil {
		if set, err = etcdmetrics.New(nil); err != nil {
			return err
		}
	}
	n.extensive = cfg.ExtensiveMetrics
	n.quota = cmp.Or(cfg.QuotaBackendBytes, defaultQuota)
	n.progressEvery = cmp.Or(cfg.ProgressNotify, defaultProgressNotify)
	n.jwt, n.tokenTTL, n.bcryptCost = cfg.JWT, cmp.Or(cfg.TokenTTL, defaultTokenTTL), cmp.Or(cfg.BcryptCost, bcrypt.DefaultCost)
	n.compactor = newCompactor(cfg.AutoCompactPeriod, cfg.AutoCompactRevisions, time.Now())
	n.wal.prom.Store(set)
	n.prom = set
	prom, err := newPromHandler(set, cfg.ExtensiveMetrics, l.scrape, &n.metrics.peerSent)
	if err != nil {
		return err
	}
	api := &httpAPI{m: n.metrics, prom: prom, set: set, cors: cfg.CORS, hosts: cfg.HostWhitelist, pprof: cfg.Pprof, raftStatus: l.raftStatus, read: l.healthRead}
	if l.http1, err = startHTTP1(ctx, l.self.ActiveConnection(), api); err != nil {
		return errors.Join(err, l.self.Close())
	}
	for _, ln := range cfg.MetricsListeners {
		srv := &http.Server{Handler: api.metricsHandler(), ReadHeaderTimeout: time.Minute}
		l.metricsSrvs = append(l.metricsSrvs, srv)
		go func() { _ = srv.Serve(ln) }()
	}
	defer l.shut(slices.Concat(cfg.ClientListeners, cfg.PeerListeners)...)
	go func() {
		<-ctx.Done()
		l.mu.Lock()
		defer l.mu.Unlock()
		l.wake()
	}()
	for _, ln := range cfg.ClientListeners {
		go l.accept(ln, cliTLS, kindClient, logger)
	}
	for _, ln := range cfg.PeerListeners {
		go l.accept(ln, cfg.PeerServerTLS, kindPeerIn, logger)
	}
	n.metrics.learner.Store(n.isLearner())
	l.syncDials()
	logger.Printf("node %s (id %d) up: clients %v, peers %v", cfg.Name, n.id, addrs(cfg.ClientListeners), addrs(cfg.PeerListeners))
	return l.run()
}

func addrs(lns []net.Listener) []string {
	var out []string
	for _, ln := range lns {
		out = append(out, ln.Addr().String())
	}
	return out
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
			h, err := handshake(l.n.sys, nc, cfg, kind)
			if err != nil {
				return
			}
			if h.h1 != nil {
				l.http1.serve(h.h1)
				return
			}
			if kind == kindPeerIn {
				l.peerHello(h)
				return
			}
			l.handOver(h)
		}()
	}
}

// dialer keeps a connection to one member: it dials addr again each
// time redial says the connection closed, until stop.
type dialer struct {
	addr         string
	redial, stop chan struct{}
}

// syncDials dials the members the list holds, or the seed while it is
// empty, and stops dialing the others: a member that moved its peer URL
// is dialed at the new one.
func (l *loop) syncDials() {
	n := l.n
	want := n.members
	if len(want) == 0 {
		want = n.seed
	}
	keep := map[uint64]bool{}
	for i := range want {
		m := &want[i]
		if m.id == n.id {
			continue
		}
		keep[m.id] = true
		addr := m.peerAddr()
		if d := l.dials[m.id]; d != nil && d.addr == addr {
			continue
		}
		l.stopDial(m.id)
		d := &dialer{addr: addr, redial: make(chan struct{}, 1), stop: make(chan struct{})}
		l.dials[m.id] = d
		go l.dial(m.id, d)
	}
	for id := range l.dials {
		if !keep[id] {
			l.stopDial(id)
		}
	}
}

// stopDial stops dialing member id and closes the connection to it.
func (l *loop) stopDial(id uint64) {
	d := l.dials[id]
	if d == nil {
		return
	}
	close(d.stop)
	delete(l.dials, id)
	if p := l.n.peersOut[id]; p != nil && !p.dead {
		l.closeSock(p.idx)
	}
}

// dialRedial is the channel that tells member id's dialer to dial again,
// nil if the node no longer dials it.
func (l *loop) dialRedial(id uint64) chan struct{} {
	if d := l.dials[id]; d != nil {
		return d.redial
	}
	return nil
}

// membersChanged dials the members the list holds, and closes the
// connections of those it no longer holds: one that dials again is told
// it was removed.
func (l *loop) membersChanged() {
	l.n.metrics.learner.Store(l.n.isLearner())
	l.syncDials()
	for _, x := range l.n.socks {
		if p, ok := x.(*peerIn); ok && !p.dead && !l.n.mayPeer(p.from) {
			l.closeSock(p.idx)
		}
	}
}

// mayPeer reports whether a peer may connect as id: a member, or anyone
// while the node does not know the list yet.
func (n *node) mayPeer(id uint64) bool {
	return len(n.members) == 0 || n.memberAt(id) >= 0
}

// peerHelloBytes is what a node says first on a connection to a peer:
// its ID, with snapMark for a snapshot's, and its cluster's.
func peerHelloBytes(id, cluster uint64) []byte {
	b := binary.LittleEndian.AppendUint64(nil, id)
	return binary.LittleEndian.AppendUint64(b, cluster)
}

func (l *loop) dial(id uint64, d *dialer) {
	c := l.peerTLS.Clone()
	host, _, _ := net.SplitHostPort(d.addr)
	c.ServerName = host
	pause := func() bool {
		select {
		case <-l.ctx.Done():
			return false
		case <-d.stop:
			return false
		case <-time.After(100 * time.Millisecond):
			return true
		}
	}
	for {
		nc, err := l.n.sys.dial(l.ctx, d.addr, time.Second)
		if err != nil {
			if !pause() {
				return
			}
			continue
		}
		h, err := handshake(l.n.sys, nc, c, kindPeerOut)
		if err != nil {
			if !pause() {
				return
			}
			continue
		}
		h.peer = id
		l.handOver(h)
		select {
		case <-d.redial:
		case <-d.stop:
			return
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
		_ = sysClose(h.fd)
		return
	}
	l.handoffs = append(l.handoffs, h)
	l.wake()
	l.mu.Unlock()
}

// post hands the loop fn to run in its next turn.
func (l *loop) post(fn func() error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	l.posts = append(l.posts, fn)
	l.wake()
}

func (l *loop) logf(format string, args ...any) { l.logger.Printf(format, args...) }

// offLoop runs work in a goroutine of its own, and what it returns on the
// loop.
func (l *loop) offLoop(work func() func()) {
	go func() {
		done := work()
		l.post(func() error {
			done()
			return nil
		})
	}()
}

// healthRead is the read of etcd's health checks, made on the loop as
// etcd makes it in its server: a serializable one reads the store as it
// is, a linearizable one waits for a ReadIndex as a range does. It fails
// if the loop does not answer within etcd's request timeout.
func (l *loop) healthRead(serializable bool) error {
	got := make(chan struct{})
	l.post(func() error {
		if serializable {
			close(got)
			return nil
		}
		l.n.reads = append(l.n.reads, pendingRead{h: noHandle, done: func() { close(got) }})
		return nil
	})
	select {
	case <-got:
		return nil
	case <-time.After(healthTimeout):
		return errHealthTimeout
	}
}

var errHealthTimeout = errors.New("etcdserver: request timed out")

// raftStatus is the node's Raft status in JSON, read on the loop, or nil
// if the loop does not answer in time.
func (l *loop) raftStatus() []byte {
	got := make(chan []byte, 1)
	l.post(func() error {
		b, err := l.n.rn.Status().MarshalJSON()
		if err != nil {
			b = nil
		}
		got <- b
		return nil
	})
	select {
	case b := <-got:
		return b
	case <-time.After(healthTimeout):
		return nil
	}
}

// wakeLoop wakes the loop from another goroutine.
func (l *loop) wakeLoop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.wake()
}

// leave starts handing the leadership of a node that stops to the voter
// furthest along that it has a connection to, as etcd's leader does when it stops, so a peer
// leads once the process is gone. It reports whether the loop waits for
// that.
func (l *loop) leave() bool {
	n := l.n
	if !n.leading() {
		return false
	}
	to, match := uint64(0), uint64(0)
	for id, pr := range n.rn.Status().Progress {
		if p := n.peersOut[id]; p == nil || p.dead || id == n.id || pr.IsLearner {
			continue
		}
		if to == 0 || pr.Match > match {
			to, match = id, pr.Match
		}
	}
	if to == 0 {
		return false
	}
	l.logger.Printf("stopping: handing the leadership to %x", to)
	n.rn.TransferLeader(to)
	l.leaveBy = n.now.Add(moveTimeout)
	return true
}

// runPosts runs what other goroutines posted.
func (l *loop) runPosts() error {
	l.mu.Lock()
	ps := l.posts
	l.posts = nil
	l.mu.Unlock()
	l.turnPosts += len(ps)
	for _, fn := range ps {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// wake wakes the loop through its handoff eventfd, unless it stopped and
// closed it. The caller holds l.mu.
func (l *loop) wake() {
	if l.stopped {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(l.hoFD, b[:])
}

// shut closes what a stopped loop had open: its listeners, sockets, log
// and file descriptors.
func (l *loop) shut(listeners ...net.Listener) {
	for _, ln := range listeners {
		_ = ln.Close()
	}
	for _, srv := range l.metricsSrvs {
		_ = srv.Close()
	}
	if err := errors.Join(l.http1.stop(), l.self.Close()); err != nil {
		l.logger.Printf("stop the HTTP/1.1 server and the node's own client: %v", err)
	}
	l.mu.Lock()
	l.stopped = true
	hs := l.handoffs
	l.handoffs = nil
	l.mu.Unlock()
	for _, h := range hs {
		_ = sysClose(h.fd)
	}
	for idx, x := range l.n.socks {
		if x != nil {
			l.closeSock(idx)
		}
	}
	l.n.abortCkpt()
	l.n.wal.close()
	_ = sysClose(l.epfd)
	_ = sysClose(l.hoFD)
}

func (l *loop) takeHandoffs() {
	var b [8]byte
	_, _ = sysRead(l.hoFD, b[:])
	l.mu.Lock()
	hs := l.handoffs
	l.handoffs = nil
	l.mu.Unlock()
	l.turnHandoffs += len(hs)
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
			if !n.mayPeer(h.peer) {
				if n.removed[h.peer] {
					// As etcd does, it tells a removed member so.
					h.pre.put(binary.LittleEndian.AppendUint64(nil, removedNotice))
					_, _ = h.pre.flush()
				}
				_ = sysClose(h.fd)
				continue
			}
			p := &peerIn{sock: *h.pre, from: h.peer}
			p.stats = &n.stats
			n.socks[idx] = p
			s = &p.sock
		case kindPeerOut:
			if l.dials[h.peer] == nil {
				_ = sysClose(h.fd)
				continue
			}
			p := &peerOut{sock: *newSock(h, &n.stats), peer: h.peer}
			p.sent = &n.metrics.peerSent
			n.socks[idx] = p
			s = &p.sock
			if old := n.peersOut[h.peer]; old != nil && !old.dead {
				l.closeSock(old.idx)
			}
			n.peersOut[h.peer] = p
			s.put(peerHelloBytes(n.id, n.clusterID))
			n.markDirty(s)
		}
		s.idx, s.gen = idx, n.gens[idx]
		if err := n.sys.epollCtl(l.epfd, unix.EPOLL_CTL_ADD, h.fd, unix.EPOLLIN|unix.EPOLLRDHUP, int32(idx)); err != nil {
			l.closeSock(idx)
			continue
		}
		// Bytes read with the handshake or the hello wait already.
		if s.iw > s.ir || s.pw > s.pr {
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
	_ = sysClose(s.fd)
	switch v := x.(type) {
	case *client:
		n.cancelSides(idx)
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
			case l.dialRedial(v.peer) <- struct{}{}:
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
			// Nothing comes back on an outgoing peer connection but the
			// notice that this node was removed.
			if s.pw-s.pr >= 8 && binary.LittleEndian.Uint64(s.pt[s.pr:]) == removedNotice {
				n.stopErr = errRemoved
			}
			s.pr = s.pw
		}
	}
	if err != nil {
		l.closeSock(idx)
	}
}

// run turns the loop until the node fails.
func (l *loop) run() error {
	n := l.n
	n.sys.lockOSThread()
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
		k, err := n.sys.epollWait(l.epfd, events, timeout)
		if err != nil && !errors.Is(err, unix.EINTR) {
			return err
		}
		n.now = time.Now()
		if n.stopErr != nil {
			return n.stopErr
		}
		if !l.leaveBy.IsZero() && (n.lead != 0 && n.lead != n.id || n.now.After(l.leaveBy)) {
			return errStopped
		}
		for i := range k {
			ev := events[i]
			switch ev.Fd {
			case evHandoff:
				if l.ctx.Err() != nil && l.leaveBy.IsZero() && !l.leave() {
					return errStopped
				}
				l.takeHandoffs()
				if err := l.runPosts(); err != nil {
					return err
				}
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
		tickStart := time.Now()
		if now := tickStart; !now.Before(l.next) {
			l.next = now.Add(tickEvery)
			l.tick++
			n.rn.Tick()
			n.retryReads(l.tick)
			n.expireProposals()
			n.expireMemberRequests()
			n.checkMoves()
			n.autoCompact()
			n.publish()
			n.expireTokens()
			n.notifyProgress()
			n.retryProgress()
			if err := n.maybeCkpt(); err != nil {
				return err
			}
			n.limitGo()
			l.tickStall = n.stallStats(now)
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
		tickEnd := time.Now()
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
		if err := n.ckptStep(false); err != nil {
			return err
		}
		if len(n.clientSnaps) > 0 {
			n.sendClientSnaps()
		}
		t3 := time.Now()
		l.flush()
		t4 := time.Now()
		if !n.rn.HasReady() {
			// Raft holds no message now that could point into a ring.
			n.sweepRings(n.now)
		}
		if t5 := time.Now(); t5.Sub(n.now) > slowTurn {
			l.logSlowTurn(n.now, t1, t2, t3, t4, t5, ops, tickEnd.Sub(tickStart))
		}
		clear(n.turnReqs[:])
		l.turnPosts, l.turnHandoffs = 0, 0
		n.stats.turns++
		n.simTurn()
		if l.tick%50 == 0 && l.tick != l.logged {
			l.logged = l.tick
			l.logStats()
		}
	}
}

// limitGo takes Go's live heap into the store's budget, and sets Go's
// memory limit to what the budget leaves Go, when that moved by more than
// a 64th of the budget since it was last set.
func (n *node) limitGo() {
	st := n.st
	if st.budget == 0 {
		return
	}
	st.goLive = n.sys.goLiveHeap()
	limit := st.goLimit()
	if d := limit - n.goLimit; d > -st.budget/64 && d < st.budget/64 {
		return
	}
	n.goLimit = limit
	n.sys.setMemoryLimit(limit)
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
			_ = n.sys.epollCtl(l.epfd, unix.EPOLL_CTL_MOD, s.fd, unix.EPOLLIN|unix.EPOLLRDHUP|unix.EPOLLOUT, int32(s.idx))
		} else if ok && s.wantOut {
			s.wantOut = false
			_ = n.sys.epollCtl(l.epfd, unix.EPOLL_CTL_MOD, s.fd, unix.EPOLLIN|unix.EPOLLRDHUP, int32(s.idx))
		}
	}
	n.dirty = n.dirty[:0]
}

// counters are the loop's own, for its log: per 5 s, how many turns,
// socket reads and writes, WAL writes and fsyncs, replies and Raft work.
type counters struct {
	turns, reads, writes, replies, proposals, readIndex, peerMsgs uint64
	ckpts, snapsSent, snapsInstalled                              uint64

	// What the batches carried, and how the peers named or carried them.
	opsByKind, bytesByKind [kinds]int
	named, held            int
	carried, carriedBytes  [32]int
}

// slowTurn is a turn long enough to log, so that a stall shows where
// its time went.
const slowTurn = 200 * time.Millisecond

// logSlowTurn logs a slow turn's phases and the ops it applied, by kind;
// ops are the counts before the turn's Ready.
func (l *loop) logSlowTurn(start, input, ready, sync, flush, end time.Time, ops [kinds]int, tick time.Duration) {
	var applied [kinds]int
	for k := range applied {
		applied[k] = l.n.stats.opsByKind[k] - ops[k]
	}
	l.logger.Printf("slow turn %v: input and proposals %v (of it the tick %v), ready and apply %v, watch catch-up %v, flush %v, sweep %v; requests by kind %v, posts %d, connections taken %d; applied by kind %v; keys %d, watchers %d, catching up %d, held flows %d; %s",
		end.Sub(start), input.Sub(start), tick, ready.Sub(input), sync.Sub(ready), flush.Sub(sync), end.Sub(flush),
		l.n.turnReqs, l.turnPosts, l.turnHandoffs, applied,
		l.n.st.nkeys, len(l.n.w.ws), len(l.n.w.unsynced), l.heldFlows(), l.n.stallStats(end).since(l.tickStall))
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
	m := l.n.memStats()
	l.logger.Printf("memory MB: rss %d, go %d (live heap %d, goal %d); tables: keys %d hash %d history %d key arena %d slab %d (used %d) tree %d changes %d; sockets out %d in %d, held %d",
		m.rss>>20, m.goTotal>>20, m.goLive>>20, m.goGoal>>20, m.keys>>20, m.hash>>20, m.hist>>20, m.arena>>20, m.slabTop>>20, m.slabUsed>>20, m.tree>>20, m.ch>>20,
		m.sockOut>>20, m.sockIn>>20, m.held>>20)
	n := l.n
	l.logger.Printf("rings: spans %d/%d/%d, on the heap %d/%d/%d; storage let go %d, read back %d",
		n.ring.stats.spans, n.rx.stats.spans, n.side.stats.spans, n.ring.stats.heap, n.rx.stats.heap, n.side.stats.heap,
		n.storage.stats.letGo, n.storage.stats.reads)
	l.logger.Printf("rings: sweeps %d; oldest held: %s / %s / %s", n.ring.stats.sweeps, n.ring.held(), n.rx.held(), n.side.held())
	l.last, l.lastAt = d, now
	l.logger.Printf("ops by kind %v bytes %v named %d carried %v bytes %v held %d", d.opsByKind, d.bytesByKind, d.named, d.carried, d.carriedBytes, d.held)
	c := l.n.cpuStats()
	pc := l.lastCPU
	l.lastCPU = c
	ms := func(ns int64) int64 { return ns / 1e6 }
	l.logger.Printf("cpu ms: process %d, loop %d, others %d; log writes %d ms for %d syncs; kernel dirty %d MB, writeback %d MB",
		ms(c.process-pc.process), ms(c.loop-pc.loop), ms(c.process-pc.process-(c.loop-pc.loop)),
		ms(c.walWrite-pc.walWrite), ws.syncs-pw.syncs, c.dirty>>20, c.writeback>>20)
	if r := n.ckpt; r != nil {
		l.logger.Printf("checkpoint at index %d running %v: phase %d, key ids %d of %d, %d MB encoded; values %d MB taken in, %d MB left in the log; loop encoding %d ms; writer read %d MB in %d ms, wrote %d MB in %d ms",
			r.meta.index, time.Since(r.started).Round(time.Second), r.phase, r.next, r.end, r.at>>20, r.taken>>20, r.leftInLog>>20,
			ms(c.ckptEncode-pc.ckptEncode), r.w.readBytes.Load()>>20, ms(c.ckptRead-pc.ckptRead), r.w.writeBytes.Load()>>20, ms(c.ckptWrite-pc.ckptWrite))
	}
}
