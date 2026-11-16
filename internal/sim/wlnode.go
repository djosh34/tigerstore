package sim

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simos"
	"github.com/djosh34/tigerstore/internal/wl"
)

// Config.wl runs the nodes of internal/wl instead of main's node driver:
// each process is wl.Run on simos (internal/simos), whose files are the
// node's simfs disk behind the run's disk calls (disk.go), and whose
// sockets are the run's connections (net.go). The node's hooks (wl.Hooks)
// tell the run what it recovered, applied and made durable, and hold its
// loop while it is paused. Every client reaches it through gRPC on its
// client port. wl's seam needs the build tag sim (gosim) or simio (native
// runs under the race detector).

// wlOS is the descriptor table of every wl node of the process: wl sends
// the calls that name only a descriptor to the table of the first node
// started (wl.Sys), so every run shares one.
var wlOS = simos.New()

// wlCluster is the cluster ID of the runs' wl nodes.
const wlCluster = 0x7469676572

// wlProc is what the run keeps of a wl process.
type wlProc struct {
	// conn is the clients' gRPC connection to the node.
	conn *grpc.ClientConn
	// turn is what the node showed at the end of its last turn.
	turn wl.TurnState
}

// wlMembers are the first three members of a wl cluster.
func wlMembers() []wl.Member {
	var ms []wl.Member
	for i := range 3 {
		ms = append(ms, wl.Member{
			ID: uint64(i + 1), Name: fmt.Sprintf("node%d", i+1),
			PeerURLs: []string{peerURL(i)}, ClientURLs: []string{"https://" + clientAddr(i)},
		})
	}
	return ms
}

// startWL starts a wl process p on n, from its disk. The node recovers in
// its own goroutine, and reports what it recovered through its hooks
// (wlHooks.Recovered); a start that fails ends as the process does
// (exited). The lock is held.
func (s *Sim) startWL(n *node, p *proc) error {
	if n.idx >= 3 {
		return fmt.Errorf("node %d: a wl run has three members", n.id)
	}
	ms := wlMembers()
	srvTLS := &tls.Config{Certificates: []tls.Certificate{s.net.cert}, MinVersion: tls.VersionTLS12}
	peerTLS := &tls.Config{
		Certificates: []tls.Certificate{s.net.cert}, RootCAs: s.net.ca, ClientCAs: s.net.ca,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	cfg := wl.Config{
		Name: ms[n.idx].Name, ID: n.id, ClusterID: wlCluster, DataDir: wlDataDir, Members: ms,
		ClientURLs:      ms[n.idx].ClientURLs,
		ClientListeners: []net.Listener{s.net.listen(s, p, []string{clientAddr(n.idx)})},
		PeerListeners:   []net.Listener{s.net.listen(s, p, n.addrs)},
		ClientTLS:       srvTLS, PeerServerTLS: peerTLS, PeerClientTLS: peerTLS,
		MemoryBudget: s.cfg.MemoryBytes,
		Logger:       log.New(&nodeLog{s: s, n: n}, "", 0),
		Sys:          s.os.NewNode(wlDataDir, &disk{s: s, p: p}, wlNet{s: s, p: p}),
		Hooks:        &wlHooks{s: s, p: p},
	}
	conn, err := grpc.NewClient(
		"passthrough:///"+clientAddr(n.idx),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: s.net.ca, MinVersion: tls.VersionTLS12})),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) { return s.net.dial(ctx, s, nil, addr) }),
	)
	if err != nil {
		return err
	}
	p.wl = &wlProc{conn: conn}
	n.proc, n.since, n.why = p, s.now(), ""
	s.procs.Go(func() {
		err := wl.Run(p.ctx, cfg)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		s.exited(p, errors.Join(err, conn.Close()))
	})
	return nil
}

// wlDataDir is a wl node's data dir; its disk holds what is under it.
const wlDataDir = "/data"

// wlNet dials the run's network for process p.
type wlNet struct {
	s *Sim
	p *proc
}

func (w wlNet) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return w.s.net.dial(ctx, w.s, w.p, addr)
}

var _ simos.Net = wlNet{}

// nodeLog takes a node's log lines into the trace.
type nodeLog struct {
	s *Sim
	n *node
}

func (l *nodeLog) Write(b []byte) (int, error) {
	l.s.logf(l.n, "%s", strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}

// wlHooks are the run's hooks of wl process p (wl.Hooks): the checks
// main's committer hooks make (turn, persisted), on what wl shows.
type wlHooks struct {
	s *Sim
	p *proc
}

// Recovered checks that the start recovered at least what the node
// reported durable before it went down, and a term and vote no older.
func (h *wlHooks) Recovered(last uint64, hs *raftpb.HardState) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	switch {
	case last < n.mustLast:
		s.fail("node %d recovered entries to %d, but reported %d durable", n.id, last, n.mustLast)
	case n.mustAtMost > 0 && last > n.mustAtMost:
		s.fail("node %d recovered entries to %d, past %d, the last it handed its log before its crash", n.id, last, n.mustAtMost)
	case hs.GetTerm() < n.mustHS.term || hs.GetTerm() == n.mustHS.term && n.mustHS.vote != 0 && hs.GetVote() != n.mustHS.vote:
		s.fail("node %d started at term %d with vote %d, but held term %d and vote %d durable", n.id, hs.GetTerm(), hs.GetVote(), n.mustHS.term, n.mustHS.vote)
	}
	if n.mustAtMost > 0 {
		s.stats.StageRecoveries++
		s.tracef("node %d: recovered entries to %d after its torn write, reported %d durable, handed its log %d", n.id, last, n.mustLast, n.mustAtMost)
	}
	n.durableLast, n.hs, n.tornCrash, n.mustAtMost, n.appended = last, hardState{term: hs.GetTerm(), vote: hs.GetVote()}, false, 0, last
}

// Append keeps an append on its way to the disk, which may lower what a
// crash leaves (node.floor).
func (h *wlHooks) Append(first, last uint64, install bool) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	n.appended = max(n.appended, last)
	n.pending = append(n.pending, pendingJob{first: first, install: install, replaces: first <= n.durableLast, at: s.now()})
}

// Applied checks an entry the node applied, and the changes it made,
// against what the other nodes applied at its index.
func (h *wlHooks) Applied(e *raftpb.Entry, hash uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	s.record(n, core.Applied{Index: e.GetIndex(), Term: e.GetTerm(), Type: e.GetType(), Data: e.GetData(), Hash: hash})
	s.applied = max(s.applied, e.GetIndex())
}

// Durable notes how far the log the node holds durably reaches.
func (h *wlHooks) Durable(last uint64, install bool, hs *raftpb.HardState) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	if len(n.pending) > 0 {
		n.pending = n.pending[1:]
	}
	if last > 0 {
		n.durableLast = last
	}
	if hs != nil {
		n.hs = hardState{term: hs.GetTerm(), vote: hs.GetVote()}
	}
	_ = install
}

// Turn keeps what the node shows, and holds its loop while the process
// is paused, frozen, or the run failed.
func (h *wlHooks) Turn(st wl.TurnState) {
	s, p := h.s, h.p
	s.mu.Lock()
	n := p.n
	if n.proc != p {
		s.mu.Unlock()
		return
	}
	p.wl.turn = st
	role := "follower"
	if st.Lead == n.id {
		role = "leader"
	}
	n.last = coreState{at: s.now(), role: role, term: st.Term, lead: st.Lead, commit: st.Commit, applied: st.Applied, first: st.First, version: uint32(st.Format)}
	s.mu.Unlock()
	s.holdTurn(p)
}

// holdTurn holds p's loop, at the end of a turn, while the run failed,
// until the run ends its process, and while the process is paused or
// frozen, telling the checks that it holds still.
func (s *Sim) holdTurn(p *proc) {
	n := p.n
	for {
		s.mu.Lock()
		failed := s.err != nil
		if (failed || n.frozen) && p.parked != nil {
			close(p.parked)
			p.parked = nil
		}
		until, wake := n.paused(), n.wake
		if failed {
			until = math.MaxInt64
		}
		s.mu.Unlock()
		d := until - s.now()
		if d <= 0 || p.dead() {
			return
		}
		t := time.NewTimer(time.Duration(min(d, math.MaxInt64/2)))
		select {
		case <-t.C:
		case <-wake:
		case <-p.ctx.Done():
		}
		t.Stop()
	}
}

// callWL sends req to n's process through gRPC, as a client on the other
// end of a connection does, and returns its answer.
func (s *Sim) callWL(p *proc, req proto.Message, ctx context.Context) (proto.Message, error) {
	conn := p.wl.conn
	switch r := req.(type) {
	case *pb.PutRequest:
		return pb.NewKVClient(conn).Put(ctx, r)
	case *pb.RangeRequest:
		return pb.NewKVClient(conn).Range(ctx, r)
	case *pb.DeleteRangeRequest:
		return pb.NewKVClient(conn).DeleteRange(ctx, r)
	case *pb.TxnRequest:
		return pb.NewKVClient(conn).Txn(ctx, r)
	case *pb.CompactionRequest:
		return pb.NewKVClient(conn).Compact(ctx, r)
	case *pb.LeaseGrantRequest:
		return pb.NewLeaseClient(conn).LeaseGrant(ctx, r)
	case *pb.LeaseRevokeRequest:
		return pb.NewLeaseClient(conn).LeaseRevoke(ctx, r)
	case *pb.LeaseKeepAliveRequest:
		ka, err := pb.NewLeaseClient(conn).LeaseKeepAlive(ctx)
		if err != nil {
			return nil, err
		}
		if err := ka.Send(r); err != nil {
			return nil, err
		}
		resp, err := ka.Recv()
		_ = ka.CloseSend()
		return resp, err
	}
	return nil, fmt.Errorf("%w: %T", errNoV3Call, req)
}

// wlStageCrash crashes wl node n, once a run, under the clients' load,
// once it handed its log entries past those it reported durable, right
// after its log wrote and before the write's sync: the
// helper writes the stage's last block whole, from the block's start,
// while the loop appends past the durable end in it, so the crash image,
// torn at sectors, keeps that block half new and half old. Its next start
// must recover every entry it reported durable, and none past the last it
// handed its log (wlHooks.Recovered), and each entry it applies is checked
// against the other nodes' (wlHooks.Applied). It reports whether it
// crashed n. The lock is held.
func (s *Sim) wlStageCrash(n *node) bool {
	if s.stats.StageCrashes > 0 || s.healed || s.now() < 2*Second || n.proc == nil || n.appended <= n.floor() {
		return false
	}
	s.stats.StageCrashes++
	s.stats.Crashes++
	s.stats.PowerLosses++
	n.why = "the run crashed it between a write of its log and the write's sync, torn at sectors"
	s.tracef("node %d: crash between a write of its log and its sync, torn at sectors; reported %d durable, handed its log %d", n.id, n.floor(), n.appended)
	n.mustLast, n.mustHS, n.mustAtMost = n.floor(), n.hs, n.appended
	s.down(n)
	from := len(n.diskLog)
	s.loseUnsyncedAs(n, simfs.Sectors)
	for _, l := range n.diskLines()[min(from, len(n.diskLog)):] {
		// What the image kept of the segment it tore.
		if strings.Contains(l, ".seg") && strings.Contains(l, "unsynced") {
			s.tracef("node %d: %s", n.id, l)
		}
	}
	n.crashImage = n.disk.Files()
	s.restartLater(n, 1+s.rng.Int64N(Second))
	return true
}
