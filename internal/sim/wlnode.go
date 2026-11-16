package sim

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"slices"
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
	// recovered is set once the node recovered its log (wlHooks.Recovered);
	// armed, while its start may crash at a disk call (armRecoveryCrash).
	recovered, armed bool
	// silent is whether the node's last turn showed it silent.
	silent bool
	// applied is the last entry the node applied, from its Applied
	// hooks, or the snapshot it installed; 0 until its start applies one.
	applied uint64
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
	ms := wlMembers()
	join := n.idx == 3
	if join {
		// The fourth member, which the membership work added (Config.Membership),
		// joins the running cluster at the peer URL it has now.
		url := memberURL
		if s.membership.stage >= 2 {
			url = updatedMemberURL
		}
		ms = append(ms, wl.Member{ID: n.id, Name: "node4", PeerURLs: []string{url}, ClientURLs: []string{"https://" + clientAddr(3)}})
	}
	self := slices.IndexFunc(ms, func(m wl.Member) bool { return m.ID == n.id })
	srvTLS := &tls.Config{Certificates: []tls.Certificate{s.net.cert}, MinVersion: tls.VersionTLS12}
	peerTLS := &tls.Config{
		Certificates: []tls.Certificate{s.net.cert}, RootCAs: s.net.ca, ClientCAs: s.net.ca,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	cfg := wl.Config{
		Name: ms[self].Name, ID: n.id, ClusterID: wlCluster, DataDir: wlDataDir, Members: ms, Join: join,
		ClientURLs:      ms[self].ClientURLs,
		ClientListeners: []net.Listener{s.net.listen(s, p, []string{clientAddr(n.idx)})},
		PeerListeners:   []net.Listener{s.net.listen(s, p, n.addrs)},
		ClientTLS:       srvTLS, PeerServerTLS: peerTLS, PeerClientTLS: peerTLS,
		MemoryBudget: s.cfg.MemoryBytes,
		// A checkpoint, which cuts the log behind it, every DiskBudget of
		// log, as main's node cleans within it.
		CheckpointLog: s.cfg.Faults.DiskBudget, CatchupEntries: wlCatchup,
		Logger: log.New(&nodeLog{s: s, n: n}, "", 0),
		Sys:    s.os.NewNode(wlDataDir, &disk{s: s, p: p}, wlNet{s: s, p: p}),
		Hooks:  &wlHooks{s: s, p: p},
	}
	conn, err := grpc.NewClient(
		"passthrough:///"+clientAddr(n.idx),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: s.net.ca, MinVersion: tls.VersionTLS12})),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) { return s.net.dial(ctx, s, nil, addr) }),
		// Seeds 12024, 12036, 12505 and 13996: refused dials while a
		// client link was down left the connection in gRPC's backoff 4 s
		// past healing.
		grpc.WithConnectParams(simConnect),
		// Seeds 12731, 12780, 12985, 13172, 13318 and 13699 spun for good
		// in gRPC's transparent retries under gosim (#440).
		grpc.WithStatsHandler(retryPause{}),
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

// wlCatchup is the applied entries a wl node's Raft storage keeps for
// followers that lag: few enough that the log behind a checkpoint is cut
// on the run's small disks, and that a follower down for a while catches
// up from a checkpoint.
const wlCatchup = 64

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
// reported durable before it went down, and a term and vote no older, and
// that it did not start on a disk the run damaged.
func (h *wlHooks) Recovered(last uint64, hs *raftpb.HardState, values func() []wl.LogValue) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	p.wl.recovered = true
	if p.wl.armed {
		p.wl.armed = false
		n.disk.Fault = nil
	}
	if n.corrupted && n.rotted {
		if ptr, err := wlPointer(n.disk); err == nil && n.rotOff < ptr.LogFrom && !namesByte(values(), n.rotOff) {
			// The start reads the log from the checkpoint's place on,
			// past the rotted byte, and checks the values its store
			// names before it, which the rotted one no longer is:
			// nothing reads it.
			n.corrupted = false
			s.tracef("node %d: started; its store no longer names the rotted value", n.id)
		}
	}
	switch {
	case n.corrupted:
		// The run damaged the disk where the start reads it.
		s.fail("node %d started on a damaged disk without noticing", n.id)
	case n.misread:
		s.fail("node %d recovered a log that it read wrong without noticing", n.id)
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
	if install {
		p.wl.applied = first - 1
	}
	n.pending = append(n.pending, pendingJob{first: first, install: install, replaces: first <= n.durableLast, at: s.now()})
	if n == s.stalledLeader && s.heldAppendAt == 0 && first != math.MaxUint64 && !install {
		s.heldAppendAt = s.now()
	}
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
	p.wl.applied = e.GetIndex()
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

// Expired notes when a node first expired lease id, for the check that
// no lease expires before its TTL ran out (leasesExpire), as main's
// committer's Expired effects do.
func (h *wlHooks) Expired(id int64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.n.proc != p {
		return
	}
	if _, seen := s.expiredAt[id]; !seen {
		s.expiredAt[id] = s.now()
		s.expiredOrder = append(s.expiredOrder, id)
	}
}

// Checkpoint checks that a checkpoint names the index its store is at:
// the last entry the node applied, as its Applied hooks and installs
// showed the run. On main the run compares the checkpoints nodes write
// at an index; wl's nodes take theirs at indexes of their own, so the run
// compares each with what the node applied (B21).
func (h *wlHooks) Checkpoint(index uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.n.proc != p || p.wl.applied == 0 {
		return
	}
	if index != p.wl.applied {
		s.fail("node %d started a checkpoint at index %d, but applied to %d", p.n.id, index, p.wl.applied)
	}
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
	if s.rotDue == n {
		s.rotDue = nil
		s.rotWL(n, st.LogValues)
	}
	role := "follower"
	if st.Lead == n.id {
		role = "leader"
	}
	n.last = coreState{at: s.now(), role: role, term: st.Term, lead: st.Lead, commit: st.Commit, applied: st.Applied, first: st.First, version: uint32(st.Format)}
	if st.Silent && !p.wl.silent {
		s.stats.Silences++
		s.silences = append(s.silences, s.now())
		s.tracef("node %d: silent on a stalled disk", n.id)
	}
	p.wl.silent = st.Silent
	if n == s.stalledLeader && s.steppedDownAt == 0 && st.Lead != n.id {
		s.steppedDownAt = s.now()
		s.tracef("node %d: the stalled leader stepped down", n.id)
	}
	if st.First > st.Applied+1 {
		// Raft would need the entries it let go to apply them.
		s.fail("node %d holds entries from %d in memory, but applied only to %d", n.id, st.First, st.Applied)
	}
	s.mu.Unlock()
	s.holdTurn(p)
}

// SnapshotChunk applies the snapshot faults to a wl node's snapshot
// stream, as server does to main's: half the streams during the faults
// start late (SlowFetch), and the first stream of a run, and one in four
// after it, is damaged on its way (FlipSnapshot). Peers talk TLS, so
// only this hook reaches the stream's own CRC. A stream larger than a
// chunk is damaged in its first.
func (h *wlHooks) SnapshotChunk(to uint64, first bool, p []byte) {
	if !first {
		return
	}
	s, n := h.s, h.p.n
	s.mu.Lock()
	var wait int64
	if s.cfg.Faults.SlowFetch && !s.healed && s.rng.IntN(2) == 0 {
		s.stats.SlowFetches++
		wait = 200*Millisecond + s.rng.Int64N(1800*Millisecond)
	}
	s.stats.Snapshots++
	if s.cfg.Faults.FlipSnapshot && !s.healed && len(p) > 0 && (s.stats.SnapshotFlips == 0 || s.rng.IntN(4) == 0) {
		p[s.rng.IntN(len(p))] ^= byte(1 + s.rng.IntN(255))
		s.stats.SnapshotFlips++
		s.tracef("node %d: damaged a snapshot stream to %d", n.id, to)
	}
	s.mu.Unlock()
	if wait > 0 {
		t := time.NewTimer(time.Duration(wait))
		select {
		case <-t.C:
		case <-h.p.ctx.Done():
			t.Stop()
		}
	}
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
	case *pb.MemberAddRequest:
		return pb.NewClusterClient(conn).MemberAdd(ctx, r)
	case *pb.MemberUpdateRequest:
		return pb.NewClusterClient(conn).MemberUpdate(ctx, r)
	case *pb.MemberPromoteRequest:
		return pb.NewClusterClient(conn).MemberPromote(ctx, r)
	case *pb.MemberRemoveRequest:
		return pb.NewClusterClient(conn).MemberRemove(ctx, r)
	case *pb.MemberListRequest:
		return pb.NewClusterClient(conn).MemberList(ctx, r)
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

// wlRefused handles err, which stopped the wl process of n before it
// recovered its log, as start does an error of a start of main's node: a
// start that refused a misread log starts again, and one that refused a
// damaged disk is fenced and stays down. It reports whether it handled
// err; exited handles the rest. The lock is held.
func (s *Sim) wlRefused(n *node, err error) bool {
	if p := n.proc; p.wl.armed {
		p.wl.armed = false
		n.disk.Fault = nil
	}
	if errors.Is(err, errRecoveryCrash) {
		// Crashed again while recovering; it starts over from its disk.
		s.stats.RecoveryCrashes++
		n.why = "the run crashed it during recovery"
		s.tracef("node %d: crashed during recovery", n.id)
		s.down(n)
		s.loseUnsynced(n)
		s.restartLater(n, 1+s.rng.Int64N(Second))
		return true
	}
	if n.misread && refusesDamage(err) {
		// The disk holds the right data, so the next start recovers.
		n.misread = false
		s.stats.MisreadRefusals++
		n.why = fmt.Sprintf("refused a misread log: %v", err)
		s.tracef("node %d: refused a misread log: %v", n.id, err)
		s.down(n)
		s.restartLater(n, 1+s.rng.Int64N(Second))
		return true
	}
	if injected(err) && (!n.corrupted || !errors.Is(err, simfs.ErrBadSector)) {
		return false
	}
	if n.tornCrash && !n.corrupted && refusesDamage(err) {
		// A crash tore writes so that the log looks damaged.
		n.corrupted = true
		s.stats.TornRefusals++
		s.tracef("node %d: its torn crash image looks damaged: %v", n.id, err)
	}
	if !n.corrupted {
		return false
	}
	if !refusesDamage(err) && !errors.Is(err, simfs.ErrBadSector) {
		s.fail("node %d started on a damaged disk: %w", n.id, err)
		return true
	}
	n.fenced = true
	n.why = fmt.Sprintf("refused to start on its damaged disk: %v", err)
	s.stats.Fenced++
	s.tracef("node %d: fenced: %v", n.id, err)
	s.down(n)
	return true
}
