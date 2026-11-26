package sim

import (
	"cmp"
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

// wlMembers are the members a wl cluster of voters starts with.
func wlMembers(voters int) []wl.Member {
	var ms []wl.Member
	for i := range voters {
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
	voters := s.cfg.voters()
	ms := wlMembers(voters)
	if s.synth != nil {
		// A synthetic image's members have IDs near the largest
		// (syntheticImage), its nodes' since the fork took them.
		for i := range ms {
			ms[i].ID = s.nodes[i].id
		}
	}
	join := n.idx == voters
	if n.joins {
		// A voter an operator replaced joins as the member added for it.
		ms[n.idx].ID = n.id
	}
	if join {
		// The member the membership work added (Config.Membership) joins
		// the running cluster at the peer URL it has now.
		url := memberURL
		if s.membership.stage >= 2 {
			url = updatedMemberURL
		}
		ms = append(ms, wl.Member{ID: n.id, Name: fmt.Sprintf("node%d", voters+1), PeerURLs: []string{url}, ClientURLs: []string{"https://" + s.hostName(n.idx) + ":2379"}})
	}
	self := slices.IndexFunc(ms, func(m wl.Member) bool { return m.ID == n.id })
	srvTLS := &tls.Config{Certificates: []tls.Certificate{s.net.cert}, MinVersion: tls.VersionTLS12}
	peerTLS := &tls.Config{
		Certificates: []tls.Certificate{s.net.cert}, RootCAs: s.net.ca, ClientCAs: s.net.ca,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	cfg := wl.Config{
		Name: ms[self].Name, ID: n.id, ClusterID: wlCluster, DataDir: wlDataDir, Members: ms,
		ClientURLs: ms[self].ClientURLs, Join: join || n.joins,
		ClientListeners: []net.Listener{s.net.listen(s, p, []string{s.hostName(n.idx) + ":2379"})},
		PeerListeners:   []net.Listener{s.net.listen(s, p, n.addrs)},
		ClientTLS:       srvTLS, PeerServerTLS: peerTLS, PeerClientTLS: peerTLS,
		MemoryBudget: s.cfg.MemoryBytes, QuotaBackendBytes: s.cfg.Quota,
		// A checkpoint, which cuts the log behind it, every DiskBudget of
		// log, as main's node cleans within it.
		CheckpointLog: s.cfg.Faults.DiskBudget, CatchupEntries: cmp.Or(s.cfg.Catchup, wlCatchup), CheckpointChunkKeys: s.cfg.ckptChunkKeys,
		LogRing: s.ringSize(),
		Logger:  log.New(&nodeLog{s: s, n: n}, "", 0),
		Sys:     s.os.NewNode(wlDataDir, &disk{s: s, p: p}, wlNet{s: s, p: p}),
		Hooks:   &wlHooks{s: s, p: p},
	}
	conn, err := grpc.NewClient(
		"passthrough:///"+s.hostName(n.idx)+":2379",
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

// wlRing is the smallest log ring (wl's logring.go) of a run with a disk
// budget: eight of the simulator's segments of 16 KiB, the fewest a sized
// ring holds, which a run wraps many times, under crashes that
// tear writes into reused space. With fewer the ring is low from its
// first write: every cut is forced, and followers catch up only from
// checkpoints (#751's review).
const wlRing = 8 * 16 << 10

// wlRingFree is the ring of a run with no disk budget, which before the
// ring never checkpointed: 64 segments, so that it checkpoints every 256
// KiB of log, as a quarter of its ring, and its log keeps whole segments
// past the newest checkpoint for damage to land on. With eight, a run
// checkpointed every 32 KiB, and planted's damage found no segment it
// could check (B04a, #751's CI).
const wlRingFree = 64 * 16 << 10

// ringSize is a node's log ring in this run: as a disk budget sizes it
// (wl's logRing), half the budget, and at least wlRing. A fixed wlRing
// gave kubeConfig's nodes, with a budget of 16 MiB or more, 128 KiB: its
// writes filled it at once, the nodes answered NOSPACE, and seeds of
// TestKubernetesLoad did too little to test anything (main's race-sim on
// ae01b0a4).
func (s *Sim) ringSize() int64 {
	if b := s.cfg.Faults.DiskBudget; b > 0 {
		return max(b/2, wlRing)
	}
	return wlRingFree
}

// diskCapacity is a node's disk's capacity: the faults' DiskCapacity, 0
// for none, past the log's ring, which the format takes at the start.
func (s *Sim) diskCapacity() int {
	if s.cfg.Faults.DiskCapacity == 0 {
		return 0
	}
	return s.cfg.Faults.DiskCapacity + int(s.ringSize())
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
	s.wlSyncFailStarted(n)
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
func (h *wlHooks) Applied(e *raftpb.Entry, hash uint64, state func() uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	p.wl.applied = e.GetIndex()
	if n.catchUp > 0 && e.GetIndex() >= n.catchUp {
		// A replacing member caught up: the liveness rule holds it once
		// it had the time to recover (replace).
		n.stalls[n.catchUpWin][1] = s.now() + max(s.cfg.Faults.MaxDown, Second) + s.cfg.LiveAfter
		n.catchUp = 0
	}
	s.record(n, core.Applied{Index: e.GetIndex(), Term: e.GetTerm(), Type: e.GetType(), Data: e.GetData(), Hash: hash})
	s.noteRevokes(e)
	s.applied = max(s.applied, e.GetIndex())
	if c, ok := s.ckpts[e.GetIndex()]; ok && c.node != n.id {
		// Another node took a checkpoint here: this node's store must
		// hold the same state.
		s.sameState(n, e.GetIndex(), state(), "applied")
	}
}

// noteRevokes notes when the first node to apply entry e applied the
// lease revokes in it (revokesKeepPromises). The lock is held.
func (s *Sim) noteRevokes(e *raftpb.Entry) {
	if e.GetIndex() <= s.revokesTo {
		return
	}
	s.revokesTo = e.GetIndex()
	if e.GetType() != raftpb.EntryNormal || len(e.GetData()) == 0 {
		return
	}
	ops, err := wlOps(e.GetData())
	if err != nil {
		// The check of the committed log reports it (checkWL).
		return
	}
	for _, op := range ops {
		if r, ok := op.(*pb.LeaseRevokeRequest); ok {
			if _, seen := s.revokedAt[r.ID]; !seen {
				s.revokedAt[r.ID] = s.now()
			}
		}
	}
}

// sameState checks that n's store after index has the state, by hash,
// that the first node to take a checkpoint at index had, and records it
// if none did yet: wl's nodes take checkpoints at indexes of their own,
// so each node's store is compared at every index another's checkpoint
// was taken at, as main's compares the checkpoint files nodes write at an
// index (ckptWritten). The lock is held.
func (s *Sim) sameState(n *node, index, state uint64, what string) {
	c, ok := s.ckpts[index]
	if !ok {
		s.ckpts[index] = ckptRec{sum: state, node: n.id}
		return
	}
	if c.sum != state {
		s.fail("node %d %s index %d with another state than node %d's checkpoint there", n.id, what, index, c.node)
	}
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
func (h *wlHooks) Checkpoint(index, state uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.n.proc != p {
		return
	}
	if p.wl.applied != 0 && index != p.wl.applied {
		s.fail("node %d started a checkpoint at index %d, but applied to %d", p.n.id, index, p.wl.applied)
		return
	}
	s.sameState(p.n, index, state, "started a checkpoint at")
}

// Loaded checks that a store a start or an install loaded from the
// checkpoint at index has the state the store it was taken of had.
func (h *wlHooks) Loaded(index, state uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.n.proc != p {
		return
	}
	s.sameState(p.n, index, state, "loaded a checkpoint at")
}

// Installed notes when the node's install of a snapshot was done, with the
// checkpoint it loaded named in CURRENT: it serves again. Only the install
// this process began ends here; one that failed, or that a crash cut
// short, never ends (#812's review).
func (h *wlHooks) Installed(index uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	if k := len(p.n.installs) - 1; p.n.proc == p && p.installing && k >= 0 {
		p.installing = false
		w := &p.n.installs[k]
		w.installed = s.now()
		// The span ends once the node applied what the leader had
		// committed now (Turn): a fixed target.
		w.target = index
		if lead := s.leaderIdx(); lead >= 0 && s.nodes[lead].proc != nil {
			w.target = max(index, s.nodes[lead].proc.wl.turn.Commit)
		}
		s.tracef("node %d: installed the snapshot at index %d %s after it reached the node; to catch up to %d", p.n.id, index, seconds(w.installed-w.from), w.target)
	}
}

// Installing notes when the node's loop began to install a snapshot, and
// opens the install's span from when the snapshot reached the node: it
// can answer no client until the install is done (Installed), and the
// liveness rule excuses its calls that failed in between
// (installExcused). With
// Config.installWait, the loop waits here that long.
func (h *wlHooks) Installing(index uint64) {
	s, p := h.s, h.p
	s.mu.Lock()
	if p.n.proc != p {
		s.mu.Unlock()
		return
	}
	// The span starts when the snapshot reached the node: the first stream
	// to it since its process started, if one was served before.
	from := s.now()
	if f := p.n.snapFrom; f != 0 && f < from {
		from = f
	}
	p.n.snapFrom = 0
	p.n.installs = append(p.n.installs, installSpan{from: from})
	p.installing = true
	s.tracef("node %d: installing a snapshot at index %d", p.n.id, index)
	wait := s.cfg.installWait
	s.mu.Unlock()
	if wait > 0 {
		t := time.NewTimer(time.Duration(wait))
		select {
		case <-t.C:
		case <-p.ctx.Done():
			t.Stop()
		}
	}
}

// CheckpointDone checks that the checkpoint a node finished holds, as its
// next start would load it from the disk, the state its store had when
// it started it, and every value it names as the apply took it
// (wl.CheckpointStateHash). It reads the disk's durable bytes, not the
// node's files, so that the check draws no disk fault.
func (h *wlHooks) CheckpointDone(index uint64, name string) {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	b := n.disk.DurableBytes("ckpt/" + name)
	if b == nil {
		return
	}
	segs := map[int64][]byte{}
	readLog := func(p []byte, off int64) error {
		for got := 0; got < len(p); {
			k, at := wl.SegmentPlace(off + int64(got))
			seg, ok := segs[k]
			if !ok {
				seg = n.disk.DurableBytes(wlSegment(k))
				segs[k] = seg
			}
			if int64(len(seg)) <= at {
				return fmt.Errorf("the log holds no byte at %d durably", off+int64(got))
			}
			got += copy(p[got:], seg[at:])
		}
		return nil
	}
	state, err := wl.CheckpointStateHash(b, name, readLog)
	if err != nil {
		if re, ok := errors.AsType[*wl.ReadBackError](err); ok && n.rotted && !re.InCheckpoint() && re.Off <= n.rotOff && n.rotOff < re.Off+int64(re.Len) {
			// The checkpoint names the value the run rotted in the log.
			return
		}
		s.fail("node %d wrote checkpoint %d that does not load: %w", n.id, index, err)
		return
	}
	s.stats.Checkpoints++
	if s.stats.DiskGrowths > 0 {
		s.stats.GrownCheckpoints++
	}
	if c, ok := s.ckpts[index]; ok && c.sum != state {
		s.fail("node %d wrote checkpoint %d with another state than node %d's store had at it", n.id, index, c.node)
	}
	if s.cfg.Faults.Checkpoint && s.stats.ErasedAtRoot == 0 {
		// The run's first checkpoint is durable and a start would load
		// it; the log behind it may be cut from now on. The cold restarts
		// erase each disk in turn here (main's root, afterCall).
		s.stats.ErasedAtRoot++
		s.takeCheckpoint()
	}
}

// Turn keeps what the node shows, and holds its loop while the process
// is paused, frozen, or the run failed.
func (h *wlHooks) Turn(st wl.TurnState, values func() []wl.LogValue) {
	s, p := h.s, h.p
	s.mu.Lock()
	n := p.n
	if n.proc != p {
		s.mu.Unlock()
		return
	}
	p.wl.turn = st
	if k := len(n.installs) - 1; k >= 0 {
		if w := &n.installs[k]; w.installed != 0 && w.caughtUp == 0 && st.Applied >= w.target {
			w.caughtUp = s.now()
			s.tracef("node %d: caught up to %d after its snapshot, %s after it reached the node", n.id, w.target, seconds(w.caughtUp-w.from))
		}
	}
	if s.rotDue == n {
		s.rotDue = nil
		s.rotWL(n, values)
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
// start late (SlowFetch); the source of the first stream of a run
// crashes partway through it (FetchCrash, SnapshotCut), and otherwise the
// first stream, and one in four after it, is damaged on its way
// (FlipSnapshot). Peers talk TLS, so only this hook reaches the stream's
// own CRC. A stream larger than a chunk is cut or damaged in its first.
func (h *wlHooks) SnapshotChunk(to uint64, first bool, p []byte) int {
	// Only a cold restart waits on snapshots (coldRestart's progress), and
	// traces them: the main runs keep their draws.
	if h.s.inColdRestart {
		if mib, at := h.s.snaps.chunk(to, first, len(p)); first || mib {
			h.s.mu.Lock()
			h.s.tracef("node %d: serving a snapshot stream to %d, %d MiB of it so far", h.p.n.id, to, at>>20)
			h.s.mu.Unlock()
		}
	}
	if w := h.s.cfg.snapshotWait; w > 0 {
		t := time.NewTimer(time.Duration(w))
		select {
		case <-t.C:
		case <-h.p.ctx.Done():
			t.Stop()
		}
	}
	keep := len(p)
	if !first {
		return keep
	}
	s, n := h.s, h.p.n
	s.mu.Lock()
	var wait int64
	if s.cfg.Faults.SlowFetch && !s.healed && s.rng.IntN(2) == 0 {
		s.stats.SlowFetches++
		wait = 200*Millisecond + s.rng.Int64N(1800*Millisecond)
	}
	s.stats.Snapshots++
	// A snapshot reaches its node from here: the install's span starts
	// at the first stream to it (Installing, #775's ruling).
	for _, m := range s.nodes {
		if m.id == to && m.snapFrom == 0 {
			m.snapFrom = s.now()
		}
	}
	crash := s.cfg.Faults.FetchCrash && !s.healed && s.stats.FetchCrashes == 0 && len(p) > 1
	switch {
	case crash:
		// A prefix of at least a byte.
		keep = 1 + s.rng.IntN(len(p)-1)
		s.tracef("node %d: serving %d bytes of a snapshot stream to %d, then crashing", n.id, keep, to)
	case s.cfg.Faults.FlipSnapshot && !s.healed && len(p) > 0 && (s.stats.SnapshotFlips == 0 || s.rng.IntN(4) == 0):
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
	return keep
}

// NoRoom counts a checkpoint the node held or dropped for lack of room on
// its disk, with NOSPACE raised: a full disk's effect on a node that
// stays up (#769). When a disk fault made it, a full disk or a short
// write, an operator comes to disarm the alarm once the fault is over; a
// disk too small for the store keeps it up.
func (h *wlHooks) NoRoom() {
	s, p := h.s, h.p
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	s.stats.NoRoom++
	short := n.shortArmed && !n.disk.ShortPending()
	if short {
		n.shortArmed = false
	}
	if !n.diskFull && !short {
		n.noRoomAt = s.now()
	}
	switch {
	case n.disarming:
	case n.diskFull || short:
		n.disarming = true
		s.wg.Go(func() { s.disarmNoSpace(n) })
	case s.cfg.Faults.GrowDisk && !s.growing:
		s.growing, n.disarming = true, true
		s.wg.Go(func() { s.growDisk(n) })
	}
}

// SnapshotCut crashes the source of a stream SnapshotChunk cut, once the
// follower read what it kept.
func (h *wlHooks) SnapshotCut(to uint64) { h.s.crashSource(h.p, to, h.p.ctx.Done()) }

// Deliver drops a peer's append to or from the gray node, as main's
// transport does (deliverer). A wl node's snapshot comes on a stream of
// its own, which SnapshotChunk's faults reach.
func (h *wlHooks) Deliver(m *raftpb.Message) bool {
	s := h.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return h.p.n.proc != h.p || !s.grayDrops(h.p.n, m)
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
	// A fork's reads of every key, and its deletes of an image's prefix
	// with their previous values (agedRequest), may pass gRPC's 4 MiB a
	// client takes by default, as the final reads may (finalReads).
	var opts []grpc.CallOption
	if s.startRev > 0 {
		opts = append(opts, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	}
	switch r := req.(type) {
	case *pb.PutRequest:
		return pb.NewKVClient(conn).Put(ctx, r)
	case *pb.RangeRequest:
		return pb.NewKVClient(conn).Range(ctx, r, opts...)
	case *pb.DeleteRangeRequest:
		return pb.NewKVClient(conn).DeleteRange(ctx, r, opts...)
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
	case *pb.AlarmRequest:
		return pb.NewMaintenanceClient(conn).Alarm(ctx, r)
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
// against the other nodes' (wlHooks.Applied). With Config.stageUnwritten
// the crash waits for a write that leaves zeros past what was synced: one
// that grows segment name, whose growth the image keeps with zeros
// (simfs.Unwritten), as ext4 may leave blocks allocated ahead (wal.go's
// makeSegment); or, in a log ring's segment formatted with zeros, one
// over zeros that the image loses (simfs.Lost). It reports whether it
// crashed n. The lock is held.
func (s *Sim) wlStageCrash(n *node, name string) bool {
	if s.stats.StageCrashes > 0 || s.healed || s.now() < 2*Second || n.proc == nil || n.appended <= n.floor() {
		return false
	}
	img := simfs.Sectors
	if s.cfg.stageUnwritten {
		switch {
		case n.disk.Grown(name):
			img = simfs.Unwritten
		case n.disk.PendingOverZeros(name):
			// A log ring's segment, formatted with zeros: the write never
			// reaches the medium, and its space reads as the format's
			// zeros past the last record and mark synced (#751's CI).
			img = simfs.Lost
		default:
			return false
		}
		s.stats.StageZeroed++
	}
	s.stats.StageCrashes++
	s.stats.Crashes++
	s.stats.PowerLosses++
	n.why = fmt.Sprintf("the run crashed it between a write of its log and the write's sync, keeping %v", img)
	s.tracef("node %d: crash between a write of its log and its sync, keeping %v; reported %d durable, handed its log %d", n.id, img, n.floor(), n.appended)
	n.mustLast, n.mustHS, n.mustAtMost = n.floor(), n.hs, n.appended
	s.down(n)
	from := len(n.diskLog)
	s.loseUnsyncedAs(n, img)
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

// wlSyncFailArm arms, once, the failed sync of Config.syncFailWarmCold on
// n, after a write of its log, segment name, past what it synced and over
// two blocks or more, once the cluster ran 2 s: the write's sync fails
// with none of it on the disk. The node then exits, and starts again warm
// (Config.syncFaultExits). A write within one block a start writes again
// whole anyway, its last; a write over more left blocks before the last
// that only the page cache held (#752). The lock is held.
func (s *Sim) wlSyncFailArm(n *node, name string) {
	if s.syncFail.phase != 0 || s.healed || s.now() < 2*Second || n.proc == nil || n.appended <= n.floor() || n.disk.PendingBytes(name) < 2*4096 {
		return
	}
	s.syncFail.n, s.syncFail.phase = n, 1
	n.disk.FailSyncLosingAll()
	s.tracef("node %d: its log's next sync fails, with none of its writes on the disk", n.id)
}

// wlSyncFailStarted moves Config.syncFailWarmCold's scenario on when its
// node recovered: after its warm start it writes on for a second, then
// loses power, every write it did not sync lost whole, and starts cold;
// that start's recovery (Recovered's checks) is the scenario's end. The
// lock is held.
func (s *Sim) wlSyncFailStarted(n *node) {
	f := &s.syncFail
	if f.n != n {
		return
	}
	switch f.phase {
	case 1:
		f.phase = 2
		s.tracef("node %d: started warm after its failed sync", n.id)
		gen := n.gen
		s.after(Second, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if f.phase != 2 || n.gen != gen || n.proc == nil {
				return
			}
			f.phase = 3
			n.why = "the run cut its power after its warm start from a failed sync"
			s.crashWith(n, func(n *node) { s.loseUnsyncedAs(n, simfs.Writes) })
			s.restartLater(n, 100*Millisecond)
		})
	case 3:
		f.phase = 4
		s.stats.SyncFailColdStarts++
		s.tracef("node %d: started cold after its failed sync and warm start", n.id)
	}
}

// wlCheckpointCrash crashes n right after a write of a checkpoint it is
// writing, while the file holds four writes or more since its last sync,
// keeping whole writes at random, until a crash kept a later write and
// lost an earlier one (Config.ckptCrash): each crash does in about one
// in three, as the file's creation is kept in one in two. wl's log never has two writes
// unsynced (wal.go), so a checkpoint being written is the one place a
// crash can do that: the file is renamed only after its sync, and the
// start must not take it. At most 16 crashes. The lock is held.
func (s *Sim) wlCheckpointCrash(n *node, name string) bool {
	if s.healed || n.proc == nil || s.stats.CkptCrashes >= 16 || !strings.HasSuffix(name, ".tmp") || n.disk.Pending(name) < 4 {
		return false
	}
	for _, m := range s.nodes {
		if m.disk.Counts.Reordered > 0 {
			return false
		}
	}
	s.stats.CkptCrashes++
	n.why = "the run crashed it partway through a checkpoint"
	s.tracef("node %d: crash after a write of %s, %d writes unsynced", n.id, name, n.disk.Pending(name))
	s.crashWith(n, func(n *node) { s.loseUnsyncedAs(n, simfs.Writes) })
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
	s.lastFenceAt = s.now()
	s.tracef("node %d: fenced: %v", n.id, err)
	s.down(n)
	if s.cfg.LoseQuorum && s.quorumLostAt == 0 && s.count(func(m *node) bool { return m.fenced }) > (s.cfg.voters()-1)/2 {
		s.quorumLostAt, s.stats.QuorumLostAt = s.now(), s.now()
		s.tracef("a majority of the voters is fenced for good: the cluster lost its quorum")
	}
	if s.cfg.Faults.Replace && !s.cfg.LoseQuorum {
		s.wg.Go(func() { s.replace(n) })
	}
	return true
}
