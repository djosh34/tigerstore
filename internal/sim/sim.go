// Package sim runs a cluster of real tigerstore nodes in one process, on
// simulated disks and a simulated network, inside a testing/synctest
// bubble, from one seed.
//
// Each node is the real node driver (internal/node) with its WAL and its
// peer transport (internal/peer). Its disk is a simfs disk, and its
// transport speaks TLS over the simulator's in-memory connections.
// Clients enter where the server does: Node.Do, watch streams, and the
// peer frames the transport delivers. gRPC and HTTP stay outside.
//
// The seed decides the workload, the faults and their times, every
// network delay and log sync, and the yields at every disk call. Go's
// scheduler decides the order of the work between two of them, so one
// seed can play out differently from run to run. A failed run reports
// its trace: what the simulator did, and the nodes' log lines.
//
// Run must be called inside a synctest bubble (synctest.Test), whose
// virtual clock the nodes run on.
package sim

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	tnode "github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Times are virtual nanoseconds since the start of a run.
const (
	Microsecond int64 = 1000
	Millisecond       = 1000 * Microsecond
	Second            = 1000 * Millisecond

	// TickTime is the length of one Raft tick, as in etcd.
	TickTime = 100 * Millisecond
)

// Config describes one run.
type Config struct {
	Seed uint64
	// Duration is the length of the phase with faults.
	Duration int64
	// Heal is how long clients run after scheduled healing. The liveness
	// rule allows LiveAfter for recovery after healing. A gray fsync in
	// flight at healing and a deferred disk failure can start it later;
	// they extend the client phase if needed. Nodes restarted at healing
	// need up to two election timeouts (2 s) to elect a leader; at 1 s,
	// seed 126 fails with "no leader".
	Heal      int64
	LiveAfter int64
	// Clients is the number of clients. Each has one call in flight.
	Clients int
	// ClientTimeout is when a client gives up on a call.
	ClientTimeout int64
	// GrayLeaderAt, if set, makes whichever node leads then gray until
	// healing: its heartbeats get through, its appends do not.
	// GrayLeaderSync also makes its fsyncs slow, so one in flight at
	// healing ends after it.
	GrayLeaderAt   int64
	GrayLeaderSync bool
	// HalfOpenAt, if set, makes the link from whichever node leads then
	// to a follower half-open for the peer write timeout.
	HalfOpenAt int64
	// StallLeaderAt, if set, stalls the disk of whichever node leads at
	// that time until healing.
	StallLeaderAt int64
	// LiveFrom, if set, is when the liveness rule starts instead of
	// LiveAfter into healing.
	LiveFrom int64
	// NoLeases leaves leases out of the workload. Porcupine's runs need
	// that: every lease adds an expiry that may apply at any time, and its
	// search grows too fast with them.
	NoLeases bool
	// Membership adds, updates, promotes and removes a fourth member
	// while the ordinary clients and faults continue.
	Membership bool
	// MemoryBytes is each node's memory budget (core.Config.MemoryBytes):
	// it turns the value cache on and bounds Range answers. Zero leaves
	// both off.
	MemoryBytes int64
	Faults      Faults
}

// Faults sets what goes wrong in the faulty phase. A zero field turns its
// fault off.
type Faults struct {
	// CrashEvery is the mean time between node crashes. A crashed node
	// loses what its disk did not sync and restarts after up to MaxDown.
	CrashEvery int64
	MaxDown    int64
	// LinkEvery is the mean time between changes of the set of directed
	// links that are up. A link that goes down resets the connections
	// between its two nodes, and refuses new ones until it is up.
	LinkEvery int64
	// MaxDelay is the largest one-way delay of a message.
	MaxDelay int64
	// SlowDisk is the chance in percent that one sync of the log takes
	// 20 to 100 ms instead of under 1 ms.
	SlowDisk int
	// DiskErrorEvery is the mean time between failed writes or syncs:
	// EIO on a write or a sync, or a short write with ENOSPC. The node
	// stops, never retries, and restarts from its disk.
	DiskErrorEvery int64
	// StallEvery is the mean time between disk stalls of 1.5 to 4 s, in
	// which the log's syncs wait.
	StallEvery int64
	// Corrupt damages one node's synced bytes at a crash, once per run
	// and only sometimes; that node must refuse to start.
	Corrupt bool
	// Checkpoint takes the erase-one-disk checkpoint during the faults.
	Checkpoint bool
	// LinkStallEvery is the mean time between stalls of 0.5 to 2 s on one
	// directed link: everything on it waits, in order, like a stalled
	// TCP stream.
	LinkStallEvery int64
	// PauseEvery is the mean time between process pauses of 0.2 to 2 s:
	// the committer, the log's syncs and value reads, and reads from the
	// network wait.
	PauseEvery int64
	// DriftPerMille is the most a node's clock runs fast or slow, per
	// mille: the declared clock envelope, which the lease lower bound
	// allows for. A node's ticker drops the ticks it misses, as Go's
	// does, so a pause makes its clock slow, never fast.
	DriftPerMille int64
	// HalfOpenEvery is the mean time between half-open links: one
	// directed link swallows everything for 0.5 to 5 s, the peer write
	// timeout, and then its connections fail and the peer transport
	// reports the peer unreachable.
	HalfOpenEvery int64
	// GrayEvery is the mean time between gray periods of 2 to 6 s, in
	// which one node at a time is gray: Raft heartbeats and votes get
	// through, but appends to and from it are dropped as they arrive, and
	// in half of them its fsyncs take 0.5 to 3 s. The gray node can
	// change every 1 to 3 s.
	GrayEvery int64
	// GrayClient adds a client that opens a stream with 10 watches, never
	// reads it, and reconnects every 50 to 300 ms until healing.
	GrayClient bool
	// RecoveryCrash crashes one restart in three again at its first to
	// fourth writing disk operation inside recovery.
	RecoveryCrash bool
	// StepCrash crashes a node once right after a disk call that changes
	// what is durable outside appends: a write or a sync of a checkpoint,
	// whether the node's own or a snapshot's, a directory sync, or a
	// removal.
	StepCrash bool
	// FlipSnapshot damages the first snapshot stream, and one in four
	// after it, as its source writes it.
	FlipSnapshot bool
	// FetchCrash crashes the node that serves the first snapshot stream
	// of a run partway through it.
	FetchCrash bool
	// SlowFetch makes the source of half the snapshot fetches during the
	// faults wait 0.2 to 2 s before it serves, so that it may first make
	// a newer checkpoint durable, or give up on the snapshot and remove
	// what it needs.
	SlowFetch bool
	// Rot flips a bit of a value on a running node's disk, once per run,
	// as the run's corruption if it has none yet.
	Rot bool
	// HoldDown keeps a follower down for 3 to 5 s once per run, so that
	// it falls behind the leader's window and needs a snapshot.
	HoldDown bool
	// SwitchEvery is the mean time between switches to a random link
	// mask. It runs alongside LinkEvery; set only one for fast switching.
	SwitchEvery int64
	// HoldMask, if HoldMaskAt is set, sets the link mask at that time.
	// Other link faults can change it later; with none, it holds until
	// healing.
	HoldMask   uint64
	HoldMaskAt int64
}

// DefaultConfig is a run of 20 virtual seconds with crashes, link changes,
// delays and slow disks, then 10 seconds of healing.
func DefaultConfig(seed uint64) Config {
	return Config{
		Seed:          seed,
		Duration:      20 * Second,
		Heal:          10 * Second,
		LiveAfter:     4 * Second,
		Clients:       6,
		ClientTimeout: 3 * Second,
		Faults: Faults{
			CrashEvery: 3 * Second,
			MaxDown:    2 * Second,
			LinkEvery:  2 * Second,
			MaxDelay:   5 * Millisecond,
			SlowDisk:   2,

			DiskErrorEvery: 6 * Second,
			StallEvery:     8 * Second,
			Corrupt:        true,
			Checkpoint:     true,

			LinkStallEvery: 4 * Second,
			PauseEvery:     6 * Second,
			DriftPerMille:  50,
			HalfOpenEvery:  8 * Second,
			GrayEvery:      8 * Second,
			GrayClient:     true,
			RecoveryCrash:  true,
			StepCrash:      true,
			FlipSnapshot:   true,
			FetchCrash:     true,
			SlowFetch:      true,
			Rot:            true,
			HoldDown:       true,
		},
	}
}

// node is one member's machine: its disk, which outlives its processes,
// and the process running on it, if any.
type node struct {
	idx  int
	id   uint64
	disk *simfs.Disk
	// addrs are the host:port the node listens on; the first is its own
	// peer URL's.
	addrs []string
	// proc is the running process, nil while the node is down; starting
	// is set while one recovers. gen counts the times the node went down,
	// so a restart meant for an earlier stop does nothing.
	proc     *proc
	starting bool
	gen      uint64

	// graySync makes its fsyncs slow while it is gray.
	graySync bool
	// pausedUntil holds the process back; frozen holds it for good, for
	// the checks at the end. wake is closed and replaced when either
	// changes, or the stall ends.
	pausedUntil int64
	frozen      bool
	wake        chan struct{}
	// stallUntil holds back the log's syncs until then; stalls keeps every
	// stall window, for the liveness rule.
	stallUntil int64
	stalls     [][2]int64
	// reloads are the times a client saw the node fail its call to start
	// again from its log, after a snapshot install or a failed fetch, as
	// on a restart; the client tries again.
	reloads []int64
	// durableLast is the last entry index the node reported durable to
	// its core; recovery must give at least that. pending are the jobs
	// its core handed the disk that it has not reported yet, oldest
	// first: one that replaces entries reported durable may lower what
	// a crash leaves, as Raft may replace acknowledged entries that never
	// committed.
	durableLast uint64
	pending     []pendingJob
	// corrupted is set when the run damaged the disk on purpose; fenced
	// when the node then refused to start and stays down.
	corrupted, fenced bool
	// inactive is a member not yet added, or a process stopped because
	// it was removed. Cold recovery must still read that process's disk.
	inactive bool
	// rotted is set when the run flipped a bit of a value of entry
	// rotEntry, in segment rotSeg, file rotFile, on the disk of this
	// running node.
	rotted           bool
	rotFile          string
	rotEntry, rotSeg uint64
	// ckptDone are the checkpoint files closed right after their last
	// sync, whose bytes count once the directory is synced;
	// removedSegment marks a segment removal whose erase-one-disk copy
	// waits for the next sync of the directory.
	ckptDone       []string
	removedSegment bool
	// root is the index of the node's newest durable root, rootDue that
	// of the first root its core asked for, until the erase-one-disk copy
	// at it, and servedDamage is set once a serve failed on its own
	// damage. duringInstall are the checkpoint files the node finished
	// while an install was on its way to disk, the install's own among
	// them.
	root, rootDue uint64
	servedDamage  bool
	duringInstall []string
}

// pendingJob is a job a node's core handed its disk: the first entry it
// writes, and whether that replaces entries reported durable.
type pendingJob struct {
	first    uint64
	replaces bool
	install  bool
}

// proc is one process of a node: the node driver, its peer transport and
// its handle on the disk. Its context ends when it dies; from then on its
// disk calls and connections fail.
type proc struct {
	n      *node
	node   *tnode.Node
	t      *peer.TCP
	ctx    context.Context
	cancel context.CancelFunc
	// core is the core the committer ran last, and parked, while set, is
	// closed once the committer holds still for the checks at the end.
	core   *core.Node
	parked chan struct{}
}

func (p *proc) dead() bool { return p.ctx.Err() != nil }

// out reports whether n is out of the cluster for now: fenced, or not a
// member.
func (n *node) out() bool { return n.fenced || n.inactive }

// link is a directed link between two nodes.
type link struct {
	up bool
	// base and jitter give the link's own delay, so links differ and one
	// direction can be much slower than the other; stallUntil holds
	// everything on the link back, like a stalled TCP stream.
	base, jitter int64
	stallUntil   int64
	// halfOpenUntil swallows what the link carries until its connections
	// fail.
	halfOpenUntil int64
}

// Sim is one run.
type Sim struct {
	cfg Config
	// mu guards everything below and every simulated disk. Nothing holds
	// it while it waits or calls into a node.
	mu    sync.Mutex
	rng   *rand.Rand
	t0    time.Time
	nodes []*node
	links [][]link
	net   network
	trace []string
	// end is closed when the run ends. wg counts the run's goroutines
	// but the processes', which procs counts.
	end       chan struct{}
	ended     bool
	wg, procs sync.WaitGroup

	healAt, stopAt, endAt int64
	// servedRoot is the newest durable root of the source of the last
	// snapshot served to each node, by Raft ID, when it was served.
	servedRoot map[uint64]uint64
	healed     bool
	// downFor is how long HoldDown keeps its follower down.
	downFor int64

	clients  []*client
	watchers []*watchClient
	// expiredAt is when a lease first expired on any node.
	expiredAt    map[int64]int64
	expiredOrder []int64
	checkpoints  []*checkpoint
	halfOpens    []halfOpenLink
	// applied is the highest index any node has applied.
	applied       uint64
	stallHandOffs []int64
	commitStops   []int64
	// gray is the gray node, or nil, and grayEnd when its gray period ends.
	gray     *node
	grayEnd  int64
	grayCl   *grayClient
	lastGray int
	// grayDropped is set once the gray node lost an append; grayOnce marks
	// the run's guaranteed gray period, which lasts until then.
	grayDropped, grayOnce bool
	// grayStays keeps the gray period on the leader it started on.
	grayStays bool
	// grayHealedAt is when the last gray fsync that outlasted healing
	// ended.
	grayHealedAt int64
	// recoveryDue crashes the next node that writes its log, before the
	// sync, until a recovery crashed; stepCrashDue crashes a node at its
	// next checkpoint, removal or install step, until one did; rotDue rots
	// a value of that node at its next turn.
	recoveryDue, stepCrashDue bool
	rotDue                    *node
	// log is every committed entry as the first node applied it, with the
	// state's hash after it, up to index maxLogged; ckpts are the hashes
	// of the checkpoint files, by index.
	log       map[uint64]logRec
	maxLogged uint64
	ckpts     map[uint64]ckptRec
	// base, in a cold restart, is the run's record, which holds up to
	// baseUpTo.
	base     map[uint64]logRec
	baseUpTo uint64
	// diskFaultDue arms a disk fault at the next write of a log segment,
	// after healing too; lateStopAt is when a node last stopped after
	// healing, which moves the start of the liveness rule.
	diskFaultDue bool
	lateStopAt   int64
	history      []*Record
	stats        Stats
	membership   membershipWork
	err          error
}

// Stats counts what happened in a run.
type Stats struct {
	Messages, Dropped     int
	Crashes, LinkChanges  int
	OK, Failed, Ambiguous int
	// Compactions counts successful compactions and WatchCompacted the
	// watches that a compaction canceled.
	Compactions, WatchCompacted int
	// Revokes counts lease revokes in the log, expiries included.
	Revokes int
	// DiskFaults counts failed writes and syncs that stopped a node,
	// StallHandOffs leaders that handed over on a stalled disk, Fenced
	// nodes that found their disk damaged, and ColdRestarts the
	// erase-one-disk restarts checked.
	DiskFaults, StallHandOffs, Fenced, ColdRestarts int
	// Pauses counts process pauses, LinkStalls stalled links, HalfOpens
	// half-open links, and Asymmetric the links that drew a delay at least
	// 10 times that of the other direction.
	Pauses, LinkStalls, HalfOpens, Asymmetric int
	// GrayNodes counts gray nodes that lost an append, CommitStops leaders
	// that stopped because their entries stopped committing, GrayStreams
	// the streams with watches a gray client opened, and RecoveryCrashes
	// crashes inside the recovery of an existing log.
	GrayNodes, CommitStops, GrayStreams, RecoveryCrashes int
	// GrayLeaderStops counts commit-watchdog stops of a gray leader.
	GrayLeaderStops int
	// Checkpoints counts checkpoint files completed, Snapshots snapshot
	// streams served, ServeFailures fetches that the source could not
	// serve, SnapshotFlips those damaged on the way, FetchCrashes sources
	// that crashed during a stream once the follower read a prefix of it,
	// SlowFetches fetches served late,
	// Reloads starts of a node from its log after a snapshot install or
	// a failed fetch, StepCrashes crashes at a checkpoint, removal or
	// install step, and Rots values damaged under a running node.
	Checkpoints, Snapshots, ServeFailures, SnapshotFlips, FetchCrashes, SlowFetches, Reloads, StepCrashes, Rots int
	// OlderCheckpoints counts installed snapshots whose checkpoint is
	// older than the source's newest durable root, CheckpointsDuringInstall
	// checkpoints finished while an install was on its way to disk
	// (#182), and FencedAfterServe nodes fenced after a serve failed on
	// their own damage (#201).
	OlderCheckpoints, CheckpointsDuringInstall, FencedAfterServe int
	// ErasedAtRoot and ErasedAtRemoval count the erase-one-disk copies
	// taken right after the first root synced and right after the first
	// segment removal.
	ErasedAtRoot, ErasedAtRemoval int
	// MembershipChanges counts each completed stage, and MembershipFaults
	// those completed before healing. Attempts include failed calls.
	MembershipChanges, MembershipFaults [4]int
	MembershipAttempts                  int
}

// Result is the outcome of a run.
type Result struct {
	History []*Record
	// Log is every committed write in commit order, and revs the
	// revision right after each.
	Log     []proto.Message
	revs    []int64
	Watches []WatchResult
	Stats   Stats
	// StallHandOffs are the times leaders handed over on a stalled disk,
	// and CommitStops the times leaders stopped because nothing committed.
	StallHandOffs, CommitStops []int64
	// Trace is what the run did, last of all, for a failure's report.
	Trace []string
	// grayNode is the index of the last gray node, for tests.
	grayNode int
	// grayHealedAt is when the last gray fsync after healing ended, for
	// tests.
	grayHealedAt int64
	halfOpens    []halfOpenLink
	// FinalRev is the revision of the converged cluster at the end.
	FinalRev int64
	// Clients is the number of request clients.
	Clients int
}

// Run runs one seed and checks it. The error describes the first broken
// rule. It must run inside a synctest bubble.
func Run(cfg Config) (*Result, error) { return runInspect(cfg, nil) }

// runInspect runs cfg, then calls inspect, if set, while the nodes still
// hold still for the checks.
func runInspect(cfg Config, inspect func(*Sim) error) (*Result, error) {
	s, err := newSim(cfg)
	if err != nil {
		return nil, err
	}
	res := &Result{Clients: cfg.Clients}
	err = s.run()
	if err == nil {
		res.History, res.Stats, res.StallHandOffs, res.CommitStops = s.history, s.stats, s.stallHandOffs, s.commitStops
		res.grayNode, res.grayHealedAt, res.halfOpens = s.lastGray, s.grayHealedAt, s.halfOpens
		if err = s.check(res); err != nil {
			err = fmt.Errorf("seed %d: %w", cfg.Seed, err)
		}
	}
	if err == nil && inspect != nil {
		err = inspect(s)
	}
	s.shutdown()
	res.Trace = s.trace
	if err == nil {
		err = s.failure()
	}
	return res, err
}

// newSim sets up an empty run.
func newSim(cfg Config) (*Sim, error) {
	s := &Sim{
		cfg:        cfg,
		rng:        rand.New(rand.NewPCG(cfg.Seed, 0x7469676572)),
		t0:         time.Now(),
		end:        make(chan struct{}),
		expiredAt:  map[int64]int64{},
		log:        map[uint64]logRec{},
		ckpts:      map[uint64]ckptRec{},
		servedRoot: map[uint64]uint64{},
		healAt:     cfg.Duration,
		stopAt:     cfg.Duration + cfg.Heal,
	}
	// After the clients stop, give the cluster time to settle: leases
	// still expire for up to leaseGrace, and the watch events of the last
	// expiry need time to arrive.
	s.endAt = s.stopAt + leaseGrace + 2*Second
	count := 3
	if cfg.Membership {
		count = 4
	}
	s.links = make([][]link, count)
	for i := range s.links {
		s.links[i] = make([]link, count)
	}
	s.setLinks(s.allLinks())
	if err := s.net.init(); err != nil {
		return nil, err
	}
	for i := range count {
		s.addNode(i, simfs.New())
	}
	return s, nil
}

func (s *Sim) addNode(i int, disk *simfs.Disk) {
	n := &node{idx: i, id: uint64(i + 1), disk: disk, addrs: []string{peerAddr(i)}, wake: make(chan struct{})}
	if i == 3 {
		// The fourth member is added at memberURL and moves to
		// updatedMemberURL.
		n.addrs = []string{hostPort(memberURL), hostPort(updatedMemberURL)}
	}
	s.nodes = append(s.nodes, n)
}

// run runs the workload and the faults until the end, then holds every
// node still for the checks.
func (s *Sim) run() error {
	if s.cfg.Membership {
		s.nodes[3].inactive = true
	}
	for _, n := range s.nodes[:3] {
		if err := s.start(n); err != nil {
			return err
		}
	}
	s.schedule()
	// The end can move while the run goes on (extendLiveness).
	for {
		s.mu.Lock()
		end, err := s.endAt, s.err
		s.mu.Unlock()
		if err != nil {
			return err
		}
		if s.now() >= end {
			break
		}
		select {
		case <-time.After(time.Duration(end - s.now())):
		case <-s.failed():
		}
	}
	s.mu.Lock()
	s.stop()
	s.mu.Unlock()
	s.wg.Wait()
	return s.freeze()
}

// schedule starts the clients, the watchers and the faults.
func (s *Sim) schedule() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Membership {
		s.after(2*Second, s.changeMembership)
	}
	for i := range s.cfg.Clients {
		c := &client{id: i, sim: s}
		s.clients = append(s.clients, c)
		s.wg.Go(c.run)
	}
	s.startWatchers()
	f := s.cfg.Faults
	if f.CrashEvery > 0 {
		s.every(f.CrashEvery, s.crashRandom)
		// One crash at a uniform time, so that no run goes without.
		s.at(1+s.rng.Int64N(s.cfg.Duration), s.crashRandom)
	}
	if f.LinkEvery > 0 {
		s.wg.Go(s.changeLinks)
		// One change at a uniform time, so that no run goes without.
		s.at(1+s.rng.Int64N(s.cfg.Duration), func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.healed {
				s.stats.LinkChanges++
				// Any mask but all links up.
				s.setLinks(s.rng.Uint64N(s.allLinks()))
			}
		})
	}
	s.scheduleDiskFaults()
	s.scheduleNetFaults()
	s.scheduleGrayFaults()
	if s.cfg.HalfOpenAt > 0 {
		s.at(s.cfg.HalfOpenAt, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.halfOpen(s.leaderIdx(), halfOpenTimeout)
		})
	}
	if s.cfg.StallLeaderAt > 0 {
		s.at(s.cfg.StallLeaderAt, s.stallLeader)
	}
	s.at(s.healAt, s.heal)
}

// now is the run's virtual time.
func (s *Sim) now() int64 { return int64(time.Since(s.t0)) }

// failed is closed once the run failed.
func (s *Sim) failed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return s.end
}

// failure returns the run's first broken rule, with when it broke.
func (s *Sim) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// fail records the first broken rule. The lock is held.
func (s *Sim) fail(format string, args ...any) {
	if s.err == nil {
		s.err = fmt.Errorf("seed %d at %d ms: %w", s.cfg.Seed, s.now()/Millisecond, fmt.Errorf(format, args...))
		s.tracef("failed: %v", s.err)
		s.stop()
	}
}

// stop ends the run: its goroutines see end closed. The lock is held.
func (s *Sim) stop() {
	if !s.ended {
		s.ended = true
		close(s.end)
	}
}

// traceLines is how many lines of its trace a run keeps.
const traceLines = 3000

// tracef adds a line to the trace. The lock is held.
func (s *Sim) tracef(format string, args ...any) {
	if len(s.trace) == traceLines {
		s.trace = slices.Delete(s.trace, 0, traceLines/4)
	}
	s.trace = append(s.trace, fmt.Sprintf("%9.3f ms ", float64(s.now())/float64(Millisecond))+fmt.Sprintf(format, args...))
}

// sleep waits d, and reports false if the run ended meanwhile.
func (s *Sim) sleep(d int64) bool {
	t := time.NewTimer(time.Duration(max(d, 0)))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.end:
		return false
	}
}

// after runs f on a goroutine of the run d from now, unless the run ended.
func (s *Sim) after(d int64, f func()) {
	s.wg.Go(func() {
		if s.sleep(d) {
			f()
		}
	})
}

// at runs f at time t of the run, unless the run ended.
func (s *Sim) at(t int64, f func()) { s.after(t-s.now(), f) }

// every runs f at exponential intervals of the given mean until healing.
func (s *Sim) every(mean int64, f func()) {
	s.wg.Go(func() {
		for {
			s.mu.Lock()
			d := s.exp(mean) - s.now()
			s.mu.Unlock()
			if !s.sleep(d) {
				return
			}
			f()
			s.mu.Lock()
			healed := s.healed
			s.mu.Unlock()
			if healed {
				return
			}
		}
	})
}

// once runs f at a uniform time in the first part of the faults, then
// again every retry until it reports true, unless the run healed.
func (s *Sim) once(within, retry int64, f func() bool) {
	s.at(1+s.rng.Int64N(within), func() {
		for !f() {
			s.mu.Lock()
			healed := s.healed
			s.mu.Unlock()
			if healed || !s.sleep(retry) {
				return
			}
		}
	})
}

// exp draws an exponential delay with the given mean, as a time. The lock
// is held.
func (s *Sim) exp(mean int64) int64 {
	return s.now() + int64(s.rng.ExpFloat64()*float64(mean)) + 1
}

// wakeNode wakes whatever waits on n's pause, freeze or stall. The lock
// is held.
func (s *Sim) wakeNode(n *node) {
	close(n.wake)
	n.wake = make(chan struct{})
}

// hold waits while until gives a time later than now, for a goroutine of
// process p. It returns once p died too.
func (s *Sim) hold(p *proc, until func() int64) {
	for {
		s.mu.Lock()
		t, wake := until(), p.n.wake
		s.mu.Unlock()
		d := t - s.now()
		if d <= 0 || p.dead() {
			return
		}
		timer := time.NewTimer(time.Duration(min(d, math.MaxInt64/2)))
		select {
		case <-timer.C:
		case <-wake:
		case <-p.ctx.Done():
		}
		timer.Stop()
	}
}

// paused is how long n's process holds still: until its pause ends, or
// for good while frozen. The lock is held.
func (n *node) paused() int64 {
	if n.frozen {
		return math.MaxInt64
	}
	return n.pausedUntil
}

// nodeConfig is the configuration of a process of n.
func (s *Sim) nodeConfig(n *node, p *proc) tnode.Config {
	cfg := core.DefaultConfig(n.id, []uint64{1, 2, 3})
	cfg.Join = n.id > 3
	for i := range 3 {
		cfg.Members = append(cfg.Members, &pb.Member{ID: uint64(i + 1), PeerURLs: []string{peerURL(i)}})
	}
	if s.cfg.Membership {
		cfg.Name = fmt.Sprintf("node%d", n.id)
	}
	// Progress notifications every 2 s instead of 10 minutes, so runs
	// see them.
	cfg.ProgressTicks = 20
	tinyLimits(&cfg)
	cfg.MemoryBytes = s.cfg.MemoryBytes
	tick := TickTime
	if d := s.cfg.Faults.DriftPerMille; d > 0 && !s.healed {
		tick += TickTime * (s.rng.Int64N(2*d+1) - d) / 1000
	}
	return tnode.Config{
		Core: cfg,
		Tick: time.Duration(tick),
		// Small segments, so that most of the log is in sealed ones,
		// where any damage is detectable.
		SegmentBytes: 8 << 10,
		BeforeSync:   func() { s.beforeSync(p) },
		// A value read only yields, as a disk call does. The node holds
		// its read lock there, which its close and a reload wait for,
		// and synctest moves no time while a goroutine waits on a lock,
		// so a read that waited for time could hang the run (seeds 256949
		// and 222975). So a pause does not hold value reads either.
		BeforeRead: s.yield,
		Turn:       func(c *core.Node, eff core.Effects, err error) { s.turn(p, c, eff, err) },
		Persisted:  func(j core.PersistJob) { s.persisted(p, j) },
		// Several nodes share the process, so none can measure its
		// memory; the value cache takes the whole budget.
		Memory: func(int64) (int64, bool) { return 0, false },
		Logf:   func(format string, args ...any) { s.logf(n, format, args...) },
	}
}

// logf adds a line of a node's log to the trace.
func (s *Sim) logf(n *node, format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracef("node %d: %s", n.id, fmt.Sprintf(format, args...))
}

// start starts a process on n from its disk, as a process start does.
func (s *Sim) start(n *node) error {
	s.mu.Lock()
	if n.proc != nil || n.starting || n.out() || s.ended {
		s.mu.Unlock()
		return nil
	}
	if n.rotted && !slices.Contains(n.disk.DurableNames(), n.rotFile) {
		// The damaged value's segment was removed with its other values.
		n.corrupted, n.rotted = false, false
	}
	n.starting = true
	// A process takes up to a tick to start, so that the nodes' ticks
	// fall apart, as on real machines.
	wait := s.rng.Int64N(TickTime)
	s.mu.Unlock()
	if !s.sleep(wait) {
		s.mu.Lock()
		n.starting = false
		s.mu.Unlock()
		return nil
	}
	s.mu.Lock()
	armed := s.armRecoveryCrash(n)
	ctx, cancel := context.WithCancel(context.Background())
	p := &proc{n: n, ctx: ctx, cancel: cancel}
	cfg := s.nodeConfig(n, p)
	s.tracef("node %d: starting", n.id)
	s.mu.Unlock()
	tn, err := tnode.NewOnFS(cfg, &disk{s: s, p: p})
	s.mu.Lock()
	defer s.mu.Unlock()
	n.starting = false
	if armed {
		n.disk.Fault = nil
	}
	if p.dead() || s.ended {
		cancel()
		return nil
	}
	if errors.Is(err, errRecoveryCrash) {
		// Crashed again while recovering; it starts over from its disk.
		s.stats.RecoveryCrashes++
		cancel()
		n.disk.Crash(s.rng)
		s.tracef("node %d: crashed during recovery", n.id)
		s.restartLater(n, 1+s.rng.Int64N(Second))
		return nil
	}
	if injected(err) {
		// An injected fault hit recovery, which can write: a failed boot.
		// The node tries again later from what its disk holds.
		s.stats.DiskFaults++
		cancel()
		n.disk.ClearFaults()
		n.disk.Crash(s.rng)
		s.tracef("node %d: failed to boot: %v", n.id, err)
		s.restartLater(n, 1+s.rng.Int64N(Second))
		return nil
	}
	if s.cfg.Membership && core.FailureOf(err) == core.Leave {
		cancel()
		n.inactive = true
		return nil
	}
	if n.corrupted {
		cancel()
		// The disk was damaged on purpose; the node must notice and stay
		// down, as an operator would replace the disk.
		if err == nil {
			return fmt.Errorf("node %d started on a damaged disk without noticing", n.id)
		}
		if !errors.Is(err, wal.ErrCorrupt) {
			return fmt.Errorf("node %d started on a damaged disk: %w", n.id, err)
		}
		n.fenced = true
		s.stats.Fenced++
		if n.servedDamage {
			s.stats.FencedAfterServe++
		}
		s.tracef("node %d: fenced: %v", n.id, err)
		return nil
	}
	if err != nil {
		cancel()
		return fmt.Errorf("node %d cannot recover its log: %w", n.id, err)
	}
	ln := s.net.listen(s, p)
	t, err := peer.Start(peer.Config{
		ID: n.id, Listeners: []net.Listener{ln}, Peers: s.peerAddrs(n),
		Certificate: s.net.cert, CA: s.net.ca,
		Deliver: s.deliverer(p, tn), Unreachable: tn.Unreachable, Serve: s.server(p, tn),
		Removed: tn.Removed, Answer: tn.Answer,
		Dial: func(ctx context.Context, addr string) (net.Conn, error) { return s.net.dial(ctx, s, p, addr) },
		Logf: func(format string, args ...any) { s.logf(n, format, args...) },
	})
	if err != nil {
		cancel()
		return err
	}
	p.node, p.t = tn, t
	n.proc = p
	s.procs.Go(func() {
		err := tn.Run(ctx, t)
		err = errors.Join(err, t.Close())
		s.exited(p, err)
	})
	return nil
}

// peerAddrs are the addresses of n's peers it starts with. The node hands
// its transport the members once it runs.
func (s *Sim) peerAddrs(n *node) map[uint64]string {
	out := map[uint64]string{}
	for _, m := range s.nodes {
		if m != n && !m.inactive {
			out[m.id] = m.addrs[0]
		}
	}
	return out
}

// injected reports whether err is a disk fault the run injected.
func injected(err error) bool {
	return errors.Is(err, simfs.ErrIO) || errors.Is(err, simfs.ErrNoSpace)
}

// restartLater starts n again d from now, unless it went down again or
// started meanwhile. The lock is held.
func (s *Sim) restartLater(n *node, d int64) {
	gen := n.gen
	s.after(d, func() {
		s.mu.Lock()
		again := n.gen == gen && n.proc == nil
		s.mu.Unlock()
		if again {
			if err := s.start(n); err != nil {
				s.mu.Lock()
				s.fail("%w", err)
				s.mu.Unlock()
			}
		}
	})
}

// exited handles the end of p's Run. A process the run took down ends as
// told; one that stopped on its own did so for a reason the run must
// expect.
func (s *Sim) exited(p *proc, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	s.tracef("node %d: stopped: %v", n.id, err)
	switch {
	case n.rotted && rotStop(n, err):
		// It read the value the run damaged and stopped instead of
		// answering with it.
		s.stopNode(n)
	case n.corrupted && core.FailureOf(err) == core.Reload && errors.Is(err, wal.ErrCorrupt):
		// It started again from its log after a snapshot install or a
		// failed fetch, and recovery found the damage and ended the
		// process, as a start on this disk refuses it. The restart
		// fences it.
		s.stopNode(n)
	case s.cfg.Membership && core.FailureOf(err) == core.Leave:
		n.inactive = true
		s.down(n)
	case errors.Is(err, core.ErrCommitStalled):
		s.stopNode(n)
	case injected(err):
		// Only injected faults may fail; the node stops at once and never
		// retries, as a failed fsync leaves the file in an unknown state.
		s.stats.DiskFaults++
		n.disk.Fault = nil
		s.stopNode(n)
	default:
		s.fail("node %d stopped: %w", n.id, err)
	}
}

// rotStop reports whether err is how a rotted node stops when it reads
// the damaged value: alone, where its checksum fails, or in the whole
// record of its entry, where the record's crc fails. Any other error
// fails the run, as on a node that was not rotted.
func rotStop(n *node, err error) bool {
	if errors.Is(err, mvcc.ErrBadValue) {
		return true
	}
	re, ok := errors.AsType[*wal.RecordError](err)
	return ok && re.Entry == n.rotEntry && re.Segment == n.rotSeg
}

// turn is the Turn hook of p's committer: it records what the core
// applied and checks it, then holds the process while it is paused.
func (s *Sim) turn(p *proc, c *core.Node, eff core.Effects, err error) {
	s.mu.Lock()
	n := p.n
	if n.proc != p {
		s.mu.Unlock()
		return
	}
	if p.core != c {
		if p.core != nil {
			// A snapshot install or a failed fetch started the core
			// again from the log; what the old one queued is gone.
			s.stats.Reloads++
			s.tracef("node %d: started again from its log", n.id)
		}
		p.core, n.pending, n.duringInstall = c, nil, nil
	}
	for _, a := range eff.Applied {
		s.record(n, a)
	}
	if err == nil && c.FirstIndex() > c.Applied()+1 {
		s.fail("node %d holds entries from %d in memory, but applied only to %d", n.id, c.FirstIndex(), c.Applied())
	}
	for _, j := range eff.Persist {
		pj := pendingJob{first: math.MaxUint64}
		if len(j.Entries) > 0 {
			pj.first = j.Entries[0].GetIndex()
		}
		if j.Install != nil {
			// The install may replace what the node holds after the
			// index it applied.
			pj.first, pj.install = j.Install.Applied+1, true
		}
		pj.replaces = pj.first <= n.durableLast
		n.pending = append(n.pending, pj)
		if j.Root != nil && n.rootDue == 0 && s.cfg.Faults.Checkpoint && s.stats.ErasedAtRoot == 0 {
			n.rootDue = j.Root.Index
		}
	}
	for _, id := range eff.Expired {
		if _, seen := s.expiredAt[id]; !seen {
			s.expiredAt[id] = s.now()
			s.expiredOrder = append(s.expiredOrder, id)
		}
	}
	s.applied = max(s.applied, c.Applied())
	if eff.HandOff != 0 {
		s.stats.StallHandOffs++
		s.stallHandOffs = append(s.stallHandOffs, s.now())
	}
	if errors.Is(err, core.ErrCommitStalled) {
		s.stats.CommitStops++
		if s.gray == n {
			s.stats.GrayLeaderStops++
		}
		s.commitStops = append(s.commitStops, s.now())
	}
	if s.rotDue == n {
		s.rotDue = nil
		s.rot(n, c)
	}
	s.mu.Unlock()
	// Once the run failed, the committer holds still until the run ends
	// its process: a core that broke a rule can go on to break Raft's
	// own, which panics (planted B20).
	s.mu.Lock()
	failed := s.err != nil
	s.mu.Unlock()
	if failed {
		<-p.ctx.Done()
		return
	}
	// A paused process holds still here, and so does a frozen one, which
	// tells the checks that it does.
	for {
		s.mu.Lock()
		if n.frozen && p.parked != nil {
			close(p.parked)
			p.parked = nil
		}
		until, wake := n.paused(), n.wake
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

// persisted is the Persisted hook of p's committer: it remembers how far
// the log the node reported durable reaches.
func (s *Sim) persisted(p *proc, j core.PersistJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := p.n
	if n.proc != p {
		return
	}
	if len(n.pending) > 0 {
		n.pending = n.pending[1:]
	}
	if j.Install != nil {
		n.durableLast = j.Install.Index
		c := j.Install.Checkpoint.Index
		if c > 0 && c < s.servedRoot[n.id] {
			s.stats.OlderCheckpoints++
		}
		for _, f := range n.duringInstall {
			if f != wal.CheckpointName(c) {
				s.stats.CheckpointsDuringInstall++
			}
		}
		n.duringInstall = nil
	}
	if j.Root != nil {
		n.root = j.Root.Index
	}
	if len(j.Entries) > 0 {
		n.durableLast = j.Entries[len(j.Entries)-1].GetIndex()
	}
}

// floor is how far recovery of n's disk must reach if it crashed now:
// what it reported durable, below any entries a job on its way replaces.
// The lock is held.
func (n *node) floor() uint64 {
	f := n.durableLast
	for _, j := range n.pending {
		if j.replaces || j.first <= f {
			f = min(f, j.first-1)
		}
	}
	return f
}

// beforeSync is the BeforeSync hook of p: a sync of the log takes 50 us
// to 1 ms, sometimes 20 to 100 ms, and 0.5 to 3 s on a gray node with
// slow fsyncs, and waits out a stall or a pause first.
func (s *Sim) beforeSync(p *proc) {
	n := p.n
	s.hold(p, func() int64 { return max(n.stallUntil, n.pausedUntil) })
	s.mu.Lock()
	lat := 50*Microsecond + s.rng.Int64N(950*Microsecond)
	if s.cfg.Faults.SlowDisk > 0 && !s.healed && s.rng.IntN(100) < s.cfg.Faults.SlowDisk {
		lat = 20*Millisecond + s.rng.Int64N(80*Millisecond)
	}
	gray := s.gray == n && n.graySync && !s.healed
	if gray {
		lat = max(lat, 500*Millisecond+s.rng.Int64N(2500*Millisecond))
	}
	s.mu.Unlock()
	t := time.NewTimer(time.Duration(lat))
	defer t.Stop()
	select {
	case <-t.C:
	case <-p.ctx.Done():
		return
	}
	if gray {
		s.mu.Lock()
		s.graySyncEnded()
		s.mu.Unlock()
	}
}

// crashRandom crashes a node that is up and restarts it up to MaxDown
// later.
func (s *Sim) crashRandom() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return
	}
	n := s.upNode()
	if n == nil {
		return
	}
	s.crash(n)
	s.maybeCorrupt(n)
	s.restartLater(n, 1+s.rng.Int64N(s.cfg.Faults.MaxDown))
}

// crash takes n down and drops what its disk did not sync. Recovery of
// what is left must reach what the node reported durable. The lock is
// held.
func (s *Sim) crash(n *node) {
	if n.proc == nil {
		return
	}
	s.stats.Crashes++
	floor := n.floor()
	s.down(n)
	n.disk.Crash(s.rng)
	s.tracef("node %d: crashed", n.id)
	w, rec, err := wal.Open(n.disk.Durable())
	if err != nil {
		// A damaged disk; the next start finds out.
		return
	}
	if err := w.Close(); err != nil {
		s.fail("%w", err)
		return
	}
	if rec.Last < floor {
		s.fail("node %d recovered entries to %d, but reported %d durable", n.id, rec.Last, floor)
		return
	}
	n.durableLast = rec.Last
}

// down takes n's process down: its goroutines, disk calls and
// connections fail, and its clients and watchers lose their connections.
// The lock is held.
func (s *Sim) down(n *node) {
	p := n.proc
	if p == nil {
		return
	}
	n.proc, n.pending, n.duringInstall = nil, nil, nil
	n.gen++
	// A crash ends a pause.
	n.pausedUntil = 0
	p.cancel()
	s.wakeNode(n)
	s.net.cut(n)
}

// stopNode stops a node like a crash and restarts it later from its disk.
// The lock is held.
func (s *Sim) stopNode(n *node) {
	if s.healed && s.now() <= s.recovered() {
		// The guaranteed disk fault fired after healing, or a watchdog
		// whose wait began before it. The liveness rule starts once the
		// node is back (check.go); the clients keep going for 3 s more
		// than that, so the rule has calls to check. Only a stop before
		// the rule would have started counts, so the bound stays fixed:
		// a node that keeps stopping later fails the rule. A run with a
		// node fenced and another gray past healing may write nothing,
		// and so not fire the fault, until then (seed 78783).
		s.lateStopAt = s.now()
		s.extendLiveness()
	}
	s.crash(n)
	s.restartLater(n, 1+s.rng.Int64N(max(s.cfg.Faults.MaxDown, Second)))
}

// changeLinks changes the set of links that are up, at random times,
// until healing.
func (s *Sim) changeLinks() {
	every := s.cfg.Faults.LinkEvery
	s.mu.Lock()
	next := min(s.exp(every), 3*every)
	s.mu.Unlock()
	for s.sleep(next - s.now()) {
		s.mu.Lock()
		if s.healed {
			s.mu.Unlock()
			return
		}
		s.stats.LinkChanges++
		// Half of the changes heal every link; the rest pick a random set
		// of directed links, which covers partitions and one-way links.
		mask := s.allLinks()
		if s.rng.IntN(2) == 0 {
			mask = s.rng.Uint64() & mask
		}
		s.setLinks(mask)
		// At most three times the mean, so that one random partition
		// cannot hold through most of the faults; TestAllLinkMasks holds
		// each mask.
		next = min(s.exp(every), s.now()+3*every)
		s.mu.Unlock()
	}
}

func (s *Sim) allLinks() uint64 { return 1<<uint(len(s.links)*(len(s.links)-1)) - 1 }

// setLinks sets the directed links between nodes from a mask:
// bit k is the k-th pair (i, j) with i != j in order. The lock is held.
func (s *Sim) setLinks(mask uint64) {
	s.tracef("links %0*b", len(s.links)*(len(s.links)-1), mask)
	k := 0
	for i := range s.links {
		for j := range s.links[i] {
			if i == j {
				continue
			}
			l := &s.links[i][j]
			l.up = mask&(1<<k) != 0
			// Each change draws new delays per link: a base of 20 us to
			// 2 ms and jitter up to the base, so delays switch and differ
			// by direction.
			l.base = 20*Microsecond + s.rng.Int64N(2*Millisecond)
			l.jitter = s.rng.Int64N(l.base + 1)
			k++
		}
	}
	for i := range s.links {
		for j := range i {
			if a, b := s.links[i][j], s.links[j][i]; !a.up || !b.up {
				s.net.resetBetween(i, j)
			}
		}
	}
	for i := range s.links {
		for j := range i {
			a, b := s.links[i][j].base, s.links[j][i].base
			if !s.healed && (a >= 10*b || b >= 10*a) {
				s.stats.Asymmetric++
			}
		}
	}
}

// heal ends the faults: every link comes up, half-open links and disk
// stalls end, and every node that is down starts.
func (s *Sim) heal() {
	s.mu.Lock()
	s.healed = true
	s.tracef("healing")
	if !s.grayOnce || s.grayDropped {
		s.gray, s.grayOnce = nil, false
	}
	s.setLinks(s.allLinks())
	// Link stalls and pauses end by healing, as they are cut to it.
	// Half-open links end now, with their connections.
	for i := range s.links {
		for j := range s.links[i] {
			if s.links[i][j].halfOpenUntil > s.now() {
				s.links[i][j].halfOpenUntil = 0
				s.net.resetBetween(i, j)
			}
		}
	}
	var down []*node
	for _, n := range s.nodes {
		if n.stallUntil > s.now() && len(n.stalls) > 0 {
			n.stalls[len(n.stalls)-1][1] = s.now()
		}
		n.stallUntil = 0
		s.wakeNode(n)
		n.disk.ClearFaults()
		if n.proc == nil && !n.fenced {
			down = append(down, n)
		}
	}
	s.mu.Unlock()
	for _, n := range down {
		if err := s.start(n); err != nil {
			s.mu.Lock()
			s.fail("%w", err)
			s.mu.Unlock()
			return
		}
	}
}

func (s *Sim) clientLatency() int64 {
	return 20*Microsecond + s.rng.Int64N(300*Microsecond)
}

var (
	errClientTimeout = errors.New("sim: client timed out")
	errConnLost      = errors.New("sim: connection lost")
	errConnRefused   = errors.New("sim: connection refused")
)

// freeze holds every running node still at its next turn, for the checks
// at the end, and waits until each does.
func (s *Sim) freeze() error {
	s.mu.Lock()
	var parked []chan struct{}
	var procs []*proc
	for _, n := range s.nodes {
		n.frozen = true
		s.wakeNode(n)
		if p := n.proc; p != nil {
			p.parked = make(chan struct{})
			parked = append(parked, p.parked)
			procs = append(procs, p)
		}
	}
	s.mu.Unlock()
	for i, c := range parked {
		select {
		case <-c:
		case <-procs[i].ctx.Done():
		}
	}
	return s.failure()
}

// thaw lets the frozen nodes go on.
func (s *Sim) thaw() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		n.frozen = false
		s.wakeNode(n)
	}
}

// shutdown ends the run: every process goes down and every goroutine of
// the run ends.
func (s *Sim) shutdown() {
	s.mu.Lock()
	s.stop()
	for _, n := range s.nodes {
		s.down(n)
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.procs.Wait()
}
