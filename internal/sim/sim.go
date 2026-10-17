// Package sim runs the real core of a cluster in one process, on
// simulated disks, links and clocks, from one seed.
//
// Everything that happens is an event in one queue ordered by virtual time.
// Only the seeded random source decides delays, faults and the workload, so
// one seed always produces the same run. Raft's own randomness comes from
// crypto/rand, which the test makes deterministic with
// testing/cryptotest.SetGlobalRandom. Every event goes into a trace hash;
// two runs of one seed must end with the same hash.
package sim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Times are virtual nanoseconds.
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
	// links that are up. A link that goes down drops what it carries.
	LinkEvery int64
	// MaxDelay is the largest one-way delay of a message.
	MaxDelay int64
	// SlowDisk is the chance in percent that one disk write takes 20 to
	// 100 ms instead of under 1 ms.
	SlowDisk int
	// DiskErrorEvery is the mean time between failed writes or syncs:
	// EIO on a write or a sync, or a short write with ENOSPC. The node
	// stops, never retries, and restarts from its disk.
	DiskErrorEvery int64
	// StallEvery is the mean time between disk stalls of 1.5 to 4 s.
	StallEvery int64
	// Corrupt damages one node's synced bytes at a crash, once per run
	// and only sometimes; that node must refuse to start.
	Corrupt bool
	// Checkpoint takes the erase-one-disk checkpoint during the faults.
	Checkpoint bool
	// LinkStallEvery is the mean time between stalls of 0.5 to 2 s on one
	// directed link: everything on it waits, in order.
	LinkStallEvery int64
	// PauseEvery is the mean time between process pauses of 0.2 to 2 s.
	PauseEvery int64
	// DriftPerMille is the most a node's clock runs fast or slow, per
	// mille; ClockJumpEvery the mean time between forward clock jumps of
	// up to 3 ticks on one node. Together they are the declared clock
	// envelope, which the lease lower bound allows for.
	DriftPerMille  int64
	ClockJumpEvery int64
	// HalfOpenEvery is the mean time between half-open links: one
	// directed link swallows everything for 0.5 to 5 s, the peer write
	// timeout, and then the sender's connection fails and Raft hears the
	// peer is unreachable.
	HalfOpenEvery int64
	// GrayEvery is the mean time between gray periods of 2 to 6 s, in
	// which one node at a time is gray: Raft heartbeats and votes get
	// through, but appends to and from it do not, and in half of them its
	// fsyncs take 0.5 to 3 s. The gray node can change every 1 to 3 s.
	GrayEvery int64
	// GrayClient adds a client that opens a stream with 10 watches, never
	// reads it, and reconnects every 50 to 300 ms until healing.
	GrayClient bool
	// RecoveryCrash crashes one restart in three again at its first to
	// fourth writing disk operation inside recovery.
	RecoveryCrash bool
	// StepCrash crashes a node once at a step of a checkpoint (after a
	// part is written, after the file's sync, after the directory's sync,
	// after the root) or of a removal, or inside a snapshot install.
	StepCrash bool
	// FlipSnapshot damages the first snapshot stream, and one in four
	// after it, on its way.
	FlipSnapshot bool
	// FetchCrash crashes the node that serves the first snapshot stream
	// of a run partway through it.
	FetchCrash bool
	// SlowFetch starts half the snapshot fetches during the faults 0.2 to
	// 2 s late, as a busy disk goroutine would, so that the leader may
	// first make a newer checkpoint durable, or give up on the snapshot
	// and remove what it needs.
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
			ClockJumpEvery: 6 * Second,
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

type kind int

const (
	evTick kind = iota
	evDeliver
	evDiskDone
	evCrash
	evCrashOnce
	evLinksOnce
	evDiskFault
	evDiskFaultOnce
	evStall
	evStallLeader
	evDownFollower
	evCheckpoint
	evLinkStall
	evLinkStallOnce
	evPause
	evPauseOnce
	evResume
	evClockJump
	evClockJumpOnce
	evHalfOpen
	evHalfOpenOnce
	evHalfOpenAt
	evHalfOpenEnd
	evSwitch
	evHoldMask
	evStreamClose
	evGray
	evGrayOnce
	evGraySwitch
	evGrayLeader
	evGrayConnect
	evGrayCreate
	evRecoveryDue
	evStepCrashDue
	evRot
	evCheckpointSync
	evSnapshotSent
	evFetch
	evRestart
	evLinks
	evHeal
	evClientStart
	evSubmit
	evClientReply
	evClientTimeout
	evWatchOpen
	evWatchCreate
	evWatchResponse
	evWatchBroken
	evWatchDrop
	evValuesRead
	evMembership
	evRemoved
	evMemberConnect
)

type event struct {
	at   int64
	seq  uint64
	kind kind
	node int
	gen  uint64 // node generation or link epoch the event belongs to
	from int
	// stream is the watch stream of an evStreamClose; number is the
	// message's place on its link.
	stream, number uint64
	data           []byte
	jobs           []core.PersistJob
	op             *clientOp
	watch          *watchClient
	resp           proto.Message
	err            error
	// grayDisk marks an fsync slowed by a gray fault. Its delay can
	// outlast healing even after the gray network fault has ended.
	grayDisk bool
	// ckpt is the checkpoint file that an evCheckpointSync syncs.
	ckpt wal.File
	// read is the value read an evValuesRead runs.
	read *core.ValueRead
}

// queue is a binary min-heap of events by time, then sequence number.
type queue []*event

func (q queue) less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q *queue) push(e *event) {
	*q = append(*q, e)
	h := *q
	for i := len(h) - 1; i > 0; {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func (q *queue) pop() *event {
	h := *q
	top := h[0]
	last := len(h) - 1
	h[0] = h[last]
	h[last] = nil
	h = h[:last]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(h) && h.less(l, m) {
			m = l
		}
		if r < len(h) && h.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
	*q = h
	return top
}

type node struct {
	idx  int
	id   uint64
	disk *simfs.Disk
	log  *wal.WAL
	core *core.Node
	up   bool
	// gen counts restarts. Events of an earlier generation are dropped.
	gen uint64

	diskQ    []core.PersistJob
	diskBusy bool

	nextReq     uint64
	reqs        map[uint64]*clientOp
	outstanding []*clientOp

	nextStream uint64
	streams    map[uint64]*watchClient
	// closing are streams the client left that the node has not yet
	// handled the close of; what it sends on them goes nowhere.
	closing map[uint64]bool

	// graySync makes its fsyncs slow while it is gray.
	graySync bool
	// grayStreams are the open streams of the gray client on this node.
	grayStreams map[uint64]bool
	// tickEvery is this node's tick interval, off by up to the clock drift;
	// pausedUntil holds the whole process back, and held keeps its events
	// in order until it resumes.
	tickEvery, pausedUntil int64
	held                   []*event
	lastJump               int64
	// stallUntil holds back disk writes until then; stalls keeps every
	// stall window, for the liveness rule.
	stallUntil int64
	stalls     [][2]int64
	// durableLast is the last entry index of the last append the node
	// reported durable to its core; recovery must give at least that.
	durableLast uint64
	// syncing is the event that ends the disk write in flight.
	syncing *event
	// corrupted is set when the run damaged the disk on purpose; fenced
	// when the node then refused to start and stays down.
	corrupted, fenced bool
	// inactive is a member not yet added, or a process stopped because
	// it was removed. Cold recovery must still read that process's disk.
	inactive bool
	// inflight are the jobs of the disk write under way. replaceFrom is the
	// lowest index at which they replace entries already reported durable,
	// or 0: Raft may replace acknowledged entries that never committed.
	inflight    []core.PersistJob
	replaceFrom uint64
	// recoverFloor is how far recovery must reach after the last crash.
	recoverFloor uint64
	// runs counts the node's starts (core.Config.Run).
	runs uint64
	// ckptFile is the checkpoint being written, at ckptIndex; root is the
	// index of the newest synced root, and roots are the synced roots
	// whose checkpoint files are on disk, for the snapshots it serves.
	ckptFile   wal.File
	ckptIndex  uint64
	ckptSyncAt int64
	root       uint64
	roots      []wal.Root
	// rotted is set when the run flipped a bit of a value of entry
	// rotEntry, in segment rotSeg, file rotFile, on the disk of this
	// running node.
	rotted           bool
	rotFile          string
	rotEntry, rotSeg uint64
}

type link struct {
	up    bool
	epoch uint64
	last  int64
	// base and jitter give the link's own delay, so links differ and one
	// direction can be much slower than the other; stallUntil holds
	// everything on the link back, like a stalled TCP stream.
	base, jitter int64
	stallUntil   int64
	// halfOpenUntil swallows what the link carries until the sender's
	// connection fails.
	halfOpenUntil int64
	// sent numbers the messages put on the link; delivered is the number
	// of the last one that arrived, which only grows.
	sent, delivered uint64
}

// Sim is one run.
type Sim struct {
	cfg Config
	rng *rand.Rand
	// readRng times value reads, apart from rng, so that they do not move
	// every fault that rng draws after them.
	readRng *rand.Rand
	now     int64
	seq     uint64
	q       queue
	nodes   []*node
	links   [][]link
	trace   hash.Hash64

	healAt, stopAt, endAt int64
	healed                bool
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
	// recoveryDue crashes the next node that writes entries before it
	// syncs them, until a recovery crashed.
	recoveryDue bool
	// stepCrashDue crashes a node at its next checkpoint, removal or
	// install step, until one did.
	stepCrashDue bool
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
	// diskFaultDue arms a disk fault at the next append with entries,
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
	Events, Messages, Dropped, DiskWrites int
	Crashes, LinkChanges                  int
	OK, Failed, Ambiguous                 int
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
	// Pauses counts process pauses, LinkStalls stalled links, ClockJumps
	// clock jumps, HalfOpens half-open links, and Asymmetric the links
	// that drew a delay at least 10 times that of the other direction.
	Pauses, LinkStalls, ClockJumps, HalfOpens, Asymmetric int
	// GrayNodes counts gray nodes that lost an append, CommitStops leaders
	// that stopped because their entries stopped committing, GrayStreams
	// the streams with watches a gray client opened, and RecoveryCrashes
	// crashes inside the recovery of an existing log.
	GrayNodes, CommitStops, GrayStreams, RecoveryCrashes int
	// GrayLeaderStops counts commit-watchdog stops of a gray leader.
	GrayLeaderStops int
	// Checkpoints counts checkpoints written, Snapshots snapshot streams
	// sent, CheckpointSnapshots those that carried a checkpoint because
	// the entries were gone, OlderCheckpoints those whose checkpoint was
	// older than the source's newest root, ServeFailures fetches that the
	// source could not serve because it no longer had what the snapshot
	// needs, SnapshotFlips those damaged on the way, FetchCrashes sources
	// that crashed during a stream, SlowFetches fetches that started late,
	// InstallFailures installs that failed, each followed by a reload,
	// StepCrashes crashes at a checkpoint, removal or install step, Rots
	// values damaged under a running node, and CheckpointsDuringInstall
	// checkpoints finished while an install was on its way to disk (#182).
	Checkpoints, Snapshots, CheckpointSnapshots, OlderCheckpoints, ServeFailures, SnapshotFlips, FetchCrashes, SlowFetches, InstallFailures, StepCrashes, Rots int
	// ErasedAtRoot and ErasedAtRemoval count the erase-one-disk copies
	// taken right after a root synced and right after a segment removal.
	ErasedAtRoot, ErasedAtRemoval int
	CheckpointsDuringInstall      int
	// MembershipChanges counts each completed stage, and MembershipFaults
	// those completed before healing. Attempts include failed calls.
	MembershipChanges, MembershipFaults [4]int
	MembershipAttempts                  int
}

// Result is the outcome of a run.
type Result struct {
	// Trace is the hash of every event.
	Trace   uint64
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
// rule.
func Run(cfg Config) (*Result, error) {
	_, res, err := runWithState(cfg)
	return res, err
}

func runWithState(cfg Config) (*Sim, *Result, error) {
	s := newSim(cfg)
	for i := range 3 {
		s.addNode(i, simfs.New())
		if err := s.start(s.nodes[i]); err != nil {
			return s, nil, err
		}
	}
	if cfg.Membership {
		s.addNode(3, simfs.New())
		s.nodes[3].inactive = true
		s.push(&event{at: 2 * Second, kind: evMembership})
	}
	for i := range cfg.Clients {
		c := &client{id: i, sim: s}
		s.clients = append(s.clients, c)
		s.push(&event{at: s.rng.Int64N(100 * Millisecond), kind: evClientStart, op: &clientOp{client: c}})
	}
	s.startWatchers()
	if cfg.Faults.CrashEvery > 0 {
		s.push(&event{at: s.exp(cfg.Faults.CrashEvery), kind: evCrash})
		// One crash at a uniform time, so that no run goes without.
		s.push(&event{at: 1 + s.rng.Int64N(cfg.Duration), kind: evCrashOnce})
	}
	if cfg.Faults.LinkEvery > 0 {
		s.push(&event{at: min(s.exp(cfg.Faults.LinkEvery), 3*cfg.Faults.LinkEvery), kind: evLinks})
		// One change at a uniform time, so that no run goes without.
		s.push(&event{at: 1 + s.rng.Int64N(cfg.Duration), kind: evLinksOnce})
	}
	s.scheduleDiskFaults()
	s.scheduleNetFaults()
	s.scheduleGrayFaults()
	if cfg.HalfOpenAt > 0 {
		s.push(&event{at: cfg.HalfOpenAt, kind: evHalfOpenAt})
	}
	if cfg.StallLeaderAt > 0 {
		s.push(&event{at: cfg.StallLeaderAt, kind: evStallLeader})
	}
	s.push(&event{at: s.healAt, kind: evHeal})
	if err := s.loop(); err != nil {
		return s, nil, err
	}
	res := &Result{Trace: s.trace.Sum64(), History: s.history, Stats: s.stats, StallHandOffs: s.stallHandOffs, CommitStops: s.commitStops, grayNode: s.lastGray, grayHealedAt: s.grayHealedAt, halfOpens: s.halfOpens, Clients: cfg.Clients}
	if err := s.check(res); err != nil {
		return s, res, fmt.Errorf("seed %d: %w", cfg.Seed, err)
	}
	return s, res, nil
}

// newSim sets up an empty run.
func newSim(cfg Config) *Sim {
	s := &Sim{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x7469676572)),
		readRng:   rand.New(rand.NewPCG(cfg.Seed, 0x72656164)),
		trace:     fnv.New64a(),
		expiredAt: map[int64]int64{},
		log:       map[uint64]logRec{},
		ckpts:     map[uint64]ckptRec{},
		healAt:    cfg.Duration,
		stopAt:    cfg.Duration + cfg.Heal,
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
	return s
}

func (s *Sim) addNode(i int, disk *simfs.Disk) {
	s.nodes = append(s.nodes, &node{idx: i, id: uint64(i + 1), disk: disk, reqs: map[uint64]*clientOp{}, streams: map[uint64]*watchClient{}, closing: map[uint64]bool{}, grayStreams: map[uint64]bool{}})
}

// loop runs events until the end.
func (s *Sim) loop() error {
	// The first event past the end stays queued, so a later call can go
	// on from where this one stopped.
	for len(s.q) > 0 && s.err == nil && s.q[0].at <= s.endAt {
		e := s.q.pop()
		s.now = e.at
		s.stats.Events++
		s.mix(uint64(e.at), uint64(e.kind), uint64(e.node), uint64(e.from))
		s.handle(e)
	}
	if s.err != nil {
		return fmt.Errorf("seed %d at %d ms: %w", s.cfg.Seed, s.now/Millisecond, s.err)
	}
	return nil
}

func (s *Sim) push(e *event) {
	s.seq++
	e.seq = s.seq
	s.q.push(e)
}

// exp draws an exponential delay with the given mean.
func (s *Sim) exp(mean int64) int64 {
	return s.now + int64(s.rng.ExpFloat64()*float64(mean)) + 1
}

func (s *Sim) mix(vs ...uint64) {
	var b [8]byte
	for _, v := range vs {
		binary.LittleEndian.PutUint64(b[:], v)
		s.trace.Write(b[:])
	}
}

func (s *Sim) mixBytes(b []byte) {
	s.mix(uint64(len(b)))
	s.trace.Write(b)
}

func (s *Sim) fail(format string, args ...any) {
	if s.err == nil {
		s.err = fmt.Errorf(format, args...)
	}
}

func (s *Sim) handle(e *event) {
	if n := s.target(e); n != nil && (n.pausedUntil > s.now || len(n.held) > 0) {
		// A paused process sees nothing until it resumes; then it gets
		// everything at once, in order.
		n.held = append(n.held, e)
		return
	}
	switch e.kind {
	case evTick:
		n := s.nodes[e.node]
		if !n.up || n.gen != e.gen {
			return
		}
		n.core.Tick()
		s.process(n)
		s.push(&event{at: s.now + n.tickEvery, kind: evTick, node: n.idx, gen: n.gen})
	case evDeliver:
		s.deliver(e)
	case evMemberConnect:
		n := s.nodes[e.node]
		if n.up && n.gen == e.gen {
			s.memberConnect(n)
			s.push(&event{at: s.now + Second, kind: evMemberConnect, node: n.idx, gen: n.gen})
		}
	case evRemoved:
		n := s.nodes[e.node]
		l := &s.links[e.from][e.node]
		if n.up && l.up && l.epoch == e.gen && l.halfOpenUntil <= s.now {
			if l.stallUntil > s.now {
				e.at = l.stallUntil
				s.push(e)
				return
			}
			n.core.Removed()
			s.process(n)
		}
	case evDiskDone:
		s.diskDone(e)
	case evCrash:
		s.crashRandom()
		if !s.healed {
			s.push(&event{at: s.exp(s.cfg.Faults.CrashEvery), kind: evCrash})
		}
	case evCrashOnce:
		s.crashRandom()
	case evDiskFault:
		s.injectDiskFault()
		if !s.healed {
			s.push(&event{at: s.exp(s.cfg.Faults.DiskErrorEvery), kind: evDiskFault})
		}
	case evDiskFaultOnce:
		// Armed on the next disk write, since a node picked now may write
		// nothing until healing. Again every 500 ms until one has fired:
		// an armed fault can stay unused, on a node that is then fenced.
		if s.stats.DiskFaults == 0 && !s.healed {
			s.diskFaultDue = true
			s.push(&event{at: s.now + 500*Millisecond, kind: evDiskFaultOnce})
		}
	case evStall:
		s.injectStall()
	case evStallLeader:
		s.stallLeader()
	case evDownFollower:
		s.downFollower()
	case evCheckpoint:
		s.takeCheckpoint()
	case evLinkStall:
		s.stallLink()
		s.again(e, s.cfg.Faults.LinkStallEvery)
	case evLinkStallOnce:
		s.retry(e, s.stallLink())
	case evPause:
		s.pause()
		s.again(e, s.cfg.Faults.PauseEvery)
	case evPauseOnce:
		s.retry(e, s.pause())
	case evResume:
		s.resume(s.nodes[e.node])
	case evClockJump:
		s.jumpClock()
		s.again(e, s.cfg.Faults.ClockJumpEvery)
	case evClockJumpOnce:
		s.retry(e, s.jumpClock())
	case evHalfOpen:
		s.halfOpen(-1, 0)
		s.again(e, s.cfg.Faults.HalfOpenEvery)
	case evHalfOpenOnce:
		s.retry(e, s.halfOpen(-1, 0))
	case evHalfOpenAt:
		s.halfOpen(s.leaderIdx(), halfOpenTimeout)
	case evHalfOpenEnd:
		s.halfOpenEnd(e)
	case evSwitch:
		s.switchLinks()
	case evHoldMask:
		if !s.healed {
			s.stats.LinkChanges++
			s.setLinks(s.cfg.Faults.HoldMask)
		}
	case evGray:
		s.startGray()
		s.again(e, s.cfg.Faults.GrayEvery)
	case evGrayOnce:
		// One gray period that lasts, past healing if need be, until its
		// gray node lost an append: clients may write nothing for a while.
		if s.stats.GrayNodes == 0 && !s.healed {
			// A gray period already on becomes the guaranteed one.
			s.grayOnce = s.gray != nil || s.startGray()
			s.push(&event{at: s.now + 500*Millisecond, kind: evGrayOnce})
		}
	case evGraySwitch:
		s.switchGray()
	case evGrayLeader:
		s.grayLeader()
	case evGrayConnect:
		s.grayConnect()
	case evGrayCreate:
		s.grayCreate(e)
	case evStepCrashDue:
		if s.stats.StepCrashes == 0 && !s.healed {
			s.stepCrashDue = true
			s.push(&event{at: s.now + 500*Millisecond, kind: evStepCrashDue})
		}
	case evRot:
		if !s.rot() {
			s.push(&event{at: s.now + 500*Millisecond, kind: evRot})
		}
	case evCheckpointSync:
		s.checkpointSynced(e)
	case evSnapshotSent:
		if n := s.nodes[e.node]; n.up && n.gen == e.gen {
			n.core.SnapshotSent(s.nodes[e.from].id, e.err)
			s.process(n)
		}
	case evRecoveryDue:
		// Until a recovery crashed: most crashes leave no torn tail, and
		// a clean log needs no writes to recover.
		if s.stats.RecoveryCrashes == 0 && !s.healed {
			s.recoveryDue = true
			s.push(&event{at: s.now + 500*Millisecond, kind: evRecoveryDue})
		}
	case evStreamClose:
		if n := s.nodes[e.node]; n.up && n.gen == e.gen {
			delete(n.closing, e.stream)
			n.core.CloseWatchStream(e.stream)
			s.process(n)
		}
	case evFetch:
		if n := s.nodes[e.node]; n.up && n.gen == e.gen {
			s.writeJobs(n, e.jobs)
		}
	case evRestart:
		n := s.nodes[e.node]
		if !n.up && n.gen == e.gen {
			if err := s.start(n); err != nil {
				s.fail("%w", err)
			}
		}
	case evLinks:
		s.changeLinks()
	case evLinksOnce:
		if !s.healed {
			s.stats.LinkChanges++
			// Any mask but all links up.
			s.setLinks(s.rng.Uint64N(s.allLinks()))
		}
	case evHeal:
		s.heal()
	case evClientStart:
		e.op.client.start()
	case evSubmit:
		s.submit(e)
	case evClientReply:
		s.complete(e.op, e.resp, e.err)
	case evClientTimeout:
		s.complete(e.op, nil, errClientTimeout)
	case evMembership:
		s.changeMembership()
	case evWatchOpen:
		e.watch.openWatch()
	case evWatchCreate:
		e.watch.create(s.nodes[e.node], e.gen)
	case evWatchResponse:
		e.watch.receive(e)
	case evWatchBroken:
		e.watch.broken(e)
	case evWatchDrop:
		e.watch.drop()
	case evValuesRead:
		if n := s.nodes[e.node]; n.up && n.gen == e.gen {
			n.core.ValuesRead(e.read, e.read.Run())
			s.process(n)
		}
	}
}

// start opens a node from its disk, as a process start does.
func (s *Sim) start(n *node) error {
	if n.fenced || n.inactive {
		return nil
	}
	if n.rotted && !slices.Contains(n.disk.DurableNames(), n.rotFile) {
		// The damaged value's segment was removed with its other values.
		n.corrupted, n.rotted = false, false
	}
	armed := s.armRecoveryCrash(n)
	w, rec, err := wal.Open(n.disk)
	if armed {
		n.disk.Fault = nil
	}
	if errors.Is(err, errRecoveryCrash) {
		// Crashed again while recovering; it starts over from its disk.
		s.stats.RecoveryCrashes++
		n.disk.Crash(s.rng)
		s.mix(0xcc, uint64(n.idx))
		s.push(&event{at: s.now + 1 + s.rng.Int64N(Second), kind: evRestart, node: n.idx, gen: n.gen})
		return nil
	}
	if errors.Is(err, simfs.ErrIO) || errors.Is(err, simfs.ErrNoSpace) {
		// An injected fault hit recovery, which can write: a failed boot.
		// The node tries again later from what its disk holds.
		s.stats.DiskFaults++
		n.disk.ClearFaults()
		n.disk.Crash(s.rng)
		s.mix(0xfb, uint64(n.idx))
		s.push(&event{at: s.now + 1 + s.rng.Int64N(Second), kind: evRestart, node: n.idx, gen: n.gen})
		return nil
	}
	var c *core.Node
	if err == nil {
		if rec.Last < n.recoverFloor {
			return fmt.Errorf("node %d recovered entries to %d, but reported %d durable", n.id, rec.Last, n.recoverFloor)
		}
		// Small segments, so that most of the log is in sealed ones,
		// where any damage is detectable.
		w.SetSegmentBytes(8 << 10)
		cfg := core.DefaultConfig(n.id, []uint64{1, 2, 3})
		cfg.Join = n.id > 3
		if s.cfg.Membership {
			cfg.Name = fmt.Sprintf("node%d", n.id)
		}
		// The run number lives with the node, as the real node keeps it on
		// a disk that survives what the simulator does to the log.
		n.runs++
		cfg.Boot, cfg.Run = s.rng.Uint64(), n.runs
		// Progress notifications every 2 s instead of 10 minutes, so runs
		// see them.
		cfg.ProgressTicks = 20
		tinyLimits(&cfg)
		cfg.MemoryBytes = s.cfg.MemoryBytes
		c, err = core.New(cfg, rec, w)
	}
	if s.cfg.Membership && errors.Is(err, core.ErrRemoved) {
		n.inactive = true
		if w != nil {
			return w.Close()
		}
		return nil
	}
	if n.corrupted {
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
		s.mix(0xfe, uint64(n.idx))
		return nil
	}
	if err != nil {
		return fmt.Errorf("node %d cannot recover its log: %w", n.id, err)
	}
	// What recovery returned is now durable, and nothing is in flight.
	n.durableLast, n.recoverFloor, n.inflight, n.replaceFrom = rec.Last, 0, nil, 0
	n.root, n.roots, n.ckptFile = rec.Root.Index, nil, nil
	if rec.Root.Index > 0 {
		n.roots = []wal.Root{rec.Root}
	}
	n.gen++
	n.log, n.core, n.up = w, c, true
	n.tickEvery = TickTime
	if d := s.cfg.Faults.DriftPerMille; d > 0 && !s.healed {
		n.tickEvery += TickTime * (s.rng.Int64N(2*d+1) - d) / 1000
	}
	n.diskQ, n.diskBusy, n.syncing = nil, false, nil
	s.mix(0xb007, uint64(n.idx), rec.Last, rec.HardState.Commit)
	s.push(&event{at: s.now + s.rng.Int64N(TickTime) + 1, kind: evTick, node: n.idx, gen: n.gen})
	if s.cfg.Membership && n.id > 3 {
		s.push(&event{at: s.now + Second, kind: evMemberConnect, node: n.idx, gen: n.gen})
	}
	s.process(n)
	return nil
}

// process runs the node's core and carries out its effects.
func (s *Sim) process(n *node) {
	if n.pausedUntil > s.now {
		s.fail("node %d works while paused, at %d ms", n.id, s.now/Millisecond)
	}
	n.core.CopyCheckpoint()
	eff, err := n.core.Process()
	for _, a := range eff.Applied {
		s.record(n, a)
	}
	if n.rotted && rotStop(n, err) {
		// It read the value the run damaged and stopped instead of
		// answering with it.
		s.stopNode(n)
		return
	}
	if err == nil && n.core.FirstIndex() > n.core.Applied()+1 {
		s.fail("node %d holds entries from %d in memory, but applied only to %d", n.id, n.core.FirstIndex(), n.core.Applied())
		return
	}
	n.diskQ = append(n.diskQ, eff.Persist...)
	for _, m := range eff.Send {
		s.send(n, m)
	}
	for _, r := range eff.Replies {
		s.replied(n, r)
	}
	for _, r := range eff.Reads {
		// A helper reads the values, each read in its own time, so that
		// they finish out of order.
		s.push(&event{at: s.now + 50*Microsecond + s.readRng.Int64N(950*Microsecond), kind: evValuesRead, node: n.idx, gen: n.gen, read: r})
	}
	for _, r := range eff.Watch {
		s.watchResponse(n, r)
	}
	for _, id := range eff.Closed {
		s.streamClosed(n, id)
	}
	if len(eff.Checkpoint) > 0 && err == nil {
		s.writeCheckpoint(n, eff.Checkpoint)
		if !n.up {
			return
		}
	}
	for _, id := range eff.Expired {
		if _, seen := s.expiredAt[id]; !seen {
			s.expiredAt[id] = s.now
			s.expiredOrder = append(s.expiredOrder, id)
		}
	}
	s.applied = max(s.applied, n.core.Applied())
	if eff.HandOff != 0 {
		s.stats.StallHandOffs++
		s.stallHandOffs = append(s.stallHandOffs, s.now)
	}
	if errors.Is(err, core.ErrRemoved) && s.cfg.Membership {
		n.inactive = true
		s.down(n)
		return
	}
	if errors.Is(err, core.ErrCommitStalled) {
		s.stats.CommitStops++
		if s.gray == n {
			s.stats.GrayLeaderStops++
		}
		s.commitStops = append(s.commitStops, s.now)
		s.stopNode(n)
		return
	}
	if err != nil {
		s.fail("node %d stopped: %w", n.id, err)
		return
	}
	if !n.diskBusy && len(n.diskQ) > 0 {
		s.startDisk(n)
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
	var re *wal.RecordError
	return errors.As(err, &re) && re.Entry == n.rotEntry && re.Segment == n.rotSeg
}

// startDisk writes every queued job and schedules the fsync, or, for a
// slow fetch, does so later.
func (s *Sim) startDisk(n *node) {
	jobs := n.diskQ
	n.diskQ = nil
	if s.cfg.Faults.SlowFetch && !s.healed && slices.ContainsFunc(jobs, func(j core.PersistJob) bool { return j.Install != nil }) && s.rng.IntN(2) == 0 {
		s.stats.SlowFetches++
		n.diskBusy = true
		s.push(&event{at: s.now + 200*Millisecond + s.rng.Int64N(1800*Millisecond), kind: evFetch, node: n.idx, gen: n.gen, jobs: jobs})
		return
	}
	s.writeJobs(n, jobs)
}

// writeJobs writes jobs and schedules the fsync. A crash before the fsync
// finishes leaves a torn tail.
func (s *Sim) writeJobs(n *node, jobs []core.PersistJob) {
	n.inflight, n.replaceFrom = jobs, 0
	for _, j := range jobs {
		if len(j.Entries) > 0 && j.Entries[0].GetIndex() <= n.durableLast && (n.replaceFrom == 0 || j.Entries[0].GetIndex() < n.replaceFrom) {
			n.replaceFrom = j.Entries[0].GetIndex()
		}
	}
	if s.diskFaultDue && slices.ContainsFunc(jobs, func(j core.PersistJob) bool { return len(j.Entries) > 0 }) {
		// Only an append with entries surely writes, and a write fault
		// fires in the Flush below; a sync can stall until healing. It
		// replaces a fault that waits for a sync. If no node appended
		// during the faults, it fires after healing, when clients still
		// write.
		s.diskFaultDue = false
		s.armDiskFault(n, 1+s.rng.IntN(2))
	}
	for _, j := range jobs {
		if j.Install != nil {
			if i := s.firstReplaced(n, j.Install); i <= n.durableLast && (n.replaceFrom == 0 || i < n.replaceFrom) {
				n.replaceFrom = i
			}
			if err := s.install(n, j.Install); err != nil {
				s.installFailed(n, err)
				return
			}
		}
		n.log.Append(j.HardState, j.Entries)
		if j.Root != nil {
			n.log.AppendRoot(*j.Root)
		}
	}
	if err := n.log.Flush(); err != nil {
		s.diskFailed(n, err)
		return
	}
	if s.recoveryDue && !s.healed && slices.ContainsFunc(jobs, func(j core.PersistJob) bool { return len(j.Entries) > 0 }) {
		// Crash between the write and its sync, so that the restart finds
		// a torn tail to repair, and its recovery crashes too.
		s.recoveryDue = false
		s.stopNode(n)
		return
	}
	n.diskBusy = true
	s.stats.DiskWrites++
	lat := 50*Microsecond + s.rng.Int64N(950*Microsecond)
	if s.cfg.Faults.SlowDisk > 0 && !s.healed && s.rng.IntN(100) < s.cfg.Faults.SlowDisk {
		lat = 20*Millisecond + s.rng.Int64N(80*Millisecond)
	}
	grayDisk := s.gray == n && n.graySync && !s.healed
	if grayDisk {
		lat = max(lat, 500*Millisecond+s.rng.Int64N(2500*Millisecond))
	}
	lat = max(lat, n.stallUntil-s.now)
	leader := n.core.Leader() == n.id
	if (planted.On("B02") && leader) || (planted.On("B03") && !leader) {
		// Planted: report the jobs durable before the fsync.
		n.syncing = &event{at: s.now + lat, kind: evDiskDone, node: n.idx, gen: n.gen, grayDisk: grayDisk}
		s.push(n.syncing)
		for _, j := range jobs {
			s.persisted(n, j)
		}
		s.process(n)
		return
	}
	n.syncing = &event{at: s.now + lat, kind: evDiskDone, node: n.idx, gen: n.gen, jobs: jobs, grayDisk: grayDisk}
	s.push(n.syncing)
}

func (s *Sim) diskDone(e *event) {
	n := s.nodes[e.node]
	if !n.up || n.gen != e.gen || n.syncing != e {
		return
	}
	n.syncing = nil
	// A group of jobs that only move the commit index is not synced, as
	// the real node does; the planted early reports carry no jobs.
	if e.jobs == nil || slices.ContainsFunc(e.jobs, core.PersistJob.MustSync) {
		if err := n.log.Sync(); err != nil {
			s.diskFailed(n, err)
			return
		}
	}
	n.diskBusy = false
	if e.grayDisk {
		s.graySyncEnded()
	}
	n.inflight, n.replaceFrom = nil, 0
	for _, j := range e.jobs {
		if j.Install != nil && j.Install.Checkpoint.Index > 0 {
			n.root, n.roots = j.Install.Checkpoint.Index, []wal.Root{j.Install.Checkpoint}
		}
		if j.Root != nil && !s.removed(n, j) {
			return
		}
	}
	for _, j := range e.jobs {
		s.persisted(n, j)
	}
	s.process(n)
}

var marshal = proto.MarshalOptions{Deterministic: true}

// send puts a message on its link. Links are ordered streams: a message
// never overtakes an earlier one on the same link.
func (s *Sim) send(from *node, m *raftpb.Message) {
	to := slices.IndexFunc(s.nodes, func(n *node) bool { return n.id == m.GetTo() })
	if to < 0 {
		if s.cfg.Membership && slices.ContainsFunc(from.core.Members(), func(member *pb.Member) bool { return member.ID == m.GetTo() }) {
			// An added member may not have started yet.
			s.stats.Dropped++
			return
		}
		s.fail("node %d sends to unknown node %d", from.id, m.GetTo())
		return
	}
	s.stats.Messages++
	l := &s.links[from.idx][to]
	if !l.up || !s.nodes[to].up {
		s.stats.Dropped++
		return
	}
	b, err := marshal.Marshal(m)
	if err != nil {
		s.fail("marshal message: %w", err)
		return
	}
	if s.gray != nil && (s.gray == from || s.gray.idx == to) && (m.GetType() == raftpb.MessageType_MsgApp || m.GetType() == raftpb.MessageType_MsgSnap) {
		// A gray node: heartbeats get through, appends do not.
		s.stats.Dropped++
		if !s.grayDropped {
			s.grayDropped = true
			s.stats.GrayNodes++
		}
		if s.healed {
			// The guaranteed gray period, which waited for this.
			s.gray, s.grayOnce = nil, false
		}
		return
	}
	if l.halfOpenUntil > s.now {
		// The sender still sees its connection, but nothing arrives.
		s.stats.Dropped++
		return
	}
	var delay int64
	if s.healed {
		delay = 20*Microsecond + s.rng.Int64N(200*Microsecond)
	} else {
		delay = l.base + s.rng.Int64N(l.jitter+1)
		if s.cfg.Faults.MaxDelay > 0 && s.rng.IntN(10) == 0 {
			delay = s.rng.Int64N(s.cfg.Faults.MaxDelay) + 1
		}
	}
	l.sent++
	// After everything sent before on the link, which includes what a
	// stall holds back (holdLink).
	at := max(s.now+delay, l.last, l.stallUntil)
	l.last = at
	s.push(&event{at: at, kind: evDeliver, node: to, from: from.idx, gen: l.epoch, data: b, number: l.sent})
}

func (s *Sim) deliver(e *event) {
	n := s.nodes[e.node]
	l := &s.links[e.from][e.node]
	if !n.up || !l.up || l.epoch != e.gen {
		s.stats.Dropped++
		return
	}
	if e.number <= l.delivered {
		s.fail("message %d from node %d to node %d arrived after message %d", e.number, e.from+1, e.node+1, l.delivered)
		return
	}
	l.delivered = e.number
	s.mixBytes(e.data)
	m := &raftpb.Message{}
	if err := proto.Unmarshal(e.data, m); err != nil {
		s.fail("unmarshal message: %w", err)
		return
	}
	if slices.Contains(n.core.RemovedMembers(), m.GetFrom()) {
		// The real peer transport closes a removed member's connection
		// and answers its next handshake with removedReply. Deliver that
		// answer through the reverse link, rather than reviving a removed
		// node forever during an erase-one-disk restart.
		back := &s.links[e.node][e.from]
		if back.up && back.halfOpenUntil <= s.now {
			s.push(&event{at: max(s.now+back.base, back.stallUntil), kind: evRemoved, node: e.from, from: e.node, gen: back.epoch})
		}
		return
	}
	if err := n.core.Step(m); err != nil {
		// Raft rejects some messages, for example from a peer it does not
		// track yet. That is not a failure, but it is part of the trace.
		s.mix(0xe5, uint64(len(err.Error())))
	}
	s.process(n)
}

// cut breaks every connection of a node, as a crash does.
func (s *Sim) cut(i int) {
	for j := range s.nodes {
		s.links[i][j].epoch++
		s.links[j][i].epoch++
	}
}

// crashRandom crashes a node that is up and restarts it up to MaxDown
// later.
func (s *Sim) crashRandom() {
	if s.healed {
		return
	}
	var up []*node
	for _, n := range s.nodes {
		if n.up {
			up = append(up, n)
		}
	}
	if len(up) == 0 {
		return
	}
	n := up[s.rng.IntN(len(up))]
	s.crash(n)
	s.maybeCorrupt(n)
	s.push(&event{at: s.now + 1 + s.rng.Int64N(s.cfg.Faults.MaxDown), kind: evRestart, node: n.idx, gen: n.gen})
}

func (s *Sim) crash(n *node) {
	s.stats.Crashes++
	s.down(n)
	n.disk.Crash(s.rng)
}

// down takes n down: its core and log are gone, its links cut, and its
// clients and watchers lose their connections.
func (s *Sim) down(n *node) {
	// The crash may keep part of an unsynced write that replaces entries
	// reported durable; recovery may then end below them.
	n.recoverFloor = n.durableLast
	if n.inflight != nil && n.replaceFrom > 0 {
		n.recoverFloor = min(n.recoverFloor, n.replaceFrom-1)
	}
	n.up = false
	if n.pausedUntil > s.now {
		// A crash ends a pause; the node handles what it held, as a node
		// that is down.
		n.pausedUntil = 0
		s.push(&event{at: s.now, kind: evResume, node: n.idx})
	}
	n.core, n.log = nil, nil
	s.cut(n.idx)
	for _, op := range n.outstanding {
		if !op.done {
			s.push(&event{at: s.now + s.clientLatency(), kind: evClientReply, op: op, err: errConnLost})
		}
	}
	n.outstanding = nil
	n.reqs = map[uint64]*clientOp{}
	for _, w := range s.watchers {
		if w.open && w.node == n.idx && w.gen == n.gen {
			s.push(&event{at: s.now + s.clientLatency(), kind: evWatchBroken, watch: w, node: n.idx, stream: w.stream, gen: w.gen})
		}
	}
	n.streams = map[uint64]*watchClient{}
	n.closing = map[uint64]bool{}
	n.grayStreams = map[uint64]bool{}
}

func (s *Sim) changeLinks() {
	if s.healed {
		return
	}
	s.stats.LinkChanges++
	// Half of the changes heal every link; the rest pick a random set of
	// directed links, which covers partitions and one-way links.
	mask := s.allLinks()
	if s.rng.IntN(2) == 0 {
		mask = s.rng.Uint64() & mask
	}
	s.setLinks(mask)
	// At most three times the mean, so that one random partition cannot
	// hold through most of the faults; TestAllLinkMasks holds each mask.
	s.push(&event{at: min(s.exp(s.cfg.Faults.LinkEvery), s.now+3*s.cfg.Faults.LinkEvery), kind: evLinks})
}

func (s *Sim) allLinks() uint64 { return 1<<uint(len(s.links)*(len(s.links)-1)) - 1 }

// setLinks sets the directed links between nodes from a mask:
// bit k is the k-th pair (i, j) with i != j in order.
func (s *Sim) setLinks(mask uint64) {
	s.mix(0x11, mask)
	k := 0
	for i := range s.links {
		for j := range s.links[i] {
			if i == j {
				continue
			}
			l := &s.links[i][j]
			up := mask&(1<<k) != 0
			if l.up && !up {
				l.epoch++
			}
			l.up = up
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
			a, b := s.links[i][j].base, s.links[j][i].base
			if !s.healed && (a >= 10*b || b >= 10*a) {
				s.stats.Asymmetric++
			}
		}
	}
}

func (s *Sim) heal() {
	s.healed = true
	if !s.grayOnce || s.grayDropped {
		s.gray, s.grayOnce = nil, false
	}
	if s.grayCl != nil {
		// Closes its stream and stops.
		s.grayConnect()
	}
	s.setLinks(s.allLinks())
	// Link stalls and pauses end by healing, as they are cut to it, and
	// ending them here would let new messages overtake held ones.
	// Half-open links end now.
	for i := range s.links {
		for j := range s.links[i] {
			s.links[i][j].halfOpenUntil = 0
		}
	}
	for _, n := range s.nodes {
		if n.stallUntil > s.now && len(n.stalls) > 0 {
			n.stalls[len(n.stalls)-1][1] = s.now
			if n.syncing != nil {
				// The stalled write finishes now instead.
				e := *n.syncing
				e.at = s.now + 50*Microsecond + s.rng.Int64N(950*Microsecond)
				n.syncing = &e
				s.push(n.syncing)
			}
		}
		n.stallUntil = 0
		n.disk.ClearFaults()
		if !n.up && !n.fenced {
			if err := s.start(n); err != nil {
				s.fail("%w", err)
				return
			}
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

func (s *Sim) submit(e *event) {
	n := s.nodes[e.node]
	op := e.op
	if !n.up || n.gen != e.gen {
		s.push(&event{at: s.now + s.clientLatency(), kind: evClientReply, op: op, err: errConnLost})
		return
	}
	n.nextReq++
	n.reqs[n.nextReq] = op
	n.outstanding = slices.DeleteFunc(n.outstanding, func(o *clientOp) bool { return o.done })
	n.outstanding = append(n.outstanding, op)
	req := core.Request{ID: n.nextReq, Op: op.rec.req}
	if put, ok := op.rec.req.(*pb.PutRequest); ok {
		// Puts go in wire form, as the client port hands them over.
		b, err := marshal.Marshal(put)
		if err != nil {
			s.fail("marshal put: %w", err)
			return
		}
		if req.Put, err = core.ParsePut(b); err != nil {
			s.fail("parse put: %w", err)
			return
		}
		req.Op = nil
	}
	n.core.Submit(req)
	s.process(n)
}

func (s *Sim) replied(n *node, r core.Reply) {
	op, ok := n.reqs[r.ID]
	if !ok {
		s.fail("node %d replied to unknown request %d", n.id, r.ID)
		return
	}
	delete(n.reqs, r.ID)
	if r.Resp == nil && r.Err == nil {
		r.Resp = n.core.PutResponse(r)
	}
	if r.Resp != nil {
		b, err := marshal.Marshal(r.Resp)
		if err != nil {
			s.fail("marshal reply: %w", err)
			return
		}
		s.mixBytes(b)
	} else {
		s.mixBytes([]byte(r.Err.Error()))
	}
	s.push(&event{at: s.now + s.clientLatency(), kind: evClientReply, op: op, resp: r.Resp, err: r.Err})
}

// persisted tells a node's core that an append is durable, and remembers
// how far its log then reached.
func (s *Sim) persisted(n *node, j core.PersistJob) {
	if j.Install != nil {
		n.durableLast = j.Install.Index
	}
	if len(j.Entries) > 0 {
		n.durableLast = j.Entries[len(j.Entries)-1].GetIndex()
	}
	n.core.Persisted(j)
}
