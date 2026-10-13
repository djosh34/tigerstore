// Package sim runs the real core of a 3-node cluster in one process, on
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

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
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
	// Heal is the fault-free phase after it. Clients keep running in it,
	// and every call that starts LiveAfter into it must succeed. Nodes
	// restarted at healing need up to two election timeouts (2 s) to elect
	// a leader; at 1 s, seed 126 fails with "no leader".
	Heal      int64
	LiveAfter int64
	// Clients is the number of clients. Each has one call in flight.
	Clients int
	// ClientTimeout is when a client gives up on a call.
	ClientTimeout int64
	// GrayLeaderAt, if set, makes whichever node leads then gray until
	// healing: its heartbeats get through, its appends do not.
	GrayLeaderAt int64
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
	Faults   Faults
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
		LiveAfter:     2 * Second,
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
	// inflight are the jobs of the disk write under way.
	inflight []core.PersistJob
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
	cfg   Config
	rng   *rand.Rand
	now   int64
	seq   uint64
	q     queue
	nodes []*node
	links [][]link
	trace hash.Hash64

	healAt, stopAt, endAt int64
	healed                bool

	clients  []*client
	watchers []*watchClient
	// expiredAt is when a lease first expired on any node.
	expiredAt    map[int64]int64
	expiredOrder []int64
	checkpoints  []*checkpoint
	halfOpens    []halfOpenLink
	// applied is the highest index any node has applied.
	applied     uint64
	stallStops  []int64
	commitStops []int64
	// gray is the gray node, or nil, and grayEnd when its gray period ends.
	gray     *node
	grayEnd  int64
	grayCl   *grayClient
	lastGray int
	// diskFaultDue arms a disk fault at the next append with entries,
	// after healing too; lateFaultAt is when such a fault fired after
	// healing, which moves the start of the liveness rule.
	diskFaultDue bool
	lateFaultAt  int64
	history      []*Record
	stats        Stats
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
	// StalledStops leaders that stopped on a stalled disk, Fenced nodes
	// that found their disk damaged, and ColdRestarts the erase-one-disk
	// restarts checked.
	DiskFaults, StalledStops, Fenced, ColdRestarts int
	// Pauses counts process pauses, LinkStalls stalled links, ClockJumps
	// clock jumps, HalfOpens half-open links, and Asymmetric the links
	// that drew a delay at least 10 times that of the other direction.
	Pauses, LinkStalls, ClockJumps, HalfOpens, Asymmetric int
	// GrayNodes counts nodes made gray, CommitStops leaders that stopped
	// because their entries stopped committing, GrayStreams the streams
	// gray clients opened, and RecoveryCrashes crashes inside recovery.
	GrayNodes, CommitStops, GrayStreams, RecoveryCrashes int
}

// Result is the outcome of a run.
type Result struct {
	// Trace is the hash of every event.
	Trace   uint64
	History []*Record
	// Log is every committed write in commit order.
	Log     []proto.Message
	Watches []WatchResult
	Stats   Stats
	// StallStops are the times leaders stopped on a stalled disk, and
	// CommitStops the times leaders stopped because nothing committed.
	StallStops, CommitStops []int64
	// grayNode is the index of the last gray node, for tests.
	grayNode  int
	halfOpens []halfOpenLink
	// FinalRev is the revision of the converged cluster at the end.
	FinalRev int64
	// Clients is the number of request clients.
	Clients int
}

// Run runs one seed and checks it. The error describes the first broken
// rule.
func Run(cfg Config) (*Result, error) {
	s := newSim(cfg)
	for i := range 3 {
		s.addNode(i, simfs.New())
		if err := s.start(s.nodes[i]); err != nil {
			return nil, err
		}
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
		return nil, err
	}
	res := &Result{Trace: s.trace.Sum64(), History: s.history, Stats: s.stats, StallStops: s.stallStops, CommitStops: s.commitStops, grayNode: s.lastGray, halfOpens: s.halfOpens, Clients: cfg.Clients}
	if err := s.check(res); err != nil {
		return res, fmt.Errorf("seed %d: %w", cfg.Seed, err)
	}
	return res, nil
}

// newSim sets up an empty run.
func newSim(cfg Config) *Sim {
	s := &Sim{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x7469676572)),
		trace:     fnv.New64a(),
		expiredAt: map[int64]int64{},
		healAt:    cfg.Duration,
		stopAt:    cfg.Duration + cfg.Heal,
	}
	// After the clients stop, give the cluster time to settle: leases
	// still expire for up to leaseGrace, and the watch events of the last
	// expiry need time to arrive.
	s.endAt = s.stopAt + leaseGrace + 2*Second
	s.links = make([][]link, 3)
	for i := range s.links {
		s.links[i] = make([]link, 3)
	}
	s.setLinks(1<<6 - 1)
	return s
}

func (s *Sim) addNode(i int, disk *simfs.Disk) {
	s.nodes = append(s.nodes, &node{idx: i, id: uint64(i + 1), disk: disk, reqs: map[uint64]*clientOp{}, streams: map[uint64]*watchClient{}, closing: map[uint64]bool{}, grayStreams: map[uint64]bool{}})
}

// loop runs events until the end.
func (s *Sim) loop() error {
	for len(s.q) > 0 && s.err == nil {
		e := s.q.pop()
		if e.at > s.endAt {
			break
		}
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
		s.retry(e, s.startGray())
	case evGraySwitch:
		s.switchGray()
	case evGrayLeader:
		s.grayLeader()
	case evGrayConnect:
		s.grayConnect()
	case evGrayCreate:
		s.grayCreate(e)
	case evStreamClose:
		if n := s.nodes[e.node]; n.up && n.gen == e.gen {
			delete(n.closing, e.stream)
			n.core.CloseWatchStream(e.stream)
			s.process(n)
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
			s.setLinks(s.rng.Uint64N(1<<6 - 1))
		}
	case evHeal:
		s.heal()
	case evClientStart:
		e.op.client.start()
	case evSubmit:
		s.submit(e)
	case evClientReply:
		e.op.client.done(e.op, e.resp, e.err)
	case evClientTimeout:
		e.op.client.done(e.op, nil, errClientTimeout)
	case evWatchOpen:
		e.watch.openWatch()
	case evWatchCreate:
		e.watch.create(s.nodes[e.node], e.gen)
	case evWatchResponse:
		e.watch.receive(e)
	case evWatchBroken:
		e.watch.broken()
	case evWatchDrop:
		e.watch.drop()
	}
}

// start opens a node from its disk, as a process start does.
func (s *Sim) start(n *node) error {
	if n.fenced {
		return nil
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
	if last := lastIndex(rec); last < n.durableLast {
		// A replaced suffix moves durableLast down, so this is a loss.
		return fmt.Errorf("node %d recovered entries to %d, but reported %d durable", n.id, last, n.durableLast)
	}
	// Small segments, so that most of the log is in sealed ones, where
	// any damage is detectable.
	w.SetSegmentBytes(32 << 10)
	cfg := core.DefaultConfig(n.id, []uint64{1, 2, 3})
	cfg.Boot = s.rng.Uint64()
	// Progress notifications every 2 s instead of 10 minutes, so runs see
	// them.
	cfg.ProgressTicks = 20
	c, err := core.New(cfg, rec)
	if err != nil {
		return fmt.Errorf("node %d cannot start: %w", n.id, err)
	}
	n.gen++
	n.log, n.core, n.up = w, c, true
	n.tickEvery = TickTime
	if d := s.cfg.Faults.DriftPerMille; d > 0 && !s.healed {
		n.tickEvery += TickTime * (s.rng.Int64N(2*d+1) - d) / 1000
	}
	n.diskQ, n.diskBusy, n.syncing = nil, false, nil
	s.mix(0xb007, uint64(n.idx), uint64(len(rec.Entries)), rec.HardState.Commit)
	s.push(&event{at: s.now + s.rng.Int64N(TickTime) + 1, kind: evTick, node: n.idx, gen: n.gen})
	s.process(n)
	return nil
}

// process runs the node's core and carries out its effects.
func (s *Sim) process(n *node) {
	if n.pausedUntil > s.now {
		s.fail("node %d works while paused, at %d ms", n.id, s.now/Millisecond)
	}
	eff, err := n.core.Process()
	n.diskQ = append(n.diskQ, eff.Persist...)
	for _, m := range eff.Send {
		s.send(n, m)
	}
	for _, r := range eff.Replies {
		s.replied(n, r)
	}
	for _, r := range eff.Watch {
		s.watchResponse(n, r)
	}
	for _, id := range eff.Expired {
		if _, seen := s.expiredAt[id]; !seen {
			s.expiredAt[id] = s.now
			s.expiredOrder = append(s.expiredOrder, id)
		}
	}
	s.applied = max(s.applied, n.core.Applied())
	if errors.Is(err, core.ErrDiskStalled) {
		s.stats.StalledStops++
		s.stallStops = append(s.stallStops, s.now)
		s.stopNode(n)
		return
	}
	if errors.Is(err, core.ErrCommitStalled) {
		s.stats.CommitStops++
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

// startDisk writes every queued job and schedules the fsync. A crash before
// the fsync finishes leaves a torn tail.
func (s *Sim) startDisk(n *node) {
	jobs := n.diskQ
	n.diskQ = nil
	n.inflight = jobs
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
		n.log.Append(j.HardState, j.Entries)
	}
	if err := n.log.Flush(); err != nil {
		s.diskFailed(n, err)
		return
	}
	n.diskBusy = true
	s.stats.DiskWrites++
	lat := 50*Microsecond + s.rng.Int64N(950*Microsecond)
	if s.cfg.Faults.SlowDisk > 0 && !s.healed && s.rng.IntN(100) < s.cfg.Faults.SlowDisk {
		lat = 20*Millisecond + s.rng.Int64N(80*Millisecond)
	}
	if s.gray == n && n.graySync {
		lat = max(lat, 500*Millisecond+s.rng.Int64N(2500*Millisecond))
	}
	lat = max(lat, n.stallUntil-s.now)
	leader := n.core.Leader() == n.id
	if (planted.On("B02") && leader) || (planted.On("B03") && !leader) {
		// Planted: report the jobs durable before the fsync.
		n.syncing = &event{at: s.now + lat, kind: evDiskDone, node: n.idx, gen: n.gen}
		s.push(n.syncing)
		for _, j := range jobs {
			s.persisted(n, j)
		}
		s.process(n)
		return
	}
	n.syncing = &event{at: s.now + lat, kind: evDiskDone, node: n.idx, gen: n.gen, jobs: jobs}
	s.push(n.syncing)
}

func (s *Sim) diskDone(e *event) {
	n := s.nodes[e.node]
	if !n.up || n.gen != e.gen || n.syncing != e {
		return
	}
	n.syncing = nil
	if err := n.log.Sync(); err != nil {
		s.diskFailed(n, err)
		return
	}
	n.diskBusy = false
	for _, j := range e.jobs {
		s.persisted(n, j)
	}
	s.process(n)
}

var marshal = proto.MarshalOptions{Deterministic: true}

// send puts a message on its link. Links are ordered streams: a message
// never overtakes an earlier one on the same link.
func (s *Sim) send(from *node, m *raftpb.Message) {
	to := int(m.GetTo()) - 1
	if to < 0 || to >= len(s.nodes) {
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
	at := max(s.now+delay, l.last)
	if l.stallUntil > s.now {
		// After what the stall holds back, which arrives at stallUntil.
		at = max(at, l.stallUntil+1)
	}
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
	if l.stallUntil > s.now {
		// The link stalled while this was on it; it waits too, in order.
		e.at = l.stallUntil
		s.push(e)
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
	n.up = false
	if n.pausedUntil > s.now {
		// A crash ends a pause; the node handles what it held, as a node
		// that is down.
		n.pausedUntil = 0
		s.push(&event{at: s.now, kind: evResume, node: n.idx})
	}
	n.core, n.log = nil, nil
	n.disk.Crash(s.rng)
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
			s.push(&event{at: s.now + s.clientLatency(), kind: evWatchBroken, watch: w})
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
	mask := uint64(1<<6 - 1)
	if s.rng.IntN(2) == 0 {
		mask = s.rng.Uint64() & mask
	}
	s.setLinks(mask)
	// At most three times the mean, so that one random partition cannot
	// hold through most of the faults; TestAllLinkMasks holds each mask.
	s.push(&event{at: min(s.exp(s.cfg.Faults.LinkEvery), s.now+3*s.cfg.Faults.LinkEvery), kind: evLinks})
}

// setLinks sets the six directed links between three nodes from a mask:
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
	s.gray = nil
	if s.grayCl != nil {
		// Closes its stream and stops.
		s.grayConnect()
	}
	s.setLinks(1<<6 - 1)
	// Link stalls and pauses end on their own, within 2 s, before the
	// liveness rule starts; ending them early would let new messages
	// overtake held ones. Half-open links end now.
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
	n.core.Submit(core.Request{ID: n.nextReq, Op: op.rec.req})
	s.process(n)
}

func (s *Sim) replied(n *node, r core.Reply) {
	op, ok := n.reqs[r.ID]
	if !ok {
		s.fail("node %d replied to unknown request %d", n.id, r.ID)
		return
	}
	delete(n.reqs, r.ID)
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
	if len(j.Entries) > 0 {
		n.durableLast = j.Entries[len(j.Entries)-1].GetIndex()
	}
	n.core.Persisted(j)
}

func lastIndex(rec *wal.Recovered) uint64 {
	if len(rec.Entries) == 0 {
		return 0
	}
	return rec.Entries[len(rec.Entries)-1].GetIndex()
}
