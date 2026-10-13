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
	Faults        Faults
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
	at    int64
	seq   uint64
	kind  kind
	node  int
	gen   uint64 // node generation or link epoch the event belongs to
	from  int
	data  []byte
	jobs  []core.PersistJob
	op    *clientOp
	watch *watchClient
	resp  proto.Message
	err   error
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
}

type link struct {
	up    bool
	epoch uint64
	last  int64
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
	history  []*Record
	stats    Stats
	err      error
}

// Stats counts what happened in a run.
type Stats struct {
	Events, Messages, Dropped, DiskWrites int
	Crashes, LinkChanges                  int
	OK, Failed, Ambiguous                 int
	// Compactions counts successful compactions and WatchCompacted the
	// watches that a compaction canceled.
	Compactions, WatchCompacted int
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
	// FinalRev is the revision of the converged cluster at the end.
	FinalRev int64
	// Clients is the number of request clients.
	Clients int
}

// Run runs one seed and checks it. The error describes the first broken
// rule.
func Run(cfg Config) (*Result, error) {
	s := &Sim{
		cfg:    cfg,
		rng:    rand.New(rand.NewPCG(cfg.Seed, 0x7469676572)),
		trace:  fnv.New64a(),
		healAt: cfg.Duration,
		stopAt: cfg.Duration + cfg.Heal,
	}
	// After the clients stop, give the cluster time to settle.
	s.endAt = s.stopAt + 3*Second
	ids := []uint64{1, 2, 3}
	s.links = make([][]link, len(ids))
	for i, id := range ids {
		s.links[i] = make([]link, len(ids))
		for j := range s.links[i] {
			s.links[i][j].up = true
		}
		n := &node{idx: i, id: id, disk: simfs.New(), reqs: map[uint64]*clientOp{}, streams: map[uint64]*watchClient{}}
		s.nodes = append(s.nodes, n)
		if err := s.start(n); err != nil {
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
		s.push(&event{at: s.exp(cfg.Faults.LinkEvery), kind: evLinks})
		// One change at a uniform time, so that no run goes without.
		s.push(&event{at: 1 + s.rng.Int64N(cfg.Duration), kind: evLinksOnce})
	}
	s.push(&event{at: s.healAt, kind: evHeal})

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
		return nil, fmt.Errorf("seed %d at %d ms: %w", cfg.Seed, s.now/Millisecond, s.err)
	}
	res := &Result{Trace: s.trace.Sum64(), History: s.history, Stats: s.stats, Clients: cfg.Clients}
	if err := s.check(res); err != nil {
		return res, fmt.Errorf("seed %d: %w", cfg.Seed, err)
	}
	return res, nil
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
	switch e.kind {
	case evTick:
		n := s.nodes[e.node]
		if !n.up || n.gen != e.gen {
			return
		}
		n.core.Tick()
		s.process(n)
		s.push(&event{at: s.now + TickTime, kind: evTick, node: n.idx, gen: n.gen})
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
	w, rec, err := wal.Open(n.disk)
	if err != nil {
		return fmt.Errorf("node %d cannot recover its log: %w", n.id, err)
	}
	w.SetSegmentBytes(256 << 10)
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
	n.diskQ, n.diskBusy = nil, false
	s.mix(0xb007, uint64(n.idx), uint64(len(rec.Entries)), rec.HardState.Commit)
	s.push(&event{at: s.now + s.rng.Int64N(TickTime) + 1, kind: evTick, node: n.idx, gen: n.gen})
	s.process(n)
	return nil
}

// process runs the node's core and carries out its effects.
func (s *Sim) process(n *node) {
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
	for _, j := range jobs {
		n.log.Append(j.HardState, j.Entries)
	}
	if err := n.log.Flush(); err != nil {
		s.fail("node %d write: %w", n.id, err)
		return
	}
	n.diskBusy = true
	s.stats.DiskWrites++
	lat := 50*Microsecond + s.rng.Int64N(950*Microsecond)
	if s.cfg.Faults.SlowDisk > 0 && !s.healed && s.rng.IntN(100) < s.cfg.Faults.SlowDisk {
		lat = 20*Millisecond + s.rng.Int64N(80*Millisecond)
	}
	s.push(&event{at: s.now + lat, kind: evDiskDone, node: n.idx, gen: n.gen, jobs: jobs})
}

func (s *Sim) diskDone(e *event) {
	n := s.nodes[e.node]
	if !n.up || n.gen != e.gen {
		return
	}
	if err := n.log.Sync(); err != nil {
		s.fail("node %d sync: %w", n.id, err)
		return
	}
	n.diskBusy = false
	for _, j := range e.jobs {
		n.core.Persisted(j)
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
	delay := 20*Microsecond + s.rng.Int64N(200*Microsecond)
	if s.cfg.Faults.MaxDelay > 0 && !s.healed && s.rng.IntN(10) == 0 {
		delay = s.rng.Int64N(s.cfg.Faults.MaxDelay) + 1
	}
	at := max(s.now+delay, l.last)
	l.last = at
	s.push(&event{at: at, kind: evDeliver, node: to, from: from.idx, gen: l.epoch, data: b})
}

func (s *Sim) deliver(e *event) {
	n := s.nodes[e.node]
	l := &s.links[e.from][e.node]
	if !n.up || !l.up || l.epoch != e.gen {
		s.stats.Dropped++
		return
	}
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
	s.push(&event{at: s.now + 1 + s.rng.Int64N(s.cfg.Faults.MaxDown), kind: evRestart, node: n.idx, gen: n.gen})
}

func (s *Sim) crash(n *node) {
	s.stats.Crashes++
	n.up = false
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
	s.push(&event{at: s.exp(s.cfg.Faults.LinkEvery), kind: evLinks})
}

// setLinks sets the six directed links between three nodes from a mask:
// bit k is the k-th pair (i, j) with i != j in order.
func (s *Sim) setLinks(mask uint64) {
	s.mix(0x11, mask)
	k := 0
	for i := range s.nodes {
		for j := range s.nodes {
			if i == j {
				continue
			}
			l := &s.links[i][j]
			up := mask&(1<<k) != 0
			if l.up && !up {
				l.epoch++
			}
			l.up = up
			k++
		}
	}
}

func (s *Sim) heal() {
	s.healed = true
	s.setLinks(1<<6 - 1)
	for _, n := range s.nodes {
		if !n.up {
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
