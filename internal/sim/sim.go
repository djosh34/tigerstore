// Package sim runs a cluster of real tigerstore nodes in one process, on
// simulated disks and a simulated network, from one seed.
//
// Each node is the real node driver (internal/node) with its WAL and its
// peer transport (internal/peer). Its disk is a simfs disk, and its
// transport speaks TLS over the simulator's in-memory connections.
// Clients enter where the server does: Node.Do, watch streams, and the
// peer frames the transport delivers. gRPC and HTTP stay outside.
//
// The seed decides the workload, the faults and their times, and every
// network delay and log sync. The tests run under gosim (internal/simrun),
// whose scheduler and clock the same seed decides too, so a seed plays out
// the same way every time. Natively, under the race detector, Run must be
// called inside a synctest bubble (synctest.Test), whose virtual clock the
// nodes run on, and Go's scheduler decides the order of the work, so a
// seed can play out differently from run to run. A failed run reports its
// trace: what the simulator did, and the nodes' log lines.
package sim

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simos"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
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
	// Nodes is the number of voting members the cluster starts with: 1, 3
	// or 5; 3 if 0.
	Nodes int
	// FormUnderFaults forms the cluster under the faults, from the run's
	// first moment, as one seed in formUnderFaults does, for a scenario
	// about a cluster's first elections.
	FormUnderFaults bool
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
	// Clients is the number of clients that enter at Node.Do, and
	// V3Clients of those that use etcd's Go client (v3client.go). Each has
	// one call in flight.
	Clients, V3Clients int
	// PinClients makes client i call voter i%Nodes only, so that the
	// clients waiting on a node that cannot answer leave the other nodes
	// theirs (#279). Otherwise each call picks a voting node at random.
	PinClients bool
	// ClientTimeout is when a client gives up on a call.
	ClientTimeout int64
	// GrayLeaderAt, if set, makes whichever node leads then gray until
	// healing: its heartbeats get through, its appends do not.
	// GrayLeaderSync also makes its fsyncs slow: one in flight at healing
	// ends 1.5 to 3 s after it, the shape of #81.
	GrayLeaderAt   int64
	GrayLeaderSync bool
	// HalfOpenAt, if set, makes the link from whichever node leads then
	// to a follower half-open for the peer write timeout.
	HalfOpenAt int64
	// StallLeaderAt, if set, stalls the disk of whichever node leads at
	// that time until healing.
	StallLeaderAt int64
	// LagAtEnd crashes a follower once the clients' last calls have ended,
	// commits a write of the run's own while it is down, and starts it
	// again with its links to the other nodes down until the end: the end
	// finds it behind, whatever the seed or the build's speed, and waits
	// while it catches up (#269, #745).
	LagAtEnd bool
	// LiveFrom, if set, is when the liveness rule starts instead of
	// LiveAfter into healing.
	LiveFrom int64
	// NoLeases leaves leases out of the workload. Porcupine's runs need
	// that: every lease adds an expiry that may apply at any time, and its
	// search grows too fast with them.
	NoLeases bool
	// LoseQuorum makes every crash damage the node's disk until a
	// majority of the voters is fenced for good, and no operator replaces
	// one: the cluster must stay safe, and refuse every write from then
	// on (checkUnavailable).
	LoseQuorum bool
	// Membership adds, updates, promotes and removes a member after the
	// voters while the ordinary clients and faults continue.
	Membership bool
	// MemoryBytes is each node's memory budget (core.Config.MemoryBytes):
	// it turns the value cache on and bounds Range answers. Zero leaves
	// both off.
	MemoryBytes int64
	// SegmentBytes is the size of the nodes' log segments, 8 KiB if 0.
	SegmentBytes int64
	// Objects, if set, makes the clients run the Kubernetes-shaped load
	// of cmd/soak on that many objects instead of the small keys, with a
	// watch on every resource prefix and a compaction every CompactEvery
	// (kube.go).
	Objects int
	// ListPage is the limit of a page of the load's lists, kubePage if 0.
	ListPage int
	// Window, if set, gives the nodes the core's default window of applied
	// entries in memory instead of tinyLimits', for a test that needs a
	// follower a little behind to catch up from the log, not a snapshot.
	Window       bool
	CompactEvery int64
	Faults       Faults
	// Waves change the rates of the faults that come at random times
	// during the faults: from each wave's At on, their mean times apart
	// are Scale times Faults', and with Scale 0 none come. The waves are
	// in order of At.
	Waves []Wave
	// LinkDelay, if set, is the most of a link's own one-way delay,
	// which each change of the links draws from 20 us up to it, 2 ms
	// otherwise; the links keep their delays after healing too.
	LinkDelay int64
	// ValueBytes, if set, pads each value the small keys' clients put to
	// that many bytes.
	ValueBytes int
	// Catchup is the applied entries a wl node keeps for followers that
	// lag (wl.Config.CatchupEntries), wlCatchup if 0.
	Catchup uint64
	// damageKinds and netemKinds, if set, are the kinds of damage after a
	// sync and of link faults that the run picks from, for the tests.
	damageKinds []damageKind
	netemKinds  []netemKind
	// restartRotted restarts a wl node soon after its disk rotted a
	// value, while its store still names the value (rotWL).
	restartRotted bool
	// scenario, if set, drives the cluster once its nodes started and
	// before the clients start, and noCheckpoints keeps the nodes from
	// writing checkpoints as their log grows, so that no checkpoint or
	// cleaner reads a record the scenario damaged before a snapshot serve
	// does; a compaction still writes one, and the scenario sends none.
	// For the tests.
	scenario func(*Sim) error
	// ckptCreateFaults makes every disk fault EIO on the next creation of
	// a checkpoint file, and every such creation take 50 ms; killLosses
	// makes every crash a kill followed by a power loss; syncFaultExits
	// makes every disk fault a failed sync, after which the node starts
	// again with its page cache kept, and cutFails fails the next truncate
	// with it. For the tests.
	ckptCreateFaults, killLosses, syncFaultExits, cutFails bool
	// stageCrash crashes a node once, after its log wrote and before that
	// write's sync, keeping the write torn at sectors (wlStageCrash).
	stageCrash bool
	// ckptCrash crashes wl nodes partway through a checkpoint until a
	// crash kept a later write of it and lost an earlier one
	// (wlCheckpointCrash), and ckptChunkKeys, if set, is the most keys a
	// wl checkpoint writes in one write (wl.Config.CheckpointChunkKeys).
	ckptCrash     bool
	ckptChunkKeys int
	// stageUnwritten makes stageCrash's image keep the segment's growth by
	// the write with zeros past what was synced (simfs.Unwritten).
	stageUnwritten bool
	// syncFailWarmCold makes, once, a wl node's sync of its log fail with
	// none of the write on the disk (simfs.FailSyncLosingAll), so that the
	// node starts again warm, its page cache kept, and writes on; a second
	// later it loses power and starts cold (wlSyncFailArm, Recovered).
	syncFailWarmCold bool
	// from, if set, is the aged image the run forks from (aged.go): its
	// nodes start from copies of the image's disks. noFence keeps the
	// crash images to whole writes, which never fence a node, so that an
	// aging run ends with every node up for its image.
	from    *image
	noFence bool
	// flipRecords lets a wl node's damage, where the kind drawn finds no
	// place, flip a record of its log (flipWLRecord), once the cluster
	// formed first: TestSeeds' runs, whose rule over a set needs fenced
	// nodes.
	flipRecords bool
}

// Wave is a change of the faults' rates (Config.Waves).
type Wave struct {
	At    int64
	Scale float64
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
	// stops, never retries, and restarts from its disk, half the time
	// with its page cache kept.
	DiskErrorEvery int64
	// StallEvery is the mean time between disk stalls of 1.5 to 4 s, in
	// which the log's syncs wait.
	StallEvery int64
	// FSDownEvery is the mean time between file systems that shut down or
	// go read-only for 0.5 to 3 s, and DiskFullEvery between disks that
	// fill up for 1.5 to 3.5 s.
	FSDownEvery, DiskFullEvery int64
	// DiskBudget is each node's disk budget (node.Config.DiskBudget): the
	// node cleans to stay below it, and refuses writes with the NOSPACE
	// alarm at it. DiskCapacity is how many bytes its disk holds; a write
	// past it fails with ENOSPC. Zero turns either off.
	DiskBudget   int64
	DiskCapacity int
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
	// SlowWatcherEvery is the mean time between watch clients that turn
	// slow for 2 to 5 s (slowWatcher).
	SlowWatcherEvery int64
	// RecoveryCrash crashes one restart in three again at its first to
	// fourth writing disk operation inside recovery.
	RecoveryCrash bool
	// InstallCrash crashes a follower once right before the first sync of
	// its log during a snapshot install.
	InstallCrash bool
	// VoteCrash crashes a node once right after it synced a new term or
	// vote with no entries.
	VoteCrash bool
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
	// Replace has an operator replace a node fenced for good: remove its
	// member, empty its disk and add it again (replace.go).
	Replace bool
	// Uncached makes one in four of the wl nodes' reads that must not
	// wait on the disk (wl.Sys.PreadNoWait) find their bytes out of the
	// page cache: the read waits while its values are read off the loop.
	Uncached bool
	// SwitchEvery is the mean time between switches to a random link
	// mask. It runs alongside LinkEvery; set only one for fast switching.
	SwitchEvery int64
	// NetemEvery is the mean time between faults on a link to or from the
	// leader (netemKind), and ResetEvery between resets of the
	// connections between two nodes.
	NetemEvery, ResetEvery int64
	// HoldMask, if HoldMaskAt is set, sets the link mask at that time.
	// Other link faults can change it later; with none, it holds until
	// healing.
	HoldMask   uint64
	HoldMaskAt int64
}

// voters is the number of voting members c's cluster starts with.
func (c Config) voters() int {
	if c.Nodes == 0 {
		return 3
	}
	return c.Nodes
}

// length is how long a run of c lasts, unless the liveness rule makes its
// clients run longer (extendLiveness): the faults and the healing, then
// time for the cluster to settle after the clients stopped: leases still
// expire for up to the leaseGrace of the longest TTL the clients ask for,
// and the watch events of the last expiry need 2 s to arrive.
func (c Config) length() int64 {
	return c.Duration + c.Heal + leaseGrace(c.maxLeaseTTL()) + 2*Second
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
		V3Clients:     3,
		ClientTimeout: 3 * Second,
		Faults: Faults{
			CrashEvery: 3 * Second,
			MaxDown:    2 * Second,
			LinkEvery:  2 * Second,
			MaxDelay:   5 * Millisecond,
			SlowDisk:   2,

			DiskErrorEvery: 6 * Second,
			StallEvery:     8 * Second,
			FSDownEvery:    10 * Second,
			DiskFullEvery:  10 * Second,
			// Every run cleans, and some raise the NOSPACE alarm: the
			// nodes' files take 40 to 70 KiB without a budget.
			DiskBudget:   32 << 10,
			DiskCapacity: 128 << 10,
			Corrupt:      true,
			Checkpoint:   true,

			LinkStallEvery:   4 * Second,
			PauseEvery:       6 * Second,
			DriftPerMille:    50,
			HalfOpenEvery:    8 * Second,
			NetemEvery:       3 * Second,
			ResetEvery:       6 * Second,
			GrayEvery:        8 * Second,
			GrayClient:       true,
			SlowWatcherEvery: 6 * Second,
			RecoveryCrash:    true,
			StepCrash:        true,
			FlipSnapshot:     true,
			FetchCrash:       true,
			SlowFetch:        true,
			Rot:              true,
			HoldDown:         true,
			Uncached:         true,
		},
	}
}

// CalmConfig is DefaultConfig with no faults: the run whose answered
// calls per core-second the simulator's speed is measured by, against the
// floor of the same calls (floor.go).
func CalmConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{}
	return cfg
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
	// on a restart; the client tries again. fromLog are the times it did.
	reloads, fromLog []int64
	// hs is the term and vote the node holds durable: what its process
	// recovered or later reported durable. mustLast and mustHS are what
	// recovery must give after its process went down: the floor and hs
	// then, which its next start checks. tornCrash is set from a crash
	// image that tore writes until a start recovers the log (mayBeDamaged).
	hs        hardState
	mustLast  uint64
	mustHS    hardState
	tornCrash bool
	// appended is the last entry a wl process handed its log, and
	// mustAtMost, after a crash that tore a write of the log, what its
	// next start may recover at most (wlStageCrash).
	appended, mustAtMost uint64
	// misread is set while the next read of a segment by recovery returns
	// wrong data.
	misread bool
	// durableLast is the last entry index the node reported durable to
	// its core; recovery must give at least that. pending are the jobs
	// its core handed the disk that it has not reported yet, oldest
	// first: one that replaces entries reported durable may lower what
	// a crash leaves, as Raft may replace acknowledged entries that never
	// committed.
	durableLast uint64
	pending     []pendingJob
	// corrupted is set when the run damaged the disk on purpose; fenced
	// when the node then refused to start and stays down, until an
	// operator replaces it (replace), after which it joins the cluster
	// as a new member.
	corrupted, fenced, joins bool
	// inactive is a member not yet added, or a process stopped because
	// it was removed. Cold recovery must still read that process's disk.
	inactive bool
	// rotted is set when the run flipped a bit of a value of entry
	// rotEntry, in segment rotSeg, file rotFile, on the disk of this
	// running node.
	rotted           bool
	rotFile          string
	rotEntry, rotSeg uint64
	// rotOff is where a wl node's log holds the rotted byte (rotWL).
	rotOff int64
	// rotStopped is set when it stopped on the rotted value too late for
	// its restart to come before the end; it stays down (#265).
	rotStopped bool
	// diskLog holds the node's last disk calls (diskCall), and
	// crashImage its files right after its last crash, for a failure's
	// report (#274).
	diskLog    []diskCall
	crashImage map[string][]byte
	// ckptDone are the checkpoint files closed right after their last
	// sync, whose bytes count once the directory is synced;
	// removedSegment marks a segment removal whose erase-one-disk copy
	// waits for the next sync of the directory.
	ckptDone       []string
	removedSegment bool
	// servedDamage is set once a serve failed on its own damage.
	// duringInstall are the checkpoint files the node finished
	// while an install was on its way to disk, the install's own among
	// them.
	duringInstall []string
	// since is when the node's process last started or went down, why
	// says why it went down, and last is what its core showed at its last
	// turn, for the report of a failure.
	since int64
	why   string
	last  coreState
}

// hardState is the term and vote of a Raft hard state.
type hardState struct{ term, vote uint64 }

// pendingJob is a job a node's core handed its disk at time at: the first
// entry it writes, and whether that replaces entries reported durable.
type pendingJob struct {
	first    uint64
	replaces bool
	install  bool
	at       int64
}

// proc is one process of a node. Its context ends when it dies; from
// then on its disk calls and connections fail.
type proc struct {
	n      *node
	ctx    context.Context
	cancel context.CancelFunc
	// parked, while set, is closed once the node holds still for the
	// checks at the end.
	parked chan struct{}
	// wl is the node's process (wlnode.go).
	wl *wlProc
}

func (p *proc) dead() bool { return p.ctx.Err() != nil }

// leader is the leader p's node knows, or 0. The lock is held.
func (p *proc) leader() uint64 { return p.wl.turn.Lead }

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
	// netem is the link's fault until netemUntil (netemKind): bw is the
	// bandwidth of a slow link, extra the delay a delay fault adds, and
	// switchAt when a switching delay switches next; hits counts the
	// writes it hit.
	netem      netemKind
	netemUntil int64
	bw, extra  int64
	switchAt   int64
	hits       int
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
	// clientLinks are the links between the clients' host and each node:
	// to the node, and back.
	clientLinks [][2]link
	net         network
	trace       []string
	// end is closed when the run ends. wg counts the run's goroutines
	// but the processes', which procs counts.
	end       chan struct{}
	ended     bool
	wg, procs sync.WaitGroup

	healAt, stopAt, endAt int64
	// wallStacks is every goroutine's stack when the run passed its limit
	// of real time (watchWall).
	wallStacks []byte
	// leaseCheckpoints is set when the nodes checkpoint their leases, and
	// formed is how long the cluster took to form before the run, if it
	// did (formCluster).
	leaseCheckpoints bool
	formed           int64
	// servedRoot is the newest durable root of the source of the last
	// snapshot served to each node, by Raft ID, when it was served.
	servedRoot map[uint64]uint64
	healed     bool
	// startRev is the revision of the image a fork starts from, 0 for a
	// run from an empty store.
	startRev int64
	// quorumLostAt is when a majority of the voters was fenced for good
	// (Config.LoseQuorum), 0 while a quorum stays.
	quorumLostAt int64
	// lagged is LagAtEnd's follower, whose links are down until the end.
	lagged *node
	// downFor is how long HoldDown keeps its follower down.
	downFor int64

	clients  []*client
	watchers []*watchClient
	// os is the descriptor table of a wl run's nodes (Config.wl), wlOS.
	os *simos.OS
	// kube is the Kubernetes-shaped load, if Config.Objects is set, and
	// values the one copy of each of its values.
	kube   *kubeLoad
	values *values
	// expiredAt is when a lease first expired on any node.
	expiredAt    map[int64]int64
	expiredOrder []int64
	// revokedAt is when a wl node first applied a lease's revoke, and
	// revokesTo the last index whose revokes it holds (noteRevokes).
	revokedAt   map[int64]int64
	revokesTo   uint64
	checkpoints []*checkpoint
	// syncFail is the node of Config.syncFailWarmCold and how far its
	// scenario got: 1 its sync failed, 2 it started warm, 3 it lost
	// power, 4 it started cold.
	syncFail struct {
		n     *node
		phase int
	}
	// cold holds the disks of the first cold restart that failed, and
	// votes makes the trace show every vote request and answer, as in a
	// cold restart, so that one that elects no leader shows why.
	cold      *coldDisks
	votes     bool
	halfOpens []halfOpenLink
	// applied is the highest index any node has applied.
	applied  uint64
	silences []int64
	// stalledLeader is the leader StallLeaderAt stalled, heldAppendAt
	// when it handed its disk the oldest append that the stall holds, and
	// steppedDownAt when it first stopped leading.
	stalledLeader *node
	heldAppendAt  int64
	steppedDownAt int64
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
	// installCrashDue crashes the next node that syncs its log during a
	// snapshot install (Faults.InstallCrash).
	installCrashDue bool
	// voteCrashDue crashes the next node that syncs a new term or vote
	// alone (Faults.VoteCrash).
	voteCrashDue bool
	rotDue       *node
	// log is every committed entry as the first node applied it, with the
	// state's hash after it, up to index maxLogged; ckpts are the hashes
	// of the checkpoint files, by index.
	log       *entryLog
	maxLogged uint64
	ckpts     map[uint64]ckptRec
	// base, in a cold restart, is the run's record, which holds up to
	// baseUpTo.
	base     *entryLog
	baseUpTo uint64
	// diskFaultDue arms a disk fault at the next write of a log segment,
	// after healing too; lateStopAt is when a node last stopped after
	// healing, which moves the start of the liveness rule.
	diskFaultDue bool
	lateStopAt   int64
	// records holds the clients' calls (recordlog.go).
	records *recordLog
	// pages is where the nodes' disks keep their large files' pages, and
	// chunks where the run's records keep their older bytes (arena).
	pages, chunks *keptPages
	stats         Stats
	uncached      atomic.Int64 // the reads Faults.Uncached sent off the loop
	membership    membershipWork
	err           error
	// loadFrom is the process's CPU time when the clients started, and
	// loadCPU the CPU time from then until the last client stopped
	// (markLoad); firedFrom and loadFired count the timers that fired, and
	// loadAt and loadTime are the run's time, the same way.
	loadFrom, loadCPU    time.Duration
	firedFrom, loadFired uint64
	loadAt, loadTime     int64
}

// Stats counts what happened in a run.
type Stats struct {
	// KeptPages counts the synced pages the nodes' disks moved to the
	// runner's disk (keptPages).
	KeptPages             int
	Messages, Dropped     int
	Crashes, LinkChanges  int
	OK, Failed, Ambiguous int
	// V3OK counts the calls of the clients of etcd's Go client that
	// succeeded, among OK.
	V3OK int
	// Compactions counts successful compactions and WatchCompacted the
	// watches that a compaction canceled.
	Compactions, WatchCompacted int
	// Revokes counts lease revokes in the log, expiries included, and
	// LeaseCheckpoints the lease checkpoints in it.
	Revokes, LeaseCheckpoints int
	// Formed is set when the run's cluster formed with no faults first
	// (formCluster).
	Formed bool
	// DiskFaults counts failed writes and syncs that stopped a node,
	// Silences leaders that went silent on a stalled disk, Fenced
	// nodes that found their disk damaged, and ColdRestarts the
	// erase-one-disk restarts checked.
	DiskFaults, Silences, Fenced, ColdRestarts int
	// Pauses counts process pauses, LinkStalls stalled links, HalfOpens
	// half-open links, and Asymmetric the links that drew a delay at least
	// 10 times that of the other direction.
	Pauses, LinkStalls, HalfOpens, Asymmetric int
	// Settles counts the seconds the nodes ran on after the end because
	// they disagreed (settle).
	Settles int
	// GrayNodes counts gray nodes that lost an append, GrayStreams the
	// streams with watches a gray client opened, RecoveryCrashes crashes
	// inside the recovery of an existing log, and InstallCrashes those
	// during a snapshot install (Faults.InstallCrash).
	GrayNodes, GrayStreams, RecoveryCrashes, InstallCrashes int
	// StageCrashes counts the crashes of wl nodes that tore a write of
	// their log (Config.stageCrash), and StageRecoveries the starts that
	// recovered after one within its bounds.
	StageCrashes, StageRecoveries int
	// StageZeroed counts the stage crashes that left zeros past what the
	// log synced (Config.stageUnwritten).
	StageZeroed int
	// SyncFailColdStarts counts the cold starts of Config.syncFailWarmCold's
	// node that recovered.
	SyncFailColdStarts int
	// CkptCrashes counts the crashes of wl nodes partway through a
	// checkpoint (Config.ckptCrash).
	CkptCrashes int
	// Checkpoints counts checkpoint files completed, Snapshots snapshot
	// streams served, ServeFailures fetches that the source could not
	// serve, SnapshotFlips those damaged on the way, FetchCrashes sources
	// that crashed during a stream once the follower read a prefix of it,
	// SlowFetches fetches served late, StepCrashes crashes at a
	// checkpoint, removal or install step, Rots values damaged under a
	// running node, and RotStops stops of a node that read such a value
	// back.
	Checkpoints, Snapshots, ServeFailures, SnapshotFlips, FetchCrashes, SlowFetches, StepCrashes, Rots, RotStops int
	// OlderCheckpoints counts installed snapshots whose checkpoint is
	// older than the source's newest durable root, CheckpointsDuringInstall
	// checkpoints finished while an install was on its way to disk
	// (#182), and FencedAfterServe nodes fenced after a serve failed on
	// their own damage (#201).
	OlderCheckpoints, CheckpointsDuringInstall, FencedAfterServe int
	// HeldNamed counts the entries leaders' appends named instead of
	// carrying them, HeldParked the appends that waited for a side copy,
	// and HeldAsked the times a follower asked for entries in full
	// (core/held.go).
	HeldNamed, HeldParked, HeldAsked int
	// ErasedAtRoot and ErasedAtRemoval count the erase-one-disk copies
	// taken right after the first root synced and right after the first
	// segment removal.
	ErasedAtRoot, ErasedAtRemoval int
	// Kills counts crashes that kept the page cache, and PowerLosses those
	// that did not, of a running node or of one that was down.
	// TornRefusals counts nodes whose torn crash image recovery refused,
	// MisreadRefusals starts that refused a log they read wrong, IOErrors
	// the injected errors of disk operations, CheckpointCreateErrors those
	// of them that failed the creation of a checkpoint file, FSDowns file
	// systems that shut down or went read-only, DiskFulls disks that
	// filled up, and Damage the damage done after a sync, by kind
	// (damageKinds).
	Kills, PowerLosses, TornRefusals, MisreadRefusals, IOErrors, CheckpointCreateErrors, FSDowns, DiskFulls int
	// ReadOnlyStops counts nodes that stopped on a read-only file system,
	// and ClientLinksDown the directions of the clients' links that went
	// down.
	ReadOnlyStops, ClientLinksDown int
	Damage                         [numDamage]int
	// KillLosses counts the power losses that followed a kill, and Disk
	// what the faults of the nodes' disks did.
	KillLosses int
	Disk       simfs.Counts
	// LogCuts counts the log's segments a running node removed behind a
	// checkpoint, NoSpace the calls the NOSPACE alarm refused, and
	// OutOfSpace the nodes that stopped on a write that ran out of space.
	LogCuts, NoSpace, OutOfSpace int
	// Netem counts the writes each kind of link fault hit (netemKind),
	// Resets the resets of the connections between two nodes,
	// and BlockedWrites the writes that waited for room in a connection's
	// buffer.
	Netem                 [numNetem]int
	Resets, BlockedWrites int
	// SlowWatchers counts the watch clients that turned slow.
	SlowWatchers int
	// Uncached counts the wl nodes' reads that found their bytes out of
	// the page cache (Faults.Uncached).
	Uncached int
	// Replaced counts the nodes fenced for good that an operator replaced,
	// and QuorumLostAt is when a majority of the voters was fenced for
	// good (Config.LoseQuorum), 0 if never.
	Replaced     int
	QuorumLostAt int64
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
	// Silences are the times leaders went silent on a stalled disk.
	// HeldAppendAt is when the leader StallLeaderAt stalled handed its
	// disk the oldest append the stall holds, and SteppedDownAt when it
	// first stopped leading.
	Silences      []int64
	HeldAppendAt  int64
	SteppedDownAt int64
	// Trace is what the run did, last of all, and Report how it left each
	// node, for a failure's report.
	Trace  []string
	Report string
	// grayNode is the index of the last gray node, for tests.
	grayNode int
	// grayHealedAt is when the last gray fsync after healing ended, for
	// tests.
	grayHealedAt int64
	halfOpens    []halfOpenLink
	// nodes are the run's nodes, and cold the disks of a cold restart
	// that failed, for a failure's report.
	nodes []*node
	cold  *coldDisks
	// FinalRev is the revision of the converged cluster at the end, and
	// logged is set once the checks built the committed log.
	FinalRev int64
	logged   bool
	// Clients is the number of request clients.
	Clients int
	// long is set for a run of an hour or more, whose history takes
	// gigabytes.
	long bool
	// stacks is every goroutine's stack when the run passed its limit of
	// real time.
	stacks []byte
	// values is the one copy of each value of a Kubernetes-shaped load,
	// which the checkers' history shares too.
	values *values
	// LoadCPU is the process's CPU time, on all of its threads, from the
	// clients' start until the last of them stopped: the cost of the
	// load, without the formation of the cluster and the checks.
	LoadCPU time.Duration
	// LoadFired is how many timers fired in that time, under gosim, and
	// LoadTime how long it lasted in the run's time: the wake-ups of the
	// nodes, their clients and the simulator per simulated second.
	LoadFired uint64
	LoadTime  int64
	// records is the run's record of its calls, from which History was
	// decoded: a long run's checks drop History once they have their own
	// copy, and a report decodes it again (history). chunks is where
	// records keeps its older bytes.
	records *recordLog
	chunks  *keptPages
	// hashed is the run's hashes, taken before its checks (hashes).
	hashed string
	// start and base, in a fork, are the image's store and the number of
	// its writes at the head of Log: the checkers of package check start
	// from the store and get the writes after it.
	start *history.Start
	base  int
}

// history returns the run's calls: History, decoded again from the
// run's records if a check dropped it.
func (r *Result) history() ([]*Record, error) {
	if r.History != nil || r.records == nil {
		return r.History, nil
	}
	var history []*Record
	var err error
	simrun.Aside(func() { history, err = r.records.records() })
	if r.chunks != nil && r.chunks.err != nil {
		// A chunk on the runner's disk that failed to read decoded as
		// zeros.
		err = errors.Join(err, fmt.Errorf("%w: the history's chunks: %w", errRunnerDisk, r.chunks.err))
	}
	return history, err
}

// Run runs one seed and checks it. The error describes the first broken
// rule. It must run under gosim or inside a synctest bubble.
func Run(cfg Config) (*Result, error) { return runInspect(cfg, nil) }

// runInspect runs cfg, then calls inspect, if set, while the nodes still
// hold still for the checks.
func runInspect(cfg Config, inspect func(*Sim) error) (*Result, error) {
	var f *Sim
	if cfg.from != nil {
		f = cfg.from.src
	} else if formsFirst(cfg) {
		var err error
		f, err = formCluster(cfg)
		if f == nil {
			return nil, err
		}
		if err != nil {
			// A failed formation is the run's failure, with its report.
			res := &Result{Clients: cfg.Clients + cfg.V3Clients, nodes: f.nodes, Report: f.report()}
			f.shutdown()
			// Every goroutine of the formation has ended.
			res.Stats, res.Trace, res.stacks = f.stats, f.trace, f.wallStacks
			err = fmt.Errorf("forming the cluster: %w", err)
			if pe := f.pagesErr(); pe != nil {
				err = errors.Join(pe, err)
			}
			return res, err
		}
		f.shutdown()
	}
	s, err := newSimFrom(cfg, f)
	if err != nil {
		return nil, err
	}
	if cfg.from != nil {
		s.fork(cfg.from)
	} else if f != nil {
		// The nodes start from the formed disks, and the run's record of
		// committed entries and checkpoints goes on from the formation's.
		for i, n := range s.nodes[:cfg.voters()] {
			n.disk = f.nodes[i].disk
			s.traceDisk(n)
		}
		s.log, s.maxLogged, s.ckpts, s.applied = f.log, f.maxLogged, f.ckpts, f.applied
		s.formed, s.stats.Formed = f.formed, true
	}
	res := &Result{Clients: cfg.Clients + cfg.V3Clients, long: cfg.Duration >= 3600*Second}
	if img := cfg.from; img != nil {
		res.start, res.base = img.start, img.ops
	}
	err = s.run()
	// A run that failed before its end keeps what it did so far, for its
	// report; it has no committed log for the checkers.
	s.mu.Lock()
	for _, n := range s.nodes {
		c := n.disk.Counts
		d := &s.stats.Disk
		d.TornWrites += c.TornWrites
		d.Reordered += c.Reordered
		d.LostDirOps += c.LostDirOps
		d.FailedSyncs += c.FailedSyncs
		d.BadReads += c.BadReads
		d.Misreads += c.Misreads
	}
	var history []*Record
	var herr error
	simrun.Aside(func() { history, herr = s.records.records() })
	if herr != nil && err == nil {
		err = fmt.Errorf("decoding the history: %w", herr)
	}
	res.History, res.Stats, res.Silences = history, s.stats, s.silences
	res.Stats.Uncached = int(s.uncached.Load())
	res.HeldAppendAt, res.SteppedDownAt = s.heldAppendAt, s.steppedDownAt
	res.grayNode, res.grayHealedAt, res.halfOpens = s.lastGray, s.grayHealedAt, s.halfOpens
	res.nodes, res.values, res.stacks, res.records, res.chunks = s.nodes, s.values, s.wallStacks, s.records, s.chunks
	res.LoadCPU, res.LoadFired, res.LoadTime = s.loadCPU, s.loadFired, s.loadTime
	s.mu.Unlock()
	if err == nil {
		if err = s.checkWL(res); err != nil {
			err = fmt.Errorf("seed %d: %w", cfg.Seed, err)
		}
	}
	// The checks run the cold restarts, which keep their disks when they
	// fail (#280).
	s.mu.Lock()
	res.cold = s.cold
	s.mu.Unlock()
	if err == nil && inspect != nil {
		err = inspect(s)
	}
	res.Report = s.report()
	s.shutdown()
	res.Trace = s.trace
	if err == nil {
		err = s.failure()
	}
	if s.pages != nil {
		res.Stats.KeptPages = s.pages.puts
	}
	// A page that failed to read reads as zeros, which can fail the run as
	// if a node were wrong: the host's error comes first.
	if pe := s.pagesErr(); pe != nil {
		err = errors.Join(pe, err)
	}
	return res, err
}

// pagesErr is the first error of the run's pages on the runner's disk,
// nil if there was none.
func (s *Sim) pagesErr() error {
	var errs []error
	if s.pages != nil && s.pages.err != nil {
		errs = append(errs, fmt.Errorf("%w: the disks' pages: %w", errRunnerDisk, s.pages.err))
	}
	if s.chunks != nil && s.chunks.err != nil {
		errs = append(errs, fmt.Errorf("%w: the history's chunks: %w", errRunnerDisk, s.chunks.err))
	}
	return errors.Join(errs...)
}

// errRunnerDisk marks a failure of the runner's own disk, which holds the
// pages and chunks a run keeps outside memory: a run with one says
// nothing of the nodes, whose reads of it may have got zeros.
var errRunnerDisk = errors.New("the runner's disk failed")

// keepPages is how many synced pages a node's disk, with its copies,
// keeps in memory as a cache: the others are on the runner's disk only
// (simfs.Disk.KeepIn, #479), 4 MiB a disk.
const keepPages = 1024

// keptPages is where the nodes' disks keep the synced pages of their
// large files: a file on the runner's disk, which the run's formation,
// the run and its cold restarts share. A read or write of it that failed
// fails the run with its first error: the disk read zeros instead.
type keptPages struct {
	pages simfs.Store
	err   error
	puts  int
}

func (k *keptPages) Put(owner any, p []byte) (int64, error) {
	k.puts++
	at, err := k.pages.Put(owner, p)
	if err != nil && k.err == nil {
		k.err = err
	}
	return at, err
}

func (k *keptPages) Get(off int64, p []byte) error {
	err := k.pages.Get(off, p)
	if err != nil && k.err == nil {
		k.err = err
	}
	return err
}

// errNodes is the error of a Config.Nodes that is not 1, 3 or 5.
var errNodes = errors.New("sim: a cluster starts with 1, 3 or 5 voters")

// newSim sets up an empty run.
func newSim(cfg Config) (*Sim, error) { return newSimFrom(cfg, nil) }

// newSimFrom is newSim with the CA and certificate of from, if set, as a
// run reuses its formation's and a cold restart its run's: making them is
// a good part of a run's fixed cost. Its disks keep their large files'
// pages where from's do.
func newSimFrom(cfg Config, from *Sim) (*Sim, error) {
	if v := cfg.voters(); v != 1 && v != 3 && v != 5 {
		return nil, fmt.Errorf("%w: %d", errNodes, v)
	}
	if cfg.voters() == 1 {
		// LagAtEnd needs a follower when the clients end, which a single
		// voter lacks once the membership work removed its member.
		cfg.LagAtEnd = false
		if !cfg.Membership {
			// A cluster of one node has no links between nodes, so the
			// faults of those links are off.
			cfg.Faults.HalfOpenEvery, cfg.Faults.GrayEvery = 0, 0
			cfg.HalfOpenAt, cfg.GrayLeaderAt = 0, 0
		}
	}
	s := &Sim{
		cfg:        cfg,
		rng:        rand.New(rand.NewPCG(cfg.Seed, 0x7469676572)),
		t0:         time.Now(),
		end:        make(chan struct{}),
		expiredAt:  map[int64]int64{},
		revokedAt:  map[int64]int64{},
		log:        &entryLog{},
		ckpts:      map[uint64]ckptRec{},
		servedRoot: map[uint64]uint64{},
		healAt:     cfg.Duration,
		stopAt:     cfg.Duration + cfg.Heal,
		os:         wlOS,
		membership: membershipWork{voters: cfg.voters()},
	}
	s.setUncached()
	var err error
	if s.records, err = newRecordLog(); err != nil {
		return nil, err
	}
	s.endAt = cfg.length()
	count := cfg.voters()
	if cfg.Membership {
		count++
	}
	s.links = make([][]link, count)
	s.clientLinks = make([][2]link, count)
	for i := range s.links {
		s.links[i] = make([]link, count)
		for j := range 2 {
			// The client links have the delays of a client's calls.
			s.clientLinks[i][j] = link{up: true, base: 20 * Microsecond, jitter: 300 * Microsecond}
		}
	}
	s.setLinks(s.allLinks())
	var certs *network
	if from != nil {
		certs, s.pages, s.chunks = &from.net, from.pages, from.chunks
	}
	if err := s.net.init(certs, cfg.voters()); err != nil {
		return nil, err
	}
	if s.pages == nil {
		pages, err := simrun.NewHostPages()
		if err != nil {
			return nil, fmt.Errorf("%w: the disks' pages: %w", errRunnerDisk, err)
		}
		chunks, err := simrun.NewHostPages()
		if err != nil {
			return nil, fmt.Errorf("%w: the history's chunks: %w", errRunnerDisk, err)
		}
		s.pages, s.chunks = &keptPages{pages: pages}, &keptPages{pages: chunks}
	}
	// The records of the calls and of the committed entries keep their
	// older bytes on the runner's disk too (arena). The values stay in
	// memory: a value read from the disk is a copy of its own, and the
	// Kubernetes load and the history share one copy of each (values).
	s.records.data.store, s.log.data.store = s.chunks, s.chunks
	for i := range count {
		s.addNode(i, simfs.New())
	}
	return s, nil
}

func (s *Sim) addNode(i int, disk *simfs.Disk) {
	n := &node{idx: i, id: uint64(i + 1), disk: disk, wake: make(chan struct{})}
	if i == s.cfg.voters() {
		// The member the membership work adds is added at memberURL and
		// moves to updatedMemberURL.
		n.addrs = []string{hostPort(memberURL), hostPort(updatedMemberURL)}
	} else {
		n.addrs = []string{peerAddr(i)}
	}
	disk.Capacity = s.diskCapacity()
	if s.pages != nil {
		disk.KeepIn(s.pages, keepPages)
	}
	s.traceDisk(n)
	s.nodes = append(s.nodes, n)
}

// run runs the workload and the faults until the end, then holds every
// node still for the checks.
func (s *Sim) run() error {
	voters := s.cfg.voters()
	if s.cfg.Membership {
		s.nodes[voters].inactive = true
	}
	// Half the seeds checkpoint their leases, as etcd does with its
	// LeaseCheckpoint feature, every second instead of 5 minutes, so
	// that the clients' short leases get checkpoints too.
	s.leaseCheckpoints = s.rng.IntN(2) == 0
	// Saved before any node runs: a node that spins at once leaves this.
	s.saveBlackbox()
	for _, n := range s.nodes[:voters] {
		if err := s.start(n); err != nil {
			return err
		}
	}
	if s.formed > 0 {
		s.mu.Lock()
		s.tracef("the cluster formed at cluster version %d in %s with no faults, in a run of its own; the nodes start from its disks", core.MaxVersion, seconds(s.formed))
		s.mu.Unlock()
	}
	if s.cfg.scenario != nil {
		if err := s.cfg.scenario(s); err != nil {
			return err
		}
	}
	s.schedule()
	s.wg.Go(s.watchWall)
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
	if err := s.freeze(); err != nil {
		return err
	}
	if s.lagged != nil {
		// LagAtEnd's follower may catch up now.
		s.mu.Lock()
		s.setLinks(s.allLinks())
		s.mu.Unlock()
	}
	return s.settle()
}

// formUnderFaults: one seed in formUnderFaults forms its cluster under
// the faults, from the run's first moment. The others form it as a
// deployment does, with no faults and no clients, in a run of its own
// (formCluster), and the run starts its nodes from that run's disks
// (#481). Under the faults a confirmation round of the upgrade rarely
// completes, and a member fenced for good blocks it for the rest of the
// run, as in etcd, so without this almost no seed ran the code that needs
// the newest version, such as lease checkpoints.
const formUnderFaults = 8

// formLimit is how long a cluster with no faults may take to form and
// take on the newest cluster version.
const formLimit = 20 * Second

// formsFirst reports whether the run of cfg forms its cluster first. It
// draws from a source of its own, so that the run's own draws do not
// depend on it.
func formsFirst(cfg Config) bool {
	return !cfg.FormUnderFaults && rand.New(rand.NewPCG(cfg.Seed, 0x666f726d)).IntN(formUnderFaults) != 0
}

// formCluster starts the voters of cfg's cluster with no faults
// and no clients, waits until each applied the newest cluster version,
// and returns the run, whose nodes' disks hold what they wrote, with its
// record of the entries they applied; the caller takes them down, as a
// process manager stops them. A run that failed comes with the error.
func formCluster(cfg Config) (*Sim, error) {
	s, err := newSim(cfg)
	if err != nil {
		return nil, err
	}
	voters := cfg.voters()
	if cfg.Membership {
		s.nodes[voters].inactive = true
	}
	// The formation has a limit of real time too (watchWall).
	s.saveBlackbox()
	s.wg.Go(s.watchWall)
	for _, n := range s.nodes[:voters] {
		if err := s.start(n); err != nil {
			return s, err
		}
	}
	for {
		s.mu.Lock()
		formed := true
		for _, n := range s.nodes[:voters] {
			formed = formed && n.proc != nil && n.formed(voters)
		}
		at, err := s.now(), s.err
		s.mu.Unlock()
		switch {
		case err != nil:
			return s, err
		case formed:
			s.formed = at
			return s, nil
		case at > formLimit:
			return s, fmt.Errorf("seed %d: with no faults the cluster did not form, with all %d members and format %d, in %s:\n%s", cfg.Seed, voters, wl.MaxFormat, seconds(formLimit), s.report())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// formed reports whether n's process is formed (formCluster). A node's
// format is the newest it reads from the start while that is the base
// format, so it must also know a leader, hold the cluster's voters and
// have applied all it knows committed. The lock is held.
func (n *node) formed(voters int) bool {
	st := n.proc.wl.turn
	return st.Format == wl.MaxFormat && st.Lead != 0 && len(st.Members) == voters && st.Applied >= st.Commit
}

var errWallLimit = errors.New("the run passed its limit of real time ($SIM_WALL)")

// watchWall fails the run once it passed its limit of real time
// (simrun.WallLimitPassed), looking every 100 ms of the run's time, so
// that a run that takes too long ends with its report. It takes every
// goroutine's stack first, while the nodes still run. A goroutine that
// never blocks stops the run's clock too, and gosim ends that run itself;
// for that run it saves the trace and how it left each node every 10 s
// of the run's time, as run and formCluster do before any node starts,
// and the driver then saves them with the stacks.
func (s *Sim) watchWall() {
	s.saveBlackbox()
	for tick := 1; s.sleep(100 * Millisecond); tick++ {
		if simrun.WallLimitPassed() {
			stacks := simrun.Stacks()
			s.mu.Lock()
			s.wallStacks = stacks
			s.fail("%w", errWallLimit)
			s.mu.Unlock()
			return
		}
		if tick%100 == 0 {
			s.saveBlackbox()
		}
	}
}

// saveBlackbox saves the trace and how the run left each node so far,
// under gosim, for the driver to keep if gosim ends a run that never
// yields or that panicked (watchWall).
func (s *Sim) saveBlackbox() {
	if !simrun.InGosim {
		return
	}
	report := s.report()
	s.mu.Lock()
	trace := strings.Join(s.trace, "\n") + "\n"
	s.mu.Unlock()
	simrun.Artifact("blackbox/trace.txt", []byte(trace))
	simrun.Artifact("blackbox/nodes.txt", []byte(report))
}

// settleSeconds is how long settle lets nodes that disagree at the end
// catch up.
const settleSeconds = 7

// settle lets the nodes run on, a simulated second at a time and up to
// settleSeconds, while they disagree at the end: a follower may still be
// catching up, from snapshots that each start it again from its log, when
// the end freezes it (#269). The checks that follow stay as strict. The
// faults and clients have stopped.
func (s *Sim) settle() error {
	for range settleSeconds {
		if a, _ := disagree(s.live()); a == nil {
			return nil
		}
		s.stats.Settles++
		s.thaw()
		time.Sleep(time.Second)
		if err := s.freeze(); err != nil {
			return err
		}
	}
	return nil
}

// schedule starts the clients, the watchers and the faults.
func (s *Sim) schedule() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Membership {
		s.after(2*Second, s.changeMembership)
	}
	var kube []*kubeClient
	if s.cfg.Objects > 0 {
		kube = s.newKubeLoad()
	}
	s.markLoad(true)
	for i := range s.cfg.Clients + s.cfg.V3Clients {
		c := &client{id: i, sim: s, node: -1}
		if i >= s.cfg.Clients {
			c.node = i % s.cfg.voters()
		}
		if kube != nil {
			c.kube = kube[i]
		}
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
			if s.healed {
				return
			}
			s.stats.LinkChanges++
			if len(s.links) == 1 {
				// A single node has only the clients' links.
				s.setClientLinks(0.75)
				return
			}
			// Any mask but all links up.
			s.setLinks(s.rng.Uint64N(s.allLinks()))
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
	if s.cfg.LagAtEnd {
		s.at(s.stopAt+s.cfg.ClientTimeout+100*Millisecond, s.holdBack)
	}
	if s.cfg.StallLeaderAt > 0 {
		s.at(s.cfg.StallLeaderAt, s.stallLeader)
	}
	s.at(s.healAt, s.heal)
}

// lagAtEndKey is the key of LagAtEnd's own write, which no client uses.
var lagAtEndKey = []byte("lag-at-end")

// holdBack is LagAtEnd: it crashes a follower, has another node commit a
// write the follower misses, and starts the follower again once that
// write is in, its links down until the end.
func (s *Sim) holdBack() {
	s.mu.Lock()
	defer s.mu.Unlock()
	lead := s.leaderIdx()
	var lagged, other *node
	for _, n := range s.nodes {
		switch {
		case n.proc == nil || n.out():
		case lagged == nil && n.idx != lead:
			lagged = n
		case other == nil:
			other = n
		}
	}
	if lagged == nil || other == nil {
		s.fail("LagAtEnd: two nodes up once the clients ended, one of them a follower, needed")
		return
	}
	s.crash(lagged)
	s.lagged = lagged
	s.setLinks(s.linksWithout(lagged.idx))
	// The write is a call of a client of its own, in the history as any
	// other; the other node forwards it to the leader if it does not lead.
	c := &client{id: len(s.clients), sim: s}
	r := &Record{Client: c.id, Node: other.idx, Call: s.now(), req: &pb.PutRequest{Key: lagAtEndKey, Value: lagAtEndKey}}
	s.wg.Go(func() {
		resp, err := s.call(other, r.req, 0)
		s.mu.Lock()
		defer s.mu.Unlock()
		c.done(r, resp, err)
		if err != nil {
			s.fail("LagAtEnd: the write the follower misses: %w", err)
			return
		}
		// Started well before the end, it is up when the end comes;
		// with its links down it cannot catch up before then.
		s.restartLater(lagged, 0)
	})
}

// linksWithout is the mask of setLinks with every link of node i down
// and the others up.
func (s *Sim) linksWithout(i int) uint64 {
	var mask uint64
	k := 0
	for a := range s.links {
		for b := range s.links[a] {
			if a == b {
				continue
			}
			if a != i && b != i {
				mask |= 1 << k
			}
			k++
		}
	}
	return mask
}

// setUncached has the descriptor table send one in four of the wl
// nodes' reads that must not wait on the disk off the loop, under
// Faults.Uncached, by a generator of the run's own: the run's other draws
// stay as they were.
func (s *Sim) setUncached() {
	if !s.cfg.Faults.Uncached {
		s.os.SetUncached(nil)
		return
	}
	var mu sync.Mutex
	rng := rand.New(rand.NewPCG(s.cfg.Seed, 0x6361636865))
	s.os.SetUncached(func() bool {
		mu.Lock()
		defer mu.Unlock()
		if rng.IntN(4) != 0 {
			return false
		}
		s.uncached.Add(1)
		return true
	})
}

// markLoad notes the process's CPU time, the timers fired and the run's
// time as the clients start, or as one stops, for what the load cost
// (Result.LoadCPU, LoadFired and LoadTime): the last client to stop ends
// it. Reading the CPU time or the timers changes nothing in the run. The
// lock is held.
func (s *Sim) markLoad(start bool) {
	cpu, err := simrun.CPUTime()
	fired := simrun.TimerFirings()
	switch {
	case err != nil:
		s.fail("reading the CPU time: %w", err)
	case start:
		s.loadFrom, s.firedFrom, s.loadAt = cpu, fired, s.now()
	default:
		s.loadCPU, s.loadFired, s.loadTime = cpu-s.loadFrom, fired-s.firedFrom, s.now()-s.loadAt
	}
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

// traceLines is how many lines of its trace a run keeps, and diskLines
// how many disk calls each node keeps.
const traceLines, diskLines = 3000, 4000

// traceDisk keeps the disk calls of n, with the run's time, in
// n.diskLog. Disk calls run with the lock held.
func (s *Sim) traceDisk(n *node) {
	n.disk.Trace = func(format string, args ...any) {
		if len(n.diskLog) == diskLines {
			n.diskLog = slices.Delete(n.diskLog, 0, diskLines/4)
		}
		n.diskLog = append(n.diskLog, diskCall{at: s.now(), format: format, args: args})
	}
}

// diskCall is a line of a node's disk log, kept unformatted: most runs
// never report it.
type diskCall struct {
	at     int64
	format string
	args   []any
}

// diskLines returns n's disk log, formatted.
func (n *node) diskLines() []string {
	out := make([]string, len(n.diskLog))
	for i, c := range n.diskLog {
		out[i] = fmt.Sprintf("%9.3f ms ", float64(c.at)/float64(Millisecond)) + fmt.Sprintf(c.format, c.args...)
	}
	return out
}

// tracef adds a line to the trace. The lock is held.
func (s *Sim) tracef(format string, args ...any) {
	if len(s.trace) == traceLines {
		s.trace = slices.Delete(s.trace, 0, traceLines/4)
	}
	s.trace = append(s.trace, fmt.Sprintf("%9.3f ms ", float64(s.now())/float64(Millisecond))+fmt.Sprintf(format, args...))
}

// sleep waits d, and reports false if the run ended meanwhile.
func (s *Sim) sleep(d int64) bool { return s.sleepUnless(d, nil) }

// sleepUnless is sleep that also returns false once done is closed; a nil
// done never is. The snapshot fault waits with it for the follower to read
// the prefix: a node that stops on its own, on a disk error, closes its
// connections with their unread bytes, and its transport's Close waits for
// the fault to return before the run sees the node stop (#301).
func (s *Sim) sleepUnless(d int64, done <-chan struct{}) bool {
	t := time.NewTimer(time.Duration(max(d, 0)))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.end:
		return false
	case <-done:
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

// every runs f at exponential intervals of the given mean, as the waves
// scale it, until healing.
func (s *Sim) every(mean int64, f func()) {
	s.wg.Go(func() {
		for {
			s.mu.Lock()
			next, wave := s.waveExp(mean)
			d := next - s.now()
			s.mu.Unlock()
			if !s.sleep(d) {
				return
			}
			if wave {
				// The rate changed: the wait is drawn again, which the
				// exponential's lack of memory allows.
				continue
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

// waveExp draws the time of the next fault of mean as the wave on now
// scales it. If the next wave starts first, it returns that wave's start
// and true. The lock is held.
func (s *Sim) waveExp(mean int64) (int64, bool) {
	scale, next := 1.0, int64(math.MaxInt64)
	for _, w := range s.cfg.Waves {
		if w.At > s.now() {
			next = w.At
			break
		}
		scale = w.Scale
	}
	if scale == 0 {
		return next, true
	}
	at := s.exp(int64(scale * float64(mean)))
	if at > next {
		return next, true
	}
	return at, false
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
	defer s.mu.Unlock()
	n.starting = false
	if s.ended {
		return nil
	}
	armed := s.armRecoveryCrash(n)
	ctx, cancel := context.WithCancel(context.Background())
	p := &proc{n: n, ctx: ctx, cancel: cancel}
	s.tracef("node %d: starting", n.id)
	if err := s.startWL(n, p); err != nil {
		cancel()
		return err
	}
	p.wl.armed = armed
	return nil
}

// refusesDamage reports whether err is recovery refusing a log it read
// damaged, or a wl node's checkpoint. A segment's header has no checksum of its own, so damage to its
// format version reads as a version this build does not know; refusing
// the log is still the safe stop (#388).
func refusesDamage(err error) bool {
	return errors.Is(err, wal.ErrCorrupt) || errors.Is(err, wal.ErrFormat) ||
		errors.Is(err, wl.ErrCorruptLog) || errors.Is(err, wl.ErrCorruptCheckpoint)
}

// injected reports whether err is a disk fault the run injected.
func injected(err error) bool {
	return errors.Is(err, simfs.ErrIO) || errors.Is(err, simfs.ErrNoSpace) || errors.Is(err, simfs.ErrReadOnly)
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
	n.why = fmt.Sprintf("its process stopped: %v", err)
	if !p.wl.recovered && s.wlRefused(n, err) {
		return
	}
	switch {
	case n.rotted && rotStop(n, err):
		// It read the value the run damaged and stopped instead of
		// answering with it. If its restart would not come before the
		// end, it stays down, as an operator would replace its disk, and
		// the checks at the end leave it out.
		s.stats.RotStops++
		if d := s.restartDelay(); s.ended || s.now()+d >= s.endAt {
			n.rotStopped = true
			s.crash(n)
		} else {
			s.stopNodeAfter(n, d)
		}
	case n.corrupted && core.FailureOf(err) == core.Reload && errors.Is(err, wal.ErrCorrupt):
		// It started again from its log after a snapshot install or a
		// failed fetch, and recovery found the damage and ended the
		// process, as a start on this disk refuses it. The restart
		// fences it.
		s.stopNode(n)
	case s.cfg.Membership && (core.FailureOf(err) == core.Leave || errors.Is(err, wl.ErrRemoved)):
		n.inactive = true
		s.down(n)
	case injected(err):
		// Only injected faults may fail; the node stops at once and never
		// retries, as a failed fsync leaves the file in an unknown state.
		s.stats.DiskFaults++
		if errors.Is(err, simfs.ErrNoSpace) {
			s.stats.OutOfSpace++
		}
		if errors.Is(err, simfs.ErrReadOnly) {
			s.stats.ReadOnlyStops++
		}
		n.disk.Fault = nil
		if s.cfg.syncFaultExits || s.rng.IntN(2) == 0 {
			// The process exits and starts again on a machine that keeps
			// its page cache, where a failed sync left bytes that never
			// reach the disk (#364).
			s.restartAfterExit(n)
		} else {
			s.stopNode(n)
		}
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
	if re, ok := errors.AsType[*wl.ReadBackError](err); ok {
		// On wl: a value, or an entry's record, read back from the log
		// that holds the rotted byte.
		return !re.InCheckpoint() && re.Off <= n.rotOff && n.rotOff < re.Off+int64(re.Len)
	}
	re, ok := errors.AsType[*wal.RecordError](err)
	return ok && re.Entry == n.rotEntry && re.Segment == n.rotSeg
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

// dropJobs forgets the jobs n's process handed its disk, as the process
// goes down. What they wrote may be on the disk, so the floor they gave
// stays what recovery must reach, until a start recovers: a start that
// crashes before it recovers must not count on entries a lost job
// replaced (TestWLFaultSeeds seed 19458). The lock is held.
func (n *node) dropJobs() {
	n.durableLast = n.floor()
	n.pending = nil
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
	switch {
	case gray && s.cfg.GrayLeaderSync:
		lat = s.healAt - s.now() + 1500*Millisecond + s.rng.Int64N(1500*Millisecond)
	case gray:
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
// later. Half the crashes are power losses; the others kill the process,
// as SIGKILL does, and the page cache keeps every write. One kill in three
// is followed by a power loss 0.1 to 3 s later, while the node is down or
// after it started again.
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
	n.why = "the run crashed it"
	if !s.cfg.killLosses && s.rng.IntN(2) == 0 {
		s.crash(n)
		s.maybeCorrupt(n)
	} else {
		n.why = "the run killed it"
		s.kill(n)
		if s.cfg.killLosses || s.rng.IntN(3) == 0 {
			s.after(100*Millisecond+s.rng.Int64N(2900*Millisecond), func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.powerLoss(n)
			})
		}
	}
	s.restartLater(n, 1+s.rng.Int64N(s.cfg.Faults.MaxDown))
}

// crash takes n down as a power loss does: its disk drops what it did not
// sync, apart from what a crash image keeps. The lock is held.
func (s *Sim) crash(n *node) { s.crashWith(n, s.loseUnsynced) }

// crashWith is crash with lose, which drops what n's disk did not sync.
// The lock is held.
func (s *Sim) crashWith(n *node, lose func(*node)) {
	if n.proc == nil {
		return
	}
	s.stats.Crashes++
	s.stats.PowerLosses++
	n.mustLast, n.mustHS = n.floor(), n.hs
	s.down(n)
	lose(n)
	n.crashImage = n.disk.Files()
	s.tracef("node %d: crashed", n.id)
}

// kill takes n's process down and leaves its disk as it is: the page
// cache keeps every write, synced or not. The lock is held.
func (s *Sim) kill(n *node) {
	if n.proc == nil {
		return
	}
	s.stats.Crashes++
	s.stats.Kills++
	n.mustLast, n.mustHS = n.floor(), n.hs
	s.down(n)
	n.crashImage = n.disk.Files()
	s.tracef("node %d: killed", n.id)
}

// powerLoss cuts n's power: a running process crashes and restarts
// later, and a disk whose process is down drops what was not synced. A
// node in the middle of its start keeps its power, and healing ends the
// faults. The lock is held.
func (s *Sim) powerLoss(n *node) {
	if s.ended || s.healed || n.starting || n.out() {
		return
	}
	s.stats.KillLosses++
	if n.proc != nil {
		n.why = "the run cut its power"
		s.crash(n)
		s.restartLater(n, 1+s.rng.Int64N(max(s.cfg.Faults.MaxDown, Second)))
		return
	}
	s.stats.PowerLosses++
	s.loseUnsynced(n)
	n.crashImage = n.disk.Files()
	s.tracef("node %d: lost power while down", n.id)
}

// loseUnsynced drops what n's disk did not sync, keeping a crash image of
// the writes since. Images that tear writes at sectors or pages can make
// recovery refuse the log, as a torn write and damage look alike (wal's
// verifiesAfter), so they come only while fewer other nodes than spare
// are out of the cluster for good, and a quorum stays, and not in membership runs, where
// a fenced voter would refuse every change, as corruption would
// (membershipConfig). An image of whole writes must never make recovery
// refuse the log (#295). The lock is held.
func (s *Sim) loseUnsynced(n *node) {
	img := simfs.Image(s.rng.IntN(4))
	if s.cfg.Membership || s.cfg.noFence || s.count(func(m *node) bool { return m != n && (m.mayBeDamaged() || m.fenced) }) >= s.spare() {
		img = simfs.Image(s.rng.IntN(2))
	}
	s.loseUnsyncedAs(n, img)
}

// loseUnsyncedAs is loseUnsynced with the crash image img. The lock is
// held.
func (s *Sim) loseUnsyncedAs(n *node, img simfs.Image) {
	n.disk.CrashImage(s.rng, img)
	// Recovery may refuse the log since any torn image, until a start
	// recovers it: a later image of whole writes keeps the tear.
	n.tornCrash = n.tornCrash || img >= simfs.Sectors
}

// mayBeDamaged reports whether n's disk is damaged, or may be: a torn
// crash image that no start recovered since may make recovery refuse its
// log (loseUnsynced), which only its next start finds out. The faults that
// must leave a quorum whole count such a node as damaged. The lock is
// held.
func (n *node) mayBeDamaged() bool { return n.corrupted || n.tornCrash }

// spare is how many voters may be out of the cluster for good while a
// quorum of its voters stays whole: one of three, two of five, none of
// one. With Config.LoseQuorum, it is one more: a majority.
func (s *Sim) spare() int {
	k := (s.cfg.voters() - 1) / 2
	if s.cfg.LoseQuorum {
		k++
	}
	return k
}

// count is the number of nodes for which f holds. The lock is held.
func (s *Sim) count(f func(*node) bool) int {
	k := 0
	for _, n := range s.nodes {
		if f(n) {
			k++
		}
	}
	return k
}

// down takes n's process down: its goroutines, disk calls and
// connections fail, and its clients and watchers lose their connections.
// The lock is held.
func (s *Sim) down(n *node) {
	p := n.proc
	if p == nil {
		return
	}
	n.dropJobs()
	n.proc, n.duringInstall = nil, nil
	n.since = s.now()
	if n.why == "" {
		n.why = "the run took it down"
	}
	n.gen++
	// A crash ends a pause.
	n.pausedUntil = 0
	p.cancel()
	s.wakeNode(n)
	s.net.cut(n)
}

// stopNode stops a node like a crash and restarts it later from its disk.
// The lock is held.
func (s *Sim) stopNode(n *node) { s.stopNodeAfter(n, s.restartDelay()) }

// stopNodeAfter is stopNode with the restart d from now.
func (s *Sim) stopNodeAfter(n *node, d int64) {
	s.lateStop()
	s.crash(n)
	s.restartLater(n, d)
}

// restartAfterExit restarts a node whose process exited, later, from its
// disk with the page cache kept, as a kill leaves it. The lock is held.
func (s *Sim) restartAfterExit(n *node) {
	s.lateStop()
	s.kill(n)
	s.restartLater(n, s.restartDelay())
}

// lateStop notes a node that stops after healing. The lock is held.
func (s *Sim) lateStop() {
	if s.healed && s.now() <= s.recovered() {
		// The guaranteed disk fault fired after healing. The liveness
		// rule starts once the node is back (check.go); the clients keep
		// going for 3 s more than that, so the rule has calls to check.
		// Only a stop before the rule would have started counts, so the
		// bound stays fixed:
		// a node that keeps stopping later fails the rule. A run with a
		// node fenced and another gray past healing may write nothing,
		// and so not fire the fault, until then (seed 78783).
		s.lateStopAt = s.now()
		s.extendLiveness()
	}
}

// restartDelay draws when stopNode restarts a node. The lock is held.
func (s *Sim) restartDelay() int64 { return 1 + s.rng.Int64N(max(s.cfg.Faults.MaxDown, Second)) }

// changeLinks changes the set of links that are up, at random times,
// until healing.
func (s *Sim) changeLinks() {
	every := s.cfg.Faults.LinkEvery
	s.mu.Lock()
	next, wave := s.waveExp(every)
	next = min(next, 3*every)
	s.mu.Unlock()
	for s.sleep(next - s.now()) {
		s.mu.Lock()
		if s.healed {
			s.mu.Unlock()
			return
		}
		if wave {
			next, wave = s.waveExp(every)
			s.mu.Unlock()
			continue
		}
		s.stats.LinkChanges++
		// Half of the changes heal every link; the rest pick a random set
		// of directed links, which covers partitions and one-way links.
		mask, clients := s.allLinks(), 1.0
		if s.rng.IntN(2) == 0 {
			mask, clients = s.rng.Uint64()&mask, 0.75
		}
		s.setLinks(mask)
		// The clients' links go down too, each direction on its own.
		s.setClientLinks(clients)
		// At most three times the mean, so that one random partition
		// cannot hold through most of the faults; TestAllLinkMasks holds
		// each mask.
		next, wave = s.waveExp(every)
		next = min(next, s.now()+3*every)
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
			// 2 ms or LinkDelay and jitter up to the base, so delays switch and differ
			// by direction.
			l.base = 20*Microsecond + s.rng.Int64N(cmp.Or(s.cfg.LinkDelay, 2*Millisecond))
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
	for i := range s.clientLinks {
		for j := range 2 {
			l := &s.clientLinks[i][j]
			l.up, l.netemUntil, l.stallUntil = true, 0, 0
		}
		s.net.resetDamaged(i, len(s.links))
	}
	for i := range s.links {
		for j := range s.links[i] {
			s.links[i][j].netemUntil = 0
			s.net.resetDamaged(i, j)
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
		s.remount(n)
		n.disk.Capacity = s.diskCapacity()
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

// applied is the index p's node applied.
func (p *proc) applied() uint64 { return p.wl.turn.Applied }
