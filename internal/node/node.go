// Package node runs one tigerstore node: the committer goroutine that owns
// the core, the disk goroutine that persists the log, and the hookup to the
// peer transport. It is the driver of core.Node, in production and in the
// simulator, which runs it on simulated disks and links.
package node

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Config sets up a node.
type Config struct {
	Core    core.Config
	DataDir string
	// WalDir is etcd's --wal-dir: where the log goes instead of
	// <DataDir>/wal.
	WalDir string
	// UnsafeNoFsync is etcd's --unsafe-no-fsync: the log never syncs, so
	// a crash can lose acknowledged writes.
	UnsafeNoFsync bool
	// WarningApplyDuration is etcd's --warning-apply-duration: an apply
	// that takes longer is logged and counted in
	// etcd_server_slow_apply_total; 0 means 100 ms.
	WarningApplyDuration time.Duration
	// ForceNewCluster is etcd's --force-new-cluster: a node that restarts
	// from its log removes every other member and goes on as a cluster of
	// one (core.Node.ForceNewCluster).
	ForceNewCluster bool
	// Tick is the length of one Raft tick.
	Tick time.Duration
	// WatchQueue is how many responses a watch stream may fall behind
	// before it is closed; 0 means watchQueue.
	WatchQueue int
	// WatchBytes is how many bytes of responses a watch stream may fall
	// behind before it is closed; 0 means watchBytes.
	WatchBytes int64
	// SegmentBytes is the size after which the log starts a new segment;
	// 0 means wal.SegmentBytes. Tests make it small, so that old segments
	// go.
	SegmentBytes int64
	// DiskBudget is how many bytes the log's files may take; 0 means no
	// budget, and cleaning only for Defragment (clean.go).
	DiskBudget int64
	// AutoCompaction is etcd's --auto-compaction-mode and
	// --auto-compaction-retention, as ParseCompaction gives them
	// (compact.go).
	AutoCompaction Compaction
	// revisionEvery, if set, replaces the 5 minutes between revision
	// compactions, for tests.
	revisionEvery time.Duration
	// CorruptCheckTime is etcd's --corrupt-check-time, and
	// CompactHashCheckTime its --compact-hash-check-time with the
	// CompactHashCheck feature gate on; 0 turns either check off
	// (corrupt.go).
	CorruptCheckTime, CompactHashCheckTime time.Duration
	// BeforeRead, if set, runs in each read of a value from the log, once
	// it found where the value is and let go of the log's lock, and
	// BeforeSync before each sync of the log. Tests use them to stall
	// those reads and syncs.
	BeforeRead, BeforeSync func()
	// snapshotSync, if set, replaces the syncBytes of a snapshot after
	// which the log syncs what came; stage, if set, runs after each such
	// sync and each item of a snapshot install, with its stage, and an
	// error it returns breaks the stream there; and reloading, if set,
	// runs when a run that ended for a reload opened the log again,
	// after the old core went and before the new one replays the log.
	// For tests.
	snapshotSync int
	stage        func(string) error
	reloading    func(*Node)
	// cleanStage, if set, runs at each step of the cleaner that changes
	// what is durable. For tests.
	cleanStage func(string)
	// Turn, if set, runs on the committer after each turn of the core,
	// with the core, what the turn produced and the error it stopped
	// with, before the node carries any of it out. Persisted, if set,
	// runs there for each job the disk made durable, once the core took
	// it. The simulator checks the core with them, and holds a paused
	// node in Turn.
	Turn      func(*core.Node, core.Effects, error)
	Persisted func(core.PersistJob)
	// Memory, if set, measures what the process uses besides the value
	// cache, and whether that is a new measure, instead of the Go
	// runtime's metrics: the simulator runs several nodes in one process.
	Memory func(cacheLow int64) (int64, bool)
	Logf   func(format string, args ...any)
	// Metrics are the metrics the node observes into; nil gives the node
	// a set of its own.
	Metrics *metrics.Set
}

// input is one event for the committer from a client, a peer or a helper
// goroutine.
type input struct {
	req         *request
	batch       *batch
	msg         *raftpb.Message
	unreachable uint64
	// advance moves the election clock (AdjustTicks).
	advance int
	// removed is a peer's answer that the cluster removed this node.
	removed    bool
	authorize  *authorize
	watchOpen  *WatchStream
	watchReq   *pb.WatchRequest
	watchClose bool
	stream     uint64
	status     chan *pb.StatusResponse
	hashes     chan []mvcc.KVHash
	// snapshotPoint asks where a client's snapshot starts.
	snapshotPoint chan snapshotPoint
	metrics       chan core.Metrics
	// written is the index of a checkpoint the writer synced, or failed
	// to write with writeErr.
	written  uint64
	writeErr error
	// snapshot is a follower that started (fetching) or ended (served,
	// with serveErr) a snapshot fetch from this node, or 0 for a backup
	// that found the log damaged.
	snapshot         uint64
	fetching, served bool
	serveErr         error
}

// watchBytes is how many bytes of responses a watch stream may fall
// behind before the node closes it, whatever their count.
const watchBytes = 64 << 20

// watchQueue is how many responses a watch stream may fall behind before
// the node closes it. The client then reconnects and resumes.
const watchQueue = 4096

// ErrWatchTooSlow closes a watch stream whose client does not keep up. It
// is Unavailable because clientv3 resumes watches after that code and
// gives up after most others.
var ErrWatchTooSlow = status.Error(codes.Unavailable, "tigerstore: watch stream fell too far behind")

// ErrSnapshotInstalled closes the watch streams of a node whose state a
// snapshot replaced. Clients resume from the revision they reached.
var ErrSnapshotInstalled = status.Error(codes.Unavailable, "tigerstore: the node installed a snapshot; watch again")

// WatchStream is one client watch stream on this node.
type WatchStream struct {
	n  *Node
	id uint64
	// caller is the stream's client, for its permissions.
	caller core.Caller
	// C carries the responses in order. Whoever takes one calls Taken.
	C chan *pb.WatchResponse
	// queued is the size of the responses in C.
	queued atomic.Int64
	done   chan struct{}
	err    error
}

// Taken reports that a response from C was taken, so its bytes no longer
// count against the stream's limit.
func (ws *WatchStream) Taken(r *pb.WatchResponse) { ws.queued.Add(-int64(proto.Size(r))) }

// Done is closed when the node dropped the stream; Err then says why.
func (ws *WatchStream) Done() <-chan struct{} { return ws.done }

// Err is why the node dropped the stream.
func (ws *WatchStream) Err() error { return ws.err }

// Send hands the node one request of the stream.
func (ws *WatchStream) Send(ctx context.Context, r *pb.WatchRequest) error {
	return ws.n.put(ctx, input{watchReq: r, stream: ws.id})
}

// Close ends the stream and its watchers. It waits for room in the
// committer's inbox, so it can wait while the committer takes no new
// inputs because the disk is behind.
func (ws *WatchStream) Close() {
	select {
	case ws.n.inbox <- input{watchClose: true, stream: ws.id}:
	case <-ws.n.done:
	}
}

// Authorize checks a call the server serves itself (Status, Snapshot)
// against the caller's permissions.
func (n *Node) Authorize(ctx context.Context, caller core.Caller, op proto.Message) error {
	ch := make(chan error, 1)
	if err := n.put(ctx, input{authorize: &authorize{caller: caller, op: op, reply: ch}}); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
}

// authorize is a check of Authorize for the committer.
type authorize struct {
	caller core.Caller
	op     proto.Message
	reply  chan error
}

// Status returns the node's Maintenance.Status answer.
func (n *Node) Status(ctx context.Context) (*pb.StatusResponse, error) {
	ch := make(chan *pb.StatusResponse, 1)
	if err := n.put(ctx, input{status: ch}); err != nil {
		return nil, err
	}
	select {
	case st := <-ch:
		return st, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, rpctypes.ErrGRPCStopped
	}
}

// Metrics returns the node's metrics.
func (n *Node) Metrics(ctx context.Context) (core.Metrics, error) {
	ch := make(chan core.Metrics, 1)
	if err := n.put(ctx, input{metrics: ch}); err != nil {
		return core.Metrics{}, err
	}
	var m core.Metrics
	select {
	case m = <-ch:
	case <-ctx.Done():
		return core.Metrics{}, ctx.Err()
	case <-n.done:
		return core.Metrics{}, rpctypes.ErrGRPCStopped
	}
	n.log.mu.Lock()
	m.MapBytes = n.log.w.MapBytes()
	n.log.mu.Unlock()
	used, err := n.log.fs.Usage()
	m.DiskBytes, m.DiskBudget, m.CleanedBytes = used, n.cfg.DiskBudget, n.log.cleaned.Load()
	m.RemoveFailures = n.log.removeFailures.Load()
	m.MaxLogWriteNanos = n.log.maxWrite.Load()
	return m, err
}

// OpenWatch starts a watch stream of a client.
func (n *Node) OpenWatch(ctx context.Context, c core.Caller) (*WatchStream, error) {
	ws := &WatchStream{n: n, id: n.nextID.Add(1), caller: c, C: make(chan *pb.WatchResponse, cmp.Or(n.cfg.WatchQueue, watchQueue)), done: make(chan struct{})}
	if err := n.put(ctx, input{watchOpen: ws}); err != nil {
		return nil, err
	}
	return ws, nil
}

func (n *Node) put(ctx context.Context, in input) error {
	select {
	case n.inbox <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
}

type request struct {
	id     uint64
	op     proto.Message
	chunk  bool // a RangeStream chunk; see core.Request.Chunk
	caller core.Caller
	reply  chan core.Reply
}

// Completer takes the replies to the requests it handed the node with
// SubmitBatch. The committer calls Complete once per turn with every reply
// for it that is ready, in a slice that the completer then owns. Complete
// must not block.
type Completer interface {
	Complete([]Completion)
}

// Completion is the reply to one request of a batch.
type Completion struct {
	Tag any
	core.Reply
}

// batch is requests that one client connection hands the committer at
// once. Their IDs are first, first+1, and so on.
type batch struct {
	first uint64
	reqs  []core.Request
	tags  []any
	to    Completer
}

// waiter is who waits for the reply to a request: a Do call, or a
// completer with its tag.
type waiter struct {
	reply chan core.Reply
	to    Completer
	tag   any
}

// SubmitBatch hands the committer every request in reqs with one inbox
// send, so a client connection does not pay a channel, a goroutine and a
// reply channel per request. The node sets their IDs. tags[i] comes back
// with the reply to reqs[i] through to.Complete. It waits while the inbox
// is full, which slows down the client connection; it fails only if ctx
// ends or the node stopped, and then none of the requests was taken. A
// batch handed over as the node stops may never be answered: the caller
// fails such requests once Done is closed.
func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {
	select {
	case <-n.done:
		// put could still win the race for a free inbox, and nobody reads
		// it any more.
		return rpctypes.ErrGRPCStopped
	default:
	}
	k := uint64(len(reqs))
	b := &batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}
	return n.put(ctx, input{batch: b})
}

// Node is a running node.
type Node struct {
	cfg       Config
	core      *core.Node
	log       *diskLog
	inbox     chan input
	persistQ  chan persistBatch
	persisted chan persistDone
	ckptQ     chan core.CheckpointChunk
	cleanQ    chan packRoot
	// wantQ asks prepareSegments for the file of a segment, and nextQ
	// tells the disk goroutine that it is ready, for its rotation.
	wantQ    chan uint64
	nextQ    chan struct{}
	defragQ  chan chan error
	readQ    chan *core.ValueRead
	readDone chan readDone
	nextID   atomic.Uint64
	done     chan struct{}

	// leader is the leader the committer saw last, for readers outside it,
	// and learner whether this node is a learner.
	leader  atomic.Uint64
	learner atomic.Bool
	// etcdVersion is etcd's cluster version the committer saw last, and
	// authEnabled whether auth was on.
	etcdVersion atomic.Pointer[string]
	authEnabled atomic.Bool
	// alarms are the active alarms the committer saw last, alarmsGen
	// their core's change count.
	alarms    atomic.Pointer[[]*pb.AlarmMember]
	alarmsGen uint64
	// transport is the peer.Transport Run got, for calls to peers.
	transport atomic.Value
	// members are the members' IDs the committer saw last.
	members atomic.Pointer[[]uint64]
	// memory measures the process's memory for the value cache, each
	// tick (Config.Memory); only the committer uses it.
	memory func(cacheLow int64) (int64, bool)
}

// diskLog is the WAL as the committer, the snapshot server, the persist
// goroutine and the cleaner share it. The lock covers the WAL's map of
// where entries are: appends and reads take it, the fsync and the
// removal of files do not. Values in segments that no longer change are
// read from a read-only mapping of the file, a copy from the page cache
// instead of a read call.
//
// A value read finds its place under the lock and copies the bytes after
// letting it go, so a read that waits for the disk does not hold up
// appends (#165). It holds reading meanwhile. Whatever unmaps a segment
// or closes a file of the log takes reading for writing, under the lock:
// the cleaner tries, and if a read is under way leaves it for its next
// turn, so appends never wait for a read. A removal that a read or a
// snapshot fetch put off waits in removal, and the end of that read or
// fetch wakes the cleaner to try it again.
type diskLog struct {
	mu      sync.Mutex
	reading sync.RWMutex
	w       *wal.WAL
	fs      FS
	dir     string
	// sealed is the newest segment that no longer changes; maps holds the
	// mappings of sealed segments by sequence number.
	sealed uint64
	maps   map[uint64][]byte
	// ckpts are the durable checkpoints whose files are on disk, which
	// the persist goroutine adds to and the cleaner drops from before it
	// removes older files, and serving counts the snapshot fetches under
	// way, during which nothing is removed.
	ckpts   []wal.Root
	serving int
	// removal is what the newest durable root made obsolete, which waits
	// for the cleaner; held is set while it waits, and retry wakes the
	// cleaner.
	removal *rootRemoval
	held    atomic.Bool
	retry   chan struct{}
	// used is the size of the log's files when the cleaner measured them
	// last, plus what the disk goroutines wrote since, cleaned the bytes
	// that cleaning wrote, and removeFailures the removals of files that
	// failed. maxWrite is the longest write and sync of a group, in
	// nanoseconds.
	used, cleaned, removeFailures, maxWrite atomic.Int64
	// before runs in each value read; see Config.BeforeRead.
	before func()
}

func (l *diskLog) Entry(index uint64) (*raftpb.Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Entry(index)
}

func (l *diskLog) Has(index uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Has(index)
}

func (l *diskLog) Value(index uint64, off, n uint32) ([]byte, error) {
	l.mu.Lock()
	seq, at, ok := l.w.ValuePlace(index, off, n)
	var m []byte
	var r io.ReaderAt
	var err error
	if ok && seq <= l.sealed && l.dir != "" {
		m, err = l.mapping(seq)
	} else {
		r, at, err = l.w.ValueReader(index, off, n)
	}
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}
	l.reading.RLock()
	l.mu.Unlock()
	defer l.readDone()
	if l.before != nil {
		l.before()
	}
	if m != nil {
		if at+int64(n) > int64(len(m)) {
			return nil, fmt.Errorf("node: value at %d+%d is past the end of segment %d", at, n, seq)
		}
		v := bytes.Clone(m[at : at+int64(n)])
		// Diag only: compare mmap with a fresh pread on the same path.
		f, e := os.Open(filepath.Join(l.dir, wal.SegmentName(seq)))
		if e == nil {
			b := make([]byte, n)
			_, e = f.ReadAt(b, at)
			_ = f.Close()
			if e == nil && !bytes.Equal(v, b) {
				fmt.Fprintf(os.Stderr, "diag270: mmap != pread entry=%d off=%d len=%d seq=%d at=%d mmap=%x pread=%x\n", index, off, n, seq, at, v, b)
			}
		}
		return v, nil
	}
	return wal.ReadValue(r, at, index, off, n)
}

// rootRemoval is what a synced root made obsolete: the log below index
// below and the checkpoints older than root.
type rootRemoval struct {
	below uint64
	root  wal.Root
}

// readDone ends a value read.
func (l *diskLog) readDone() {
	l.reading.RUnlock()
	l.wake()
}

// wake wakes the cleaner if a removal waits.
func (l *diskLog) wake() {
	if l.held.Load() {
		select {
		case l.retry <- struct{}{}:
		default:
		}
	}
}

// mapping returns the mapping of sealed segment seq, mapping it first.
func (l *diskLog) mapping(seq uint64) ([]byte, error) {
	if m, ok := l.maps[seq]; ok {
		return m, nil
	}
	f, err := os.Open(filepath.Join(l.dir, wal.SegmentName(seq)))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	m, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err == nil {
		// Read-around would fetch the pages around each value; #133
		// measured it halving cold reads.
		if err = syscall.Madvise(m, syscall.MADV_RANDOM); err != nil {
			err = errors.Join(err, syscall.Munmap(m))
		}
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return nil, err
	}
	l.maps[seq] = m
	return m, nil
}

// unmap unmaps the segments up to upto, which the log dropped. The lock
// and reading are held.
func (l *diskLog) unmap(upto uint64) error {
	var err error
	for seq, m := range l.maps {
		if seq <= upto {
			err = errors.Join(err, syscall.Munmap(m))
			delete(l.maps, seq)
		}
	}
	return err
}

// close unmaps every segment and closes the WAL.
func (l *diskLog) close() error {
	if l.w == nil {
		return nil
	}
	l.reading.Lock()
	defer l.reading.Unlock()
	return errors.Join(l.w.Close(), l.unmap(l.w.Seq()))
}

func (l *diskLog) Checkpoint(index uint64) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Checkpoint(index)
}

// FS is the directory of a node's log. It also reports its usage to the
// paced cleaner.
type FS interface {
	wal.FS
	Usage() (int64, error)
}

// New recovers the node's state from its checkpoint and log on disk.
func New(cfg Config) (*Node, error) {
	path := cmp.Or(cfg.WalDir, filepath.Join(cfg.DataDir, "wal"))
	dir, err := osfs.New(path)
	if err != nil {
		return nil, err
	}
	if cfg.UnsafeNoFsync {
		dir = dir.WithoutSync()
	}
	return newOnDisk(cfg, path, dir)
}

// NewOnFS recovers the node's state from a log in dir, which is not an OS
// directory: the simulator runs the node on its simulated disks. Values
// are read with File.ReadAt instead of mappings.
func NewOnFS(cfg Config, dir FS) (*Node, error) { return newOnDisk(cfg, "", dir) }

// An empty path uses File.ReadAt rather than OS mappings. Tests run the same
// driver and cleaner on a simulated filesystem with a virtual clock.
func newOnDisk(cfg Config, path string, dir FS) (*Node, error) {
	var err error
	if cfg.Metrics == nil {
		if cfg.Metrics, err = metrics.New(nil); err != nil {
			return nil, err
		}
	}
	if cfg.Memory == nil {
		cfg.Memory = newMemoryGauge().read
	}
	n := &Node{
		cfg:    cfg,
		log:    &diskLog{fs: dir, dir: path, maps: map[uint64][]byte{}, before: cfg.BeforeRead, retry: make(chan struct{}, 1)},
		inbox:  make(chan input, 8192),
		done:   make(chan struct{}),
		memory: cfg.Memory,
		// Defragment sends to it from gRPC goroutines, so a reload keeps
		// it.
		defragQ: make(chan chan error),
	}
	if err := n.open(); err != nil {
		return nil, errors.Join(err, n.log.close())
	}
	if cfg.ForceNewCluster && n.core.Applied() > 0 {
		if err := n.forceNewCluster(); err != nil {
			return nil, errors.Join(err, n.log.close())
		}
	}
	return n, nil
}

// forceNewCluster writes the entries that leave this node the only
// member, committed, and starts the core again from the log.
func (n *Node) forceNewCluster() error {
	ents, hs, err := n.core.ForceNewCluster()
	if err != nil || len(ents) == 0 {
		return err
	}
	n.logf("forcing a new cluster: removing %d members after commit index %d", len(ents), n.core.Applied())
	n.log.w.Append(&hs, ents)
	if err := n.log.w.Sync(); err != nil {
		return err
	}
	return n.open()
}

// open recovers the log and starts a core from it, with empty queues to
// the disk goroutines. The new log replaces the old one, if any, at once,
// for snapshots this node serves meanwhile.
func (n *Node) open() error {
	n.logf("recovering the log")
	w, rec, err := wal.Open(n.log.fs)
	if err != nil {
		return err
	}
	if n.cfg.SegmentBytes > 0 {
		w.SetSegmentBytes(n.cfg.SegmentBytes)
	}
	n.log.mu.Lock()
	err = n.log.close()
	n.log.w, n.log.sealed, n.log.ckpts, n.log.removal = w, w.Seq()-1, nil, nil
	n.log.held.Store(false)
	n.log.mu.Unlock()
	if err != nil {
		return err
	}
	// Only a reload finds a core of an earlier run. It goes before the
	// new one loads, so that the two are never in memory at once.
	reload := n.core != nil
	n.core = nil
	if reload && n.cfg.reloading != nil {
		n.cfg.reloading(n)
	}
	run, err := nextRun(n.log.fs)
	if err != nil {
		return err
	}
	cfg := n.cfg.Core
	cfg.Boot, cfg.Run = rand.Uint64(), run
	cfg.ConnectedTicks = n.connectedTicks
	cfg.Timer = newTimer(n.cfg.Metrics, cmp.Or(n.cfg.WarningApplyDuration, warningApply), n.logf)
	c, err := core.New(cfg, rec, n.log)
	if err != nil {
		return err
	}
	if r := c.DurableCheckpoint(); r.Index > 0 {
		n.log.mu.Lock()
		n.log.ckpts = []wal.Root{r}
		n.log.mu.Unlock()
	}
	n.core = c
	n.persistQ = make(chan persistBatch, maxBacklog)
	n.persisted = make(chan persistDone, 1)
	n.ckptQ = make(chan core.CheckpointChunk, 64)
	n.cleanQ = make(chan packRoot)
	n.wantQ = make(chan uint64, 1)
	n.nextQ = make(chan struct{})
	n.readQ = make(chan *core.ValueRead)
	n.readDone = make(chan readDone, readers)
	n.leader.Store(0)
	v := c.EtcdVersion()
	n.etcdVersion.Store(&v)
	n.authEnabled.Store(c.AuthEnabled())
	n.setMembers(c.Members())
	n.alarmsGen = 0
	as, _ := c.Alarms()
	as = slices.Clone(as)
	n.alarms.Store(&as)
	n.logf("recovered the log up to entry %d", c.Applied())
	return nil
}

// loadAlarms copies the core's alarms if they changed.
func (n *Node) loadAlarms() {
	if as, gen := n.core.Alarms(); gen != n.alarmsGen {
		n.alarmsGen = gen
		as = slices.Clone(as)
		n.alarms.Store(&as)
	}
}

// Alarms are the active alarms, as this node applied them last.
func (n *Node) Alarms() []*pb.AlarmMember { return *n.alarms.Load() }

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logf != nil {
		n.cfg.Logf(format, args...)
	}
}

// ID is the node's Raft ID.
func (n *Node) ID() uint64 { return n.cfg.Core.ID }

// ClusterID is the cluster ID in the node's response headers.
func (n *Node) ClusterID() uint64 { return n.cfg.Core.ClusterID }

// Learner reports whether the node is a learner.
func (n *Node) Learner() bool { return n.learner.Load() }

// setMembers keeps the members' IDs for readers outside the committer.
func (n *Node) setMembers(ms []*pb.Member) {
	ids := make([]uint64, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	n.members.Store(&ids)
}

// connectedTicks is how many ticks the transport has had a connection
// to peer id, -1 while it has none or Run has not started.
func (n *Node) connectedTicks(id uint64) int64 {
	t, ok := n.transport.Load().(peer.Transport)
	if !ok {
		return -1
	}
	if since := t.ActiveSince(id); !since.IsZero() {
		return int64(time.Since(since) / n.cfg.Tick)
	}
	return -1
}

// PeerAddrs are the host:port of each member's first peer URL, by Raft ID.
func PeerAddrs(members []*pb.Member) map[uint64]string {
	out := map[uint64]string{}
	for _, m := range members {
		if len(m.PeerURLs) == 0 {
			continue
		}
		if u, err := url.Parse(m.PeerURLs[0]); err == nil {
			out[m.ID] = u.Host
		}
	}
	return out
}

// Leader is the leader the node knows of, or 0.
func (n *Node) Leader() uint64 { return n.leader.Load() }

// EtcdVersion is etcd's cluster version, which is also the storage
// version, as /version and Snapshot report them.
func (n *Node) EtcdVersion() string { return *n.etcdVersion.Load() }

// AuthEnabled reports whether auth was on when the committer looked last.
func (n *Node) AuthEnabled() bool { return n.authEnabled.Load() }

// Done is closed when the node stopped.
func (n *Node) Done() <-chan struct{} { return n.done }

// Deliver hands the node a message from a peer. It blocks while the
// committer is behind, which slows down that peer's connection.
func (n *Node) Deliver(m *raftpb.Message) {
	select {
	case n.inbox <- input{msg: m}:
	case <-n.done:
	}
}

// Unreachable reports a dropped message to a peer. It never blocks.
func (n *Node) Unreachable(id uint64) {
	select {
	case n.inbox <- input{unreachable: id}:
	default:
	}
}

// Removed tells the node that a peer answered that the cluster removed
// it; the node stops, as etcd's does.
func (n *Node) Removed() {
	select {
	case n.inbox <- input{removed: true}:
	case <-n.done:
	}
}

// Do runs one request and waits for its reply.
func (n *Node) Do(ctx context.Context, op proto.Message) (proto.Message, error) {
	return n.DoAs(ctx, core.Caller{}, op)
}

// DoAs runs one request of caller and waits for its reply. The caller
// counts in the size limit of a write.
func (n *Node) DoAs(ctx context.Context, caller core.Caller, op proto.Message) (proto.Message, error) {
	return n.do(ctx, &request{id: n.nextID.Add(1), op: op, caller: caller, reply: make(chan core.Reply, 1)})
}

// RangeChunk reads one chunk of a RangeStream: a range that counts keys
// only up to its limit, as etcd's chunks do.
func (n *Node) RangeChunk(ctx context.Context, caller core.Caller, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := n.do(ctx, &request{id: n.nextID.Add(1), op: r, chunk: true, caller: caller, reply: make(chan core.Reply, 1)})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RangeResponse), nil
}

// DoReply runs one request of caller and returns the node's whole reply.
func (n *Node) DoReply(ctx context.Context, caller core.Caller, op proto.Message) (core.Reply, error) {
	return n.doReply(ctx, &request{id: n.nextID.Add(1), op: op, caller: caller, reply: make(chan core.Reply, 1)})
}

func (n *Node) do(ctx context.Context, r *request) (proto.Message, error) {
	rep, err := n.doReply(ctx, r)
	return rep.Resp, err
}

func (n *Node) doReply(ctx context.Context, r *request) (core.Reply, error) {
	if err := n.put(ctx, input{req: r}); err != nil {
		return core.Reply{}, err
	}
	select {
	case rep := <-r.reply:
		return rep, rep.Err
	case <-ctx.Done():
		return core.Reply{}, ctx.Err()
	case <-n.done:
		return core.Reply{}, rpctypes.ErrGRPCStopped
	}
}

// readers is how many goroutines read values from the log for the
// committer.
const readers = 16

// readDone is a value read a reader ran, with its error.
type readDone struct {
	read *core.ValueRead
	err  error
}

// maxBacklog is the number of persist batches that may wait for the disk.
// Beyond it the committer stops taking new inputs, but it keeps ticking
// and taking finished disk writes.
const maxBacklog = 64

// Run drives the node until ctx ends or the node fails. Messages for peers
// go to t.
func (n *Node) Run(ctx context.Context, t peer.Transport) error {
	n.transport.Store(t)
	if n.cfg.CorruptCheckTime > 0 || n.cfg.CompactHashCheckTime > 0 {
		ctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() { n.checkHashes(ctx, t) })
		defer wg.Wait()
		defer cancel()
	}
	// A negative retention, which etcd's parsing lets through, turns auto
	// compaction off, as 0 does.
	if c := n.cfg.AutoCompaction; c.Revisions > 0 || c.Period > 0 {
		ctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() {
			if c.Revisions > 0 {
				n.compactByRevision(ctx, c.Revisions, cmp.Or(n.cfg.revisionEvery, revisionInterval))
				return
			}
			n.compactPeriodically(ctx, c.Period)
		})
		defer wg.Wait()
		defer cancel()
	}
	for {
		err := n.run(ctx, t)
		if core.FailureOf(err) == core.Reload && ctx.Err() == nil {
			n.logf("%v; starting again from the log", err)
			oerr := n.open()
			if oerr == nil {
				continue
			}
			err = errors.Join(err, oerr)
		}
		close(n.done)
		return errors.Join(err, n.log.close())
	}
}

// run drives the core until ctx ends or the core or a disk goroutine
// stops it.
func (n *Node) run(ctx context.Context, t peer.Transport) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() { n.prepareSegments(ctx) })
	wg.Go(func() { n.persist(ctx, t) })
	wg.Go(func() { n.writeCheckpoints(ctx) })
	wg.Go(func() { n.clean(ctx) })
	for range readers {
		wg.Go(func() { n.readValues(ctx) })
	}
	err := n.commit(ctx, t)
	cancel()
	wg.Wait()
	return err
}

// commit is the committer loop. It is the only goroutine that touches the
// core, and it never blocks on the disk.
func (n *Node) commit(ctx context.Context, t peer.Transport) error {
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	waiters := map[uint64]waiter{}
	var groups []completions
	groupOf := map[Completer]int{}
	var backlog []persistBatch
	// chunks are checkpoint parts for the writer; pace wakes the committer
	// to copy the next one once the writer took them all, so a slow disk
	// slows the copy instead of piling parts up in memory.
	var chunks []core.CheckpointChunk
	var pace *time.Timer
	// reads wait for a free reader.
	var reads []*core.ValueRead
	streams := map[uint64]*WatchStream{}
	var term uint64 // the term logged last
	for {
		inbox := n.inbox
		if len(backlog) >= maxBacklog {
			inbox = nil
		}
		var toDisk chan<- persistBatch
		var next persistBatch
		if len(backlog) > 0 {
			toDisk, next = n.persistQ, backlog[0]
		}
		var toWriter chan<- core.CheckpointChunk
		var chunk core.CheckpointChunk
		if len(chunks) > 0 {
			toWriter, chunk = n.ckptQ, chunks[0]
		}
		var toReader chan<- *core.ValueRead
		var read *core.ValueRead
		if len(reads) > 0 {
			toReader, read = n.readQ, reads[0]
		}
		var paced <-chan time.Time
		if n.core.Copying() && len(chunks) == 0 {
			if pace == nil {
				pace = time.NewTimer(checkpointPace)
			}
			paced = pace.C
		}
		select {
		case in := <-inbox:
			n.handle(in, waiters, streams)
		drain:
			for range 4096 {
				select {
				case in := <-n.inbox:
					n.handle(in, waiters, streams)
				default:
					break drain
				}
			}
		case done := <-n.persisted:
			if done.err != nil {
				if core.FailureOf(done.err) == core.Reload {
					n.dropAll(waiters, streams, rpctypes.ErrGRPCStopped)
				}
				return done.err
			}
			for _, j := range done.jobs {
				n.core.Persisted(j)
				if n.cfg.Persisted != nil {
					n.cfg.Persisted(j)
				}
			}
			n.logPersistTiming(done)
		case toDisk <- next:
			backlog[0] = persistBatch{}
			backlog = backlog[1:]
			continue
		case toWriter <- chunk:
			chunks[0] = core.CheckpointChunk{}
			chunks = chunks[1:]
			continue
		case toReader <- read:
			reads[0] = nil
			reads = reads[1:]
			continue
		case d := <-n.readDone:
			n.core.ValuesRead(d.read, d.err)
		case <-paced:
			pace = nil
			n.core.CopyCheckpoint()
		case <-ticker.C:
			n.core.Tick()
			n.measureMemory(streams)
		case <-ctx.Done():
			return nil
		}
		if b := n.cfg.DiskBudget; b > 0 {
			n.core.SetDisk(n.log.used.Load(), b)
		}
		eff, err := n.core.Process()
		if n.cfg.Turn != nil {
			n.cfg.Turn(n.core, eff, err)
		}
		if len(eff.Persist) > 0 {
			backlog = append(backlog, persistBatch{jobs: eff.Persist, queued: time.Now()})
		}
		chunks = append(chunks, eff.Checkpoint...)
		reads = append(reads, eff.Reads...)
		if eff.HandOff != 0 {
			n.logf("the leader's disk stalled; handing leadership to %x", eff.HandOff)
		}
		if eff.Members != nil {
			n.setMembers(eff.Members)
			t.SetPeers(PeerAddrs(eff.Members), n.core.RemovedMembers())
			n.learner.Store(n.core.IsLearner())
		}
		for _, m := range eff.Send {
			t.Send(m)
		}
		groups = n.completeAll(eff.Replies, waiters, groups, groupOf)
		for _, w := range eff.Watch {
			ws, ok := streams[w.Stream]
			if !ok {
				continue
			}
			if planted.On("B13") {
				// Planted: wait for a slow watcher instead of closing it.
				ws.C <- w.Resp
				continue
			}
			// A stream falls too far behind by count or by bytes.
			size := int64(proto.Size(w.Resp))
			sent := false
			if ws.queued.Add(size) <= cmp.Or(n.cfg.WatchBytes, watchBytes) {
				select {
				case ws.C <- w.Resp:
					sent = true
				default:
				}
			}
			if !sent {
				n.closeTooSlow(streams, w.Stream)
			}
		}
		if l, tm := n.core.Leader(), n.core.Term(); l != n.leader.Load() || tm != term {
			n.logf("leader is %d at term %d", l, tm)
			n.leader.Store(l)
			term = tm
		}
		if v := n.core.EtcdVersion(); v != *n.etcdVersion.Load() {
			n.etcdVersion.Store(&v)
		}
		n.authEnabled.Store(n.core.AuthEnabled())
		n.loadAlarms()
		if err != nil {
			if errors.Is(err, core.ErrInstalled) {
				n.dropAll(waiters, streams, ErrSnapshotInstalled)
			}
			return err
		}
	}
}

// measureMemory gives the core what the process uses besides the value
// cache, so the cache takes only what the rest leaves. When the rest alone
// is at the limit, which leaves the cache nothing, it closes the watch
// stream that is furthest behind, as one too slow: its client resumes
// from the last revision it got. One stream goes per collection, which
// shows what that freed.
func (n *Node) measureMemory(streams map[uint64]*WatchStream) {
	limit := n.cfg.Core.MemoryBytes
	if limit <= 0 {
		return
	}
	other, fresh := n.memory(n.core.CacheLow())
	n.core.SetOtherMemory(other)
	if !fresh || other < limit {
		return
	}
	var slowest uint64
	var behind int64
	for id, ws := range streams {
		if q := ws.queued.Load(); q > behind {
			slowest, behind = id, q
		}
	}
	if behind > 0 {
		n.logf("memory: %d bytes in use besides the cache, limit %d; closing watch stream %d, %d bytes behind", other, limit, slowest, behind)
		n.closeTooSlow(streams, slowest)
	}
}

// closeTooSlow closes a watch stream whose client fell too far behind.
func (n *Node) closeTooSlow(streams map[uint64]*WatchStream, id uint64) {
	ws := streams[id]
	delete(streams, id)
	n.core.CloseWatchStream(id)
	ws.err = ErrWatchTooSlow
	close(ws.done)
}

// dropAll fails every request and ends every watch stream, with
// streamErr, of a run that ends for a reload; the node goes on, so their
// clients would otherwise wait for good. Clients try again, as after a
// restart.
func (n *Node) dropAll(waiters map[uint64]waiter, streams map[uint64]*WatchStream, streamErr error) {
	replies := make([]core.Reply, 0, len(waiters))
	for id := range waiters {
		replies = append(replies, core.Reply{ID: id, Err: rpctypes.ErrGRPCStopped})
	}
	n.completeAll(replies, waiters, nil, map[Completer]int{})
	for id, ws := range streams {
		delete(streams, id)
		ws.err = streamErr
		close(ws.done)
	}
}

// completions are the replies of one turn for one completer.
type completions struct {
	to Completer
	cs []Completion
}

// completeAll hands every reply to its waiter: a Do call gets its own,
// and each completer gets all of its replies in one Complete call.
func (n *Node) completeAll(replies []core.Reply, waiters map[uint64]waiter, groups []completions, groupOf map[Completer]int) []completions {
	for _, r := range replies {
		w, ok := waiters[r.ID]
		if !ok {
			continue
		}
		delete(waiters, r.ID)
		if w.reply != nil {
			w.reply <- r
			continue
		}
		i, ok := groupOf[w.to]
		if !ok {
			i = len(groups)
			groupOf[w.to] = i
			groups = append(groups, completions{to: w.to})
		}
		groups[i].cs = append(groups[i].cs, Completion{Tag: w.tag, Reply: r})
	}
	for _, g := range groups {
		g.to.Complete(g.cs)
	}
	clear(groupOf)
	clear(groups)
	return groups[:0]
}

func (n *Node) handle(in input, waiters map[uint64]waiter, streams map[uint64]*WatchStream) {
	switch {
	case in.batch != nil:
		b := in.batch
		for i := range b.reqs {
			r := &b.reqs[i]
			r.ID = b.first + uint64(i)
			waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}
		}
		n.core.SubmitAll(b.reqs)
	case in.status != nil:
		in.status <- n.core.Status()
	case in.metrics != nil:
		in.metrics <- n.core.Metrics()
	case in.hashes != nil:
		in.hashes <- n.core.CompactHashes()
	case in.snapshotPoint != nil:
		var p snapshotPoint
		p.info.Index, p.info.Term, p.info.Revision, p.err = n.core.SnapshotPoint()
		in.snapshotPoint <- p
	case in.watchOpen != nil:
		streams[in.watchOpen.id] = in.watchOpen
	case in.watchReq != nil:
		if _, ok := streams[in.stream]; ok {
			n.core.Watch(in.stream, streams[in.stream].caller, in.watchReq)
		}
	case in.watchClose:
		delete(streams, in.stream)
		n.core.CloseWatchStream(in.stream)
	case in.req != nil:
		waiters[in.req.id] = waiter{reply: in.req.reply}
		n.core.Submit(core.Request{ID: in.req.id, Op: in.req.op, Chunk: in.req.chunk, Caller: in.req.caller})
	case in.msg != nil:
		if err := n.core.Step(in.msg); err != nil {
			n.logf("step message from %d: %v", in.msg.GetFrom(), err)
		}
	case in.unreachable != 0:
		n.core.Unreachable(in.unreachable)
	case in.advance != 0:
		n.core.AdvanceElection(in.advance)
	case in.removed:
		n.core.Removed()
	case in.authorize != nil:
		in.authorize.reply <- n.core.Authorize(in.authorize.caller, in.authorize.op)
	case in.written != 0:
		n.core.CheckpointWritten(in.written, in.writeErr)
	case in.fetching:
		n.core.SnapshotStarted(in.snapshot)
	case in.served:
		n.core.SnapshotSent(in.snapshot, in.serveErr)
	}
}

// persistDone reports one group of synced jobs, or the error that stops
// the node.
type persistDone struct {
	jobs  []core.PersistJob
	err   error
	times persistTimes
}

// persistBatch starts its clock before the committer's backlog, not when
// the writer receives it. Coalesced batches keep the oldest queue time.
type persistBatch struct {
	jobs   []core.PersistJob
	queued time.Time
}

// syncsInFlight is how many written groups may wait for their sync.
const syncsInFlight = 4

// written is a group of jobs, and roots of cleaning passes, written to
// the log: f is the segment to sync for it, nil when nothing in it must be
// synced (core.PersistJob.MustSync), and dir whether the directory must be
// synced after it, as the segment took its name.
type written struct {
	jobs           []core.PersistJob
	packs          []packRoot
	f              wal.File
	dir            bool
	entries, bytes int
	times          persistTimes
}

// syncedGroup is a written group after its sync, or after the error that
// stops the node.
type syncedGroup struct {
	g   written
	err error
}

// persist writes every queued job, hands the group to syncGroups, and
// writes the next group while that one syncs, so the disk always has the
// next group to take. syncGroups reports durable jobs to the committer
// before returning them here: a later blocked Flush must not hold back
// an earlier append's response. A failed write or sync stops the node; it
// is never retried, and no later group is reported. A job with a snapshot
// first fetches it from the leader into the log, and a failed fetch ends
// the run for a reload. A group that comes back hands its durable roots
// to the cleaner, which removes what they made obsolete.
//
// A full segment is cut to its records before its last group's sync, and
// the log goes on in the file that prepareSegments readied, without a
// sync of its own. The new segment takes its name, and gets its first
// write, once every group of the old one is durable, as its header vouches
// for the old one's last record: until then, the jobs that come wait in
// the log's buffer. Its first group's sync also syncs the directory.
func (n *Node) persist(ctx context.Context, t peer.Transport) {
	w := n.log.w
	toSync := make(chan written, syncsInFlight)
	synced := make(chan syncedGroup, syncsInFlight)
	// report runs on the sync goroutine.
	report := func(g written, err error) error {
		d := time.Since(g.times.start)
		if err == nil && d > time.Duration(n.log.maxWrite.Load()) {
			n.log.maxWrite.Store(int64(d))
		}
		if err == nil && d > n.cfg.Tick {
			n.logf("slow write to the log: %d entries, %d bytes in %v", g.entries, g.bytes, d)
		}
		done := persistDone{jobs: g.jobs, err: err, times: g.times}
		select {
		case n.persisted <- done:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
		return err
	}
	var wg sync.WaitGroup
	wg.Go(func() { n.syncGroups(toSync, synced, report) })
	defer func() {
		close(toSync)
		wg.Wait()
	}()
	fail := func(err error) {
		select {
		case n.persisted <- persistDone{err: err}:
		case <-ctx.Done():
		}
	}
	inFlight := 0
	// old is the segment the log rotated out of, while groups written to
	// it wait for their syncs; release closes it once none does and names
	// the new segment, and reports whether the node goes on. dir is set
	// while the name waits for a directory sync.
	var old wal.File
	dir := false
	release := func() bool {
		if inFlight > 0 || old == nil {
			return true
		}
		n.log.mu.Lock()
		err := w.Place()
		n.log.mu.Unlock()
		err = errors.Join(old.Close(), err)
		old, dir = nil, true
		if err != nil {
			fail(fmt.Errorf("%w: start a segment: %w", core.ErrDisk, err))
		}
		return err == nil
	}
	// back takes a synced group back, and reports whether the node goes
	// on.
	back := func(s syncedGroup) bool {
		inFlight--
		if s.err != nil {
			return false
		}
		n.durable(s.g)
		return release()
	}
	// drain waits until every earlier group was reported to the committer.
	drain := func() bool {
		for inFlight > 0 {
			if !back(<-synced) {
				return false
			}
		}
		return true
	}
	var g written
	for {
		var pending persistBatch
		queue, cleanQ := n.persistQ, n.cleanQ
		if inFlight == syncsInFlight {
			queue, cleanQ = nil, nil
		}
		select {
		case pending = <-queue:
		more:
			for {
				select {
				case js := <-n.persistQ:
					pending.jobs = append(pending.jobs, js.jobs...)
				default:
					break more
				}
			}
			if g.times.start.IsZero() {
				g.times = persistTimes{queued: pending.queued, start: time.Now()}
			}
		case s := <-synced:
			if !back(s) {
				return
			}
		case pr := <-cleanQ:
			n.log.mu.Lock()
			w.InstallPacks(pr.add, pr.drop)
			n.cleaningStage("root-buffered")
			n.log.mu.Unlock()
			g.packs = append(g.packs, pr)
			if g.times.start.IsZero() {
				g.times.start = time.Now()
			}
		case <-ctx.Done():
			return
		}
		jobs, installs := core.UpToInstall(pending.jobs)
		for _, j := range jobs {
			if j.Install != nil {
				if !drain() {
					return
				}
				// The install checks the entries this group already
				// holds, which must be readable.
				n.log.used.Add(w.Buffered())
				if err := w.Flush(); err != nil {
					fail(err)
					return
				}
				if err := n.install(ctx, t, j.Install); err != nil {
					fail(fmt.Errorf("install the snapshot at %d from %d: %w", j.Install.Index, j.Install.From, err))
					return
				}
			}
			n.log.mu.Lock()
			w.Append(j.HardState, j.Entries)
			if j.Root != nil {
				w.AppendRoot(*j.Root)
			}
			n.log.mu.Unlock()
			g.jobs = append(g.jobs, j)
			g.entries += len(j.Entries)
			for _, e := range j.Entries {
				g.bytes += len(e.Data)
			}
		}
		if old != nil || len(g.jobs) == 0 && len(g.packs) == 0 {
			continue
		}
		buffered := w.Buffered()
		n.log.used.Add(buffered)
		m := n.cfg.Metrics
		g.times.flush = time.Now()
		err := w.Flush()
		g.times.flushed = time.Now()
		writeTime := g.times.flushed.Sub(g.times.flush)
		m.WALWrite.Observe(writeTime.Seconds())
		if writeTime > n.cfg.Tick {
			n.logf("slow log flush: %d bytes in %v", buffered, writeTime)
		}
		m.WALWriteBytes.Add(float64(buffered))
		full := err == nil && w.Full()
		if full {
			// The group's sync makes the cut durable too.
			err = w.Seal()
		}
		if err != nil {
			fail(fmt.Errorf("%w: %w", core.ErrDisk, err))
			return
		}
		g.dir, dir = dir, false
		if full || g.dir || len(g.packs) > 0 || slices.ContainsFunc(g.jobs, core.PersistJob.MustSync) {
			g.f = w.Segment()
		}
		if full && planted.On("B16") {
			// Planted: the group that fills the segment goes without its
			// sync.
			g.f = nil
		}
		toSync <- g
		inFlight++
		g = written{}
		if installs {
			// The run ends at the install and starts again from the log.
			drain()
			return
		}
		if full {
			if old, err = n.rotate(ctx); err != nil || !release() {
				return
			}
		}
	}
}

// syncGroups syncs the written groups in order and hands them back. One
// sync covers every group written before it started, so it takes all the
// groups queued at once. It reports each result before returning the
// group to the writer. After a failed sync or report it syncs nothing more
// and hands every later group back with that error.
func (n *Node) syncGroups(groups <-chan written, synced chan<- syncedGroup, report func(written, error) error) {
	var failed error
	for g := range groups {
		batch := []written{g}
	more:
		for {
			select {
			case g, ok := <-groups:
				if !ok {
					break more
				}
				batch = append(batch, g)
			default:
				break more
			}
		}
		err := failed
		var f wal.File
		dir := false
		for _, g := range batch {
			if g.f != nil {
				f = g.f
			}
			dir = dir || g.dir
		}
		var early []syncedGroup
		if err == nil && f != nil && (planted.On("B02") && n.Leader() == n.ID() || planted.On("B03") && n.Leader() != n.ID()) {
			// Planted: report the appends durable before their sync, up to
			// the first group with a root or a snapshot. The writer still
			// waits for the sync.
			for len(batch) > 0 && !slices.ContainsFunc(batch[0].jobs, func(j core.PersistJob) bool { return j.Root != nil || j.Install != nil }) {
				err = report(batch[0], err)
				early = append(early, syncedGroup{g: batch[0], err: err})
				batch = batch[1:]
			}
		}
		start := time.Now()
		if err == nil && f != nil {
			if n.cfg.BeforeSync != nil {
				n.cfg.BeforeSync()
			}
			err = f.Sync()
			// Planted: B05 takes records into the new segment before its
			// directory entry is durable.
			if err == nil && dir && !planted.On("B05") {
				err = n.log.fs.SyncDir()
			}
			if planted.On("B11") {
				// Planted: carry on as if the sync worked.
				err = nil
			}
			syncTime := time.Since(start)
			n.cfg.Metrics.WALFsync.Observe(syncTime.Seconds())
			if syncTime > n.cfg.Tick {
				n.logf("slow log sync: %v", syncTime)
			}
		}
		end := time.Now()
		for _, e := range early {
			synced <- e
		}
		for _, g := range batch {
			g.times.sync, g.times.synced = start, end
			err = report(g, err)
			synced <- syncedGroup{g: g, err: err}
		}
		failed = err
	}
}

// durable notes what a synced group made durable, on the persist
// goroutine: a snapshot's checkpoint, and a root, whose removal of what it
// made obsolete the cleaner takes on; and the roots of cleaning passes,
// which the cleaner waits for.
func (n *Node) durable(g written) {
	l := n.log
	l.mu.Lock()
	for _, j := range g.jobs {
		if in := j.Install; in != nil && in.Checkpoint.Index > 0 {
			l.ckpts = []wal.Root{in.Checkpoint}
			l.removal = nil
			l.held.Store(false)
		}
		if j.Root != nil {
			l.ckpts = append(l.ckpts, *j.Root)
			if j.RemoveBelow > 0 {
				l.removal = &rootRemoval{j.RemoveBelow, *j.Root}
				l.held.Store(true)
			}
		}
	}
	l.mu.Unlock()
	l.wake()
	for _, pr := range g.packs {
		n.cleaningStage("root-synced")
		close(pr.synced)
	}
}

// rotate goes on in the next segment, which prepareSegments readied, and
// returns the old segment's file. The old segment must be sealed, and the
// new one waits for its name (wal.WAL.Place). It asks prepareSegments for
// the file of the one after.
func (n *Node) rotate(ctx context.Context) (wal.File, error) {
	start := time.Now()
	select {
	case <-n.nextQ:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if d := time.Since(start); d > n.cfg.Tick {
		n.logf("slow log rotation: waited %v for the next segment", d)
	}
	n.log.mu.Lock()
	old := n.log.w.Rotate()
	n.log.sealed = n.log.w.Seq() - 1
	n.log.mu.Unlock()
	n.wantQ <- n.log.w.Seq() + 1
	return old, nil
}

// syncLog writes and syncs what the log buffered, for the persist
// goroutine while no group is in flight, as in a snapshot install, and
// observes the sync in h. A full segment then ends, and the log goes on in
// the next one.
func (n *Node) syncLog(ctx context.Context, h prometheus.Observer) error {
	w := n.log.w
	if err := w.Flush(); err != nil {
		return err
	}
	if err := syncTimed(w.Segment(), h); err != nil || !w.Full() {
		return err
	}
	if err := errors.Join(w.Seal(), w.Segment().Sync()); err != nil {
		return err
	}
	old, err := n.rotate(ctx)
	if err != nil {
		return err
	}
	n.log.mu.Lock()
	err = w.Place()
	n.log.mu.Unlock()
	return errors.Join(old.Close(), err, n.log.fs.SyncDir())
}

// prepareSegments readies the file of each segment the disk goroutine
// asks for, ahead of the rotation that takes it, as etcd's file pipeline
// does: a spare or a new file, under a name that is not a segment's until
// the segment starts. A failure stops the node, as a failed write of the
// log does.
func (n *Node) prepareSegments(ctx context.Context) {
	n.log.mu.Lock()
	seq := n.log.w.Seq() + 1
	n.log.mu.Unlock()
	for {
		if err := wal.PrepareSegment(n.log.fs, seq); err != nil {
			select {
			case n.persisted <- persistDone{err: fmt.Errorf("%w: prepare segment %d: %w", core.ErrDisk, seq, err)}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case n.nextQ <- struct{}{}:
		case <-ctx.Done():
			// The file stays on disk, where the next run takes it.
			return
		}
		select {
		case seq = <-n.wantQ:
		case <-ctx.Done():
			return
		}
	}
}

// install fetches a snapshot from its leader and writes it to the log.
// A failed write stops the node, and a failed fetch reloads it
// (core.FetchFailed).
func (n *Node) install(ctx context.Context, t peer.Transport, in *core.Install) error {
	m, from := n.cfg.Metrics, peerLabel(in.From)
	n.logf("fetching the snapshot at %d from %d", in.Index, in.From)
	m.SnapshotApplyInProgress.Set(1)
	m.SnapshotReceiveInflights.WithLabelValues(from).Inc()
	start := time.Now()
	sink := &core.LogSink{WAL: n.log.w, Lock: &n.log.mu, Step: n.snapshotStep(ctx, cmp.Or(n.cfg.snapshotSync, syncBytes))}
	r, err := t.Fetch(ctx, in.From, in.Applied+1, in.Index)
	if err == nil {
		err = core.ReceiveSnapshot(r, sink, n.log, in)
		err = errors.Join(err, r.Close())
	}
	err = errors.Join(err, sink.Close())
	m.SnapshotApplyInProgress.Set(0)
	m.SnapshotReceiveInflights.WithLabelValues(from).Dec()
	if err == nil {
		d := time.Since(start).Seconds()
		m.SnapDBSave.Observe(d)
		m.SnapshotReceiveSeconds.WithLabelValues(from).Observe(d)
		m.SnapshotReceive.WithLabelValues(from).Inc()
	} else {
		m.SnapshotReceiveFailures.WithLabelValues(from).Inc()
	}
	switch {
	case err == nil:
		n.logf("installed the snapshot at %d from %d, checkpoint %d", in.Index, in.From, in.Checkpoint.Index)
		return nil
	case ctx.Err() != nil || planted.On("B26"):
		return err
	}
	return core.FetchFailed(err, func() error { return n.syncLog(ctx, m.SnapDBFsync) })
}

// syncBytes is how much of a snapshot the WAL buffers before a sync.
const syncBytes = 16 << 20

// snapshotStep is the Step of a snapshot sink: it syncs the log every
// syncEvery bytes of entries and values, and runs Config.stage, if set,
// after each such sync and each item.
func (n *Node) snapshotStep(ctx context.Context, syncEvery int) func(string, int) error {
	pending := 0
	return func(stage string, bytes int) error {
		if pending += bytes; pending >= syncEvery {
			pending = 0
			if err := n.syncLog(ctx, n.cfg.Metrics.SnapDBFsync); err != nil {
				return fmt.Errorf("%w: %w", core.ErrDisk, err)
			}
			if err := n.staged("sync"); err != nil {
				return err
			}
		}
		return n.staged(stage)
	}
}

// staged runs Config.stage, if set.
func (n *Node) staged(stage string) error {
	if n.cfg.stage != nil {
		return n.cfg.stage(stage)
	}
	return nil
}

// checkpointPace is how often the committer copies the next part of a
// checkpoint: with 32 KiB parts, about 32 MB/s.
const checkpointPace = time.Millisecond

// checkpointSyncBytes is how much of a checkpoint the writer writes
// between syncs.
const checkpointSyncBytes = 1 << 20

// writeCheckpoints writes the checkpoint parts the committer hands over.
// Once a checkpoint is whole, synced and its directory synced, it tells
// the committer, which asks for the root that makes it count. A failed
// write stops the node.
func (n *Node) writeCheckpoints(ctx context.Context) {
	for {
		var c core.CheckpointChunk
		select {
		case c = <-n.ckptQ:
		case <-ctx.Done():
			return
		}
		m := n.cfg.Metrics
		start := time.Now()
		f, err := n.log.w.CreateCheckpoint(c.Index)
		unsynced := 0
		for err == nil {
			_, err = f.Write(c.Data)
			n.log.used.Add(int64(len(c.Data)))
			if err != nil || c.Done {
				break
			}
			// Synced a little at a time, so that one large flush does not
			// hold up the log's fsyncs on the same disk.
			if unsynced += len(c.Data); unsynced >= checkpointSyncBytes {
				if err = syncTimed(f, m.SnapFsync); err != nil {
					break
				}
				unsynced = 0
			}
			select {
			case next := <-n.ckptQ:
				if next.Index != c.Index {
					err = fmt.Errorf("the checkpoint at %d started before the one at %d was done", next.Index, c.Index)
				}
				c = next
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		if f != nil {
			if err == nil {
				err = syncTimed(f, m.SnapFsync)
			}
			err = errors.Join(err, f.Close())
		}
		if err == nil {
			err = n.log.w.SyncDir()
		}
		if err == nil {
			m.SnapSave.Observe(time.Since(start).Seconds())
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case n.inbox <- input{written: c.Index, writeErr: err}:
		case <-ctx.Done():
			return
		}
	}
}

// Serve writes the snapshot stream a follower asked for: the entries
// first to last, or this node's newest durable checkpoint at or before
// last, the values it names from first on and the entries after it. The
// peer transport calls it.
func (n *Node) Serve(id, first, last uint64, w io.Writer) error {
	n.tell(input{snapshot: id, fetching: true})
	n.log.mu.Lock()
	n.log.serving++
	ckpts := slices.Clone(n.log.ckpts)
	n.log.mu.Unlock()
	m, to := n.cfg.Metrics, peerLabel(id)
	m.SnapshotSendInflights.WithLabelValues(to).Inc()
	start := time.Now()
	err := core.ServeSnapshot(n.log, ckpts, first, last, w)
	m.SnapshotSendInflights.WithLabelValues(to).Dec()
	if err == nil {
		m.SnapshotSend.WithLabelValues(to).Inc()
		m.SnapshotSendSeconds.WithLabelValues(to).Observe(time.Since(start).Seconds())
	} else {
		m.SnapshotSendFailures.WithLabelValues(to).Inc()
	}
	n.log.mu.Lock()
	n.log.serving--
	n.log.mu.Unlock()
	n.log.wake()
	n.tell(input{snapshot: id, served: true, serveErr: err})
	return err
}

// readValues runs the value reads the committer hands it.
func (n *Node) readValues(ctx context.Context) {
	for {
		var d readDone
		select {
		case d.read = <-n.readQ:
		case <-ctx.Done():
			return
		}
		d.err = d.read.Run()
		select {
		case n.readDone <- d:
		case <-ctx.Done():
			return
		}
	}
}

// tell hands the committer an input from a helper goroutine.
func (n *Node) tell(in input) {
	select {
	case n.inbox <- in:
	case <-n.done:
	}
}
