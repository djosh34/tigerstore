// Package node runs one tigerstore node: the committer goroutine that owns
// the core, the disk goroutine that persists the log, and the hookup to the
// peer transport. It is the real-world driver of core.Node; the simulator
// is the other one.
package node

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
	// AutoCompactionMode and AutoCompactionRetention are etcd's
	// --auto-compaction-mode and --auto-compaction-retention, the latter
	// as ParseCompaction gives it; a retention of 0 turns auto compaction
	// off (compact.go). RevisionCompactEvery, if set, replaces the 5
	// minutes between revision compactions, for tests.
	AutoCompactionMode      string
	AutoCompactionRetention time.Duration
	RevisionCompactEvery    time.Duration
	// CorruptCheckTime is etcd's --corrupt-check-time, and
	// CompactHashCheckTime its --compact-hash-check-time with the
	// CompactHashCheck feature gate on; 0 turns either check off
	// (corrupt.go).
	CorruptCheckTime, CompactHashCheckTime time.Duration
	// BeforeRead, if set, runs before each read of a value from the log.
	// Tests use it to stall those reads.
	BeforeRead func()
	Logf       func(format string, args ...any)
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
	// served ok) a snapshot fetch from this node.
	snapshot         uint64
	fetching, served bool
	servedOK         bool
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
	if n.delay() > 0 && slices.ContainsFunc(reqs, core.TakesSpace) {
		if err := n.throttle(ctx); err != nil {
			return err
		}
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
	persistQ  chan []core.PersistJob
	persisted chan persistDone
	ckptQ     chan core.CheckpointChunk
	cleanQ    chan packRoot
	defragQ   chan chan error
	readQ     chan *core.ValueRead
	readDone  chan readDone
	nextID    atomic.Uint64
	done      chan struct{}

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
}

// diskLog is the WAL as the committer, the snapshot server and the
// persist goroutine share it. The lock covers the WAL's map of where
// entries are: appends and reads take it, the fsync does not. Values in
// segments that no longer change are read from a read-only mapping of the
// file, a copy from the page cache instead of a read call.
type diskLog struct {
	mu  sync.Mutex
	w   *wal.WAL
	fs  osfs.Dir
	dir string
	// sealed is the newest segment that no longer changes; maps holds the
	// mappings of sealed segments by sequence number.
	sealed uint64
	maps   map[uint64][]byte
	// ckpts are the durable checkpoints whose files are on disk, which
	// the persist goroutine sets before it removes older files, and
	// serving counts the snapshot fetches under way, during which nothing
	// is removed.
	ckpts   []wal.Root
	serving int
	// used is the size of the log's files when the cleaner measured them
	// last, plus what the disk goroutines wrote since, and cleaned the
	// bytes that cleaning wrote.
	used, cleaned atomic.Int64
	// before runs before each value read; see Config.BeforeRead.
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
	if l.before != nil {
		l.before()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	seq, at, ok := l.w.ValuePlace(index, off, n)
	if !ok || seq > l.sealed {
		return l.w.Value(index, off, n)
	}
	m, err := l.mapping(seq)
	if err != nil {
		return nil, err
	}
	if at+int64(n) > int64(len(m)) {
		return nil, fmt.Errorf("node: value at %d+%d is past the end of segment %d", at, n, seq)
	}
	return bytes.Clone(m[at : at+int64(n)]), nil
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

// synced notes, with the lock held, the segments that no longer change
// after a sync, and unmaps those that were removed.
func (l *diskLog) synced() error {
	l.sealed = l.w.Seq() - 1
	return l.unmap(l.w.Oldest() - 1)
}

// unmap unmaps the segments up to upto, which are removed. The lock is
// held.
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
	err := l.w.Close()
	for seq, m := range l.maps {
		err = errors.Join(err, syscall.Munmap(m))
		delete(l.maps, seq)
	}
	return err
}

func (l *diskLog) Checkpoint(index uint64) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Checkpoint(index)
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
	if cfg.Metrics == nil {
		if cfg.Metrics, err = metrics.New(nil); err != nil {
			return nil, err
		}
	}
	n := &Node{
		cfg:   cfg,
		log:   &diskLog{fs: dir, dir: path, maps: map[uint64][]byte{}, before: cfg.BeforeRead},
		inbox: make(chan input, 8192),
		done:  make(chan struct{}),
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
	w, rec, err := wal.Open(n.log.fs)
	if err != nil {
		return err
	}
	if n.cfg.SegmentBytes > 0 {
		w.SetSegmentBytes(n.cfg.SegmentBytes)
	}
	n.log.mu.Lock()
	err = n.log.close()
	n.log.w, n.log.sealed, n.log.ckpts = w, w.Seq()-1, nil
	n.log.mu.Unlock()
	if err != nil {
		return err
	}
	cfg := n.cfg.Core
	cfg.Boot = rand.Uint64()
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
	n.persistQ = make(chan []core.PersistJob, maxBacklog)
	n.persisted = make(chan persistDone, 1)
	n.ckptQ = make(chan core.CheckpointChunk, 64)
	n.cleanQ = make(chan packRoot)
	n.defragQ = make(chan chan error)
	n.readQ = make(chan *core.ValueRead)
	n.readDone = make(chan readDone, readers)
	n.leader.Store(0)
	v := c.EtcdVersion()
	n.etcdVersion.Store(&v)
	n.authEnabled.Store(c.AuthEnabled())
	n.alarmsGen = 0
	as, _ := c.Alarms()
	as = slices.Clone(as)
	n.alarms.Store(&as)
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
	if core.TakesSpace(core.Request{Op: r.op}) {
		if err := n.throttle(ctx); err != nil {
			return core.Reply{}, err
		}
	}
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

// errReload ends a run whose snapshot could not be fetched. Raft cannot
// take the snapshot back, so the node starts a new core from its log, as
// a restart does, and Raft sends another snapshot.
var errReload = errors.New("node: the snapshot fetch failed")

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
	if r := n.cfg.AutoCompactionRetention; r > 0 {
		ctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() {
			if n.cfg.AutoCompactionMode == CompactRevision {
				n.compactByRevision(ctx, int64(r), cmp.Or(n.cfg.RevisionCompactEvery, revisionInterval))
				return
			}
			n.compactPeriodically(ctx, r)
		})
		defer wg.Wait()
		defer cancel()
	}
	for {
		err := n.run(ctx, t)
		if errors.Is(err, errReload) && ctx.Err() == nil {
			n.logf("%v; starting again from the log", err)
			if err = n.open(); err == nil {
				continue
			}
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
	var backlog [][]core.PersistJob
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
		var toDisk chan<- []core.PersistJob
		var next []core.PersistJob
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
				if errors.Is(done.err, errReload) {
					dropAll(waiters, streams)
				}
				return done.err
			}
			for _, j := range done.jobs {
				n.core.Persisted(j)
			}
		case toDisk <- next:
			backlog[0] = nil
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
		case <-ctx.Done():
			return nil
		}
		if n.cfg.DiskBudget > 0 {
			n.core.SetNoSpace(n.log.used.Load() >= n.cfg.DiskBudget)
		}
		eff, err := n.core.Process()
		if len(eff.Persist) > 0 {
			backlog = append(backlog, eff.Persist)
		}
		chunks = append(chunks, eff.Checkpoint...)
		reads = append(reads, eff.Reads...)
		for _, id := range eff.Closed {
			if ws, ok := streams[id]; ok {
				delete(streams, id)
				ws.err = ErrSnapshotInstalled
				close(ws.done)
			}
		}
		if eff.Members != nil {
			t.SetPeers(PeerAddrs(eff.Members), n.core.RemovedMembers())
			n.learner.Store(n.core.IsLearner())
		}
		for _, m := range eff.Send {
			t.Send(m)
		}
		groups = completeAll(eff.Replies, waiters, groups, groupOf)
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
				delete(streams, w.Stream)
				n.core.CloseWatchStream(w.Stream)
				ws.err = ErrWatchTooSlow
				close(ws.done)
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
			return err
		}
	}
}

// dropAll fails every request and ends every watch stream of a run that
// ends for a reload; the node goes on, so their clients would otherwise
// wait for good. Clients try again, as after a restart.
func dropAll(waiters map[uint64]waiter, streams map[uint64]*WatchStream) {
	replies := make([]core.Reply, 0, len(waiters))
	for id := range waiters {
		replies = append(replies, core.Reply{ID: id, Err: rpctypes.ErrGRPCStopped})
	}
	completeAll(replies, waiters, nil, map[Completer]int{})
	for id, ws := range streams {
		delete(streams, id)
		ws.err = rpctypes.ErrGRPCStopped
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
func completeAll(replies []core.Reply, waiters map[uint64]waiter, groups []completions, groupOf map[Completer]int) []completions {
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
		n.core.SnapshotSent(in.snapshot, in.servedOK)
	}
}

// persistDone reports one group of synced jobs, or the error that stops
// the node.
type persistDone struct {
	jobs []core.PersistJob
	err  error
}

// persist writes every queued job, syncs once, and reports the jobs back
// on their own channel, so it never waits for the committer's inbox. A
// failed write or sync stops the node; it is never retried. A job with a
// snapshot first fetches it from the leader into the log, and a failed
// fetch ends the run for a reload; a job with a root removes what the
// root made obsolete once it is synced.
func (n *Node) persist(ctx context.Context, t peer.Transport) {
	w := n.log.w
	for {
		var jobs []core.PersistJob
		select {
		case jobs = <-n.persistQ:
		case pr := <-n.cleanQ:
			if err := n.installPacks(pr); err != nil {
				select {
				case n.persisted <- persistDone{err: fmt.Errorf("install the root of a pack: %w", err)}:
				case <-ctx.Done():
				}
				return
			}
			continue
		case <-ctx.Done():
			return
		}
	more:
		for {
			select {
			case js := <-n.persistQ:
				jobs = append(jobs, js...)
			default:
				break more
			}
		}
		start := time.Now()
		entries, bytes := 0, 0
		done := persistDone{jobs: jobs}
		for _, j := range jobs {
			if j.Install != nil {
				if err := n.install(ctx, t, j.Install); err != nil {
					done = persistDone{err: fmt.Errorf("install the snapshot at %d from %d: %w", j.Install.Index, j.Install.From, err)}
					break
				}
			}
			n.log.mu.Lock()
			w.Append(j.HardState, j.Entries)
			if j.Root != nil {
				w.AppendRoot(*j.Root)
			}
			n.log.mu.Unlock()
			entries += len(j.Entries)
			for _, e := range j.Entries {
				bytes += len(e.Data)
			}
		}
		if done.err == nil {
			buffered := w.Buffered()
			n.log.used.Add(buffered)
			m := n.cfg.Metrics
			wrote := time.Now()
			err := w.Flush()
			synced := time.Now()
			m.WALWrite.Observe(synced.Sub(wrote).Seconds())
			m.WALWriteBytes.Add(float64(buffered))
			if err == nil {
				err = w.Sync()
				m.WALFsync.Observe(time.Since(synced).Seconds())
			}
			n.log.mu.Lock()
			err = errors.Join(err, n.log.synced())
			n.log.mu.Unlock()
			if err != nil {
				done = persistDone{err: err}
			}
		}
		for _, j := range jobs {
			if done.err != nil {
				break
			}
			n.log.mu.Lock()
			if in := j.Install; in != nil && in.Checkpoint.Index > 0 {
				n.log.ckpts = []wal.Root{in.Checkpoint}
			}
			var err error
			if j.Root != nil {
				n.log.ckpts = append(n.log.ckpts, *j.Root)
				// A fetch under way may read what the removal would take;
				// the next root removes it.
				if j.RemoveBelow > 0 && n.log.serving == 0 {
					err = errors.Join(w.RemoveBelow(j.RemoveBelow), w.RemoveCheckpoints(j.Root.Index), n.log.synced())
					n.log.ckpts = []wal.Root{*j.Root}
				}
			}
			n.log.mu.Unlock()
			if err != nil {
				done = persistDone{err: err}
			}
		}
		if d := time.Since(start); d > n.cfg.Tick {
			n.logf("slow write to the log: %d entries, %d bytes in %v", entries, bytes, d)
		}
		select {
		case n.persisted <- done:
		case <-ctx.Done():
			return
		}
		if done.err != nil {
			return
		}
	}
}

// install fetches a snapshot from its leader and writes it to the log.
// A failed write stops the node. When the fetch fails, it syncs what the
// stream wrote, as a crash after a sync could keep it, and returns
// errReload: the core cannot drop the snapshot, a core started from the
// log can.
func (n *Node) install(ctx context.Context, t peer.Transport, in *core.Install) error {
	m, from := n.cfg.Metrics, peerLabel(in.From)
	m.SnapshotApplyInProgress.Set(1)
	m.SnapshotReceiveInflights.WithLabelValues(from).Inc()
	start := time.Now()
	sink := &logSink{n: n}
	r, err := t.Fetch(ctx, in.From, in.Applied+1, in.Index)
	if err == nil {
		err = core.ReceiveSnapshot(r, sink, n.log, in)
		err = errors.Join(err, r.Close())
	}
	err = errors.Join(err, sink.close())
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
	case sink.err != nil || ctx.Err() != nil || planted.On("B26"):
		return err
	}
	if serr := n.log.w.Sync(); serr != nil {
		return errors.Join(err, serr)
	}
	return fmt.Errorf("%w: %w", errReload, err)
}

// logSink writes a snapshot into the log, syncing every syncBytes. err is
// its first failed disk operation.
type logSink struct {
	n       *Node
	ckpt    wal.File
	pending int
	err     error
}

// disk notes err as a failed disk operation and returns it.
func (s *logSink) disk(err error) error {
	if s.err == nil {
		s.err = err
	}
	return err
}

// syncBytes is how much of a snapshot the WAL buffers before a sync.
const syncBytes = 16 << 20

func (s *logSink) wrote(n int) error {
	s.pending += n
	if s.pending < syncBytes {
		return nil
	}
	s.pending = 0
	return s.disk(s.sync())
}

// sync syncs the log and times it as an fsync of the snapshot.
func (s *logSink) sync() error {
	start := time.Now()
	err := s.n.log.w.Sync()
	s.n.cfg.Metrics.SnapDBFsync.Observe(time.Since(start).Seconds())
	return err
}

func (s *logSink) Entries(ents []*raftpb.Entry) error {
	s.n.log.mu.Lock()
	s.n.log.w.Append(nil, ents)
	s.n.log.mu.Unlock()
	size := 0
	for _, e := range ents {
		size += len(e.Data)
	}
	return s.wrote(size)
}

func (s *logSink) Value(index uint64, off uint32, value []byte) error {
	s.n.log.mu.Lock()
	s.n.log.w.AppendValue(index, off, value)
	s.n.log.mu.Unlock()
	return s.wrote(len(value))
}

func (s *logSink) CheckpointChunk(index uint64, data []byte) error {
	if s.ckpt == nil {
		f, err := s.n.log.w.CreateCheckpoint(index)
		if errors.Is(err, fs.ErrExist) {
			// An install queued before this one wrote the checkpoint, and
			// this one asked from an older applied index. A core started
			// from the log asks from after it.
			return err
		}
		if err != nil {
			return s.disk(err)
		}
		s.ckpt = f
	}
	_, err := s.ckpt.Write(data)
	return s.disk(err)
}

func (s *logSink) CheckpointDone(uint64) error {
	f := s.ckpt
	s.ckpt = nil
	if f == nil {
		return errors.New("node: a snapshot's checkpoint has no data")
	}
	return s.disk(errors.Join(f.Sync(), f.Close(), s.n.log.w.SyncDir()))
}

func (s *logSink) Root(r wal.Root) error {
	s.n.log.mu.Lock()
	s.n.log.w.AppendRoot(r)
	s.n.log.mu.Unlock()
	return nil
}

// close closes a checkpoint file a failed fetch left open.
func (s *logSink) close() error {
	if s.ckpt == nil {
		return nil
	}
	return s.disk(s.ckpt.Close())
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
	n.tell(input{snapshot: id, served: true, servedOK: err == nil})
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
