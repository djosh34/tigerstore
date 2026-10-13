// Package node runs one tigerstore node: the committer goroutine that owns
// the core, the disk goroutine that persists the log, and the hookup to the
// peer transport. It is the real-world driver of core.Node; the simulator
// is the other one.
package node

import (
	"cmp"
	"context"
	"errors"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Config sets up a node.
type Config struct {
	Core    core.Config
	DataDir string
	// Tick is the length of one Raft tick.
	Tick time.Duration
	// WatchQueue is how many responses a watch stream may fall behind
	// before it is closed; 0 means watchQueue.
	WatchQueue int
	Logf       func(format string, args ...any)
}

// input is one event for the committer from a client or a peer.
type input struct {
	req         *request
	batch       *batch
	msg         *raftpb.Message
	unreachable uint64
	watchOpen   *WatchStream
	watchReq    *pb.WatchRequest
	watchClose  bool
	stream      uint64
	status      chan *pb.StatusResponse
}

// watchQueue is how many responses a watch stream may fall behind before
// the node closes it. The client then reconnects and resumes.
const watchQueue = 4096

// ErrWatchTooSlow closes a watch stream whose client does not keep up. It
// is Unavailable because clientv3 resumes watches after that code and
// gives up after most others.
var ErrWatchTooSlow = status.Error(codes.Unavailable, "tigerstore: watch stream fell too far behind")

// WatchStream is one client watch stream on this node.
type WatchStream struct {
	n  *Node
	id uint64
	// C carries the responses in order.
	C    chan *pb.WatchResponse
	done chan struct{}
	err  error
}

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

// OpenWatch starts a watch stream.
func (n *Node) OpenWatch(ctx context.Context) (*WatchStream, error) {
	ws := &WatchStream{n: n, id: n.nextID.Add(1), C: make(chan *pb.WatchResponse, cmp.Or(n.cfg.WatchQueue, watchQueue)), done: make(chan struct{})}
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
	id    uint64
	op    proto.Message
	reply chan core.Reply
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
	Tag  any
	Resp proto.Message
	Err  error
}

// batch is requests that one client connection hands the committer at
// once. Their IDs are first, first+1, and so on.
type batch struct {
	first uint64
	ops   []proto.Message
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

// SubmitBatch hands the committer every request in ops with one inbox
// send, so a client connection does not pay a channel, a goroutine and a
// reply channel per request. tags[i] comes back with the reply to ops[i]
// through to.Complete. It waits while the inbox is full, which slows down
// the client connection; it fails only if ctx ends or the node stopped,
// and then none of the requests was taken. A batch handed over as the
// node stops may never be answered: the caller fails such requests once
// Done is closed.
func (n *Node) SubmitBatch(ctx context.Context, ops []proto.Message, tags []any, to Completer) error {
	select {
	case <-n.done:
		// put could still win the race for a free inbox, and nobody reads
		// it any more.
		return rpctypes.ErrGRPCStopped
	default:
	}
	k := uint64(len(ops))
	b := &batch{first: n.nextID.Add(k) - k + 1, ops: ops, tags: tags, to: to}
	return n.put(ctx, input{batch: b})
}

// Node is a running node.
type Node struct {
	cfg       Config
	core      *core.Node
	wal       *wal.WAL
	inbox     chan input
	persistQ  chan []core.PersistJob
	persisted chan persistDone
	nextID    atomic.Uint64
	done      chan struct{}

	// leader is the leader the committer saw last, for readers outside it.
	leader atomic.Uint64
}

// New recovers the node's log from disk.
func New(cfg Config) (*Node, error) {
	dir, err := osfs.New(filepath.Join(cfg.DataDir, "wal"))
	if err != nil {
		return nil, err
	}
	w, rec, err := wal.Open(dir)
	if err != nil {
		return nil, err
	}
	cfg.Core.Boot = rand.Uint64()
	c, err := core.New(cfg.Core, rec)
	if err != nil {
		return nil, errors.Join(err, w.Close())
	}
	return &Node{
		cfg:       cfg,
		core:      c,
		wal:       w,
		inbox:     make(chan input, 8192),
		persistQ:  make(chan []core.PersistJob, maxBacklog),
		persisted: make(chan persistDone, 1),
		done:      make(chan struct{}),
	}, nil
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logf != nil {
		n.cfg.Logf(format, args...)
	}
}

// ID is the node's Raft ID.
func (n *Node) ID() uint64 { return n.cfg.Core.ID }

// Leader is the leader the node knows of, or 0.
func (n *Node) Leader() uint64 { return n.leader.Load() }

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

// Do runs one request and waits for its reply.
func (n *Node) Do(ctx context.Context, op proto.Message) (proto.Message, error) {
	r := &request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}
	if err := n.put(ctx, input{req: r}); err != nil {
		return nil, err
	}
	select {
	case rep := <-r.reply:
		return rep.Resp, rep.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, rpctypes.ErrGRPCStopped
	}
}

// maxBacklog is the number of persist batches that may wait for the disk.
// Beyond it the committer stops taking new inputs, but it keeps ticking
// and taking finished disk writes.
const maxBacklog = 64

// Run drives the node until ctx ends or the node fails. Messages for peers
// go to t.
func (n *Node) Run(ctx context.Context, t peer.Transport) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() { n.persist(ctx) })
	err := n.commit(ctx, t)
	close(n.done)
	cancel()
	wg.Wait()
	return errors.Join(err, n.wal.Close())
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
	streams := map[uint64]*WatchStream{}
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
				return done.err
			}
			for _, j := range done.jobs {
				n.core.Persisted(j)
			}
		case toDisk <- next:
			backlog[0] = nil
			backlog = backlog[1:]
			continue
		case <-ticker.C:
			n.core.Tick()
		case <-ctx.Done():
			return nil
		}
		eff, err := n.core.Process()
		if len(eff.Persist) > 0 {
			backlog = append(backlog, eff.Persist)
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
			select {
			case ws.C <- w.Resp:
			default:
				delete(streams, w.Stream)
				n.core.CloseWatchStream(w.Stream)
				ws.err = ErrWatchTooSlow
				close(ws.done)
			}
		}
		n.leader.Store(n.core.Leader())
		if err != nil {
			return err
		}
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
		groups[i].cs = append(groups[i].cs, Completion{Tag: w.tag, Resp: r.Resp, Err: r.Err})
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
		for i, op := range b.ops {
			id := b.first + uint64(i)
			waiters[id] = waiter{to: b.to, tag: b.tags[i]}
			n.core.Submit(core.Request{ID: id, Op: op})
		}
	case in.status != nil:
		in.status <- n.core.Status()
	case in.watchOpen != nil:
		streams[in.watchOpen.id] = in.watchOpen
	case in.watchReq != nil:
		if _, ok := streams[in.stream]; ok {
			n.core.Watch(in.stream, in.watchReq)
		}
	case in.watchClose:
		delete(streams, in.stream)
		n.core.CloseWatchStream(in.stream)
	case in.req != nil:
		waiters[in.req.id] = waiter{reply: in.req.reply}
		n.core.Submit(core.Request{ID: in.req.id, Op: in.req.op})
	case in.msg != nil:
		if err := n.core.Step(in.msg); err != nil {
			n.logf("step message from %d: %v", in.msg.GetFrom(), err)
		}
	case in.unreachable != 0:
		n.core.Unreachable(in.unreachable)
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
// failed write or sync stops the node; it is never retried.
func (n *Node) persist(ctx context.Context) {
	for {
		var jobs []core.PersistJob
		select {
		case jobs = <-n.persistQ:
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
		for _, j := range jobs {
			n.wal.Append(j.HardState, j.Entries)
		}
		done := persistDone{jobs: jobs}
		if err := n.wal.Sync(); err != nil {
			done = persistDone{err: err}
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
