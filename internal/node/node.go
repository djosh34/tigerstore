// Package node runs one tigerstore node: the committer goroutine that owns
// the core, the disk goroutine that persists the log, and the hookup to the
// peer transport. It is the real-world driver of core.Node; the simulator
// is the other one.
package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djosh34/tigerstore/internal/expclock"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Config sets up a node.
type Config struct {
	Core    core.Config
	DataDir string
	// Tick is the length of one Raft tick.
	Tick time.Duration
	Logf func(format string, args ...any)
}

// input is one event for the committer from a client or a peer.
type input struct {
	req         *request
	msg         *raftpb.Message
	unreachable uint64
	watchOpen   *WatchStream
	watchReq    *pb.WatchRequest
	watchClose  bool
	stream      uint64
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

// OpenWatch starts a watch stream.
func (n *Node) OpenWatch(ctx context.Context) (*WatchStream, error) {
	ws := &WatchStream{n: n, id: n.nextID.Add(1), C: make(chan *pb.WatchResponse, watchQueue), done: make(chan struct{})}
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
	if os.Getenv("PRE47_TRACE") == "1" {
		cfg.Core.Trace = expclock.Event
	}
	c, err := core.New(cfg.Core, rec)
	if err != nil {
		return nil, errors.Join(err, w.Close())
	}
	return &Node{
		cfg:       cfg,
		core:      c,
		wal:       w,
		inbox:     make(chan input, 8192),
		persistQ:  make(chan []core.PersistJob, 1),
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
	expclock.Event("recv", uint64(m.GetType()), m.GetFrom(), m.GetIndex(), m.GetCommit())
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
	expclock.Event("arrival", r.id, uint64(len(n.inbox)), 0, 0)
	if err := n.put(ctx, input{req: r}); err != nil {
		return nil, err
	}
	select {
	case rep := <-r.reply:
		expclock.Event("do_reply", r.id, uint64(len(n.inbox)), 0, 0)
		return rep.Resp, rep.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, rpctypes.ErrGRPCStopped
	}
}

// maxDeferred bounds client requests held during an append by count, not
// bytes. One Process may produce several capped entries for one fsync;
// MaxBatchBytes is not a physical group size limit. At the count limit the
// inbox backpressures clients and peers, but ticks and completions still run.
const maxDeferred = 8192

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
	waiters := map[uint64]chan core.Reply{}
	var next []core.PersistJob
	var deferred []*request
	inflight := false
	var group uint64
	streams := map[uint64]*WatchStream{}
	for {
		inbox := n.inbox
		if len(deferred) >= maxDeferred {
			inbox = nil
		}
		var toDisk chan<- []core.PersistJob
		if len(next) > 0 {
			toDisk = n.persistQ
		}
		select {
		case in := <-inbox:
			expclock.Event("turn_input", group, uint64(len(n.inbox)), uint64(len(deferred)), 0)
			if inflight && in.req != nil {
				deferred = append(deferred, in.req)
			} else {
				n.handle(in, waiters, streams)
			}
		case done := <-n.persisted:
			expclock.Event("completion", group, uint64(len(n.inbox)), uint64(len(deferred)), 0)
			if done.err != nil {
				return done.err
			}
			for _, j := range done.jobs {
				n.core.Persisted(j)
			}
			inflight = false
			for _, r := range deferred {
				n.handle(input{req: r}, waiters, streams)
			}
			clear(deferred)
			deferred = deferred[:0]
		case toDisk <- next:
			expclock.Event("disk_enqueue", group, uint64(len(n.inbox)), uint64(len(deferred)), 0)
			next = nil
			continue
		case <-ticker.C:
			n.core.Tick()
		case <-ctx.Done():
			return nil
		}
		// Include inputs already queued when the append completes, not only
		// requests the owner managed to dequeue while the disk was busy.
		// Snapshot the count so continuous arrivals cannot delay disk work.
		for queued := len(n.inbox); queued > 0 && len(deferred) < maxDeferred; queued-- {
			in := <-n.inbox
			if inflight && in.req != nil {
				deferred = append(deferred, in.req)
			} else {
				n.handle(in, waiters, streams)
			}
		}
		// Do not take a Ready until the previous append is durable. Requests
		// stay outside the core too: Submit can propose at the byte cap.
		// This also holds control messages, apply and leader observations;
		// a long disk stall can delay heartbeats and trigger an election.
		if inflight {
			continue
		}
		expclock.Event("turn_process", group, uint64(len(n.inbox)), uint64(len(deferred)), uint64(len(waiters)))
		eff, err := n.core.Process()
		if len(eff.Persist) > 0 {
			group++
			for i := range eff.Persist {
				eff.Persist[i].TraceGroup = group
				for _, e := range eff.Persist[i].Entries {
					expclock.Event("group_entry", group, e.GetIndex(), uint64(len(e.GetData())), 0)
				}
				if h := eff.Persist[i].HardState; h != nil {
					expclock.Event("group_hs", group, h.Term, h.Vote, h.Commit)
				}
			}
			expclock.Event("group", group, uint64(len(eff.Persist)), uint64(len(n.inbox)), uint64(len(waiters)))
			next = eff.Persist
			inflight = true
		}
		for _, m := range eff.Send {
			expclock.Event("send", uint64(m.GetType()), m.GetTo(), m.GetIndex(), m.GetCommit())
			t.Send(m)
		}
		for _, r := range eff.Replies {
			if ch, ok := waiters[r.ID]; ok {
				delete(waiters, r.ID)
				expclock.Event("reply", r.ID, group, uint64(len(n.inbox)), uint64(len(waiters)))
				ch <- r
			}
		}
		for _, w := range eff.Watch {
			ws, ok := streams[w.Stream]
			if !ok {
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

func (n *Node) handle(in input, waiters map[uint64]chan core.Reply, streams map[uint64]*WatchStream) {
	switch {
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
		expclock.Event("handle_req", in.req.id, uint64(len(n.inbox)), 0, 0)
		waiters[in.req.id] = in.req.reply
		n.core.Submit(core.Request{ID: in.req.id, Op: in.req.op})
	case in.msg != nil:
		expclock.Event("step", uint64(in.msg.GetType()), in.msg.GetFrom(), in.msg.GetIndex(), in.msg.GetCommit())
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

// persist writes one group, syncs once, and reports the jobs back on their
// own channel. Only the final hard state is needed, since no response is
// released until the whole group is durable. Entry order must not change:
// an append can replace a previous job's suffix. A failure is never retried.
func (n *Node) persist(ctx context.Context) {
	yield := os.Getenv("PRE47_DISK_YIELD")
	for {
		var jobs []core.PersistJob
		select {
		case jobs = <-n.persistQ:
		case <-ctx.Done():
			return
		}
		if yield == "before" {
			runtime.Gosched()
		}
		group := jobs[0].TraceGroup
		expclock.Event("persist_start", group, uint64(len(jobs)), 0, 0)
		var hs *wal.HardState
		for _, j := range jobs {
			n.wal.Append(nil, j.Entries)
			if j.HardState != nil {
				hs = j.HardState
			}
		}
		n.wal.Append(hs, nil)
		done := persistDone{jobs: jobs}
		expclock.Event("sync_start", group, 0, 0, 0)
		if err := n.wal.Sync(); err != nil {
			done = persistDone{err: err}
		}
		expclock.Event("persist_end", group, 0, 0, 0)
		if yield == "after" {
			runtime.Gosched()
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
