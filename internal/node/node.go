// Package node runs one tigerstore node: the committer goroutine that owns
// the core, the disk goroutine that persists the log, and the hookup to the
// peer transport. It is the real-world driver of core.Node; the simulator
// is the other one.
package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
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
	select {
	case n.inbox <- input{req: r}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, rpctypes.ErrGRPCStopped
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
	waiters := map[uint64]chan core.Reply{}
	var backlog [][]core.PersistJob
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
			n.handle(in, waiters)
		drain:
			for range 4096 {
				select {
				case in := <-n.inbox:
					n.handle(in, waiters)
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
		for _, r := range eff.Replies {
			if ch, ok := waiters[r.ID]; ok {
				delete(waiters, r.ID)
				ch <- r
			}
		}
		n.leader.Store(n.core.Leader())
		if err != nil {
			return err
		}
	}
}

func (n *Node) handle(in input, waiters map[uint64]chan core.Reply) {
	switch {
	case in.req != nil:
		waiters[in.req.id] = in.req.reply
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
