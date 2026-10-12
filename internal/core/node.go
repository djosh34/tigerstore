// Package core is the committer of one node: an event and effect machine
// around a Raft RawNode and the MVCC index.
//
// Inputs are method calls: Submit (a client request), Step (a message from
// a peer), Tick (one clock tick) and Persisted (a disk write finished).
// Process returns the effects: disk writes, messages to send and replies.
// The core never does I/O, never reads the clock, never starts a goroutine
// and has no randomness of its own, so the simulator can run it exactly
// as the real node does.
package core

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// MaxRequestBytes is etcd's default --max-request-bytes.
const MaxRequestBytes = 1536 * 1024

// Config sets up one node.
type Config struct {
	// ID is this node's Raft ID, not 0.
	ID uint64
	// Peers are the Raft IDs of all voters, this node included.
	Peers []uint64
	// ClusterID goes into every response header.
	ClusterID uint64
	// Boot tells this run of the node apart from earlier runs, so that a
	// request from before a restart never matches one after it. The driver
	// takes it from its randomness adapter.
	Boot uint64

	// ElectionTicks and HeartbeatTicks are the Raft timeouts in ticks.
	ElectionTicks, HeartbeatTicks int
	// RequestTicks is how long a request waits before it fails with a
	// timeout. It may still commit afterwards.
	RequestTicks int
	// MaxBatchBytes caps the requests packed into one Raft entry.
	MaxBatchBytes int
}

// DefaultConfig returns the timeouts of etcd with 100 ms ticks.
func DefaultConfig(id uint64, peers []uint64) Config {
	return Config{
		ID:             id,
		Peers:          peers,
		ClusterID:      0x7469676572, // "tiger"
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		RequestTicks:   70,
		MaxBatchBytes:  4 << 20,
	}
}

// Request is a client request. Op is a *pb.RangeRequest, *pb.PutRequest,
// *pb.DeleteRangeRequest or *pb.TxnRequest. ID is chosen by the driver and
// comes back in the Reply; it must be unique among waiting requests.
type Request struct {
	ID uint64
	Op proto.Message
}

// Reply answers one request. Resp is the response of the same kind as the
// request, or nil with Err set.
type Reply struct {
	ID   uint64
	Resp proto.Message
	Err  error
}

// PersistJob is one disk write: entries and a hard state to append and
// sync. The driver runs jobs in order and calls Persisted for each, in the
// same order, once it is durable.
type PersistJob struct {
	HardState *wal.HardState
	Entries   []*raftpb.Entry
	msg       *raftpb.Message
}

// Effects is what the core asks its driver to do.
type Effects struct {
	Persist []PersistJob
	Send    []*raftpb.Message
	Replies []Reply
}

// pending is a request that waits for its entry to apply or for a read
// index.
type pending struct {
	req      Request
	deadline int64
	done     bool
}

// readBatch is the set of reads that share one ReadIndex request.
type readBatch struct {
	ctx   uint64
	index uint64 // 0 until the read index is known
	sent  int64  // tick of the last ReadIndex request
	reads []*pending
}

// readRetryTicks is how long a read batch waits for its read index before
// it asks again. Raft drops a ReadIndex without a word when a follower
// knows no leader or a message is lost.
const readRetryTicks = 3

// Node is one replica's committer.
type Node struct {
	cfg     Config
	rn      *raft.RawNode
	storage *storage
	kv      *mvcc.Store

	now      int64
	applied  uint64
	term     uint64
	leader   uint64
	waiting  map[uint64]*pending // writes by request ID
	timeouts []*pending          // every waiting request in deadline order

	proposals   []*pending
	propBytes   int
	reads       []*pending
	readBatches []*readBatch
	nextReadCtx uint64

	out Effects
	err error
}

// New starts a node from what its log recovered.
func New(cfg Config, rec *wal.Recovered) (*Node, error) {
	st := newStorage(cfg.Peers)
	if err := st.load(rec); err != nil {
		return nil, err
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              cfg.ElectionTicks,
		HeartbeatTick:             cfg.HeartbeatTicks,
		Storage:                   st,
		AsyncStorageWrites:        true,
		MaxSizePerMsg:             4 << 20,
		MaxInflightMsgs:           256,
		MaxInflightBytes:          32 << 20,
		MaxUncommittedEntriesSize: 1 << 30,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    quietLogger(),
	})
	if err != nil {
		return nil, err
	}
	return &Node{
		cfg:     cfg,
		rn:      rn,
		storage: st,
		kv:      mvcc.New(),
		term:    rec.HardState.Term,
		waiting: map[uint64]*pending{},
	}, nil
}

// quietLogger drops raft's log lines. Its Panic methods still panic,
// because raft relies on them for broken invariants.
func quietLogger() raft.Logger {
	return &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
}

// Err is the error that stopped the node, if any. A stopped node does
// nothing and must be restarted from its log.
func (n *Node) Err() error { return n.err }

// Store is the node's index. Only the simulator's checks read it directly.
func (n *Node) Store() *mvcc.Store { return n.kv }

// Applied is the index of the last applied entry.
func (n *Node) Applied() uint64 { return n.applied }

// AppliedEntries returns the log up to the applied index.
func (n *Node) AppliedEntries() ([]*raftpb.Entry, error) {
	if n.applied == 0 {
		return nil, nil
	}
	return n.storage.Entries(1, n.applied+1, math.MaxUint64)
}

// Leader is the leader this node knows of, or 0.
func (n *Node) Leader() uint64 { return n.leader }

// Term is the node's current Raft term.
func (n *Node) Term() uint64 { return n.term }

func (n *Node) fail(err error) {
	if n.err == nil {
		n.err = err
	}
}

// Tick advances the node's clock by one tick.
func (n *Node) Tick() {
	if n.err != nil {
		return
	}
	n.now++
	n.rn.Tick()
	n.expire()
	for _, b := range n.readBatches {
		if b.index == 0 && n.now-b.sent >= readRetryTicks {
			b.sent = n.now
			n.rn.ReadIndex(readCtx(n.cfg.Boot, b.ctx))
		}
	}
}

// Step hands the node a message from a peer.
func (n *Node) Step(m *raftpb.Message) error {
	if n.err != nil {
		return n.err
	}
	return n.rn.Step(m)
}

// Submit hands the node a client request. The reply comes in a later
// Effects.
func (n *Node) Submit(r Request) {
	if n.err != nil {
		n.reply(r.ID, nil, rpctypes.ErrGRPCStopped)
		return
	}
	if err := check(r.Op); err != nil {
		n.reply(r.ID, nil, err)
		return
	}
	p := &pending{req: r, deadline: n.now + int64(n.cfg.RequestTicks)}
	switch op := r.Op.(type) {
	case *pb.RangeRequest:
		if op.Serializable {
			n.serveRead(p)
			return
		}
		n.reads = append(n.reads, p)
	case *pb.TxnRequest:
		if mvcc.IsTxnReadOnly(op) {
			if mvcc.IsTxnSerializable(op) {
				n.serveRead(p)
				return
			}
			n.reads = append(n.reads, p)
			break
		}
		n.queueWrite(p)
	default:
		n.queueWrite(p)
	}
	n.timeouts = append(n.timeouts, p)
}

func check(op proto.Message) error {
	if proto.Size(op) > MaxRequestBytes {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	switch op := op.(type) {
	case *pb.RangeRequest:
		return mvcc.CheckRange(op)
	case *pb.PutRequest:
		return mvcc.CheckPut(op)
	case *pb.DeleteRangeRequest:
		return mvcc.CheckDelete(op)
	case *pb.TxnRequest:
		return mvcc.CheckTxn(op)
	}
	return fmt.Errorf("core: unknown request type %T", op)
}

func (n *Node) queueWrite(p *pending) {
	n.waiting[p.req.ID] = p
	n.proposals = append(n.proposals, p)
	n.propBytes += proto.Size(p.req.Op)
	if n.propBytes >= n.cfg.MaxBatchBytes {
		n.propose()
	}
}

// expire fails requests whose deadline passed.
func (n *Node) expire() {
	i := 0
	for ; i < len(n.timeouts); i++ {
		p := n.timeouts[i]
		if p.done {
			continue
		}
		if p.deadline > n.now {
			break
		}
		n.finish(p, nil, rpctypes.ErrGRPCTimeout)
	}
	n.timeouts = n.timeouts[i:]
}

func (n *Node) finish(p *pending, resp proto.Message, err error) {
	if p.done {
		return
	}
	p.done = true
	delete(n.waiting, p.req.ID)
	n.reply(p.req.ID, resp, err)
}

func (n *Node) reply(id uint64, resp proto.Message, err error) {
	n.out.Replies = append(n.out.Replies, Reply{ID: id, Resp: resp, Err: err})
}

// propose packs the queued writes into one Raft entry.
func (n *Node) propose() {
	if len(n.proposals) == 0 {
		return
	}
	data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, n.proposals)
	if err != nil {
		n.fail(err)
		return
	}
	props := n.proposals
	n.proposals, n.propBytes = nil, 0
	if err := n.rn.Propose(data); err != nil {
		// Raft dropped the proposal, so none of these requests will apply.
		for _, p := range props {
			n.finish(p, nil, rpctypes.ErrGRPCNoLeader)
		}
	}
}

// requestReads asks Raft for one read index for all queued reads.
func (n *Node) requestReads() {
	if len(n.reads) == 0 {
		return
	}
	n.nextReadCtx++
	b := &readBatch{ctx: n.nextReadCtx, sent: n.now, reads: n.reads}
	n.reads = nil
	n.readBatches = append(n.readBatches, b)
	n.rn.ReadIndex(readCtx(n.cfg.Boot, b.ctx))
}

// Persisted reports that a persist job is durable. Jobs must be reported
// in the order Process returned them.
func (n *Node) Persisted(job PersistJob) {
	if n.err != nil {
		return
	}
	if err := n.storage.append(job); err != nil {
		n.fail(err)
		return
	}
	n.deliver(job.msg.GetResponses())
}

// deliver steps messages for this node and sends the rest.
func (n *Node) deliver(msgs []*raftpb.Message) {
	for _, m := range msgs {
		if m.GetTo() != n.cfg.ID {
			n.out.Send = append(n.out.Send, m)
			continue
		}
		if err := n.rn.Step(m); err != nil {
			n.fail(fmt.Errorf("core: step local %s: %w", m.GetType(), err))
			return
		}
	}
}

// Process runs Raft until it has nothing more to do and returns the
// effects gathered since the last call. A non-nil error means the node
// stopped; the effects are still valid and carry its last replies.
func (n *Node) Process() (Effects, error) {
	if n.err == nil {
		n.propose()
		n.requestReads()
		for n.err == nil && n.rn.HasReady() {
			n.handleReady(n.rn.Ready())
		}
	}
	if n.err != nil {
		n.stop()
	}
	out := n.out
	n.out = Effects{}
	return out, n.err
}

// stop fails every waiting request once the node stopped.
func (n *Node) stop() {
	for _, p := range n.timeouts {
		n.finish(p, nil, rpctypes.ErrGRPCStopped)
	}
	n.timeouts, n.proposals, n.reads, n.readBatches = nil, nil, nil, nil
}

func (n *Node) handleReady(rd raft.Ready) {
	if rd.SoftState != nil {
		n.leader = rd.Lead
	}
	if rd.HardState != nil {
		n.term = rd.GetTerm()
	}
	for _, rs := range rd.ReadStates {
		n.readIndexKnown(rs)
	}
	for _, m := range rd.Messages {
		switch m.GetTo() {
		case raft.LocalAppendThread:
			if m.Snapshot != nil {
				n.fail(errors.New("core: snapshots are not supported yet"))
				return
			}
			job := PersistJob{Entries: m.Entries, msg: m}
			if m.Vote != nil {
				job.HardState = &wal.HardState{Term: m.GetTerm(), Vote: m.GetVote(), Commit: m.GetCommit()}
			}
			n.out.Persist = append(n.out.Persist, job)
		case raft.LocalApplyThread:
			if err := n.apply(m.Entries); err != nil {
				n.fail(err)
				return
			}
			n.deliver(m.GetResponses())
		default:
			n.out.Send = append(n.out.Send, m)
		}
	}
	n.serveReads()
}

func (n *Node) readIndexKnown(rs raft.ReadState) {
	ctx, ok := parseReadCtx(n.cfg.Boot, rs.RequestCtx)
	if !ok {
		return
	}
	for _, b := range n.readBatches {
		if b.ctx == ctx && b.index == 0 {
			b.index = rs.Index
		}
	}
}

// serveReads answers the read batches whose index is applied and drops
// the batches whose reads all timed out. A read index is valid for every
// read that arrived before it was requested, so batches need not finish in
// order.
func (n *Node) serveReads() {
	left := n.readBatches[:0]
	for _, b := range n.readBatches {
		if b.index != 0 && b.index <= n.applied {
			for _, p := range b.reads {
				n.serveRead(p)
			}
			continue
		}
		if !allDone(b.reads) {
			left = append(left, b)
		}
	}
	clear(n.readBatches[len(left):])
	n.readBatches = left
}

func allDone(ps []*pending) bool {
	for _, p := range ps {
		if !p.done {
			return false
		}
	}
	return true
}

func (n *Node) serveRead(p *pending) {
	if p.done {
		return
	}
	var resp proto.Message
	var err error
	switch op := p.req.Op.(type) {
	case *pb.RangeRequest:
		var r *pb.RangeResponse
		r, err = n.kv.Range(op)
		if err == nil {
			n.fillHeader(r.Header)
			resp = r
		}
	case *pb.TxnRequest:
		var r *pb.TxnResponse
		r, err = n.kv.Txn(op)
		if err == nil {
			n.fillHeader(r.Header)
			resp = r
		}
	}
	n.finish(p, resp, err)
}

func (n *Node) fillHeader(h *pb.ResponseHeader) {
	h.ClusterId = n.cfg.ClusterID
	h.MemberId = n.cfg.ID
	h.RaftTerm = n.term
}

// apply runs committed entries against the index and answers the requests
// that this run of the node is waiting for.
func (n *Node) apply(ents []*raftpb.Entry) error {
	for _, e := range ents {
		if e.GetType() != raftpb.EntryNormal || len(e.GetData()) == 0 {
			n.applied = e.GetIndex()
			continue
		}
		err := DecodeBatch(e.GetData(), func(origin, boot, id uint64, op proto.Message) error {
			resp, err := n.applyOp(op)
			if errors.Is(err, errInternal) {
				return err
			}
			if origin == n.cfg.ID && boot == n.cfg.Boot {
				if p, ok := n.waiting[id]; ok {
					n.finish(p, resp, err)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("core: apply entry %d: %w", e.GetIndex(), err)
		}
		n.applied = e.GetIndex()
	}
	return nil
}

var errInternal = errors.New("core: internal error")

// applyOp runs one write. Request errors (an unknown lease, a missing key
// for ignore_value) are answers; any other error stops the node.
func (n *Node) applyOp(op proto.Message) (proto.Message, error) {
	var h *pb.ResponseHeader
	var resp proto.Message
	var err error
	switch op := op.(type) {
	case *pb.PutRequest:
		var r *pb.PutResponse
		if r, err = n.kv.Put(op); err == nil {
			h, resp = r.Header, r
		}
	case *pb.DeleteRangeRequest:
		var r *pb.DeleteRangeResponse
		if r, err = n.kv.DeleteRange(op); err == nil {
			h, resp = r.Header, r
		}
	case *pb.TxnRequest:
		var r *pb.TxnResponse
		if r, err = n.kv.Txn(op); err == nil {
			h, resp = r.Header, r
		}
	default:
		return nil, fmt.Errorf("%w: write of type %T", errInternal, op)
	}
	if err != nil {
		if isAnswer(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", errInternal, err)
	}
	n.fillHeader(h)
	return resp, nil
}

func isAnswer(err error) bool {
	return errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) || errors.Is(err, rpctypes.ErrGRPCKeyNotFound)
}
