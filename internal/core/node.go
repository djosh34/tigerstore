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
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// ErrDiskStalled stops a leader whose local append took longer than
// StallTicks election timeouts.
var ErrDiskStalled = errors.New("core: the leader's disk stalled; stopping so that another node can lead")

// StallTicks is how many election timeouts a leader's append may take.
const StallTicks = 2

// ErrCommitStalled stops a leader whose own entries have not committed for
// CommitStallTicks election timeouts.
var ErrCommitStalled = errors.New("core: the leader's entries stopped committing; stopping so that another node can lead")

// CommitStallTicks is how many election timeouts a leader's entry may wait
// to commit. It is above StallTicks, so a stalled local disk is caught
// first, and above what the healthy pair needs.
const CommitStallTicks = 3

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
	// TicksPerSecond converts lease TTLs to ticks.
	TicksPerSecond int
	// ProgressTicks is the interval of progress notifications to watchers
	// that ask for them; 0 turns them off.
	ProgressTicks int
	// CommitStallElections is how many election timeouts a leader's entry
	// may wait to commit before the leader stops; 0 means
	// CommitStallTicks. Tests set it lower to catch shorter stalls.
	CommitStallElections int
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
		ProgressTicks:  6000, // etcd's 10 minutes
		TicksPerSecond: 10,
	}
}

// Request is a client request. Op is a *pb.RangeRequest, *pb.PutRequest,
// *pb.DeleteRangeRequest, *pb.TxnRequest, *pb.CompactionRequest or a lease
// request; if Op is nil, Put is a put in its wire form. ID is chosen by
// the driver and comes back in the Reply. It must never repeat within one
// run of the node: a write that timed out can still commit later, and its
// entry carries its ID.
type Request struct {
	ID  uint64
	Op  proto.Message
	Put Put
	// Wire, if set, is Op in the client's encoding, decoded with
	// UnmarshalAlias. It goes into the Raft entry as it is.
	Wire []byte
	// Compact asks for a range's answer as Reply.Range, which the driver
	// encodes itself, instead of a RangeResponse message.
	Compact bool
	// Chunk asks for a range as one chunk of a RangeStream
	// (mvcc.Store.RangeChunk): no count beyond the limit.
	Chunk bool
}

// Reply answers one request. Resp is the response of the same kind as the
// request, or nil with Err set. A put in wire form gets no Resp: its
// answer is Rev, Term and PrevKv, which the driver encodes itself.
type Reply struct {
	ID     uint64
	Resp   proto.Message
	Err    error
	Rev    int64
	Term   uint64
	PrevKv *mvccpb.KeyValue
	// Range is the answer to a Compact range.
	Range mvcc.RangeResult
}

// PersistJob is one disk write: entries and a hard state to append and
// sync. The driver runs jobs in order and calls Persisted for each, in the
// same order, once it is durable.
type PersistJob struct {
	HardState *wal.HardState
	Entries   []*raftpb.Entry
	msg       *raftpb.Message
	// newVote is set when the job changes the term or the vote.
	newVote bool
}

// Effects is what the core asks its driver to do.
type Effects struct {
	Persist []PersistJob
	Send    []*raftpb.Message
	Replies []Reply
	Watch   []WatchResponse
	// Expired lists the leases that expired as this node applied the log.
	// Drivers may log them; the simulator checks their timing.
	Expired []int64
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
	state   *State
	kv      *mvcc.Store

	now      int64
	applied  uint64
	term     uint64
	leader   uint64
	isLeader bool
	leaseSeq uint64
	// voteTerm and vote are the newest term and vote sent to disk.
	// unsyncedVotes counts appends that change them and are not yet
	// durable; while any is, messages to peers wait in held. Raft with
	// async storage writes sends a candidate's vote requests before its
	// own term and vote are durable, so it can win, lose its term in a
	// failed write, and win the same term again with another log. held
	// keeps at most maxHeld messages.
	voteTerm, vote uint64
	unsyncedVotes  int
	held           []*raftpb.Message
	// appending holds the tick at which each local append with entries
	// went to disk, oldest first; appends finish in order. If the oldest
	// gets too old on the leader, its disk is stalled and it fences itself.
	appending []int64
	// uncommitted holds the last index and the tick of each local append
	// with entries since this node became leader, until they commit.
	uncommitted []proposedAt
	// ownIndex is the last index of the append that carries the leader's
	// last batch of writes, and durable the last index on disk. While
	// ownIndex is ahead, new writes wait. awaitOwn marks that the next
	// append with entries carries a batch just proposed. Appends of
	// forwarded proposals do not hold anything.
	ownIndex, durable uint64
	awaitOwn          bool
	waiting           map[uint64]*pending // writes by request ID
	timeouts          []*pending          // every waiting request in deadline order

	proposals   []*pending
	propBytes   int
	reads       []*pending
	readBatches []*readBatch
	nextReadCtx uint64

	// logBytes estimates the size of the durable log.
	logBytes int64

	streams  map[uint64]*watchStream
	watchers []*watcher

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
	var logBytes int64
	for _, e := range rec.Entries {
		logBytes += wal.EntryRecordBytes(e)
	}
	state := NewState(MinLeaseTTL(cfg))
	return &Node{
		logBytes: logBytes,
		cfg:      cfg,
		rn:       rn,
		storage:  st,
		state:    state,
		kv:       state.Store(),
		term:     rec.HardState.Term,
		voteTerm: rec.HardState.Term,
		vote:     rec.HardState.Vote,
		waiting:  map[uint64]*pending{},
		streams:  map[uint64]*watchStream{},
	}, nil
}

// MinLeaseTTL is etcd's smallest lease TTL: 1.5 election timeouts, rounded
// up to whole seconds; 2 s with the default timeouts.
func MinLeaseTTL(cfg Config) int64 {
	ticks := int64(3 * cfg.ElectionTicks / 2)
	tps := int64(cfg.TicksPerSecond)
	return (ticks + tps - 1) / tps
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
	if !planted.On("B17") && n.isLeader && len(n.appending) > 0 && n.now-n.appending[0] >= StallTicks*int64(n.cfg.ElectionTicks) {
		// A leader whose disk stalls still heartbeats, so the others would
		// never elect a replacement. It stops instead; the healthy pair
		// elects, and a restart brings it back from its log.
		n.fail(ErrDiskStalled)
		return
	}
	if n.isLeader && n.commitStalled() {
		// The same for a gray leader whose heartbeats get through but
		// whose appends do not: Raft's quorum check sees a live quorum,
		// yet nothing commits.
		n.fail(ErrCommitStalled)
		return
	}
	n.retryReads(false)
	n.expireLeases()
	if n.cfg.ProgressTicks > 0 && n.now%int64(n.cfg.ProgressTicks) == 0 {
		n.progressNotify()
	}
}

// retryReads asks again for the read index of batches that have none,
// after readRetryTicks or at once.
func (n *Node) retryReads(now bool) {
	for _, b := range n.readBatches {
		if b.index == 0 && (now || n.now-b.sent >= readRetryTicks) {
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

// Unreachable reports that a message to a peer was dropped, so Raft
// probes it instead of streaming to it.
func (n *Node) Unreachable(id uint64) {
	if n.err == nil {
		n.rn.ReportUnreachable(id)
	}
}

// Submit hands the node a client request. The reply comes in a later
// Effects.
func (n *Node) Submit(r Request) {
	n.submit(r, &pending{})
}

// SubmitAll hands the node several client requests, with one allocation
// for their bookkeeping.
func (n *Node) SubmitAll(rs []Request) {
	ps := make([]pending, len(rs))
	for i := range rs {
		n.submit(rs[i], &ps[i])
	}
}

// submit takes a request; p is its zero bookkeeping record.
func (n *Node) submit(r Request, p *pending) {
	if n.err != nil {
		n.reply(r.ID, nil, rpctypes.ErrGRPCStopped)
		return
	}
	if r.Op == nil {
		if err := checkPut(&r.Put); err != nil {
			n.reply(r.ID, nil, err)
			return
		}
		*p = pending{req: r, deadline: n.now + int64(n.cfg.RequestTicks)}
		n.queueWrite(p)
		n.timeouts = append(n.timeouts, p)
		return
	}
	if err := check(r.Op); err != nil {
		n.reply(r.ID, nil, err)
		return
	}
	*p = pending{req: r, deadline: n.now + int64(n.cfg.RequestTicks)}
	switch op := r.Op.(type) {
	case *pb.RangeRequest:
		if op.Serializable || planted.On("B01a") {
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
	case *pb.LeaseTimeToLiveRequest, *pb.LeaseLeasesRequest:
		n.reads = append(n.reads, p)
	case *pb.LeaseGrantRequest:
		if op.ID == 0 {
			// The node that takes the request picks the ID, so every
			// replica applies the same one, as in etcd.
			g := proto.CloneOf(op)
			g.ID = n.newLeaseID()
			p.req.Op, p.req.Wire = g, nil
		}
		n.queueWrite(p)
	default:
		n.queueWrite(p)
	}
	n.timeouts = append(n.timeouts, p)
}

// sizeRequestID stands in for etcd's request ID when sizing a write. etcd
// v3.7.2 limits the marshaled InternalRaftRequest, whose header holds an ID
// made from the low 16 bits of the member ID. When bit 15 of the member ID
// is set, the ID is a 10-byte varint, etcd's strictest case, which this
// matches to the byte; for other members etcd accepts one or more bytes
// more.
const sizeRequestID = 1 << 63

// check validates a request like etcd's gRPC layer, then limits the size of
// a write like etcd's server does. Reads do not go through Raft and have
// only gRPC's limit.
func check(op proto.Message) error {
	var err error
	r := &pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}}
	switch op := op.(type) {
	case *pb.RangeRequest:
		return mvcc.CheckRange(op)
	case *pb.PutRequest:
		err, r.Put = mvcc.CheckPut(op), op
	case *pb.DeleteRangeRequest:
		err, r.DeleteRange = mvcc.CheckDelete(op), op
	case *pb.TxnRequest:
		err, r.Txn = mvcc.CheckTxn(op), op
		if err == nil && mvcc.IsTxnReadOnly(op) {
			return nil
		}
	case *pb.CompactionRequest:
		r.Compaction = op
	case *pb.LeaseGrantRequest:
		r.LeaseGrant = op
	case *pb.LeaseRevokeRequest:
		r.LeaseRevoke = op
	case *pb.LeaseKeepAliveRequest, *pb.LeaseTimeToLiveRequest, *pb.LeaseLeasesRequest:
		return nil
	default:
		return fmt.Errorf("core: unknown request type %T", op)
	}
	if err != nil {
		return err
	}
	if proto.Size(r) > MaxRequestBytes {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

// putRequestBytes is the size of etcd's InternalRaftRequest around a put
// without the put itself: the header with sizeRequestID, and the put's tag.
var putRequestBytes = proto.Size(&pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}}) + protowire.SizeTag(4)

// checkPut checks a put in wire form as check does a decoded one. Like
// etcd, it limits the size of the request as protobuf marshals it again,
// not of the client's encoding.
func checkPut(p *Put) error {
	if err := mvcc.CheckPutOp(&p.PutOp); err != nil {
		return err
	}
	if putRequestBytes+protowire.SizeBytes(p.size()) > MaxRequestBytes {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

// newLeaseID returns a positive lease ID that differs from the IDs this
// node picked before, in this run and, by the boot ID, in earlier ones.
func (n *Node) newLeaseID() int64 {
	n.leaseSeq++
	id := int64((n.cfg.Boot<<32 ^ n.cfg.ID<<24 ^ n.leaseSeq) & (1<<63 - 1))
	if id == 0 {
		id = 1
	}
	return id
}

// batchFraming is the most a write adds to an entry besides its request:
// origin, boot, request ID, kind and length.
const batchFraming = 4*binary.MaxVarintLen64 + 1

// queueWrite adds a write to the next entry. A write that would take the
// entry past MaxBatchBytes first sends off the entry so far.
func (n *Node) queueWrite(p *pending) {
	size := len(p.req.Put.Wire) + batchFraming
	switch {
	case p.req.Wire != nil:
		size = len(p.req.Wire) + batchFraming
	case p.req.Op != nil:
		size = proto.Size(p.req.Op) + batchFraming
	}
	if n.propBytes > 0 && n.propBytes+size > n.cfg.MaxBatchBytes {
		n.propose()
	}
	if p.req.ID != 0 {
		n.waiting[p.req.ID] = p
	}
	n.proposals = append(n.proposals, p)
	n.propBytes += size
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
	if n.settle(p) {
		n.reply(p.req.ID, resp, err)
	}
}

// settle marks a request answered and reports whether a client waits for
// the answer.
func (n *Node) settle(p *pending) bool {
	if p.done {
		return false
	}
	p.done = true
	if p.req.ID == 0 {
		// An internal write, such as a lease expiry; no client waits.
		return false
	}
	delete(n.waiting, p.req.ID)
	return true
}

func (n *Node) reply(id uint64, resp proto.Message, err error) {
	n.out.Replies = append(n.out.Replies, Reply{ID: id, Resp: resp, Err: err})
}

// propose packs the queued writes into one Raft entry.
func (n *Node) propose() {
	// A write that timed out while it waited never goes out; its client
	// got an answer that allows both outcomes.
	props := slices.DeleteFunc(n.proposals, func(p *pending) bool { return p.done })
	size := n.propBytes
	n.proposals, n.propBytes = nil, 0
	if len(props) == 0 {
		return
	}
	data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)
	if err != nil {
		n.fail(err)
		return
	}
	if err := n.rn.Propose(data); err != nil {
		// Raft dropped the proposal, so none of these requests will apply.
		for _, p := range props {
			n.finish(p, nil, rpctypes.ErrGRPCNoLeader)
		}
		return
	}
	// On the leader, the next local append with entries carries this
	// batch; writes wait until it is durable (ownIndex). A follower only
	// forwards, so nothing waits.
	n.awaitOwn = n.isLeader
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
	for _, e := range job.Entries {
		n.logBytes += wal.EntryRecordBytes(e)
	}
	if len(job.Entries) > 0 {
		n.durable = job.Entries[len(job.Entries)-1].GetIndex()
		n.appending = n.appending[1:]
	}
	if job.newVote {
		n.unsyncedVotes--
		if n.unsyncedVotes == 0 {
			n.out.Send = append(n.out.Send, n.held...)
			n.held = nil
		}
	}
	n.deliver(job.msg.GetResponses())
}

// deliver steps messages for this node and sends the rest.
func (n *Node) deliver(msgs []*raftpb.Message) {
	for _, m := range msgs {
		if m.GetTo() != n.cfg.ID {
			n.send(m)
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
		// Self-clocked batching: while the leader's own append is on its
		// way to disk, writes wait and join one entry, which goes out when
		// the append is durable. Nothing else waits, and with no append in
		// flight a write goes out at once, so one client sees no delay.
		// The byte cap still sends a full entry early (queueWrite).
		if !n.isLeader || n.durable >= n.ownIndex {
			n.propose()
		}
		n.requestReads()
		for n.err == nil && n.rn.HasReady() {
			n.handleReady(n.rn.Ready())
		}
		if planted.On("B06d") {
			// Planted: a progress notification at the current revision
			// goes out ahead of the events it covers.
			for _, w := range n.watchers {
				if !w.dead && w.progress && len(w.pending) > 0 {
					n.watchReply(w.stream.id, &pb.WatchResponse{WatchId: w.id})
				}
			}
		}
		n.flushWatches()
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
		if leader := rd.RaftState == raft.StateLeader; leader != n.isLeader {
			n.isLeader = leader
			// A batch from an earlier leadership may have been replaced
			// and never reach the disk; wait for nothing from it.
			n.ownIndex, n.awaitOwn = 0, false
			n.uncommitted = nil
			n.leadershipChanged()
		}
		if rd.Lead != n.leader && rd.Lead != raft.None {
			// A term change clears Raft's pending read requests, so ask the
			// new leader at once instead of waiting for the retry.
			n.retryReads(true)
		}
		n.leader = rd.Lead
	}
	if rd.HardState != nil {
		n.term = rd.GetTerm()
	}
	for _, rs := range rd.ReadStates {
		n.readIndexKnown(rs)
	}
	// Appends first, so that a term or vote change also holds back the
	// messages of the same Ready.
	var newVotes []*raftpb.Message
	for _, m := range rd.Messages {
		if m.GetTo() == raft.LocalAppendThread && m.Vote != nil && (m.GetTerm() != n.voteTerm || m.GetVote() != n.vote) {
			n.voteTerm, n.vote = m.GetTerm(), m.GetVote()
			n.unsyncedVotes++
			newVotes = append(newVotes, m)
		}
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
				job.newVote = slices.Contains(newVotes, m)
			}
			n.out.Persist = append(n.out.Persist, job)
			if len(job.Entries) > 0 {
				n.appending = append(n.appending, n.now)
				if n.isLeader {
					n.uncommitted = append(n.uncommitted, proposedAt{job.Entries[len(job.Entries)-1].GetIndex(), n.now})
				}
			}
			if len(job.Entries) > 0 && n.awaitOwn {
				n.ownIndex = job.Entries[len(job.Entries)-1].GetIndex()
				n.awaitOwn = false
			}
		case raft.LocalApplyThread:
			if err := n.apply(m.Entries); err != nil {
				n.fail(err)
				return
			}
			n.deliver(m.GetResponses())
		default:
			n.send(m)
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
		if b.index != 0 && (b.index <= n.applied || planted.On("B01b")) {
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
		if p.req.Compact {
			res, err := n.kv.RangeKVs(op)
			if err != nil {
				n.finish(p, nil, err)
				return
			}
			if n.settle(p) {
				n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Term: n.term, Range: res})
			}
			return
		}
		var r *pb.RangeResponse
		if p.req.Chunk {
			r, err = n.kv.RangeChunk(op)
		} else {
			r, err = n.kv.Range(op)
		}
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
	case *pb.LeaseTimeToLiveRequest:
		r := n.state.timeToLive(op, n.remaining(op.ID))
		n.fillHeader(r.Header)
		resp = r
	case *pb.LeaseLeasesRequest:
		r := n.state.leaseIDs()
		n.fillHeader(r.Header)
		resp = r
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
		err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {
			var p *pending
			if origin == n.cfg.ID && boot == n.cfg.Boot {
				p = n.waiting[id]
			}
			if kind == kindPut {
				return n.applyPut(b, p)
			}
			op, err := newOp(kind)
			if err != nil {
				return err
			}
			// The op's values are slices of the entry, the one copy.
			if err := DecodeAlias(b, op); err != nil {
				return fmt.Errorf("%w: %w", errBadBatch, err)
			}
			resp, err := n.applyOp(op)
			if errors.Is(err, errInternal) {
				return err
			}
			if err == nil {
				n.matchWatches()
			}
			if p != nil {
				n.finish(p, resp, err)
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

// applyPut runs a put in place: the index keeps the key and value as
// slices of the entry, which is the one copy of them in memory. p is the
// request waiting for it on this node, if any.
func (n *Node) applyPut(b []byte, p *pending) error {
	put, err := ParsePut(b)
	if err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	prev, err := n.state.ApplyPut(&put.PutOp)
	if err != nil && !isAnswer(err) {
		return fmt.Errorf("%w: %w", errInternal, err)
	}
	if err == nil {
		n.matchWatches()
	}
	if p == nil || p.done {
		return nil
	}
	if err != nil {
		n.finish(p, nil, err)
		return nil
	}
	if p.req.Op != nil {
		r := &pb.PutResponse{Header: &pb.ResponseHeader{Revision: n.kv.Rev()}, PrevKv: prev}
		n.fillHeader(r.Header)
		n.finish(p, r, nil)
		return nil
	}
	if n.settle(p) {
		n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})
	}
	return nil
}

// applyOp runs one write. Request errors (an unknown lease, a missing key
// for ignore_value) are answers; any other error stops the node.
func (n *Node) applyOp(op proto.Message) (proto.Message, error) {
	resp, err := n.state.Apply(op)
	if err != nil {
		if errors.Is(err, errInternal) || isAnswer(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", errInternal, err)
	}
	if h, ok := resp.(interface{ GetHeader() *pb.ResponseHeader }); ok {
		n.fillHeader(h.GetHeader())
	}
	if e, ok := op.(*pb.LeaseCheckpoint); ok && resp != nil {
		n.out.Expired = append(n.out.Expired, e.ID)
	}
	if n.isLeader || planted.On("B10") {
		switch op := op.(type) {
		case *pb.LeaseGrantRequest:
			n.arm(op.ID)
		case *pb.LeaseKeepAliveRequest:
			n.arm(op.ID)
		}
	}
	return resp, nil
}

// isAnswer reports whether an error from the index answers the request.
// The index only returns etcd's gRPC errors for requests it rejects
// (unknown lease, missing key for ignore_value, a compacted or future
// revision); they leave the index unchanged and every replica gives the
// same answer. Anything else is a bug.
func isAnswer(err error) bool {
	_, ok := status.FromError(err)
	return ok
}

// proposedAt is the last index of a leader's append and the tick it began.
type proposedAt struct {
	index uint64
	at    int64
}

// commitStalled drops the leader's appends that have committed and reports
// whether the oldest one left has waited CommitStallElections election
// timeouts.
func (n *Node) commitStalled() bool {
	commit := n.rn.BasicStatus().GetCommit()
	i := 0
	for i < len(n.uncommitted) && n.uncommitted[i].index <= commit {
		i++
	}
	n.uncommitted = n.uncommitted[i:]
	limit := int64(cmp.Or(n.cfg.CommitStallElections, CommitStallTicks)) * int64(n.cfg.ElectionTicks)
	return len(n.uncommitted) > 0 && n.now-n.uncommitted[0].at >= limit
}

// maxHeld bounds the messages held while a term or vote change is not yet
// durable, such as on a candidate whose disk stalls.
const maxHeld = 1024

// send puts a message to a peer out, or holds it while a term or vote
// change is not yet durable.
func (n *Node) send(m *raftpb.Message) {
	if n.unsyncedVotes > 0 {
		// Raft resends what it loses, so past the bound the newest
		// messages are dropped instead of held.
		if len(n.held) < maxHeld {
			n.held = append(n.held, m)
		}
		return
	}
	n.out.Send = append(n.out.Send, m)
}
