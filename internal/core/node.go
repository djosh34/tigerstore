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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// StallTicks is how many election timeouts a leader's append may take
// before it hands leadership to a follower (handOff).
const StallTicks = 2

// ErrCommitStalled stops a leader with other voters whose own entries have
// not committed for CommitStallTicks election timeouts.
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
	// MaxRequestBytes and MaxTxnOps are etcd's --max-request-bytes and
	// --max-txn-ops; 0 means etcd's defaults, MaxRequestBytes and
	// mvcc.MaxTxnOps.
	MaxRequestBytes, MaxTxnOps int
	// Peers are the Raft IDs of all voters, this node included.
	Peers []uint64
	// ClusterID goes into every response header.
	ClusterID uint64
	// Boot tells this run of the node apart from earlier runs, so that a
	// request from before a restart never matches one after it. The driver
	// takes it from its randomness adapter. Run orders the runs: it is
	// higher than that of every earlier run of the node, and the driver
	// keeps it on disk (version.go).
	Boot, Run uint64

	// ElectionTicks and HeartbeatTicks are the Raft timeouts in ticks.
	ElectionTicks, HeartbeatTicks int
	// NoPreVote turns Raft's pre-vote off, as etcd's --pre-vote=false.
	NoPreVote bool
	// RequestTicks is how long a request waits before it fails with a
	// timeout. It may still commit afterwards.
	RequestTicks int
	// MaxBatchBytes caps the requests packed into one Raft entry.
	MaxBatchBytes int
	// TickMillis is the length of a tick in milliseconds, etcd's
	// --heartbeat-interval; it converts lease TTLs and other times in
	// seconds to ticks.
	TickMillis int64
	// ProgressTicks is the interval of progress notifications to watchers
	// that ask for them; 0 turns them off.
	ProgressTicks int
	// CommitStallElections is how many election timeouts a leader's entry
	// may wait to commit before the leader stops; 0 means
	// CommitStallTicks. StallElections is how many its own append may
	// take before it hands over; 0 means StallTicks. Both watch the
	// disks, so tests at shorter ticks set them higher to keep the time a
	// real node allows; the stress job sets the first lower to catch
	// shorter stalls.
	CommitStallElections, StallElections int

	// WindowBytes is how many bytes of applied entries stay in memory, and
	// WindowMaxBytes how many while a follower still needs them. Older
	// entries and their values are read from the log on disk.
	WindowBytes, WindowMaxBytes int64
	// UnappliedBytes bounds the entries in memory that are not applied
	// yet, in flight to the disk included: writes wait while it is full,
	// and Raft's limits on uncommitted entries and on bytes in flight to a
	// follower come from it.
	UnappliedBytes int64
	// A checkpoint is cut once the entries applied since the last one
	// pass CheckpointBytes and CheckpointRatio times the base, the size of
	// the newest checkpoint finished when the last one was cut. Each
	// checkpoint writes the whole index again: the ratio keeps those
	// bytes to about 1/ratio of the log's, and a restart replays about
	// max(CheckpointBytes, ratio times the base) of log. docs/design.md
	// has the measurements behind 64.
	CheckpointBytes, CheckpointRatio int64
	// CheckpointChunk is about how many bytes of the index each
	// CopyCheckpoint copies into a checkpoint under way.
	CheckpointChunk int
	// MemoryBytes is the memory the whole process may use, the value
	// cache included: the Go memory limit less room for the garbage
	// collector. The cache takes what the rest of the process leaves, as
	// the driver measures it (SetOtherMemory). A Range answer larger than
	// MemoryBytes is refused. 0 turns the cache and that check off.
	MemoryBytes int64
	// SnapshotTicks is how long a leader waits for a follower to fetch a
	// snapshot before it sends another.
	SnapshotTicks int
	// Record makes Effects.Applied carry every applied entry with the
	// state's hash after it, for the simulator's checks.
	Record bool
	// Members are the initial cluster's members, by Raft ID, with their
	// names and peer URLs, for the configuration changes that start a new
	// cluster's log; Peers without one start with no name or URL. Join
	// makes the node start with an empty log instead, as one that joins
	// a running cluster (--initial-cluster-state existing).
	Members []*pb.Member
	Join    bool
	// Name and ClientURLs are what this node publishes as its member's
	// attributes.
	Name       string
	ClientURLs []string
	// MaxLearners is etcd's --max-learners; 0 means 1.
	MaxLearners int
	// AuthTokenTicks is how long an unused simple token lives, etcd's
	// --auth-token-ttl; 0 means 300 seconds.
	AuthTokenTicks int
	// StrictReconfigCheck is etcd's --strict-reconfig-check: member
	// changes that would leave fewer started voters than a quorum, or
	// fewer connected ones, are refused.
	StrictReconfigCheck bool
	// ConnectedTicks, if set, is how many ticks this node has been
	// connected to peer id, -1 while it is not. Without it, every peer
	// counts as connected since long ago.
	ConnectedTicks func(id uint64) int64
	// QuotaBytes is etcd's --quota-backend-bytes: 0 means etcd's default
	// of 2 GiB, a negative value no quota (quota.go).
	QuotaBytes int64
	// LeaseCheckpointTicks, if set, makes the leader checkpoint the
	// remaining TTL of each lease that long after it armed it, as etcd's
	// LeaseCheckpoint feature does every 5 minutes (lease.go).
	LeaseCheckpointTicks int
	// MaxVersion is the newest cluster version the node supports; 0 means
	// MaxVersion. Tests set it lower to run an older binary.
	MaxVersion uint32
	// Timer, if set, times applies and ranges for the metrics.
	Timer Timer
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
		TickMillis:     100,

		StrictReconfigCheck: true,

		WindowBytes:     64 << 20,
		WindowMaxBytes:  128 << 20,
		UnappliedBytes:  64 << 20,
		CheckpointBytes: 256 << 20,
		CheckpointRatio: 64,
		CheckpointChunk: 32 << 10,
		SnapshotTicks:   200,
	}
}

// Log is the log on disk as the core reads it: the entries and values
// that left memory, and checkpoints. The driver implements it over its
// WAL.
type Log interface {
	// Entry reads an entry.
	Entry(index uint64) (*raftpb.Entry, error)
	// Has reports whether an entry is on disk.
	Has(index uint64) bool
	// Value reads n bytes at offset off of the data of entry index.
	Value(index uint64, off, n uint32) ([]byte, error)
	// Checkpoint opens the checkpoint file at a Raft index.
	Checkpoint(index uint64) (io.ReadCloser, error)
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
	// Caller is the client as etcd's size limit for writes sees it.
	Caller Caller
}

// Caller is a client as etcd v3.7.2's size limit for writes sees it. With
// client certificate authentication (--client-cert-auth), etcd takes the
// client's identity from its certificate even with auth disabled
// (EtcdServer.AuthInfoFromCtx), and every write's request header then holds
// the certificate's common name and the auth revision, which is 1 on a
// store that never changed auth. The zero Caller has no identity.
type Caller struct {
	// name is the common name of the client's certificate, and extra the
	// bytes it adds to the marshaled request with auth revision 1.
	name  string
	extra int
	// Token is the token of the call (etcd's "token" metadata), if any.
	// Verified says that the server checked it itself (JWT): User and
	// Revision are its claims, and an empty User means it is not valid.
	Token    string
	Verified bool
	User     string
	Revision uint64
	// root makes the caller the root user, as etcd's WithRoot does for
	// its own requests.
	root bool
}

// RootCaller is the root user, for a node's own requests such as its
// health check.
func RootCaller() Caller { return Caller{root: true} }

// NewCaller returns the caller whose certificate has this common name.
func NewCaller(commonName string) Caller {
	return Caller{name: commonName, extra: identityBytes(commonName, 1)}
}

// identityBytes is what a user and an auth revision add to the
// marshaled request.
func identityBytes(user string, revision uint64) int {
	with := proto.Size(&pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID, Username: user, AuthRevision: revision}})
	return with - proto.Size(&pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}})
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

// UpToInstall returns the jobs up to the first with an install, that one
// included, and whether there is one. The core ends at an install
// (ErrInstalled), so a driver writes nothing after it: a root that came
// later names a checkpoint of the state the install replaced (#182).
func UpToInstall(jobs []PersistJob) ([]PersistJob, bool) {
	if i := slices.IndexFunc(jobs, func(j PersistJob) bool { return j.Install != nil }); i >= 0 {
		return jobs[:i+1], true
	}
	return jobs, false
}

// ownDepth is how many appends of the leader's own writes may be on their
// way to disk at once.
const ownDepth = 2

// PersistJob is one disk write: entries and a hard state to append and
// sync. The driver runs jobs in order and calls Persisted for each, in the
// same order, once it is durable: synced, if MustSync says so, else
// written after every job before it was synced.
type PersistJob struct {
	// Install, if set, is a snapshot from Raft. The driver first brings
	// its log to the snapshot's index (ReceiveSnapshot), then appends the
	// rest of the job. Persisted then ends the core with ErrInstalled.
	Install   *Install
	HardState *wal.HardState
	Entries   []*raftpb.Entry
	// Root, if set, makes a checkpoint that the driver wrote count. The
	// driver appends it after the rest, syncs, and only then, if
	// RemoveBelow is set, removes the segments whose entries and values
	// are all below RemoveBelow and the checkpoint files before this one.
	Root        *wal.Root
	RemoveBelow uint64
	msg         *raftpb.Message
	// newVote is set when the job changes the term or the vote.
	newVote bool
	// bytes is the entry data of the job.
	bytes int64
}

// MustSync reports whether the job counts as durable only once synced: it
// has entries, a new term or vote, a snapshot or a root. A job with only a
// moved commit index need not be, as in etcd (raft.MustSync): after a
// crash the node relearns its commit from the leader. Its responses still
// wait for every job before it, which the driver syncs first.
func (j PersistJob) MustSync() bool {
	return len(j.Entries) > 0 || j.newVote || j.Install != nil || j.Root != nil
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
	// Reads are values that only the log has. The driver runs each on a
	// helper goroutine, never the committer, in any order, and hands it
	// back with ValuesRead.
	Reads []*ValueRead
	// Checkpoint is the next part of a checkpoint for the driver to write
	// in order. When the part with Done is written and synced, and the
	// directory synced, the driver calls CheckpointWritten.
	Checkpoint []CheckpointChunk
	// Applied is every entry applied, with Config.Record.
	Applied []Applied
	// Members, if set, are the members after a change; the driver
	// connects to their peer URLs.
	Members []*pb.Member
	// HandOff, if set, is the follower that this leader asked Raft to
	// make leader because its own disk stalled (handOff).
	HandOff uint64
}

// CheckpointChunk is part of the checkpoint at Index.
type CheckpointChunk struct {
	Index uint64
	Data  []byte
	Done  bool
}

// Applied is an entry as the node applied it, and the hash of its state
// after it. With Checkpoint set, the state at Index came from a
// checkpoint instead, and Data is nil.
type Applied struct {
	Index, Term uint64
	Type        raftpb.EntryType
	Data        []byte
	Hash        uint64
	Checkpoint  bool
}

// pending is a request that waits for its entry to apply or for a read
// index.
type pending struct {
	req      Request
	deadline int64
	done     bool
	// queued marks a write in proposals that was not proposed yet.
	queued bool
	// fetched holds the values read from the log for a read that needed
	// them at once, and compares how far its transaction's compares got.
	fetched  map[mvcc.Ref][]byte
	compares *mvcc.Compares
	// early marks a write in Node.early.
	early bool
	// conf is the configuration change of a member change.
	conf *raftpb.ConfChange
	// who is the identity a write carries into the log while auth is
	// enabled. ai is the identity of a read, checked when it is served;
	// aiCurrent says that it takes the auth revision of that moment, as
	// a simple token and a certificate do.
	who       *authInfo
	ai        authInfo
	aiCurrent bool
	// refuse, if set, answers the request once its write applied: the
	// NOSPACE alarm a refused write raised (quota.go).
	refuse error
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
	values  *values
	cache   *cache
	state   *State
	kv      *mvcc.Store

	now      int64
	applied  uint64
	term     uint64
	leader   uint64
	isLeader bool
	// leaderChanges counts the leaders this node has seen, as etcd's
	// etcd_server_leader_changes_seen_total does.
	leaderChanges int64
	counters      Counters
	timed         timedApply
	// membershipAt is where Counters.membershipSnapshot last counted.
	membershipAt uint64
	leaseSeq     uint64
	// announcedAt is the tick this node last proposed its version.
	announcedAt int64
	// confMessages wait for Raft to apply the preceding configuration
	// change. confGate is its index, or MaxUint64 until it reaches apply.
	confMessages []*raftpb.Message
	confGate     uint64
	confTerm     uint64
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
	// maxCommitWait is the most ticks one of them waited to commit.
	uncommitted   []proposedAt
	maxCommitWait int64
	// own holds the last index of each append not yet durable that
	// carries a batch of the leader's own writes, oldest first. While
	// ownDepth of them are on their way, new writes wait. awaitOwn marks
	// that the next append with entries carries a batch just proposed.
	// Appends of forwarded proposals do not hold anything.
	own      []uint64
	awaitOwn bool
	// otherMemory is what the process uses besides the value cache, as
	// the driver measured it last.
	otherMemory int64
	waiting     map[uint64]*pending // writes by request ID
	timeouts    []*pending          // every waiting request in deadline order

	proposals []*pending
	// early are writes that need featureVersion, which wait for it.
	early []*pending
	// confs are member changes that wait for a leader; lastMemberID is
	// the last member ID this node handed out, and publishedAt the tick
	// it last proposed its attributes.
	confs []*pending
	// transfers are the MoveLeader requests under way.
	transfers    []*pending
	lastMemberID uint64
	publishedAt  int64
	propBytes    int
	reads        []*pending
	readBatches  []*readBatch
	nextReadCtx  uint64

	// logBytes estimates the size of the durable log.
	logBytes int64
	// noSpace refuses new data: the node's disk is past its budget.
	// cutNow makes the next applied entry cut a checkpoint.
	noSpace, cutNow bool

	// log reads what left memory.
	log Log
	// copy is the checkpoint being copied, last the newest one finished,
	// sinceCkpt the entry bytes applied since the last cut, and base the
	// size the next cut is measured against (maybeCheckpoint).
	copy      *copying
	last      lastCkpt
	sinceCkpt int64
	base      int64
	// unpersisted is the entry bytes on their way to the disk, and
	// unready those proposed since the last Ready.
	unpersisted, unready int64
	// snapWait holds, on the leader, when each follower that needs a
	// snapshot started to wait for it, or 0 while its fetch runs.
	snapWait map[uint64]int64

	streams  map[uint64]*watchStream
	watchers []*watcher

	// cold are the value reads under way. watchRead is the read of this
	// turn's watch events.
	cold      map[*ValueRead]struct{}
	watchRead *ValueRead
	// applyQ holds Raft's apply messages not applied yet: while paused
	// waits for values from the log, ordered apply waits with it. resume
	// is the write that runs again after its read. readInline is set
	// while the node starts: nothing else waits then, so a write reads
	// the values it needs itself.
	applyQ         []*raftpb.Message
	paused, resume *pausedWrite
	readInline     bool

	// hashes are the hashes the last compactions took, oldest first.
	hashes []mvcc.KVHash
	// tokens are the simple tokens this node assigned (auth.go), and
	// tokenWaits the requests whose token's entry is not applied yet.
	tokens     map[string]*simpleToken
	tokenWaits []tokenWait
	// noSpaceAt is the tick this node last proposed its NOSPACE alarm,
	// or -1 once an alarm change of it applied.
	noSpaceAt int64

	// removeAt is the shutdown tick after applying our own removal. Raft
	// keeps running until then so the leader can send the commit update.
	removeAt int64

	out Effects
	err error
}

// New starts a node from what its log recovered: it loads the checkpoint
// the root names, applies the committed entries after it, and hands Raft
// the rest. It reads every entry from disk, one at a time.
func New(cfg Config, rec *wal.Recovered, disk Log) (*Node, error) {
	st := newStorage()
	c := newCache()
	vals := &values{storage: st, log: disk, cache: c}
	cfg.MaxRequestBytes = cmp.Or(cfg.MaxRequestBytes, MaxRequestBytes)
	cfg.MaxTxnOps = cmp.Or(cfg.MaxTxnOps, mvcc.MaxTxnOps)
	n := &Node{
		cfg:       cfg,
		storage:   st,
		values:    vals,
		cache:     c,
		log:       disk,
		term:      rec.HardState.Term,
		voteTerm:  rec.HardState.Term,
		vote:      rec.HardState.Vote,
		waiting:   map[uint64]*pending{},
		streams:   map[uint64]*watchStream{},
		snapWait:  map[uint64]int64{},
		cold:      map[*ValueRead]struct{}{},
		logBytes:  rec.Bytes,
		noSpaceAt: -1,
		tokens:    map[string]*simpleToken{},
	}
	n.setState(NewState(MinLeaseTTL(cfg), vals))
	if root := rec.Root; root.Index > 0 {
		state, last, err := loadCheckpoint(cfg, disk, root, vals)
		if err != nil {
			return nil, fmt.Errorf("%w at %d: %w", errLoadCheckpoint, root.Index, err)
		}
		n.setState(state)
		n.applied, n.last, n.base, st.ckpt = root.Index, last, last.base, root
		n.membershipAt = root.Index
		n.record(root.Index, root.Term)
		st.setConf(root.Index, state.confState())
	}
	// The commit index on disk can lag what was applied, which a
	// checkpoint shows.
	hs := rec.HardState
	hs.Commit = max(hs.Commit, n.applied)
	n.readInline = true
	term, err := n.replay(hs.Commit)
	n.readInline = false
	if err != nil {
		return nil, err
	}
	// Forget what the replay wrote: its entries never enter memory, so
	// none of their values go into the cache.
	n.fillCache(n.applied)
	if hs.Commit > 0 {
		snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(hs.Commit), Term: new(term), ConfState: st.confAt(hs.Commit)}}
		if err := st.reset(snap); err != nil {
			return nil, err
		}
	}
	for i := hs.Commit + 1; i <= rec.Last; i++ {
		e, err := disk.Entry(i)
		if err != nil {
			return nil, fmt.Errorf("core: read entry %d: %w", i, err)
		}
		if err := st.add([]*raftpb.Entry{e}); err != nil {
			return nil, err
		}
	}
	if err := st.setHardState(hs); err != nil {
		return nil, err
	}
	rn, err := raft.NewRawNode(raftConfig(cfg, st, hs.Commit))
	if err != nil {
		return nil, err
	}
	n.rn = rn
	if n.state.removed[cfg.ID] {
		return nil, ErrRemoved
	}
	if rec.Last == 0 && rec.Root.Index == 0 && !cfg.Join {
		// A new cluster: its log starts with its members.
		peers, err := bootstrapPeers(cfg)
		if err != nil {
			return nil, err
		}
		if err := rn.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	// A node that joins knows no members until the log or a snapshot
	// brings them; its driver keeps the peers it was started with.
	if len(n.state.members) > 0 {
		n.out.Members = n.state.cloneMembers()
	}
	return n, nil
}

// replay applies the committed entries after the checkpoint, read from
// the log, and returns the term of the entry at commit.
func (n *Node) replay(commit uint64) (uint64, error) {
	term := n.storage.ckpt.Term
	for i := n.applied + 1; i <= commit; i++ {
		e, err := n.log.Entry(i)
		if err != nil {
			return 0, fmt.Errorf("core: read entry %d: %w", i, err)
		}
		term = e.GetTerm()
		if i == commit && planted.On("B23") {
			// Planted: the replay stops one entry short; the state lacks
			// the last committed entry's writes.
			n.applied = i
			break
		}
		if err := n.apply([]*raftpb.Entry{e}); err != nil {
			return 0, err
		}
	}
	return term, nil
}

// raftConfig is the RawNode's configuration, with Raft's limits on
// uncommitted entries and on bytes in flight from the budget for entries
// that are not applied yet.
func raftConfig(cfg Config, st *storage, applied uint64) *raft.Config {
	return &raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              cfg.ElectionTicks,
		HeartbeatTick:             cfg.HeartbeatTicks,
		Storage:                   st,
		Applied:                   applied,
		AsyncStorageWrites:        true,
		MaxSizePerMsg:             uint64(min(4<<20, cfg.UnappliedBytes/4)),
		MaxInflightMsgs:           256,
		MaxInflightBytes:          uint64(cfg.UnappliedBytes / 2),
		MaxUncommittedEntriesSize: uint64(cfg.UnappliedBytes + int64(cfg.MaxBatchBytes)),
		CheckQuorum:               true,
		PreVote:                   !cfg.NoPreVote,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    quietLogger(),
	}
}

// setState makes st the replicated state.
func (n *Node) setState(st *State) {
	if n.cfg.Record {
		st.kv.TrackHash()
	}
	if n.state != nil {
		// The new state's alarms count as a change.
		st.alarmsGen = n.state.alarmsGen + 1
	}
	n.state, n.kv = st, st.Store()
	if n.cfg.MemoryBytes > 0 {
		n.kv.TrackWritten()
		n.kv.Replaced = n.cache.drop
	}
}

// MinLeaseTTL is etcd's smallest lease TTL: 1.5 election timeouts, rounded
// up to whole seconds; 2 s with the default timeouts.
func MinLeaseTTL(cfg Config) int64 {
	return cfg.ceilSeconds(int64(3 * cfg.ElectionTicks / 2))
}

// ticksOf is s seconds in ticks, rounded up.
func (c *Config) ticksOf(s int64) int64 { return (s*1000 + c.TickMillis - 1) / c.TickMillis }

// ceilSeconds and floorSeconds are a number of ticks in seconds, rounded
// up or down.
func (c *Config) ceilSeconds(ticks int64) int64  { return (ticks*c.TickMillis + 999) / 1000 }
func (c *Config) floorSeconds(ticks int64) int64 { return ticks * c.TickMillis / 1000 }

// every reports whether tick now ends a period of s seconds: once per
// period, at its last tick.
func (c *Config) every(now, s int64) bool {
	return now*c.TickMillis/(s*1000) != (now-1)*c.TickMillis/(s*1000)
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

// Hash is the hash of the replicated state: the index and the leases.
func (n *Node) Hash() uint64 { return n.state.Hash() }

// EntryBytes is the data bytes of the Raft entries the node holds in
// memory, those on their way to the disk included.
func (n *Node) EntryBytes() int64 { return n.storage.total() + n.unpersisted }

// FirstIndex is the first entry the node holds in memory.
func (n *Node) FirstIndex() uint64 { return n.storage.first }

// Leader is the leader this node knows of, or 0.
func (n *Node) Leader() uint64 { return n.leader }

// Term is the node's current Raft term.
func (n *Node) Term() uint64 { return n.term }

func (n *Node) fail(err error) {
	if n.err == nil {
		n.err = err
	}
}

// AdvanceElection moves Raft's election clock ticks ahead, and nothing
// else, as etcd's advanceTicks does at start so that a first election
// comes sooner.
func (n *Node) AdvanceElection(ticks int) {
	for range ticks {
		n.rn.Tick()
	}
}

// Tick advances the node's clock by one tick.
func (n *Node) Tick() {
	if n.err != nil {
		return
	}
	n.now++
	if n.removeAt != 0 && n.now >= n.removeAt {
		n.fail(ErrRemoved)
		return
	}
	n.rn.Tick()
	if n.removeAt != 0 {
		return
	}
	n.expire()
	// A lone voter has no one to lead in its place, so stopping would only
	// turn a slow disk into an outage; it waits for its disk instead. It
	// still calls commitStalled, which trims uncommitted.
	alone := len(n.state.voters()) == 1
	stall := int64(cmp.Or(n.cfg.StallElections, StallTicks)) * int64(n.cfg.ElectionTicks)
	if !planted.On("B17") && !alone && n.isLeader && len(n.appending) > 0 && n.now-n.appending[0] >= stall {
		n.handOff()
	}
	if n.isLeader && n.commitStalled() && !alone {
		// The same for a gray leader whose heartbeats get through but
		// whose appends do not: Raft's quorum check sees a live quorum,
		// yet nothing commits.
		n.fail(ErrCommitStalled)
		return
	}
	n.retryReads(false)
	n.publish()
	if n.cfg.every(n.now, 1) {
		n.expireTokens()
	}
	n.snapshotProgress()
	n.expireLeases()
	n.checkpointLeases()
	n.announceVersion()
	n.monitorVersion()
	if n.cfg.ProgressTicks > 0 && n.now%int64(n.cfg.ProgressTicks) == 0 {
		n.progressNotify()
	}
}

// retryReads asks again for the read index of batches that have none,
// after readRetryTicks or at once.
func (n *Node) retryReads(now bool) {
	for _, b := range n.readBatches {
		if b.index == 0 && (now || n.now-b.sent >= readRetryTicks) {
			if !now {
				n.counters.SlowReadIndexes++
			}
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
	if n.removeAt != 0 && m.GetType() == raftpb.MsgProp {
		return ErrRemoved
	}
	if m.GetType() == raftpb.MsgProp && len(m.Entries) == 1 && m.Entries[0].GetType() == raftpb.EntryConfChange {
		n.confMessages = append(n.confMessages, m)
		return nil
	}
	if m.GetType() == raftpb.MsgHeartbeatResp {
		n.snapshotDropped(m.GetFrom())
	}
	if n.noSpace && m.GetType() == raftpb.MsgProp {
		// Writes a follower forwards. Past its budget, the leader refuses
		// those that take space, as it refuses its own clients': each
		// applies as a record that answers its client with NOSPACE.
		for _, e := range m.GetEntries() {
			if e.GetType() != raftpb.EntryNormal || len(e.GetData()) == 0 {
				continue
			}
			data, err := refuseSpace(e.GetData())
			if err != nil {
				return err
			}
			e.Data = data
		}
	}
	return n.rn.Step(m)
}

// Unreachable reports that a message to a peer was dropped, so Raft
// probes it instead of streaming to it. A follower whose snapshot was not
// fetched yet gets another one.
func (n *Node) Unreachable(id uint64) {
	if n.err != nil {
		return
	}
	n.rn.ReportUnreachable(id)
	if at, ok := n.snapWait[id]; ok && at != 0 {
		delete(n.snapWait, id)
		n.rn.ReportSnapshot(id, raft.SnapshotFailure)
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
	if n.err != nil || n.removeAt != 0 {
		n.reply(r.ID, rpctypes.ErrGRPCStopped)
		return
	}
	if n.refusesSpace(r) {
		n.reply(r.ID, rpctypes.ErrGRPCNoSpace)
		return
	}
	ai, hasAI, aiErr := n.identify(r.Caller)
	if errors.Is(aiErr, errTokenNotApplied) {
		n.tokenWaits = append(n.tokenWaits, tokenWait{req: r, deadline: n.now + int64(n.cfg.RequestTicks)})
		return
	}
	extra := identityExtra(r.Caller, ai, hasAI)
	// etcd checks the quota before it validates the request, and who the
	// client is after it.
	if r.Op == nil {
		if !n.quotaLeft(putCost(&r.Put.PutOp)) {
			n.refuseNoSpace(r.ID)
			return
		}
		if err := mvcc.CheckPutOp(&r.Put.PutOp); err != nil {
			n.reply(r.ID, err)
			return
		}
		if aiErr != nil {
			n.reply(r.ID, aiErr)
			return
		}
		if err := checkPutSize(&r.Put, extra, n.cfg.MaxRequestBytes); err != nil {
			n.reply(r.ID, err)
			return
		}
		*p = pending{req: r, deadline: n.now + int64(n.cfg.RequestTicks)}
		n.tagWrite(p, ai, hasAI)
		n.queueWrite(p)
		n.timeouts = append(n.timeouts, p)
		return
	}
	if !n.quotaLeft(quotaCost(r.Op)) {
		n.refuseNoSpace(r.ID)
		return
	}
	sized, err := validate(r.Op, n.cfg.MaxTxnOps)
	if err == nil {
		err = aiErr
	}
	if err == nil && sized != nil {
		err = checkSize(sized, extra, n.cfg.MaxRequestBytes)
	}
	if err == nil {
		err = n.authorize(r.Op, ai, hasAI)
	}
	if err != nil {
		n.reply(r.ID, err)
		return
	}
	*p = pending{req: r, deadline: n.now + int64(n.cfg.RequestTicks), ai: ai, aiCurrent: hasAI && !r.Caller.Verified}
	n.tagWrite(p, ai, hasAI)
	switch op := r.Op.(type) {
	case *pb.InternalRaftRequest:
		// The identity comes from the caller only.
		op.Header = nil
		if hasAI {
			op.Header = &pb.RequestHeader{Username: ai.user, AuthRevision: ai.revision}
		}
		p.who = nil
		n.queueFeature(p)
	case *pb.AuthenticateRequest:
		n.reads = append(n.reads, p)
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
	case *pb.HashKVRequest, *pb.HashRequest:
		// etcd answers both from its local store, without Raft.
		n.serveRead(p)
		return
	case *pb.MemberListRequest:
		if !op.Linearizable {
			n.serveRead(p)
			return
		}
		n.reads = append(n.reads, p)
	case *pb.MemberAddRequest, *pb.MemberRemoveRequest, *pb.MemberUpdateRequest, *pb.MemberPromoteRequest:
		n.submitMember(p)
	case *pb.MoveLeaderRequest:
		n.moveLeader(p, op)
		return
	case *pb.DowngradeRequest:
		n.submitDowngrade(p, op)
		return
	case *pb.AlarmRequest:
		// etcd passes every alarm request through Raft; a list is a
		// linearizable read here, which answers the same.
		if !isAlarmChange(op) {
			n.reads = append(n.reads, p)
			break
		}
		n.queueFeature(p)
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

// SetDisk tells the node how many bytes its log's files use, and its disk
// budget. At the budget, the node answers requests that write new data
// with etcd's NOSPACE error, as etcd does past its quota, and raises its
// NOSPACE alarm, so that every member refuses them: a follower past its
// budget must not take the leader's appends either (#170). Once below the
// budget, with its store under the quota, it clears its own alarm; the
// alarm changes at most every leaseRetryTicks.
func (n *Node) SetDisk(used, budget int64) {
	n.noSpace = used >= budget
	switch {
	case n.noSpace:
		n.raiseNoSpace(0)
	case n.quotaLeft(kvOverhead):
		n.clearNoSpace()
	}
}

// refusesSpace reports whether the node refuses write r before it
// proposes it: past its budget one that takes space, and under a NOSPACE
// alarm one that the alarm refuses when it applies (State.refusedOp), so
// that refused writes do not fill the members' logs. A member that has
// not applied a disarm yet still refuses, though another member already
// answered it; etcd refuses only when the write applies.
func (n *Node) refusesSpace(r Request) bool {
	switch {
	case n.noSpace && TakesSpace(r):
		return true
	case !n.state.noSpace:
		return false
	case r.Op == nil:
		// A put in wire form, which CORRUPT refuses first, when it
		// applies.
		return !n.state.corrupt
	}
	return errors.Is(n.state.refusedOp(r.Op), rpctypes.ErrGRPCNoSpace)
}

// TakesSpace reports whether etcd v3.7.2 refuses a request while its
// NOSPACE alarm is active: a put, a lease grant and a transaction that is
// not read-only. A delete range and a compaction go through.
func TakesSpace(r Request) bool {
	switch op := r.Op.(type) {
	case nil, *pb.PutRequest, *pb.LeaseGrantRequest:
		return true
	case *pb.TxnRequest:
		return !mvcc.IsTxnReadOnly(op)
	}
	return false
}

// sizeRequestID stands in for etcd's request ID when sizing a write. etcd
// v3.7.2 limits the marshaled InternalRaftRequest, whose header holds an ID
// made from the low 16 bits of the member ID. When bit 15 of the member ID
// is set, the ID is a 10-byte varint, etcd's strictest case, which this
// matches to the byte; for other members etcd accepts one or more bytes
// more.
const sizeRequestID = 1 << 63

// validate checks a request like etcd's gRPC layer, and returns the
// InternalRaftRequest whose size etcd's server limits for a write, or nil
// for a request that does not go through Raft.
func validate(op proto.Message, maxTxnOps int) (*pb.InternalRaftRequest, error) {
	if r, ok := op.(*pb.RangeRequest); ok {
		// Reads are not sized; they make no request to size.
		return nil, mvcc.CheckRange(r)
	}
	r := &pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}}
	switch op := op.(type) {
	case *pb.PutRequest:
		r.Put = op
		return r, mvcc.CheckPut(op)
	case *pb.DeleteRangeRequest:
		r.DeleteRange = op
		return r, mvcc.CheckDelete(op)
	case *pb.TxnRequest:
		if err := mvcc.CheckTxn(op, maxTxnOps); err != nil || mvcc.IsTxnReadOnly(op) {
			return nil, err
		}
		r.Txn = op
	case *pb.CompactionRequest:
		r.Compaction = op
	case *pb.LeaseGrantRequest:
		r.LeaseGrant = op
	case *pb.LeaseRevokeRequest:
		r.LeaseRevoke = op
	case *pb.AlarmRequest:
		if !isAlarmChange(op) {
			return nil, nil
		}
		r.Alarm = op
	case *pb.InternalRaftRequest:
		r = proto.CloneOf(op)
		r.Header = &pb.RequestHeader{ID: sizeRequestID}
	case *pb.LeaseKeepAliveRequest, *pb.LeaseTimeToLiveRequest, *pb.LeaseLeasesRequest, *pb.HashKVRequest, *pb.HashRequest,
		*pb.MemberListRequest, *pb.MemberAddRequest, *pb.MemberRemoveRequest, *pb.MemberUpdateRequest, *pb.MemberPromoteRequest,
		*pb.AuthenticateRequest, *pb.StatusRequest, *pb.MoveLeaderRequest, *pb.DowngradeRequest:
		return nil, nil
	default:
		return nil, fmt.Errorf("core: unknown request type %T", op)
	}
	return r, nil
}

// checkSize limits a write as etcd's server does: the marshaled
// InternalRaftRequest with the client's identity (extra) to limit bytes.
func checkSize(r *pb.InternalRaftRequest, extra, limit int) error {
	var size int
	if r.Txn != nil {
		// validate sets the header and the transaction, nothing else.
		size = txnRequestBytes + protowire.SizeBytes(txnSize(r.Txn))
	} else {
		size = proto.Size(r)
	}
	if size+extra > limit {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

// txnRequestBytes is the size of etcd's InternalRaftRequest around a
// transaction without the transaction itself: the header and the tag.
var txnRequestBytes = proto.Size(&pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}}) + protowire.SizeTag(6)

// putRequestBytes is the size of etcd's InternalRaftRequest around a put
// without the put itself: the header with sizeRequestID, and the put's tag.
var putRequestBytes = proto.Size(&pb.InternalRaftRequest{Header: &pb.RequestHeader{ID: sizeRequestID}}) + protowire.SizeTag(4)

// checkPutSize limits a put in wire form as checkSize does a decoded one.
// Like etcd, it limits the size of the request as protobuf marshals it
// again, not of the client's encoding.
func checkPutSize(p *Put, extra, limit int) error {
	if putRequestBytes+extra+protowire.SizeBytes(p.size()) > limit {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

// tagWrite gives a write the identity it carries into the log while auth
// is enabled; without one it applies as etcd's empty identity.
func (n *Node) tagWrite(p *pending, ai authInfo, ok bool) {
	if ok && n.state.auth.enabled {
		p.who = &authInfo{user: ai.user, revision: ai.revision}
	}
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
	size := writeSize(p)
	if n.propBytes > 0 && n.propBytes+size > n.cfg.MaxBatchBytes {
		n.propose()
	}
	if p.req.ID != 0 {
		n.waiting[p.req.ID] = p
	}
	p.queued = true
	n.proposals = append(n.proposals, p)
	n.propBytes += size
}

// errNotProposed answers a write whose deadline passed before it was
// proposed, as etcd v3.7.2 answers a write whose context ends while its
// raft.Node waits for a leader. clientv3 retries this code for writes,
// unlike the request timeout of a write that was proposed.
var errNotProposed = status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())

// expire fails requests whose deadline passed. Writes that expired before
// they were proposed leave the queue at once, so a node without a leader
// holds only writes that may still go out.
func (n *Node) expire() {
	i, dropped, early := 0, false, false
	for ; i < len(n.timeouts); i++ {
		p := n.timeouts[i]
		if p.done {
			continue
		}
		if p.deadline > n.now {
			break
		}
		if _, write := n.waiting[p.req.ID]; write {
			n.counters.ProposalsFailed++
		} else {
			n.counters.ReadIndexFailed++
		}
		if p.early {
			early = true
			n.finish(p, errNotProposed)
			continue
		}
		if p.queued {
			dropped = true
			n.propBytes -= writeSize(p)
			n.finish(p, errNotProposed)
			continue
		}
		if _, ok := p.req.Op.(*pb.MoveLeaderRequest); ok {
			n.finish(p, ErrTimeoutLeaderTransfer)
			continue
		}
		n.finish(p, rpctypes.ErrGRPCTimeout)
	}
	n.timeouts = n.timeouts[i:]
	if dropped {
		n.proposals = slices.DeleteFunc(n.proposals, func(p *pending) bool { return p.done })
	}
	if early {
		n.early = slices.DeleteFunc(n.early, func(p *pending) bool { return p.done })
	}
}

// finish fails a request with err.
func (n *Node) finish(p *pending, err error) {
	if n.settle(p) {
		n.reply(p.req.ID, err)
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

// reply fails request id with err.
func (n *Node) reply(id uint64, err error) {
	n.out.Replies = append(n.out.Replies, Reply{ID: id, Err: err})
}

// writeSize is the bytes a queued write adds to a Raft entry.
func writeSize(p *pending) int {
	if p.who != nil {
		return writeBytes(p) + len(p.who.user) + 2*binary.MaxVarintLen64 + 1
	}
	return writeBytes(p)
}

// writeBytes is the bytes of a queued write's request.
func writeBytes(p *pending) int {
	switch {
	case p.req.Wire != nil:
		return len(p.req.Wire) + batchFraming
	case p.req.Op != nil:
		return proto.Size(p.req.Op) + batchFraming
	}
	return len(p.req.Put.Wire) + batchFraming
}

// propose packs the queued writes into Raft entries of at most
// MaxBatchBytes each. Without a leader it proposes nothing: like etcd,
// whose raft.Node takes no proposal while there is no leader, the writes
// wait for one, up to their deadline, instead of failing at once (#105).
func (n *Node) propose() {
	if n.noSpace || n.state.noSpace {
		// Writes taken before the disk passed its budget, or before the
		// alarm, get NOSPACE too, unless they were proposed: only those
		// still add to the files, and UnappliedBytes bounds them.
		n.proposals = slices.DeleteFunc(n.proposals, func(p *pending) bool {
			if !n.refusesSpace(p.req) {
				return false
			}
			n.propBytes -= writeSize(p)
			n.finish(p, rpctypes.ErrGRPCNoSpace)
			return true
		})
	}
	// Raft's own leader, not the one the last Ready reported: a Tick or a
	// Step since then may have ended it, and Propose would drop the entry.
	if n.rn.BasicStatus().Lead == raft.None {
		return
	}
	// A write that timed out while it waited already left the queue
	// (expire), so it never goes out.
	props := n.proposals
	n.proposals, n.propBytes = nil, 0
	for _, p := range props {
		p.queued = false
	}
	for len(props) > 0 {
		end, size := 0, 0
		for end < len(props) && (end == 0 || size+writeSize(props[end]) <= n.cfg.MaxBatchBytes) {
			size += writeSize(props[end])
			end++
		}
		// Writes wait while the entries not applied yet fill their
		// budget, so that memory holds no more of them.
		if unapplied := n.unapplied(); unapplied > 0 && unapplied+int64(size) > n.cfg.UnappliedBytes {
			n.requeue(props)
			return
		}
		n.proposeEntry(props[:end], size)
		props = props[end:]
	}
}

// requeue puts writes back in the queue, unproposed.
func (n *Node) requeue(props []*pending) {
	n.proposals = props
	for _, p := range props {
		p.queued = true
		n.propBytes += writeSize(p)
	}
}

// proposeEntry proposes one Raft entry with props.
func (n *Node) proposeEntry(props []*pending, size int) {
	data, err := encodeBatch(n.state.version, n.cfg.ID, n.cfg.Boot, props, size)
	if err != nil {
		n.fail(err)
		return
	}
	n.unready += int64(len(data))
	if err := n.rn.Propose(data); err != nil {
		// Raft dropped the proposal, so none of these requests will apply.
		for _, p := range props {
			n.finish(p, rpctypes.ErrGRPCNoLeader)
		}
		return
	}
	// On the leader, the next local append with entries carries this
	// batch; writes wait while ownDepth such appends are on their way
	// (own). A follower only forwards, so nothing waits.
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
	n.unpersisted -= job.bytes
	if in := job.Install; in != nil {
		if !planted.On("B27") {
			n.fail(fmt.Errorf("%w at %d from %d, checkpoint %d", ErrInstalled, in.Index, in.From, in.Checkpoint.Index))
			return
		}
		// Planted: the core goes on with the state the snapshot replaced.
		if err := n.storage.reset(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(in.Index), Term: new(in.Term), ConfState: n.storage.confAt(in.Index)}}); err != nil {
			n.fail(err)
			return
		}
		n.applied = in.Index
	}
	if job.Root != nil {
		n.storage.ckpt = *job.Root
	}
	if err := n.storage.add(job.Entries); err != nil {
		n.fail(err)
		return
	}
	if job.HardState != nil {
		if err := n.storage.setHardState(*job.HardState); err != nil {
			n.fail(err)
			return
		}
	}
	for _, e := range job.Entries {
		n.logBytes += wal.EntryRecordBytes(e)
	}
	if len(job.Entries) > 0 {
		durable := job.Entries[len(job.Entries)-1].GetIndex()
		for len(n.own) > 0 && n.own[0] <= durable {
			n.own = n.own[1:]
		}
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
		if n.removeAt != 0 && m.GetType() == raftpb.MsgAppResp {
			// The followers can commit our removal before our own append
			// finishes. Raft no longer tracks our replication progress,
			// so this late self-ack has no recipient. Storage responses
			// still have to reach Raft to mark the entries stable.
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
		// Self-clocked batching: while ownDepth of the leader's own
		// appends are on their way to disk, writes wait and join one
		// entry, which goes out when the oldest is durable. With two, the
		// disk takes the next append as soon as it synced one, instead of
		// standing idle until the leader proposes again. Nothing else
		// waits, and with no append in flight a write goes out at once,
		// so one client sees no delay. The byte cap still sends a full
		// entry early (queueWrite).
		n.releaseEarly()
		if len(n.tokenWaits) > 0 {
			n.retryTokenWaits()
		}
		if !n.isLeader || len(n.own) < ownDepth {
			n.propose()
		}
		n.proposeConfs()
		n.proposeConfMessages()
		// Apply messages left behind by a snapshot install.
		n.runApply()
		n.requestReads()
		for n.err == nil && n.rn.HasReady() {
			n.handleReady(n.rn.Ready())
		}
		if planted.On("B06d") {
			// Planted: a progress notification at the current revision
			// goes out ahead of the events it covers.
			for _, w := range n.watchers {
				if !w.dead && w.progress && len(w.pending) > 0 {
					n.watchReply(w.stream, &pb.WatchResponse{WatchId: w.id}, nil)
				}
			}
		}
		n.flushWatches()
		if n.err == nil {
			n.trimWindow()
			n.fitCache()
		}
	}
	if n.err != nil || n.removeAt != 0 {
		n.stop()
	}
	out := n.out
	n.out = Effects{}
	n.watchRead = nil
	return out, n.err
}

// stop fails every waiting request once the node stopped.
func (n *Node) stop() {
	for _, p := range n.timeouts {
		n.finish(p, rpctypes.ErrGRPCStopped)
	}
	for _, w := range n.tokenWaits {
		n.reply(w.req.ID, rpctypes.ErrGRPCStopped)
	}
	n.tokenWaits = nil
	n.confMessages = nil
	n.timeouts, n.proposals, n.reads, n.readBatches, n.early, n.confs, n.transfers = nil, nil, nil, nil, nil, nil, nil
}

func (n *Node) handleReady(rd raft.Ready) {
	// What was proposed is in this Ready's append, or went to the leader.
	n.unready = 0
	if rd.SoftState != nil {
		if leader := rd.RaftState == raft.StateLeader; leader != n.isLeader {
			n.isLeader = leader
			// A batch from an earlier leadership may have been replaced
			// and never reach the disk; wait for nothing from it.
			n.own, n.awaitOwn = nil, false
			n.uncommitted = nil
			n.leadershipChanged()
		}
		if rd.Lead != n.leader && rd.Lead != raft.None {
			// A term change clears Raft's pending read requests, so ask the
			// new leader at once instead of waiting for the retry.
			n.retryReads(true)
			n.leaderChanges++
		}
		n.leader = rd.Lead
		n.checkTransfers()
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
			job := PersistJob{Entries: m.Entries, msg: m}
			if m.Snapshot != nil {
				meta := m.Snapshot.GetMetadata()
				job.Install = &Install{From: n.rn.BasicStatus().Lead, Index: meta.GetIndex(), Term: meta.GetTerm(), Applied: n.applied}
			}
			if m.Vote != nil {
				job.HardState = &wal.HardState{Term: m.GetTerm(), Vote: m.GetVote(), Commit: m.GetCommit()}
				job.newVote = slices.Contains(newVotes, m)
			}
			for _, e := range job.Entries {
				job.bytes += int64(len(e.GetData()))
			}
			n.unpersisted += job.bytes
			n.out.Persist = append(n.out.Persist, job)
			if len(job.Entries) > 0 {
				n.appending = append(n.appending, n.now)
				if n.isLeader {
					n.uncommitted = append(n.uncommitted, proposedAt{job.Entries[len(job.Entries)-1].GetIndex(), n.now})
				}
			}
			if len(job.Entries) > 0 && n.awaitOwn {
				n.own = append(n.own, job.Entries[len(job.Entries)-1].GetIndex())
				n.awaitOwn = false
			}
		case raft.LocalApplyThread:
			n.applyQ = append(n.applyQ, m)
			n.runApply()
			if n.err != nil {
				return
			}
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

// serveRead answers a read. One that needs values from the log at once,
// to compare or sort by them, runs again once a helper read them.
func (n *Node) serveRead(p *pending) {
	if p.done {
		return
	}
	var resp proto.Message
	var err error
	rep := Reply{ID: p.req.ID}
	if p.aiCurrent {
		p.ai.revision = n.state.auth.revision
	}
	if err := n.readAllowed(p); err != nil {
		n.finish(p, err)
		return
	}
	var start int64
	if n.cfg.Timer != nil {
		start = n.cfg.Timer.Now()
	}
	switch op := p.req.Op.(type) {
	case *pb.AuthenticateRequest:
		resp, err = n.credentials(op)
		rep.Rev = int64(n.state.auth.revision)
	case *pb.RangeRequest:
		if p.req.Compact {
			var res mvcc.RangeResult
			if res, err = n.kv.RangeKVs(op, n.cfg.MemoryBytes); err == nil {
				rep.Term, rep.Range = n.term, res
			}
			break
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
		// Only the leader counts the time a lease has left. etcd's
		// followers ask it (node.TimeToLive).
		if !n.isLeader {
			err = rpctypes.ErrGRPCNotLeader
			break
		}
		r := n.state.timeToLive(op, n.remaining(op.ID))
		n.fillHeader(r.Header)
		resp = r
	case *pb.LeaseLeasesRequest:
		r := n.state.leaseIDs()
		n.fillHeader(r.Header)
		resp = r
	case *pb.HashKVRequest:
		var h mvcc.KVHash
		if h, err = n.hashKV(op.Revision); err == nil {
			r := &pb.HashKVResponse{Header: header(n.kv.Rev()), Hash: h.Hash, CompactRevision: h.CompactRevision, HashRevision: h.Revision}
			n.fillHeader(r.Header)
			resp = r
		}
	case *pb.HashRequest:
		r := &pb.HashResponse{Header: header(n.kv.Rev()), Hash: n.state.Hash32()}
		n.fillHeader(r.Header)
		resp = r
	case *pb.AlarmRequest:
		r := n.state.alarmList(op)
		n.fillHeader(r.Header)
		resp = r
	case *pb.MemberListRequest:
		resp = n.memberResponse(op, nil)
	case *pb.DowngradeRequest:
		resp, err = n.validateDowngrade(op)
	}
	if cold, ok := errors.AsType[*mvcc.ColdError](err); ok {
		n.fetch(cold.Need).read = p
		p.compares = cold.Compares
		return
	}
	if t := n.cfg.Timer; t != nil {
		switch p.req.Op.(type) {
		case *pb.RangeRequest:
			t.Read("Range", err == nil, t.Now()-start)
		case *pb.HashRequest:
			t.Read("Hash", err == nil, t.Now()-start)
		case *pb.HashKVRequest:
			t.Read("HashKV", err == nil, t.Now()-start)
		}
	}
	if err != nil && !isAnswer(err) {
		// A value that cannot be read, or does not match its checksum,
		// never reaches a client: the node stops.
		n.fail(err)
		return
	}
	rep.Resp, rep.Err = resp, err
	n.answer(p, rep, n.kv.TakeFills())
}

func (n *Node) fillHeader(h *pb.ResponseHeader) {
	h.ClusterId = n.cfg.ClusterID
	h.MemberId = n.cfg.ID
	h.RaftTerm = n.term
}

// apply runs committed entries against the index and answers the requests
// that this run of the node is waiting for. It stops at a write that
// waits for values from the log (paused) and skips entries already
// applied, before a pause or a snapshot.
func (n *Node) apply(ents []*raftpb.Entry) error {
	for _, e := range ents {
		if e.GetIndex() <= n.applied {
			continue
		}
		if err := n.applyEntry(e); err != nil {
			return fmt.Errorf("core: apply entry %d: %w", e.GetIndex(), err)
		}
		if n.paused != nil {
			return nil
		}
		n.applied = e.GetIndex()
		if n.applied > n.membershipAt+membershipSnapshotEntries {
			n.membershipAt = n.applied
			n.counters.membershipSnapshot(n.state)
		}
		n.sinceCkpt += int64(len(e.GetData()))
		if n.cfg.Record {
			n.out.Applied = append(n.out.Applied, Applied{Index: e.GetIndex(), Term: e.GetTerm(), Type: e.GetType(), Data: e.GetData(), Hash: n.state.Hash()})
		}
		n.maybeCheckpoint(e.GetIndex(), e.GetTerm())
	}
	return nil
}

// applyEntry runs the writes of one entry.
func (n *Node) applyEntry(e *raftpb.Entry) error {
	if e.GetType() == raftpb.EntryConfChange {
		return n.applyConfChange(e)
	}
	if e.GetType() != raftpb.EntryNormal || len(e.GetData()) == 0 {
		return nil
	}
	// A write may read what an earlier write of its entry put, and an
	// entry replayed from the log is not in memory.
	n.values.index, n.values.entry = e.GetIndex(), e.GetData()
	defer func() {
		n.values.entry = nil
		n.values.reset()
	}()
	// After a pause, the writes before the paused one are applied.
	skip, k := 0, -1
	if r := n.resume; r != nil && r.index == e.GetIndex() {
		skip = r.write
	}
	err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte, at int) error {
		if k++; k < skip {
			return nil
		}
		t := n.cfg.Timer
		if t == nil {
			return n.applyWrite(e, k, origin, boot, id, kind, b, at)
		}
		start := t.Now()
		n.timed = timedApply{}
		err := n.applyWrite(e, k, origin, boot, id, kind, b, at)
		if err == nil && n.paused == nil {
			t.Applied(n.timed.op, n.timed.err == nil || errors.Is(n.timed.err, rpctypes.ErrGRPCCompacted), t.Now()-start)
		}
		return err
	})
	if errors.Is(err, errPaused) {
		return nil
	}
	return err
}

// applyWrite applies the k-th write of entry e.
func (n *Node) applyWrite(e *raftpb.Entry, k int, origin, boot, id uint64, kind byte, b []byte, at int) error {
	var p *pending
	if origin == n.cfg.ID && boot == n.cfg.Boot {
		p = n.waiting[id]
	}
	n.values.reset()
	kind, b, at, who, err := unwrapAuth(kind, b, at)
	if err != nil {
		return err
	}
	n.state.who = who
	switch kind {
	case kindPut:
		return n.applyPut(b, at, p)
	case kindAuth:
		return n.applyAuthEntry(b, e.GetIndex(), p)
	case kindNoSpace:
		n.timed = timedApply{op: "NoSpace", err: rpctypes.ErrGRPCNoSpace}
		if p != nil {
			n.finish(p, rpctypes.ErrGRPCNoSpace)
		}
		return nil
	case kindVersion:
		n.timed.op = "VersionAnnounce"
		return n.applyVersion(e.GetIndex(), origin, b)
	}
	op, err := newOp(kind)
	if err != nil {
		return err
	}
	// The op's values are slices of the entry; the index names them
	// by where they are in it.
	if txn, ok := op.(*pb.TxnRequest); ok {
		err = decodeTxnPlaced(b, txn, at, &n.values.placer)
	} else {
		err = DecodeAlias(b, op)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	room := n.quotaLeft(quotaCost(op))
	resp, err := n.applyOp(op)
	for {
		cold, ok := errors.AsType[*mvcc.ColdError](err)
		if !ok {
			break
		}
		if !n.readInline {
			n.pause(e.GetIndex(), k, cold)
			return errPaused
		}
		if batch(cold.Compares) {
			// The compare is done with the batch before.
			n.values.fetched = nil
		}
		if err := n.readNow(cold.Need); err != nil {
			return err
		}
		n.kv.Resume(cold.Compares)
		resp, err = n.applyOp(op)
	}
	if n.readInline {
		n.values.fetched = nil
	}
	if errors.Is(err, errInternal) {
		return err
	}
	fills := n.kv.TakeFills()
	if err == nil {
		n.matchWatches()
		if !room {
			resp, err, fills = nil, n.overQuota(), nil
		}
	}
	n.timed = timedApply{op: opName(op), err: err}
	if p != nil {
		n.answer(p, Reply{ID: p.req.ID, Resp: resp, Err: err}, fills)
	}
	return nil
}

var errInternal = errors.New("core: internal error")

// applyPut runs a put read in place from the bytes b, which start at
// offset at of the entry. p is the request waiting for it on this node, if
// any.
func (n *Node) applyPut(b []byte, at int, p *pending) error {
	put, err := ParsePut(b)
	if err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	n.values.add(put.Value, at+put.valueAt)
	room := n.quotaLeft(putCost(&put.PutOp))
	prev, err := n.state.ApplyPut(&put.PutOp)
	if err != nil && !isAnswer(err) {
		return fmt.Errorf("%w: %w", errInternal, err)
	}
	fills := n.kv.TakeFills()
	if err == nil {
		n.matchWatches()
		if !room {
			err = n.overQuota()
		}
	}
	n.timed = timedApply{op: "Put", err: err}
	if p == nil || p.done {
		return nil
	}
	if err != nil {
		n.finish(p, err)
		return nil
	}
	if p.req.Op != nil {
		r := &pb.PutResponse{Header: &pb.ResponseHeader{Revision: n.kv.Rev()}, PrevKv: prev}
		n.fillHeader(r.Header)
		n.answer(p, Reply{ID: p.req.ID, Resp: r}, fills)
		return nil
	}
	n.answer(p, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev}, fills)
	return nil
}

// applyOp runs one write. Request errors (an unknown lease, a missing key
// for ignore_value) are answers; any other error stops the node.
func (n *Node) applyOp(op proto.Message) (proto.Message, error) {
	resp, err := n.state.Apply(op)
	if err != nil {
		if _, cold := errors.AsType[*mvcc.ColdError](err); cold || errors.Is(err, errInternal) || isAnswer(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", errInternal, err)
	}
	if h, ok := resp.(interface{ GetHeader() *pb.ResponseHeader }); ok {
		n.fillHeader(h.GetHeader())
	}
	if _, ok := op.(*pb.CompactionRequest); ok {
		n.keepHash(n.kv.CompactHash())
		// A compaction under NOSPACE cuts a checkpoint so cleaning can
		// drop the freed values (#167). Before alarms are supported,
		// local disk pressure can already refuse writes, so compacting
		// must also cut without an alarm (#213). Both conditions come
		// from replicated state; every member cuts at the same entry.
		n.cutNow = n.cutNow || n.state.noSpace || !n.capable()
	}
	switch op := op.(type) {
	case *pb.LeaseGrantRequest:
		if g, ok := resp.(*pb.LeaseGrantResponse); ok {
			n.counters.granted(g.TTL)
		}
	case *pb.LeaseRevokeRequest:
		n.counters.LeaseRevoked++
	case *pb.LeaseKeepAliveRequest:
		if k, ok := resp.(*pb.LeaseKeepAliveResponse); ok && n.isLeader && k.TTL > 0 {
			n.counters.LeaseRenewed++
		}
	case *pb.LeaseCheckpoint:
		if resp != nil {
			n.out.Expired = append(n.out.Expired, op.ID)
			n.counters.LeaseRevoked++
			if n.isLeader {
				n.counters.LeaseExpired++
			}
		}
	}
	if _, ok := op.(*pb.Member); ok {
		n.membersChanged()
	}
	if a, ok := op.(*pb.AlarmRequest); ok {
		if r, ok := resp.(*pb.AlarmResponse); ok && len(r.Alarms) > 0 {
			d := int64(1)
			if a.Action == pb.AlarmRequest_DEACTIVATE {
				d = -1
			}
			n.counters.alarm(a.MemberID, a.Alarm, d)
		}
	}
	if a, ok := op.(*pb.AlarmRequest); ok && a.Alarm == pb.AlarmType_NOSPACE && a.MemberID == n.cfg.ID {
		// The proposal raiseNoSpace waits for has applied, or the alarm
		// changed otherwise.
		n.noSpaceAt = -1
	}
	if n.isLeader || planted.On("B10") {
		switch op := op.(type) {
		case *pb.LeaseGrantRequest:
			n.arm(op.ID)
		case *pb.LeaseKeepAliveRequest:
			n.arm(op.ID)
			if l, ok := n.state.leases[op.ID]; ok && l.checkpointAt == 0 {
				n.scheduleCheckpoint(op.ID)
			}
		case *pb.LeaseCheckpointRequest:
			for _, c := range op.Checkpoints {
				n.scheduleCheckpoint(c.ID)
			}
		}
	}
	if g, ok := op.(*pb.LeaseGrantRequest); ok && n.isLeader {
		n.scheduleCheckpoint(g.ID)
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

// handOff asks Raft to make the follower with the longest log among those
// it heard from in the last election timeout the leader, unless a
// transfer is under way; Raft gives one up after an election timeout, and
// the next tick asks again. A leader whose disk stalls still heartbeats,
// so the others would never elect a replacement, and once its entries not
// yet on its disk fill their budget it holds up the writes of the whole
// cluster. As a follower it holds up only its own clients, until its disk
// is back. Effects.HandOff names the follower.
func (n *Node) handOff() {
	st := n.rn.Status()
	if st.LeadTransferee != 0 {
		return
	}
	var to, match uint64
	for id, pr := range st.Progress {
		if id == n.cfg.ID || pr.IsLearner || !pr.RecentActive {
			continue
		}
		if to == 0 || pr.Match > match || pr.Match == match && id < to {
			to, match = id, pr.Match
		}
	}
	if to == 0 {
		return
	}
	n.rn.TransferLeader(to)
	n.out.HandOff = to
}

// commitStalled drops the leader's appends that have committed and reports
// whether the oldest one left has waited CommitStallElections election
// timeouts.
func (n *Node) commitStalled() bool {
	commit := n.rn.BasicStatus().GetCommit()
	i := 0
	for i < len(n.uncommitted) && n.uncommitted[i].index <= commit {
		n.maxCommitWait = max(n.maxCommitWait, n.now-n.uncommitted[i].at)
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
