// Package core is the committer of one node: an event and effect machine
// around a Raft RawNode and the MVCC index.
//
// Inputs are method calls: Submit (a client request), Step (a message from
// a peer), Tick (one clock tick) and Persisted (a disk write finished).
// Process returns the effects: disk writes, messages to send and replies.
// The core never does I/O, never reads the clock, never starts a goroutine
// and has no randomness of its own: its driver, internal/node, does those,
// in production and in the simulator.
package core

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// StallTicks is how many election timeouts a leader's own append may
// take before the leader sends nothing more to its peers, until its disk
// catches up (stalled). As in etcd, whose leader sends nothing while its
// disk write blocks, the followers stop hearing from it and elect another
// leader, and it steps down when it hears the new term. It never stops.
const StallTicks = 2

// stallBytesPerTick is the slowest a disk that is not stuck writes: the
// oldest append also gets a tick for each stallBytesPerTick of the entry
// bytes still on their way to the disk, 10 MiB/s at a real node's 100 ms
// tick. A disk is judged by how fast it writes, not by how long one write
// takes: a 64 MiB append took more than two election timeouts under the
// race detector, its leader went silent, and the followers elected a
// leader without the entry (#553). The driver writes the jobs queued
// behind the oldest in one group with it, so their bytes count too, and
// the time counts from the disk's last progress: an append that waited
// behind a large one that finished is not late for it (#564).
const stallBytesPerTick = 1 << 20

// SlowCommitTicks is how many election timeouts a leader's entry may wait
// to commit before the leader warns (Effects.SlowCommit) and counts it
// (Counters.SlowCommits). Nothing else happens: as in etcd, a leader whose
// entries do not commit stays leader, and its writes wait until the
// cluster heals.
const SlowCommitTicks = 3

// MaxRequestBytes is etcd's default --max-request-bytes.
const MaxRequestBytes = 1536 * 1024

// A write goes to the peers in one Raft message, a frame of at most 64
// MiB, and to the log in one record of at most wal.MaxEntryBytes, also
// 64 MiB. maxWriteBytes is the most a write may add to its entry, which
// leaves room for the entry's own framing, the Raft message and the
// record's header. MaxRequestBytesCeiling is the largest
// --max-request-bytes a node takes: it leaves room below maxWriteBytes for
// what a write adds to its request in the entry (batchFraming, an
// authenticated write's identity) and for an encoding that is a little
// larger than the one etcd measures. A request of exactly 64 MiB grows to
// 67,108,880 bytes as a peer message.
const (
	maxWriteBytes          = wal.MaxEntryBytes - 1<<10
	MaxRequestBytesCeiling = maxWriteBytes - 1<<10
)

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
	// HeldEntries reports whether the link to a peer carries the messages
	// of held entries (held.go): the peer speaks peer protocol 4 or later.
	// nil means no link does. Kept, if set, is what the core before this
	// one held (Node.Kept).
	HeldEntries func(peer uint64) bool
	Kept        *Kept

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

	// SlowCommitElections is how many election timeouts a leader's entry
	// may wait to commit before a warning; 0 means SlowCommitTicks.
	// StallElections is how many its own append may take before it goes
	// silent; 0 means StallTicks. Both watch the disks, so tests at
	// shorter ticks set them higher to keep the time a real node allows.
	SlowCommitElections, StallElections int

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
	// the driver measures it (SetOtherMemory). A Range sorted by value
	// whose values pass MemoryBytes is refused. 0 turns the cache and
	// that check off.
	MemoryBytes int64
	// PieceBytes is about the most one piece of a large answer holds
	// (pieces.go); 0 means PieceBytes.
	PieceBytes int

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
	// PriorityRequest is etcd's PriorityRequest feature gate: lease
	// revokes are taken up to twice the gap between commit and apply that
	// refuses other writes (tooManyRequests).
	PriorityRequest bool
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
		// etcd's 10 minutes
		TickMillis: 100,

		WindowBytes:     64 << 20,
		WindowMaxBytes:  128 << 20,
		UnappliedBytes:  64 << 20,
		CheckpointBytes: 256 << 20,
		CheckpointRatio: 64,
		CheckpointChunk: 32 << 10,
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
	// Chunk asks for a range as one chunk of a RangeStream, as etcd
	// v3.7.2's range without a total count: the walk stops once it has
	// the limit plus one keys, and Count holds the keys it saw.
	Chunk bool
	// Caller is the client as etcd's size limit for writes sees it.
	Caller Caller
	// places are where the values of a transaction's puts start in Wire,
	// which DecodeTxn noted, so that the node that took it applies it
	// without decoding its entry again; placed says DecodeTxn ran.
	places []place
	placed bool
}

// Caller is a client as etcd v3.7.2's size limit for writes sees it. With
// client certificate authentication (--client-cert-auth), etcd takes the
// client's identity from its certificate even with auth disabled
// (EtcdServer.AuthInfoFromCtx), and every write's request header then holds
// the certificate's common name and the auth revision, which is 1 on a
// store that never changed auth. The zero Caller has no identity.
type Caller struct {
	// Token is the token of the call (etcd's "token" metadata), if any.
	// Verified says that the server checked it itself (JWT): User and
	// Revision are its claims, and an empty User means it is not valid.
	Token    string
	Verified bool
	User     string
	Revision uint64
}

// Reply answers one request. Resp is the response of the same kind as the
// request, or nil with Err set. A put in wire form gets no Resp: its
// answer is Rev, Term and PrevKv, which the driver encodes itself.
type Reply struct {
	ID   uint64
	Resp proto.Message

	Rev int64

	// Range is the answer to a range, or the next piece of an answer
	// (Next): only its KVs then.
	Range mvcc.RangeResult

	Rests []mvcc.Rest
	// cursors read the rest of the answer, for Next.
	cursors []*mvcc.Cursor
}

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
	// SlowCommit is set when the leader's oldest entry not committed has
	// just waited SlowCommitElections election timeouts, and Silent when
	// its own append has just taken StallElections and it went silent
	// (stalled). The driver logs both.
	SlowCommit, Silent bool
}

// CheckpointChunk is part of the checkpoint at Index.
type CheckpointChunk struct{}

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

	// who is the identity a write carries into the log while auth is
	// enabled. ai is the identity of a read, checked when it is served;
	// aiCurrent says that it takes the auth revision of that moment, as
	// a simple token and a certificate do.
	who *authInfo

	// refuse, if set, answers the request once its write applied: the
	// NOSPACE alarm a refused write raised (quota.go).
	refuse error
}

// readBatch is the set of reads that share one ReadIndex request.
type readBatch struct {
	ctx   uint64
	index uint64 // 0 until the read index is known
	sent  int64  // tick of the last ReadIndex request
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

	now int64

	// clock is the time requests wait by: the ticks of wall time, which
	// Elapse moves on past the ticks a busy driver missed, as etcd's
	// request timeouts run on the wall clock while its Raft loop is stuck.
	clock int64

	applied uint64

	term uint64

	isLeader bool

	counters Counters
	timed    timedApply
	// membershipAt is where Counters.membershipSnapshot last counted.
	membershipAt uint64

	// expiries holds the armed leases by the tick their expiry is next
	// due, and checkpoints those with a checkpoint by its tick: on the
	// leader only (lease.go).
	expiries, checkpoints leaseQueue

	confGate uint64

	// voteTerm and vote are the newest term and vote sent to disk.
	// unsyncedVotes counts appends that change them and are not yet
	// durable; while any is, messages to peers wait in held. Raft with
	// async storage writes sends a candidate's vote requests before its
	// own term and vote are durable, so it can win, lose its term in a
	// failed write, and win the same term again with another log. held
	// keeps at most maxHeld messages. They also wait in held while a
	// follower has a committed configuration change not yet applied: up to
	// confAt.
	voteTerm, vote uint64
	unsyncedVotes  int
	confAt         uint64

	held []*raftpb.Message
	// kept are the entries a follower holds for appends that name them
	// (held.go).
	kept keptEntries

	// appending holds the tick at which each local append with entries
	// went to disk, oldest first; appends finish in order, and durableAt
	// is the tick the last one did. If the disk makes no progress for too
	// long for the bytes still to write on the leader, it sends nothing to
	// its peers (stalled).
	appending []int64
	durableAt int64

	waiting  map[uint64]*pending // writes by request ID
	timeouts []*pending          // every waiting request in deadline order

	proposals []*pending
	// early are writes that need featureVersion, which wait for it.
	early []*pending

	propBytes int

	readBatches []*readBatch

	// cutDue makes the next applied entry cut a checkpoint, once no copy
	// is under way: enough entries or a compaction applied since the last
	// cut (maybeCheckpoint). physical are the answers of physical
	// compactions that wait for a durable checkpoint, oldest first.
	cutDue   bool
	physical []heldCompact

	// log reads what left memory.
	log Log
	// copy is the checkpoint being copied, last the newest one finished,
	// sinceCkpt the entry bytes applied since the last cut, and base the
	// size the next cut is measured against (maybeCheckpoint).
	copy *copying
	last lastCkpt
	// unwritten is the newest checkpoint finished whose write the driver
	// has not reported yet, or 0.
	unwritten uint64
	sinceCkpt int64
	base      int64
	// unpersisted is the entry bytes on their way to the disk, and
	// unready those proposed since the last Ready.
	unpersisted, unready int64

	watchers []*watcher
	// pieced are the answers that go out in pieces, by request ID.
	pieced map[uint64]*pieced

	// cold are the value reads under way. watchRead is the read of this
	// turn's watch events.
	cold      map[*ValueRead]struct{}
	watchRead *ValueRead
	// applyQ holds Raft's apply messages not applied yet: while paused
	// waits for values from the log, or revoke takes its steps, ordered
	// apply waits with it. resume is the write that runs again after its
	// read, or the one after a revoke. readInline is set while the node
	// starts: nothing else waits then, so a write reads the values it
	// needs itself.
	applyQ         []*raftpb.Message
	paused, resume *pausedWrite
	revoke         *revokeWrite
	readInline     bool

	// hashes are the hashes the last compactions took, oldest first.
	hashes []mvcc.KVHash
	// tokens are the simple tokens this node assigned (auth.go), tokenDue
	// the earliest tick one may expire at, and tokenWaits the requests
	// whose token's entry is not applied yet.
	tokens   map[string]*simpleToken
	tokenDue int64

	// noSpaceAt is the tick this node last proposed its NOSPACE alarm,
	// or -1 once an alarm change of it applied. diskAlarm says that this
	// run raised its alarm for its disk budget, and the quota has not
	// refused a write since: only such an alarm clears itself (SetDisk).
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
	vals := &values{storage: st, cache: c}
	cfg.MaxRequestBytes = cmp.Or(cfg.MaxRequestBytes, MaxRequestBytes)
	cfg.PieceBytes = cmp.Or(cfg.PieceBytes, PieceBytes)
	cfg.MaxTxnOps = cmp.Or(cfg.MaxTxnOps, mvcc.MaxTxnOps)
	n := &Node{
		cfg:      cfg,
		storage:  st,
		values:   vals,
		cache:    c,
		log:      disk,
		term:     rec.HardState.Term,
		voteTerm: rec.HardState.Term,
		vote:     rec.HardState.Vote,
		waiting:  map[uint64]*pending{},

		pieced: map[uint64]*pieced{},

		cold:        map[*ValueRead]struct{}{},
		noSpaceAt:   -1,
		tokens:      map[string]*simpleToken{},
		expiries:    leaseQueue{at: (*lease).due, pos: func(l *lease) *int { return &l.epos }},
		checkpoints: leaseQueue{at: func(l *lease) int64 { return l.checkpointAt }, pos: func(l *lease) *int { return &l.cpos }},
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
	// checkpoint shows, and the members a new cluster's log starts with.
	hs := rec.HardState
	boot, err := bootstrapped(cfg, rec, disk)
	if err != nil {
		return nil, err
	}
	hs.Commit = max(hs.Commit, n.applied, boot)
	n.readInline = true
	term, err := n.replay(hs.Commit)
	n.readInline = false
	if err != nil {
		return nil, err
	}
	// Only now does the store keep the values that puts write, for the
	// cache: the replay's entries never enter memory, so none of their
	// values go into it, and a record of each would only take memory
	// (#433).
	if n.cfg.MemoryBytes > 0 {
		n.kv.TrackWritten()
	}
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
	// The handed-over map stays out of the stored config, so that
	// expiring an entry frees it.
	n.adopt(cfg.Kept)
	n.cfg.Kept = nil
	return n, nil
}

// bootstrapped is how many entries at the start of the log are the
// cluster's first members, cfg.Peers, as Raft's Bootstrap wrote them:
// configuration changes of term 1, which it writes as committed on every
// first member. A crash can keep them without the commit index that
// follows them on disk (wal.Append). Applied on start, they give Raft its
// voters, so the node can campaign; without them it could only vote, and
// a cold restart in which it holds the longest log never elects (#247).
// The driver keeps cfg.Peers on disk from the first start on, so they are
// the persisted initial configuration, whether or not this start is
// flagged as a join. A log that holds only some of them, or other ones,
// gets 0: applying part of the members would make the driver drop the
// peers it was started with.
func bootstrapped(cfg Config, rec *wal.Recovered, disk Log) (uint64, error) {
	if rec.Root.Index > 0 || len(cfg.Peers) == 0 || rec.Last < uint64(len(cfg.Peers)) {
		return 0, nil
	}
	for i, id := range cfg.Peers {
		e, err := disk.Entry(uint64(i) + 1)
		if err != nil {
			return 0, fmt.Errorf("core: read entry %d: %w", i+1, err)
		}
		if e.GetTerm() != 1 || e.GetType() != raftpb.EntryConfChange {
			return 0, nil
		}
		var cc raftpb.ConfChange
		if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
			return 0, fmt.Errorf("core: entry %d: %w", i+1, err)
		}
		if cc.GetNodeId() != id {
			return 0, nil
		}
	}
	return uint64(len(cfg.Peers)), nil
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
		// A revoke in steps takes them all now; the entry then goes on
		// from the write after it.
		for n.revoke != nil {
			if n.revokeStep(); n.err != nil {
				return 0, n.err
			}
			if n.revoke == nil {
				if err := n.apply([]*raftpb.Entry{e}); err != nil {
					return 0, err
				}
				n.resume = nil
			}
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
		n.kv.Replaced = n.cache.drop
	}
	n.kv.SortBytes = n.cfg.MemoryBytes
	n.kv.PieceBytes = n.cfg.PieceBytes
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
func (c *Config) ceilSeconds(ticks int64) int64 { return (ticks*c.TickMillis + 999) / 1000 }

// quietLogger drops raft's log lines. Its Panic methods still panic,
// because raft relies on them for broken invariants.
func quietLogger() raft.Logger {
	return &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
}

// Store is the node's index. Only the simulator's checks read it directly.
func (n *Node) Store() *mvcc.Store { return n.kv }

func (n *Node) fail(err error) {
	if n.err == nil {
		n.err = err
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

// Errors of Step for a message the node does not take, as etcd's Process
// refuses it.
var ()

// refusesSpace reports whether the node refuses write r before it
// proposes it: one that an active NOSPACE alarm refuses when it applies
// (State.refusedOp), so that refused writes do not fill the members'
// logs. A member that has not applied a disarm yet still refuses, though
// another member already answered it; etcd refuses only when the write
// applies.
func (n *Node) refusesSpace(r Request) bool {
	switch {
	case !n.state.noSpace:
		return false
	case r.Op == nil:
		// A put in wire form, which CORRUPT refuses first, when it
		// applies.
		return !n.state.corrupt
	}
	return errors.Is(n.state.refusedOp(r.Op), rpctypes.ErrGRPCNoSpace)
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

// finish ends a request that failed.
func (n *Node) finish(p *pending) {
	if n.settle(p) {
		n.reply(p.req.ID)
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

// reply answers request id.
func (n *Node) reply(id uint64) {
	n.out.Replies = append(n.out.Replies, Reply{ID: id})
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
	if n.state.noSpace {
		// Writes taken before the alarm get NOSPACE too, unless they were
		// proposed: only those still add to the files, and UnappliedBytes
		// bounds them.
		n.proposals = slices.DeleteFunc(n.proposals, func(p *pending) bool {
			if !n.refusesSpace(p.req) {
				return false
			}
			n.propBytes -= writeSize(p)
			n.finish(p)
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
			n.finish(p)
		}
		return
	}
	// On the leader, the next local append with entries carries this
	// batch; writes wait while ownDepth such appends are on their way
	// (own). A follower only forwards, so nothing waits.
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
		if e.GetIndex() != n.applied+1 {
			// As etcd, which stops on it: the driver or the log skipped
			// committed entries.
			return fmt.Errorf("core: committed entry %d does not follow the applied entry %d", e.GetIndex(), n.applied)
		}
		if err := n.applyEntry(e); err != nil {
			return fmt.Errorf("core: apply entry %d: %w", e.GetIndex(), err)
		}
		if n.applyWaits() {
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
	if e.GetType() != raftpb.EntryNormal {
		return nil
	}
	if len(e.GetData()) == 0 {
		// A new leader's first entry: as etcd's read loop on the first
		// commit in a term, ask for the read indexes not known yet at once.
		n.retryReads(true)
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
		if err == nil && !n.applyWaits() {
			t.Applied(n.timed.op, n.timed.err == nil || errors.Is(n.timed.err, rpctypes.ErrGRPCCompacted), t.Now()-start)
		}
		return err
	})
	if errors.Is(err, errPaused) {
		return nil
	}
	return err
}

// decodeWrite decodes the write of kind in b, which starts at offset at
// of its entry, and notes where its values are, as the index names them
// by their place in the entry. A transaction that this node took and
// decoded (DecodeTxn) is not decoded again: the entry holds the bytes it
// was decoded from, so its values are at the same offsets, from at.
func (n *Node) decodeWrite(p *pending, kind byte, b []byte, at int) (proto.Message, error) {
	if p != nil && p.req.placed && kind == kindTxn && len(p.req.Wire) == len(b) {
		for _, pc := range p.req.places {
			n.values.places = append(n.values.places, place{start: pc.start, off: pc.off + uint32(at)})
		}
		return p.req.Op, nil
	}
	if p != nil && kind == kindShortTxn {
		// The entry holds the request's short form, which ends with the
		// value of its put.
		t, ok := p.req.Op.(*pb.TxnRequest)
		if !ok {
			return nil, errBadBatch
		}
		if put := t.Success[0].GetRequestPut(); put != nil && len(put.Value) > 0 {
			n.values.places = append(n.values.places, place{start: &put.Value[0], off: uint32(at + len(b) - len(put.Value))})
		}
		return t, nil
	}
	op, err := newOp(kind)
	if err != nil {
		return nil, err
	}
	if kind == kindShortTxn {
		err = decodeShort(b, op.(*pb.TxnRequest), at, &n.values.placer)
	} else if txn, ok := op.(*pb.TxnRequest); ok {
		err = decodeTxnPlaced(b, txn, at, &n.values.placer)
	} else {
		err = DecodeAlias(b, op)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errBadBatch, err)
	}
	return op, nil
}

// applyWrite applies the k-th write of entry e.
func (n *Node) applyWrite(e *raftpb.Entry, k int, origin, boot, id uint64, kind byte, b []byte, at int) error {
	var p *pending
	if origin == n.cfg.ID && boot == n.cfg.Boot {
		p = n.waiting[id]
	}
	// Only the node a client waits on builds the write's answer.
	n.kv.Unanswered = p == nil
	defer func() { n.kv.Unanswered = false }()
	n.values.reset()
	kind, b, at, who, err := unwrapAuth(kind, b, at)
	if err != nil {
		return err
	}
	n.state.who = who
	switch kind {
	case kindPut, kindDelete, kindTxn, kindShortTxn, kindCompact, kindGrant, kindRevoke, kindRenew, kindExpiry, kindLeaseCheckpoint, kindNoSpace:
	default:
		// The write may change what the state's meta hash covers.
		n.state.metaKnown = false
	}
	switch kind {
	case kindPut:
		return n.applyPut(b, at, p)
	case kindAuth:
		return n.applyAuthEntry(b, e.GetIndex(), p)
	case kindNoSpace:
		n.timed = timedApply{op: "NoSpace", err: rpctypes.ErrGRPCNoSpace}
		if p != nil {
			n.finish(p)
		}
		return nil
	case kindVersion:
		n.timed.op = "VersionAnnounce"
		return n.applyVersion(e.GetIndex(), origin, b)
	}
	op, err := n.decodeWrite(p, kind, b, at)
	if err != nil {
		return err
	}
	room := n.quotaLeft(quotaCost(op))
	resp, err := n.applyOp(op)
	if errors.Is(err, errRevoking) {
		// Ordered apply waits for the revoke's steps (revokeStep).
		n.matchWatches()
		r := n.revoke
		r.index, r.write, r.p, r.room = e.GetIndex(), k, p, room
		return errPaused
	}
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
	fills, rests := n.kv.TakeFills(), n.kv.TakeRests()
	if err == nil {
		n.matchWatches()
		if !room {
			resp, err, fills, rests = nil, n.overQuota(p), nil, nil
		}
	}
	n.timed = timedApply{op: opName(op), err: err}
	if c, ok := op.(*pb.CompactionRequest); ok && c.Physical && err == nil && p != nil {
		n.holdCompact()
		return nil
	}
	if p != nil {
		n.answer(p, Reply{ID: p.req.ID, Resp: resp, Rests: rests}, fills)
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
			err = n.overQuota(p)
		}
	}
	n.timed = timedApply{op: "Put", err: err}
	if p == nil || p.done {
		return nil
	}
	switch {
	case err != nil:
		// A request error is the request's answer, not the node's.
		n.finish(p)
	case p.req.Op != nil:
		r := &pb.PutResponse{Header: &pb.ResponseHeader{Revision: n.kv.Rev()}, PrevKv: prev}
		n.fillHeader(r.Header)
		n.answer(p, Reply{ID: p.req.ID, Resp: r}, fills)
	default:
		n.answer(p, Reply{ID: p.req.ID, Rev: n.kv.Rev()}, fills)
	}
	return nil
}

// applyOp runs one write. Request errors (an unknown lease, a missing key
// for ignore_value) are answers; any other error stops the node.
func (n *Node) applyOp(op proto.Message) (proto.Message, error) {
	var revoked *lease
	switch op := op.(type) {
	case *pb.LeaseRevokeRequest:
		revoked = n.state.leases[op.ID]
	case *pb.LeaseCheckpoint:
		revoked = n.state.leases[op.ID]
	}
	resp, err := n.state.Apply(op)
	if errors.Is(err, errRevoking) {
		// The revoke goes on in steps (revokeStep), whose last ends here.
		n.revoke = &revokeWrite{op: op, lease: revoked}
		return nil, err
	}
	return n.afterApply(op, revoked, resp, err)
}

// afterApply does what follows an applied write: revoked is the lease
// the write found, if it revokes one.
func (n *Node) afterApply(op proto.Message, revoked *lease, resp proto.Message, err error) (proto.Message, error) {
	if revoked != nil && n.state.leases[revoked.id] != revoked {
		n.expiries.remove(revoked)
		n.checkpoints.remove(revoked)
	}
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
		// Every compaction cuts a checkpoint, so that cleaning can drop
		// the values it freed (#167).
		n.cutDue = true
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

// stalled reports whether this node leads and its disk, with appends
// not yet durable, has made none durable for StallElections election
// timeouts and a tick for each stallBytesPerTick of the entry bytes still
// on their way to it: since the oldest such append went to disk, or the
// last append before it became durable, whichever is later. Its disk is
// stuck then, and it sends nothing to its peers until the disk catches
// up. The entries queued are bounded, so a disk that writes nothing still
// trips it.
func (n *Node) stalled() bool {
	if planted.On("B17") || !n.isLeader || len(n.appending) == 0 {
		return false
	}
	limit := int64(cmp.Or(n.cfg.StallElections, StallTicks))*int64(n.cfg.ElectionTicks) + n.unpersisted/stallBytesPerTick
	return n.now-max(n.appending[0], n.durableAt) >= limit
}

// maxHeld bounds the messages held while a term or vote change is not yet
// durable, such as on a candidate whose disk stalls.
const maxHeld = 1024

// send puts a message to a peer out, or holds it while a term or vote
// change is not yet durable, or while a follower has not applied a
// committed configuration change: as etcd's follower, which sends nothing
// until it applied such a change, so that it neither counts votes nor
// campaigns by the old configuration. A stalled leader drops it: Raft
// sends again what its peers miss, and messages to this node still step
// it, so it learns of a new leader's term. A message to a removed member
// is dropped, as etcd drops it.
func (n *Node) send(m *raftpb.Message) {
	if n.state.removed[m.GetTo()] {
		return
	}
	switch {
	case m.GetType() == raftpb.MsgApp && n.isLeader:
		m = n.nameHeld(m)
	case m.GetType() == raftpb.MsgProp && !HeldForm(m):
		n.forwarded(m)
	}
	if n.stalled() {
		if m.GetType() == raftpb.MsgHeartbeat {
			n.counters.HeartbeatSendFailures++
		}
		return
	}
	if n.unsyncedVotes > 0 || n.confPending() {
		// Raft resends what it loses, so past the bound the newest
		// messages are dropped instead of held.
		if len(n.held) < maxHeld {
			n.held = append(n.held, m)
		}
		return
	}
	n.out.Send = append(n.out.Send, m)
}

// confPending reports whether this node follows and has a committed
// configuration change it has not applied yet.
func (n *Node) confPending() bool { return !n.isLeader && n.applied < n.confAt }

// releaseHeld sends the held messages once nothing holds them any more.
func (n *Node) releaseHeld() {
	if len(n.held) == 0 || n.unsyncedVotes > 0 || n.confPending() {
		return
	}
	held := n.held
	n.held = nil
	for _, m := range held {
		n.send(m)
	}
}
