package core

import (
	"go.etcd.io/raft/v3"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The versions tigerstore reports, as etcd v3.7.2 does in Maintenance.Status
// and on /version. Kubernetes reads Version to decide which features to use.
// The cluster version, which the storage version follows, is
// ClusterVersion but during a downgrade (downgrade.go).
const (
	ServerVersion  = "3.7.2"
	ClusterVersion = "3.7.0"
)

// Metrics are the counters and gauges /metrics reports, under etcd's
// names.
type Metrics struct {
	HasLeader     bool
	LeaderChanges int64
	// Committed and Applied are the Raft log indexes, which etcd reports
	// as etcd_server_proposals_committed_total and _applied_total.
	Committed, Applied uint64
	mvcc.Counts
	// IndexBytes estimates what the index takes in memory, and
	// WindowBytes is what the Raft entries in memory hold, their data
	// and EntryOverhead each.
	IndexBytes, WindowBytes int64
	// The node driver adds the rest: MapBytes estimates what the map of
	// where entries and values are on disk takes in memory, DiskBytes is
	// the size of the log's files, DiskBudget the budget for them,
	// CleanedBytes what cleaning wrote, and RemoveFailures the removals of
	// old files that failed.
	MapBytes, DiskBytes, DiskBudget, CleanedBytes, RemoveFailures int64
	// MaxCommitWait is the most ticks one of this node's appends as leader
	// waited to commit, those still waiting included, and the node
	// driver adds MaxLogWriteNanos, the longest write and sync of the log.
	MaxCommitWait, MaxLogWriteNanos int64
	// CacheBytes is what the value cache holds and CacheLimit what it may
	// hold now; CacheHits are the values it returned and CacheMisses those
	// read from the log.
	CacheBytes, CacheLimit, CacheHits, CacheMisses int64
	// ClusterVersion is the cluster version the node has applied.
	ClusterVersion uint32

	Counters
	// ID is this node's member ID, Members those of every member.
	ID      uint64
	Members []uint64
	// IsLeader and IsLearner say what this node is.
	IsLeader, IsLearner bool
	// EtcdVersion is the etcd cluster version, such as 3.7.0.
	EtcdVersion string
	// Rev and CompactRev are the store's revision and its compacted
	// revision, AuthRevision the auth store's.
	Rev, CompactRev int64
	AuthRevision    uint64
	// DbSize, DbSizeInUse and QuotaBytes are Maintenance.Status's.
	DbSize, DbSizeInUse, QuotaBytes int64
	// ProposalsPending are the writes waiting for their entry to apply.
	ProposalsPending int64
	// Watchers and WatchStreams are the live watchers and their streams.
	// HeldWatchers are the watchers of streams whose responses wait for
	// values from the log, and HeldEvents the events of those responses:
	// tigerstore's slow watchers and pending events.
	Watchers, WatchStreams, HeldWatchers, HeldEvents int64
	// Raft is Raft's status, for etcd's raft.status in /debug/vars.
	Raft raft.Status
}
