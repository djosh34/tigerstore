package core

import (
	"cmp"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
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

// Status answers Maintenance.Status from this node's state. DbSize is the
// size of the log, which is all the node stores; it is never 0, because
// etcdctl divides by it. DbSizeInUse is what the space quota counts
// (mvcc.Store.Size).
func (n *Node) Status() *pb.StatusResponse {
	st := n.rn.BasicStatus()
	// Header and body take the term from one snapshot; etcdctl prints one
	// or the other depending on the output format.
	h := n.watchHeader()
	h.RaftTerm = st.GetTerm()
	resp := &pb.StatusResponse{
		Header:           h,
		DowngradeInfo:    &pb.DowngradeInfo{Enabled: n.state.downgradeEnabled, TargetVersion: n.state.downgradeTarget},
		Version:          ServerVersion,
		StorageVersion:   n.state.etcdVersion,
		DbSize:           max(n.logBytes, 1),
		DbSizeInUse:      n.kv.Size(),
		DbSizeQuota:      cmp.Or(n.cfg.QuotaBytes, QuotaBytes),
		Leader:           st.Lead,
		RaftIndex:        st.GetCommit(),
		RaftTerm:         st.GetTerm(),
		RaftAppliedIndex: n.applied,
		IsLearner:        n.IsLearner(),
	}
	if st.Lead == 0 {
		resp.Errors = append(resp.Errors, "etcdserver: no leader")
	}
	resp.Errors = append(resp.Errors, n.state.alarmStrings()...)
	return resp
}

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
	// WindowBytes is the data of the Raft entries in memory.
	IndexBytes, WindowBytes int64
	// The node driver adds the rest: MapBytes estimates what the map of
	// where entries and values are on disk takes in memory, DiskBytes is
	// the size of the log's files, DiskBudget the budget for them, and
	// CleanedBytes what cleaning wrote.
	MapBytes, DiskBytes, DiskBudget, CleanedBytes int64
	// MaxCommitWait is the most ticks one of this node's appends as leader
	// waited to commit, which the commit watchdog bounds, and the node
	// driver adds MaxLogWrite, the longest write and sync of the log.
	MaxCommitWait int64
	MaxLogWrite   time.Duration
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

// Metrics returns the node's metrics.
func (n *Node) Metrics() Metrics {
	st := n.rn.BasicStatus()
	m := Metrics{
		HasLeader:     st.Lead != 0,
		LeaderChanges: n.leaderChanges,
		Committed:     st.GetCommit(),
		Applied:       n.applied,
		Counts:        n.kv.Counts(),
		IndexBytes:    n.kv.Bytes(),
		WindowBytes:   n.storage.total(),
		CacheBytes:    n.cache.bytes,
		CacheLimit:    max(n.cache.limit, 0),
		CacheHits:     n.cache.hits,
		CacheMisses:   n.cache.misses,
		MaxCommitWait: n.maxCommitWait,

		ClusterVersion: n.state.version,

		Counters:         n.counters,
		ID:               n.cfg.ID,
		Members:          n.memberIDs(),
		IsLeader:         n.isLeader,
		IsLearner:        n.IsLearner(),
		EtcdVersion:      n.state.etcdVersion,
		Rev:              n.kv.Rev(),
		CompactRev:       n.kv.CompactRev(),
		AuthRevision:     n.state.auth.revision,
		DbSize:           max(n.logBytes, 1),
		DbSizeInUse:      n.kv.Size(),
		QuotaBytes:       cmp.Or(n.cfg.QuotaBytes, QuotaBytes),
		ProposalsPending: int64(len(n.waiting)),
		Raft:             n.rn.Status(),
	}
	m.AlarmGauges = slices.Clone(m.AlarmGauges)
	for _, s := range n.streams {
		m.WatchStreams++
		for _, w := range s.byID {
			if w.dead {
				continue
			}
			m.Watchers++
			if len(s.held) > 0 {
				m.HeldWatchers++
			}
		}
		for _, h := range s.held {
			m.HeldEvents += int64(len(h.resp.Resp.Events))
		}
	}
	return m
}

// memberIDs are the members' IDs, in order.
func (n *Node) memberIDs() []uint64 {
	ids := make([]uint64, 0, len(n.state.members))
	for id := range n.state.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
