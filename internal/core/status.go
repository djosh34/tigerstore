package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The versions tigerstore reports, as etcd v3.7.2 does in Maintenance.Status
// and on /version. Kubernetes reads Version to decide which features to use.
const (
	ServerVersion  = "3.7.2"
	ClusterVersion = "3.7.0"
	StorageVersion = "3.7.0"
)

// QuotaBytes is etcd's default --quota-backend-bytes.
const QuotaBytes = 2 << 30

// Status answers Maintenance.Status from this node's state. DbSize is the
// size of the log, which is all the node stores; it is never 0, because
// etcdctl divides by it.
func (n *Node) Status() *pb.StatusResponse {
	st := n.rn.BasicStatus()
	// Header and body take the term from one snapshot; etcdctl prints one
	// or the other depending on the output format.
	h := n.watchHeader()
	h.RaftTerm = st.GetTerm()
	resp := &pb.StatusResponse{
		Header:           h,
		DowngradeInfo:    &pb.DowngradeInfo{},
		Version:          ServerVersion,
		StorageVersion:   StorageVersion,
		DbSize:           max(n.logBytes, 1),
		DbSizeInUse:      max(n.logBytes, 1),
		DbSizeQuota:      QuotaBytes,
		Leader:           st.Lead,
		RaftIndex:        st.GetCommit(),
		RaftTerm:         st.GetTerm(),
		RaftAppliedIndex: n.applied,
	}
	if st.Lead == 0 {
		resp.Errors = append(resp.Errors, "etcdserver: no leader")
	}
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
	// CacheBytes is what the value cache holds and CacheLimit what it may
	// hold now; CacheHits are the values it returned and CacheMisses those
	// read from the log.
	CacheBytes, CacheLimit, CacheHits, CacheMisses int64
}

// Metrics returns the node's metrics.
func (n *Node) Metrics() Metrics {
	st := n.rn.BasicStatus()
	return Metrics{
		HasLeader:     st.Lead != 0,
		LeaderChanges: n.leaderChanges,
		Committed:     st.GetCommit(),
		Applied:       n.applied,
		Counts:        n.kv.Counts(),
		CacheBytes:    n.cache.bytes,
		CacheLimit:    max(n.cache.limit, 0),
		CacheHits:     n.cache.hits,
		CacheMisses:   n.cache.misses,
	}
}
