package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
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
