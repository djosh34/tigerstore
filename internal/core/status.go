package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
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

// recordBytes estimates the bytes one entry takes in the WAL: the record
// header, the entry header and the data.
func recordBytes(e *raftpb.Entry) int64 { return 9 + 17 + int64(len(e.GetData())) }

// Status answers Maintenance.Status from this node's state. DbSize is the
// size of the log, which is all the node stores; it is never 0, because
// etcdctl divides by it.
func (n *Node) Status() *pb.StatusResponse {
	st := n.rn.BasicStatus()
	resp := &pb.StatusResponse{
		Header:           n.watchHeader(),
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
