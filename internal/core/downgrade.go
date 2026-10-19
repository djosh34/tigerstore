package core

import (
	"strings"

	"github.com/coreos/go-semver/semver"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Downgrade follows etcd v3.7.2 (EtcdServer.Downgrade and
// etcdserver/version). Its state is etcd's: the cluster version that
// etcd reports, "3.7.0" here, and whether a downgrade to the minor
// version below is enabled, both replicated and held in checkpoints.
// VALIDATE checks, after a linearizable read, that the target is the
// minor version below the cluster version and that no downgrade is
// enabled. ENABLE makes the same checks after a linearizable read, as
// etcd's DowngradeEnable validates first, and then writes etcd's
// DowngradeInfoSet through Raft, which checks again when it applies, so
// that of two at once one fails. CANCEL writes it back off,
// answering without error if none was on, as etcd does. Every 4 s, as etcd's version monitor, the leader moves
// the cluster version to the target while a downgrade is enabled, and
// back to its own once none is. The storage version follows the cluster
// version. etcd ends a downgrade once every member runs the target
// version; every tigerstore member runs 3.7.2, so one ends only by
// CANCEL. This is etcd's version, which clients see, not tigerstore's
// own cluster version, the entry format (version.go), which never goes
// down.

// monitorSeconds is how often, in seconds, etcd's leader checks the
// cluster version: rafthttp's ConnWriteTimeout less a second.
const monitorSeconds = 4

// errUnknownMethod is etcd's answer to a Downgrade with an unknown
// action, which its gRPC layer has no code for.
var errUnknownMethod = status.Error(codes.Unknown, "etcdserver: unknown method")

// clusterVersion is etcd's cluster version as a semantic version.
func (st *State) clusterVersion() *semver.Version {
	v, err := semver.NewVersion(st.etcdVersion)
	if err != nil {
		return semver.New(ClusterVersion)
	}
	return v
}

// EtcdVersion is the cluster version and the storage version etcd
// reports, as /version and Status do.
func (n *Node) EtcdVersion() string { return n.state.etcdVersion }

// toClusterVersion is etcd's convertToClusterVersion: a version with its
// patch, or major.minor alone, as major.minor.0.
func toClusterVersion(v string) (*semver.Version, error) {
	ver, err := semver.NewVersion(v)
	if err != nil {
		if ver, err = semver.NewVersion(v + ".0"); err != nil {
			return nil, rpctypes.ErrGRPCWrongDowngradeVersionFormat
		}
	}
	return &semver.Version{Major: ver.Major, Minor: ver.Minor}, nil
}

// minorVersion is etcd's version.Cluster: a version without its patch.
func minorVersion(v string) string {
	vs := strings.Split(v, ".")
	if len(vs) <= 2 {
		return v
	}
	return vs[0] + "." + vs[1]
}

// submitDowngrade checks a Downgrade's version, and queues VALIDATE and
// ENABLE as linearizable reads (validateDowngrade) and CANCEL as a write
// of etcd's DowngradeInfoSet, which is checked when it applies.
func (n *Node) submitDowngrade(p *pending, r *pb.DowngradeRequest) {
	switch r.Action {
	case pb.DowngradeRequest_VALIDATE, pb.DowngradeRequest_ENABLE:
		if _, err := toClusterVersion(r.Version); err != nil {
			n.finish(p, err)
			return
		}
		n.reads = append(n.reads, p)
	case pb.DowngradeRequest_CANCEL:
		if n.tooManyRequests(false) {
			n.finish(p, rpctypes.ErrGRPCRequestTooManyRequests)
			return
		}
		p.req.Op, p.who = &pb.InternalRaftRequest{DowngradeInfoSet: &membershippb.DowngradeInfoSetRequest{}}, nil
		n.queueFeature(p)
	default:
		n.finish(p, errUnknownMethod)
		return
	}
	n.timeouts = append(n.timeouts, p)
}

// checkDowngrade is etcd's DowngradeValidate.
func (st *State) checkDowngrade(target *semver.Version) error {
	cv := st.clusterVersion()
	if allowed := (semver.Version{Major: cv.Major, Minor: cv.Minor - 1}); !target.Equal(allowed) {
		return rpctypes.ErrGRPCInvalidDowngradeTargetVersion
	}
	if st.downgradeEnabled {
		return rpctypes.ErrGRPCDowngradeInProcess
	}
	return nil
}

// downgradeResponse answers a Downgrade with the cluster version.
func (n *Node) downgradeResponse() *pb.DowngradeResponse {
	return &pb.DowngradeResponse{Header: n.authHeader(), Version: minorVersion(n.state.clusterVersion().String())}
}

// validateDowngrade answers VALIDATE once its read is linearizable. An
// ENABLE that passes the same checks then goes through Raft as a write of
// etcd's DowngradeInfoSet, and validateDowngrade answers nothing.
func (n *Node) validateDowngrade(p *pending, r *pb.DowngradeRequest) (proto.Message, error) {
	target, err := toClusterVersion(r.Version)
	if err != nil {
		return nil, err
	}
	if err := n.state.checkDowngrade(target); err != nil {
		return nil, err
	}
	if r.Action == pb.DowngradeRequest_ENABLE {
		if n.tooManyRequests(false) {
			return nil, rpctypes.ErrGRPCRequestTooManyRequests
		}
		p.req.Op, p.who = &pb.InternalRaftRequest{DowngradeInfoSet: &membershippb.DowngradeInfoSetRequest{Enabled: true, Ver: target.String()}}, nil
		n.queueFeature(p)
		return nil, nil
	}
	return n.downgradeResponse(), nil
}

// applyVersionSet applies etcd's DowngradeInfoSet or ClusterVersionSet.
// A downgrade is checked again here, so that of two at once one fails;
// a CANCEL without a downgrade changes nothing and answers without
// error, as etcd's does: its version.Manager returns
// ErrNoInflightDowngrade, but EtcdServer.downgradeCancel only logs that
// and answers with the cluster version (v3_server.go).
func (n *Node) applyVersionSet(r *pb.InternalRaftRequest) (proto.Message, error) {
	st := n.state
	switch d := r.DowngradeInfoSet; {
	case d != nil && d.Enabled:
		target, err := toClusterVersion(d.Ver)
		if err != nil {
			return nil, err
		}
		if err := st.checkDowngrade(target); err != nil {
			return nil, err
		}
		st.downgradeEnabled, st.downgradeTarget = true, target.String()
	case d != nil:
		st.downgradeEnabled, st.downgradeTarget = false, ""
	case r.ClusterVersionSet != nil:
		if v, err := semver.NewVersion(r.ClusterVersionSet.Ver); err == nil {
			st.etcdVersion = v.String()
		}
	}
	return n.downgradeResponse(), nil
}

// monitorVersion is etcd's version monitor on the leader: it moves the
// cluster version to the downgrade's target while one is enabled, and
// back to this build's once none is.
func (n *Node) monitorVersion() {
	if !n.isLeader || !n.capable() || !n.cfg.every(n.now, monitorSeconds) {
		return
	}
	st := n.state
	want := ClusterVersion
	if st.downgradeEnabled {
		want = st.downgradeTarget
	}
	if st.etcdVersion != want {
		n.proposeInternal(&pb.InternalRaftRequest{ClusterVersionSet: &membershippb.ClusterVersionSetRequest{Ver: want}})
	}
}
