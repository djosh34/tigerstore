package core

import (
	"math/bits"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LeaseTTLBuckets is the number of buckets of etcd's
// etcd_debugging_lease_ttl_total histogram: TTLs up to 1, 2, 4, ... 2^23
// seconds.
const LeaseTTLBuckets = 24

// Counters are counts the node keeps for etcd's metrics. They are not
// replicated state: they start at zero, as etcd's do, and checkpoints do
// not hold them.
type Counters struct {
	// LeaseGranted and LeaseRevoked count the leases this node granted
	// and revoked as it applied the log, expiries included. LeaseRenewed
	// counts the keepalives and LeaseExpired the expiries it applied as
	// leader, which is where etcd counts them.
	LeaseGranted, LeaseRevoked, LeaseRenewed, LeaseExpired int64
	// LeaseTTLs counts the TTLs of granted leases by bucket: [i] those
	// over 2^(i-1) and at most 2^i seconds, the last those over
	// 2^(LeaseTTLBuckets-1). LeaseTTLSum adds them up.
	LeaseTTLs   [LeaseTTLBuckets + 1]int64
	LeaseTTLSum int64
	// SlowReadIndexes counts the read index requests asked again because
	// no answer came, ReadIndexFailed the reads that timed out and
	// ProposalsFailed the writes that did.
	SlowReadIndexes, ReadIndexFailed, ProposalsFailed int64
	// Events counts the watch events this node sent.
	Events int64
	// AlarmGauges is etcd_debugging_server_alarms as etcd keeps it: each
	// alarm activation applied adds one, even of an active alarm, and
	// each deactivation of an active one takes one away. Alarms that were
	// active when the node started are not in it.
	AlarmGauges []AlarmGauge
	// StoreGets, StoreCreates and StoreSets are the reads and writes of
	// the v2 store etcd builds from the membership every
	// membershipSnapshotEntries entries it applies, for its
	// etcd_debugging_store_reads_total and writes_total.
	StoreGets, StoreCreates, StoreSets int64
}

// AlarmGauge is the value of an alarm's etcd_debugging_server_alarms.
type AlarmGauge struct {
	MemberID uint64
	Alarm    pb.AlarmType
	Value    int64
}

// alarm adds d to the gauge of an alarm.
func (c *Counters) alarm(member uint64, a pb.AlarmType, d int64) {
	for i := range c.AlarmGauges {
		if g := &c.AlarmGauges[i]; g.MemberID == member && g.Alarm == a {
			g.Value += d
			return
		}
	}
	c.AlarmGauges = append(c.AlarmGauges, AlarmGauge{MemberID: member, Alarm: a, Value: d})
}

// membershipSnapshotEntries is etcd's memorySnapshotCount: it snapshots
// the membership into a fresh v2 store once more entries than this
// applied since the last time.
const membershipSnapshotEntries = 100

// membershipSnapshot counts the v2 store reads and writes of etcd's
// GetMembershipInfoInV2Format: a recursive get of the members that finds
// none, a create of each member's Raft attributes and a set of its other
// attributes once it published them, a create of each removed ID and a
// set of the cluster version once there is one.
func (c *Counters) membershipSnapshot(st *State) {
	c.StoreGets++
	c.StoreCreates += int64(len(st.members) + len(st.removed))
	for _, m := range st.members {
		if len(m.ClientURLs) > 0 {
			c.StoreSets++
		}
	}
	if st.etcdVersion != "" {
		c.StoreSets++
	}
}

// granted counts a granted lease.
func (c *Counters) granted(ttl int64) {
	c.LeaseGranted++
	c.LeaseTTLSum += ttl
	c.LeaseTTLs[min(bits.Len64(uint64(max(ttl, 1)-1)), LeaseTTLBuckets)]++
}

// Timer times the writes a node applies, for etcd's
// etcd_server_apply_duration_seconds, and the reads it serves: Range for
// etcd_server_range_duration_seconds, Hash and HashKV for
// etcd_mvcc_hash_duration_seconds and etcd_mvcc_hash_rev_duration_seconds.
// Now is a time in nanoseconds; the times never change what the node
// does.
type Timer interface {
	Now() int64
	Applied(op string, ok bool, nanos int64)
	Read(op string, ok bool, nanos int64)
}

// timedApply is the name and the answer of the write being applied, for
// the Timer.
type timedApply struct {
	op  string
	err error
}

// opName is the name etcd's apply_duration_seconds gives a write.
// tigerstore's own writes have their own names: LeaseKeepAlive, a
// renewal, and LeaseRevoke for an expiry, which etcd proposes as one.
func opName(op proto.Message) string {
	switch op.(type) {
	case *pb.PutRequest:
		return "Put"
	case *pb.DeleteRangeRequest:
		return "DeleteRange"
	case *pb.TxnRequest:
		return "Txn"
	case *pb.CompactionRequest:
		return "Compaction"
	case *pb.LeaseGrantRequest:
		return "LeaseGrant"
	case *pb.LeaseRevokeRequest, *pb.LeaseCheckpoint:
		return "LeaseRevoke"
	case *pb.LeaseKeepAliveRequest:
		return "LeaseKeepAlive"
	case *pb.LeaseCheckpointRequest:
		return "LeaseCheckpoint"
	case *pb.AlarmRequest:
		return "Alarm"
	case *pb.Member:
		return "ClusterMemberAttrSet"
	}
	return "Unknown"
}

// internalName is the name etcd gives an InternalRaftRequest: that of its
// field that is set, such as AuthUserAdd.
func internalName(r *pb.InternalRaftRequest) string {
	name := "Unknown"
	r.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if fd.Name() == "header" || fd.Kind() != protoreflect.MessageKind {
			return true
		}
		var b strings.Builder
		for w := range strings.SplitSeq(string(fd.Name()), "_") {
			if w != "" {
				b.WriteString(strings.ToUpper(w[:1]) + w[1:])
			}
		}
		name = b.String()
		return false
	})
	return name
}
