package core

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"maps"
	"slices"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The lease rules follow etcd v3.7.2 (server/lease/lessor.go and
// server/etcdserver/v3_server.go) and the semantics pre-work on #47.

// maxLeaseTTL is etcd's largest lease TTL in seconds.
const maxLeaseTTL = 9_000_000_000

// lease is a granted lease, the keys attached to it, and its stamp: the
// value of the state's stamp counter at its last grant or keepalive.
// expiry and revokeAt are not replicated state: only the leader sets them,
// to the tick the lease expires at (0 when not armed) and the tick it
// last proposed the lease's expiry.
type lease struct {
	id, ttl int64
	keys    map[string]struct{}
	stamp   int64
	// remaining is the remaining TTL in seconds the last checkpoint took,
	// 0 for none (lease.go).
	remaining int64
	// expiry and revokeAt are the leader's ticks of the lease's expiry
	// and of the last proposal of it, and checkpointAt of its next
	// checkpoint, 0 for none.
	expiry       int64
	revokeAt     int64
	checkpointAt int64
}

// newExpiry is the leader's decision that a lease expired. It goes in the
// log as a LeaseCheckpoint, etcd's only message with a lease ID and a
// number: Remaining_TTL carries the lease's stamp when the leader decided.
// The expiry applies only if the stamp is still current, so a keepalive
// that commits first keeps the lease, and a later grant of the same ID is
// a new lease that the old expiry cannot touch, on every replica.
func newExpiry(id, stamp int64) *pb.LeaseCheckpoint {
	return &pb.LeaseCheckpoint{ID: id, Remaining_TTL: stamp}
}

// State is the replicated state machine: the key index, the leases and
// the cluster version. Every replica applies the same log to it and ends
// in the same state, apart from the leader's lease expiry times, which are
// not replicated.
type State struct {
	kv     *mvcc.Store
	leases map[int64]*lease
	minTTL int64
	// stamps counts grants and keepalives, to stamp leases.
	stamps int64
	// version is the cluster version, supports the newest version each
	// voter announced, and runs the run of the node that announced it
	// (version.go).
	version  uint32
	supports map[uint64]uint32
	runs     map[uint64]uint64
	// members are the cluster's members by ID, and removed the IDs it
	// removed, which never come back (members.go).
	members map[uint64]*pb.Member
	removed map[uint64]bool
	// auth is the auth store (auth.go), and who the identity of the write
	// being applied.
	auth *authState
	who  authInfo
	// changes are the key changes of the last applied write, none for a
	// write that changed no key.
	changes []mvcc.Change
	// alarms are the active alarms in type and member order. noSpace and
	// corrupt say whether a NOSPACE or a CORRUPT alarm is among them.
	alarms []*pb.AlarmMember
	// alarmsGen counts the changes of alarms.
	alarmsGen        uint64
	noSpace, corrupt bool
	// etcdVersion is etcd's cluster version, and downgradeEnabled and
	// downgradeTarget etcd's downgrade info (downgrade.go).
	etcdVersion      string
	downgradeEnabled bool
	downgradeTarget  string
}

// Changes are the key changes of the last write Apply ran. The slice is
// reused by the next one.
func (st *State) Changes() []mvcc.Change { return st.changes }

// NewState returns an empty state that keeps its values in v. Leases get
// at least minTTL seconds, as etcd clamps them to 1.5 election timeouts
// rounded up.
func NewState(minTTL int64, v mvcc.Values) *State {
	st := &State{
		kv: mvcc.New(v), leases: map[int64]*lease{}, minTTL: minTTL, version: 1, supports: map[uint64]uint32{}, runs: map[uint64]uint64{},
		members: map[uint64]*pb.Member{}, removed: map[uint64]bool{}, auth: newAuthState(), etcdVersion: ClusterVersion,
	}
	st.kv.LeaseExists = func(id int64) bool { _, ok := st.leases[id]; return ok }
	return st
}

// Store is the key index.
func (st *State) Store() *mvcc.Store { return st.kv }

// Hash is a hash of the whole replicated state: the index, the leases
// with their key counts, the lease stamps, the versions, the alarms, the
// members, auth and the downgrade info. The keys of the leases follow
// from the index.
func (st *State) Hash() uint64 {
	h := st.kv.Hash()
	mix := func(x uint64) {
		h = (h ^ x) * 0x100000001b3
		h ^= h >> 29
	}
	mix(uint64(st.stamps))
	for _, l := range st.sortedLeases() {
		mix(uint64(l.id))
		mix(uint64(l.ttl))
		mix(uint64(l.stamp))
		mix(uint64(l.remaining))
		mix(uint64(len(l.keys)))
	}
	mix(uint64(st.version))
	for _, id := range slices.Sorted(maps.Keys(st.supports)) {
		mix(id)
		mix(uint64(st.supports[id]))
		mix(st.runs[id])
	}
	for _, a := range st.alarms {
		mix(a.MemberID)
		mix(uint64(a.Alarm))
	}
	for _, m := range st.memberList() {
		mix(m.ID)
		b := binary.AppendUvarint(nil, uint64(len(m.Name)))
		b = append(b, m.Name...)
		for _, urls := range [][]string{m.PeerURLs, m.ClientURLs} {
			b = binary.AppendUvarint(b, uint64(len(urls)))
			for _, u := range urls {
				b = binary.AppendUvarint(b, uint64(len(u)))
				b = append(b, u...)
			}
		}
		if m.IsLearner {
			b = append(b, 1)
		}
		mix(uint64(crc32.Checksum(b, castagnoli)))
	}
	for _, id := range slices.Sorted(maps.Keys(st.removed)) {
		mix(id)
	}
	mix(st.auth.revision)
	if st.auth.enabled {
		mix(1)
	}
	for _, name := range sortedKeys(st.auth.users) {
		u := st.auth.users[name]
		mix(uint64(crc32.Checksum(u.Name, castagnoli)))
		mix(uint64(crc32.Checksum(u.Password, castagnoli)))
		mix(uint64(crc32.Checksum([]byte(strings.Join(u.Roles, "\x00")), castagnoli)))
		if u.Options != nil && u.Options.NoPassword {
			mix(1)
		}
	}
	for _, name := range sortedKeys(st.auth.roles) {
		mix(uint64(crc32.Checksum([]byte(name), castagnoli)))
		for _, p := range st.auth.roles[name].KeyPermission {
			mix(uint64(crc32.Checksum(append(append([]byte(name+"\x00"), p.Key...), p.RangeEnd...), castagnoli)))
			mix(uint64(p.PermType))
		}
	}
	mix(uint64(crc32.Checksum([]byte(st.etcdVersion), castagnoli)))
	if st.downgradeEnabled {
		mix(1)
	}
	mix(uint64(crc32.Checksum([]byte(st.downgradeTarget), castagnoli)))
	return h
}

// Hash32 answers Maintenance.Hash: a CRC-32C of every version in the
// index (mvcc.Store.HashAll), the leases and the alarms. etcd hashes its whole
// database file there, so the number differs from etcd's, but replicas
// with the same state agree on it.
func (st *State) Hash32() uint32 {
	b := binary.BigEndian.AppendUint32(nil, st.kv.HashAll())
	b = binary.AppendVarint(b, st.stamps)
	for _, l := range st.sortedLeases() {
		b = binary.AppendVarint(b, l.id)
		b = binary.AppendVarint(b, l.ttl)
		b = binary.AppendVarint(b, l.stamp)
	}
	for _, a := range st.alarms {
		b = binary.AppendUvarint(b, a.MemberID)
		b = binary.AppendVarint(b, int64(a.Alarm))
	}
	return crc32.Checksum(b, castagnoli)
}

// sortedLeases returns the leases in ID order, so that the core walks them
// the same way on every run.
func (st *State) sortedLeases() []*lease {
	ls := make([]*lease, 0, len(st.leases))
	for _, l := range st.leases {
		ls = append(ls, l)
	}
	slices.SortFunc(ls, func(a, b *lease) int { return cmp.Compare(a.id, b.id) })
	return ls
}

// track records the changes of the write just applied to the index and
// moves its keys between leases: a key leaves the lease of its old version
// and joins the lease of its new one.
func (st *State) track() {
	st.changes = st.kv.Changes()
	for _, c := range st.changes {
		k := string(c.Key())
		if id := c.PrevLease(); id != 0 {
			if l, ok := st.leases[id]; ok {
				delete(l.keys, k)
			}
		}
		if id := c.Lease(); id != 0 {
			if l, ok := st.leases[id]; ok {
				l.keys[k] = struct{}{}
			}
		}
	}
}

// ApplyPut runs one committed put, given by its fields, and returns the
// previous version if the put asked for it.
func (st *State) ApplyPut(op *mvcc.PutOp) (*mvccpb.KeyValue, error) {
	st.changes = nil
	if err := st.refused(true); err != nil {
		return nil, err
	}
	if err := st.putAllowed(st.who, op.Key, op.Lease, op.PrevKv); err != nil {
		return nil, err
	}
	prev, err := st.kv.PutKV(op)
	if err != nil {
		return nil, err
	}
	st.track()
	return prev, nil
}

// Apply runs one committed write and returns its response with the header
// revision set. An expiry that no longer applies returns nil and nil.
// Errors from etcd's set of answers leave the state unchanged
// and are the same on every replica; any other error is a bug.
func (st *State) Apply(op proto.Message) (proto.Message, error) {
	return st.applyReserved(op, nil, nil)
}

func (st *State) applyReserved(op proto.Message, reserve func(int64) error, compares *mvcc.CompareProgress) (proto.Message, error) {
	st.changes = nil
	if err := st.refusedOp(op); err != nil {
		return nil, err
	}
	if err := st.writeAllowed(st.who, op); err != nil {
		return nil, err
	}
	switch op := op.(type) {
	case *pb.PutRequest:
		r, err := st.kv.Put(op)
		if err != nil {
			return nil, err
		}
		st.track()
		return r, nil
	case *pb.DeleteRangeRequest:
		r, err := st.kv.DeleteRange(op)
		if err != nil {
			return nil, err
		}
		st.track()
		return r, nil
	case *pb.TxnRequest:
		r, err := st.kv.TxnReserved(op, reserve, true, compares)
		if err != nil {
			return nil, err
		}
		st.track()
		return r, nil
	case *pb.CompactionRequest:
		// Compact keeps the revision; physical compaction is the same,
		// because it happens at once.
		if err := st.kv.Compact(op.Revision); err != nil {
			return nil, err
		}
		return &pb.CompactionResponse{Header: header(st.kv.Rev())}, nil
	case *pb.LeaseGrantRequest:
		return st.grant(op)
	case *pb.LeaseRevokeRequest:
		return st.revoke(op.ID)
	case *pb.LeaseKeepAliveRequest:
		resp := &pb.LeaseKeepAliveResponse{Header: header(st.kv.Rev()), ID: op.ID}
		if l, ok := st.leases[op.ID]; ok {
			resp.TTL = l.ttl
			st.stamps++
			l.stamp = st.stamps
			// A renewal ends the checkpointed TTL, as etcd's does.
			l.remaining = 0
		}
		return resp, nil
	case *pb.LeaseCheckpointRequest:
		for _, c := range op.Checkpoints {
			if l, ok := st.leases[c.ID]; ok {
				l.remaining = c.Remaining_TTL
			}
		}
		return &pb.LeaseCheckpointResponse{Header: header(st.kv.Rev())}, nil
	case *pb.LeaseCheckpoint:
		// An expiry. A nil response means it no longer applies: the lease
		// is gone or was renewed after the leader decided.
		l, ok := st.leases[op.ID]
		if !ok || l.stamp != op.Remaining_TTL {
			return nil, nil
		}
		return st.revoke(op.ID)
	case *pb.AlarmRequest:
		return st.alarm(op), nil
	case *pb.Member:
		st.applyAttributes(op)
		return nil, nil
	}
	return nil, fmt.Errorf("%w: write of type %T", errInternal, op)
}

func header(rev int64) *pb.ResponseHeader { return &pb.ResponseHeader{Revision: rev} }

func (st *State) grant(r *pb.LeaseGrantRequest) (proto.Message, error) {
	if r.ID == 0 {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}
	if r.TTL > maxLeaseTTL {
		return nil, rpctypes.ErrGRPCLeaseTTLTooLarge
	}
	if _, ok := st.leases[r.ID]; ok {
		return nil, rpctypes.ErrGRPCLeaseExist
	}
	st.stamps++
	l := &lease{id: r.ID, ttl: max(r.TTL, st.minTTL), keys: map[string]struct{}{}, stamp: st.stamps}
	st.leases[r.ID] = l
	return &pb.LeaseGrantResponse{Header: header(st.kv.Rev()), ID: l.id, TTL: l.ttl}, nil
}

// revoke deletes the lease and every key attached to it in one write, so
// the keys go at one revision. A lease without keys changes no revision.
func (st *State) revoke(id int64) (proto.Message, error) {
	l, ok := st.leases[id]
	if !ok {
		return nil, rpctypes.ErrGRPCLeaseNotFound
	}
	keys := make([]string, 0, len(l.keys))
	for k := range l.keys {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if len(keys) > 0 {
		del := &pb.TxnRequest{}
		for _, k := range keys {
			del.Success = append(del.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte(k)},
			}})
		}
		if _, err := st.kv.Txn(del); err != nil {
			return nil, fmt.Errorf("%w: revoking lease %d: %w", errInternal, id, err)
		}
		st.track()
	}
	delete(st.leases, id)
	return &pb.LeaseRevokeResponse{Header: header(st.kv.Rev())}, nil
}

// timeToLive answers LeaseTimeToLive. remaining is the leader's count of
// seconds left, or -1 to report the granted TTL. A missing lease has TTL -1,
// as in etcd.
func (st *State) timeToLive(r *pb.LeaseTimeToLiveRequest, remaining int64) *pb.LeaseTimeToLiveResponse {
	resp := &pb.LeaseTimeToLiveResponse{Header: header(st.kv.Rev()), ID: r.ID, TTL: -1}
	l, ok := st.leases[r.ID]
	if !ok {
		return resp
	}
	resp.GrantedTTL, resp.TTL = l.ttl, l.ttl
	if remaining >= 0 {
		resp.TTL = remaining
	}
	if r.Keys {
		for k := range l.keys {
			resp.Keys = append(resp.Keys, []byte(k))
		}
		slices.SortFunc(resp.Keys, bytes.Compare)
	}
	return resp
}

// leaseIDs answers LeaseLeases.
func (st *State) leaseIDs() *pb.LeaseLeasesResponse {
	resp := &pb.LeaseLeasesResponse{Header: header(st.kv.Rev())}
	for _, l := range st.sortedLeases() {
		resp.Leases = append(resp.Leases, &pb.LeaseStatus{ID: l.id})
	}
	return resp
}
