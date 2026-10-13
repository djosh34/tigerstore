package core

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// The lease rules follow etcd v3.7.2 (server/lease/lessor.go and
// server/etcdserver/v3_server.go) and the semantics pre-work on #47.

// maxLeaseTTL is etcd's largest lease TTL in seconds.
const maxLeaseTTL = 9_000_000_000

// lease is a granted lease, the keys attached to it, and the number of
// keepalives applied to it. expiry and revokeAt are not replicated state:
// only the leader sets them, to the tick the lease expires at (0 when not
// armed) and the tick it last proposed the lease's expiry.
type lease struct {
	id, ttl  int64
	keys     map[string]struct{}
	renewals int64
	expiry   int64
	revokeAt int64
}

// newExpiry is the leader's decision that a lease expired. It goes in the
// log as a LeaseCheckpoint, etcd's only message with a lease ID and a
// number: Remaining_TTL carries the count of keepalives the leader had
// applied when it decided. The expiry applies only if that count is still
// current, so a keepalive that commits first keeps the lease, on every
// replica.
func newExpiry(id, renewals int64) *pb.LeaseCheckpoint {
	return &pb.LeaseCheckpoint{ID: id, Remaining_TTL: renewals}
}

// State is the replicated state machine: the key index and the leases.
// Every replica applies the same log to it and ends in the same state,
// apart from the leader's lease expiry times, which are not replicated.
type State struct {
	kv     *mvcc.Store
	leases map[int64]*lease
	minTTL int64
	// changes are the key changes of the last applied write, none for a
	// write that changed no key.
	changes []mvcc.Change
}

// Changes are the key changes of the last write Apply ran. The slice is
// reused by the next one.
func (st *State) Changes() []mvcc.Change { return st.changes }

// NewState returns an empty state. Leases get at least minTTL seconds, as
// etcd clamps them to 1.5 election timeouts rounded up.
func NewState(minTTL int64) *State {
	st := &State{kv: mvcc.New(), leases: map[int64]*lease{}, minTTL: minTTL}
	st.kv.LeaseExists = func(id int64) bool { _, ok := st.leases[id]; return ok }
	return st
}

// Store is the key index.
func (st *State) Store() *mvcc.Store { return st.kv }

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

// Apply runs one committed write and returns its response with the header
// revision set. An expiry that no longer applies returns nil and nil. Errors from etcd's set of answers leave the state unchanged
// and are the same on every replica; any other error is a bug.
func (st *State) Apply(op proto.Message) (proto.Message, error) {
	st.changes = nil
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
		r, err := st.kv.Txn(op)
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
			l.renewals++
		}
		return resp, nil
	case *pb.LeaseCheckpoint:
		// An expiry. A nil response means it no longer applies: the lease
		// is gone or was renewed after the leader decided.
		l, ok := st.leases[op.ID]
		if !ok || l.renewals != op.Remaining_TTL {
			return nil, nil
		}
		return st.revoke(op.ID)
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
	l := &lease{id: r.ID, ttl: max(r.TTL, st.minTTL), keys: map[string]struct{}{}}
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
