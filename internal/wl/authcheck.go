package wl

import (
	"encoding/binary"
	"errors"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The checks of auth, as etcd's gRPC layer, server and authApplierV3 make
// them. They run only while auth is on.

// authAllows checks a call as etcd does before it runs it, and reports
// whether it may go on; if not, it answered the call. Writes are checked
// again as they apply, with the identity they carry (kCaller).
func (n *node) authAllows(c *client, s *stream, msg []byte) bool {
	ai, ok, err := n.identify(c, s)
	if err == nil {
		err = n.authorize(s.kind, msg, ai, ok)
	}
	if err != nil {
		n.reject(c, s, err)
		return false
	}
	return true
}

// authorize is etcd's check of a call before it runs.
func (n *node) authorize(kind uint8, msg []byte, ai authInfo, ok bool) error {
	a := n.auth
	var who *authInfo
	if ok {
		who = &ai
	}
	switch kind {
	case kRange, kRangeStream:
		var r etcdpb.RangeRequest
		if err := proto.Unmarshal(msg, &r); err != nil {
			return badRequest(err)
		}
		return a.permitted(ai, r.Key, r.RangeEnd, false)
	case kCompact, kHash, kHashKV, kMoveLeader, kDefragment, kDowngrade, kSnapshot,
		kMemberAdd, kMemberRemove, kMemberUpdate, kMemberPromote:
		return a.admin(who)
	case kAlarm:
		var r etcdpb.AlarmRequest
		if err := proto.Unmarshal(msg, &r); err != nil {
			return badRequest(err)
		}
		if r.Action != etcdpb.AlarmRequest_GET {
			return a.admin(who)
		}
		return needIdentity(ok)
	case kStatus, kLeaseGrant, kLeaseRevoke, kMemberList, kPut, kDel, kTxn:
		return needIdentity(ok)
	case kLeaseKeepAlive:
		var r etcdpb.LeaseKeepAliveRequest
		if err := proto.Unmarshal(msg, &r); err != nil {
			return badRequest(err)
		}
		if !ok {
			return errUserEmpty
		}
		return n.leaseKeysAllowed(ai, r.ID, true)
	case kLeaseTTL:
		var r etcdpb.LeaseTimeToLiveRequest
		if err := proto.Unmarshal(msg, &r); err != nil {
			return badRequest(err)
		}
		if !ok {
			return errUserEmpty
		}
		// etcd checks the keys only when the call asks for them.
		if !r.Keys {
			return nil
		}
		return n.leaseKeysAllowed(ai, r.ID, false)
	case kLeaseLeases:
		if !ok {
			return errUserEmpty
		}
		for id := range n.st.leases {
			if err := n.leaseKeysAllowed(ai, id, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func needIdentity(ok bool) error {
	if !ok {
		return errUserEmpty
	}
	return nil
}

// leaseKeysAllowed is etcd's checkLeaseRenew (write) and
// checkLeaseTimeToLive (read): an admin may, others need the permission
// on each key of the lease.
func (n *node) leaseKeysAllowed(ai authInfo, id int64, write bool) error {
	a := n.auth
	if a.admin(&ai) == nil {
		return nil
	}
	l := n.st.leases[id]
	if l == nil {
		return nil
	}
	for k := range l.keys {
		if err := a.permitted(ai, n.st.key(k), nil, write); err != nil {
			return err
		}
	}
	return nil
}

// watchAllowed is etcd's isWatchPermitted: the reason a watch create is
// refused for, or "".
func (n *node) watchAllowed(c *client, s *stream, key, end []byte) string {
	ai, _, err := n.identify(c, s)
	if err == nil {
		err = n.auth.permitted(ai, key, end, false)
	}
	if re, ok := errors.AsType[*requestError](err); ok {
		return status.Error(codes.Code(re.code), re.msg).Error()
	}
	return ""
}

// addCaller puts, ahead of a write of s, an op that names its user and
// auth revision, which the write is checked with as it applies.
func (n *node) addCaller(c *client, s *stream) {
	if !n.auth.enabled {
		return
	}
	ai, _, _ := n.identify(c, s)
	b := binary.AppendUvarint(nil, ai.revision)
	n.addOp(noHandle, kCaller, append(b, ai.user...))
}

// parseCaller reads a kCaller op.
func parseCaller(b []byte) (authInfo, error) {
	rev, k := binary.Uvarint(b)
	if k <= 0 {
		return authInfo{}, errors.New("a caller op without its revision")
	}
	return authInfo{user: string(b[k:]), revision: rev}, nil
}

// writeAllowed checks a write as it applies, as etcd's authApplierV3
// does, with the identity its kCaller op named. A lease revoke without
// one is the leader's, of an expired lease.
func (n *node) writeAllowed(kind uint8, req []byte, who authInfo, named bool) error {
	switch kind {
	case kPut:
		var r etcdpb.PutRequest
		if err := proto.Unmarshal(req, &r); err != nil {
			return badRequest(err)
		}
		return n.putAllowed(who, r.Key, r.Lease, r.PrevKv)
	case kDel:
		var r etcdpb.DeleteRangeRequest
		if err := proto.Unmarshal(req, &r); err != nil {
			return badRequest(err)
		}
		return n.deleteAllowed(who, r.Key, r.RangeEnd, r.PrevKv)
	case kTxn:
		var r etcdpb.TxnRequest
		if err := proto.Unmarshal(req, &r); err != nil {
			return badRequest(err)
		}
		return n.txnAllowed(who, &r)
	case kLeaseRevoke:
		if !named {
			return nil
		}
		var r etcdpb.LeaseRevokeRequest
		if err := proto.Unmarshal(req, &r); err != nil {
			return badRequest(err)
		}
		return n.leaseAllowed(who, r.ID)
	}
	return nil
}

// putAllowed is etcd's checkPutAuth.
func (n *node) putAllowed(who authInfo, key []byte, lease int64, prevKV bool) error {
	a := n.auth
	if err := a.permitted(who, key, nil, true); err != nil {
		return err
	}
	if err := n.leaseAllowed(who, lease); err != nil {
		return err
	}
	if prevKV {
		return a.permitted(who, key, nil, false)
	}
	return nil
}

// leaseAllowed is etcd's checkLeasePuts: a lease's keys must all be
// writable, unless the user is an admin.
func (n *node) leaseAllowed(who authInfo, id int64) error {
	l := n.st.leases[id]
	if l == nil || n.auth.admin(&who) == nil {
		return nil
	}
	for k := range l.keys {
		if err := n.auth.permitted(who, n.st.key(k), nil, true); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) deleteAllowed(who authInfo, key, end []byte, prevKV bool) error {
	if prevKV {
		if err := n.auth.permitted(who, key, end, false); err != nil {
			return err
		}
	}
	return n.auth.permitted(who, key, end, true)
}

// txnAllowed is etcd's checkTxnPermission.
func (n *node) txnAllowed(who authInfo, t *etcdpb.TxnRequest) error {
	for _, c := range t.Compare {
		if err := n.auth.permitted(who, c.Key, c.RangeEnd, false); err != nil {
			return err
		}
	}
	for _, ops := range [][]*etcdpb.RequestOp{t.Success, t.Failure} {
		for _, o := range ops {
			var err error
			switch r := o.Request.(type) {
			case *etcdpb.RequestOp_RequestRange:
				err = n.auth.permitted(who, r.RequestRange.GetKey(), r.RequestRange.GetRangeEnd(), false)
			case *etcdpb.RequestOp_RequestPut:
				err = n.putAllowed(who, r.RequestPut.GetKey(), r.RequestPut.GetLease(), r.RequestPut.GetPrevKv())
			case *etcdpb.RequestOp_RequestDeleteRange:
				if d := r.RequestDeleteRange; d != nil {
					err = n.deleteAllowed(who, d.Key, d.RangeEnd, d.PrevKv)
				}
			case *etcdpb.RequestOp_RequestTxn:
				if r.RequestTxn != nil {
					err = n.txnAllowed(who, r.RequestTxn)
				}
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
