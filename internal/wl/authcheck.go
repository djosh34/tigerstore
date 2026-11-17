package wl

import (
	"encoding/binary"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// authorize is etcd's check of a call before it runs. Each request is
// read with the parser that runs it (pb.go), never with protobuf's, so
// the check and the run see the same keys: protobuf takes the last of
// a oneof's fields and merges a repeated message, while wl's parsers
// take a RequestOp's first field.
func (n *node) authorize(kind uint8, msg []byte, ai authInfo, ok bool) error {
	a := n.auth
	var who *authInfo
	if ok {
		who = &ai
	}
	switch kind {
	case kRange, kRangeStream:
		var r rangeReq
		if err := parseRange(msg, &r); err != nil {
			return badRequest(err)
		}
		return a.permitted(ai, r.key, r.end, false)
	case kCompact, kHash, kHashKV, kMoveLeader, kDefragment, kDowngrade, kSnapshot,
		kMemberAdd, kMemberRemove, kMemberUpdate, kMemberPromote:
		return a.admin(who)
	case kAlarm:
		r, err := parseAlarm(msg)
		if err != nil {
			return badRequest(err)
		}
		if r.action == alarmActivate || r.action == alarmDeactivate {
			return a.admin(who)
		}
		return needIdentity(ok)
	case kStatus, kLeaseGrant, kMemberList:
		return needIdentity(ok)
	case kPut, kDel, kTxn, kLeaseRevoke:
		// etcd checks a write only as it applies, and so does this node,
		// with the identity it carries; a write already refused here
		// never goes into the log.
		if err := needIdentity(ok); err != nil {
			return err
		}
		return n.writeAllowed(kind, msg, ai, true)
	case kLeaseKeepAlive:
		id, err := leaseID(msg)
		if err != nil {
			return badRequest(err)
		}
		if !ok {
			return errUserEmpty
		}
		return n.leaseKeysAllowed(ai, id, true)
	case kLeaseTTL:
		id, keys, err := parseLeaseTTL(msg)
		if err != nil {
			return badRequest(err)
		}
		if !ok {
			return errUserEmpty
		}
		// etcd checks the keys only when the call asks for them.
		if !keys {
			return nil
		}
		return n.leaseKeysAllowed(ai, id, false)
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
		var r putReq
		if err := parsePut(req, &r); err != nil {
			return badRequest(err)
		}
		return n.putAllowed(who, r.key, r.lease, r.prevKV)
	case kDel:
		var r delReq
		if err := parseDel(req, &r); err != nil {
			return badRequest(err)
		}
		return n.deleteAllowed(who, r.key, r.end, r.prevKV)
	case kTxn:
		return n.txnAllowed(who, req)
	case kLeaseRevoke:
		if !named {
			return nil
		}
		id, err := leaseID(req)
		if err != nil {
			return badRequest(err)
		}
		return n.leaseAllowed(who, id)
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

// txnAllowed is etcd's checkTxnPermission, over the Txn as runBranch
// reads it: each RequestOp by its first field.
func (n *node) txnAllowed(who authInfo, msg []byte) error {
	for m := msg; len(m) > 0; {
		num, _, _, p, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		m = rest
		switch num {
		case 1:
			var c compare
			if err := parseCompare(p, &c); err != nil {
				return badRequest(err)
			}
			err = n.auth.permitted(who, c.key, c.end, false)
		case 2, 3:
			err = n.opAllowed(who, p)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// opAllowed checks a Txn's RequestOp.
func (n *node) opAllowed(who authInfo, p []byte) error {
	if len(p) == 0 {
		return nil
	}
	onum, _, _, op, _, err := field(p)
	if err != nil {
		return badRequest(err)
	}
	switch onum {
	case 1:
		var r rangeReq
		if err := parseRange(op, &r); err != nil {
			return badRequest(err)
		}
		return n.auth.permitted(who, r.key, r.end, false)
	case 2:
		var r putReq
		if err := parsePut(op, &r); err != nil {
			return badRequest(err)
		}
		return n.putAllowed(who, r.key, r.lease, r.prevKV)
	case 3:
		var r delReq
		if err := parseDel(op, &r); err != nil {
			return badRequest(err)
		}
		return n.deleteAllowed(who, r.key, r.end, r.prevKV)
	case 4:
		return n.txnAllowed(who, op)
	}
	return nil
}
