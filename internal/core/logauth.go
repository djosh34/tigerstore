package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// LogAuth follows the auth state through a committed log of this engine
// as its apply does, so that a reader of a stopped node's log leaves out
// the writes the apply refused for the identity they carried. A write
// proposed before auth was turned on and applied after carries none, and
// is refused with etcd's ErrUserEmpty (#698). It keeps no leases: a write
// with a lease is checked as if its lease held no keys, which cmd/chaos,
// whose campaign sends no lease, never meets. Nor does it keep alarms,
// which refuse writes too, and which cmd/chaos's runs never raise.
type LogAuth struct{ st State }

// NewLogAuth follows a log from its first entry, with auth off.
func NewLogAuth() *LogAuth {
	return &LogAuth{st: State{auth: newAuthState(), leases: map[int64]*lease{}}}
}

// Writes calls fn, as DecodeBatch does, for each write of the committed
// entry data that the apply takes, and applies its auth requests. Entries
// go in the log's order.
func (la *LogAuth) Writes(data []byte, fn func(op proto.Message) error) error {
	return walkBatch(data, func(_, _, _ uint64, kind byte, b []byte, at int) error {
		op, who, err := decodeLogged(kind, b, at)
		if err != nil || op == nil {
			return err
		}
		if r, ok := op.(*pb.InternalRaftRequest); ok {
			if r.DowngradeInfoSet == nil && r.ClusterVersionSet == nil {
				// A refused auth request changes nothing.
				if _, err := la.st.auth.apply(r, nil, nil, 0); err != nil && !isAnswer(err) {
					return err
				}
			}
			return fn(op)
		}
		switch err := la.st.writeAllowed(who, op); {
		case err == nil:
			return fn(op)
		case isAnswer(err):
			// The apply answered the write with err and changed nothing.
			return nil
		default:
			return err
		}
	})
}
