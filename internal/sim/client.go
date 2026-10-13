package sim

import (
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"
)

// Record is one client call as the client saw it.
type Record struct {
	Client int
	Node   int
	Call   int64
	Return int64
	req    proto.Message
	Resp   proto.Message
	Err    error
	// Ambiguous is set when the call may or may not have taken effect.
	Ambiguous bool
}

// Req is the request of the call.
func (r *Record) Req() proto.Message { return r.req }

type client struct {
	id  int
	sim *Sim
	n   int
	// mods remembers the last mod revision this client saw per key, for
	// compare-and-swap.
	mods [numKeys]int64
	// rev is the newest revision this client saw.
	rev int64
	// leases are the IDs of leases this client was granted.
	leases []int64
}

type clientOp struct {
	client *client
	rec    *Record
	done   bool
}

const numKeys = 8

func key(i int) []byte { return fmt.Appendf(nil, "k%d", i) }

// start begins the client's next call.
func (c *client) start() {
	s := c.sim
	if s.now >= s.stopAt {
		return
	}
	c.n++
	n := s.nodes[s.rng.IntN(len(s.nodes))]
	op := &clientOp{client: c, rec: &Record{Client: c.id, Node: n.idx, Call: s.now, req: c.request()}}
	s.push(&event{at: s.now + s.cfg.ClientTimeout, kind: evClientTimeout, op: op})
	if !n.up {
		s.push(&event{at: s.now + s.clientLatency(), kind: evClientReply, op: op, err: errConnRefused})
		return
	}
	s.push(&event{at: s.now + s.clientLatency(), kind: evSubmit, node: n.idx, gen: n.gen, op: op})
}

// done ends a call. Later answers to the same call are ignored.
func (c *client) done(op *clientOp, resp proto.Message, err error) {
	s := c.sim
	if op.done {
		return
	}
	op.done = true
	r := op.rec
	r.Return, r.Resp, r.Err = s.now, resp, err
	r.Ambiguous = err != nil && ambiguous(err)
	s.history = append(s.history, r)
	switch {
	case err == nil:
		s.stats.OK++
		if _, ok := resp.(*pb.CompactionResponse); ok {
			s.stats.Compactions++
		}
		c.learn(resp)
	case r.Ambiguous:
		s.stats.Ambiguous++
	default:
		s.stats.Failed++
	}
	s.mix(0xc1, uint64(c.id), uint64(r.Call), uint64(r.Return))
	// At least 1 ns, so the next call starts after this one returned.
	think := 1 + s.rng.Int64N(20*Millisecond)
	s.push(&event{at: s.now + think, kind: evClientStart, op: &clientOp{client: c}})
}

// ambiguous reports whether a failed call may still have taken effect.
func ambiguous(err error) bool {
	switch {
	case errors.Is(err, errConnRefused),
		errors.Is(err, rpctypes.ErrGRPCNoLeader),
		errors.Is(err, rpctypes.ErrGRPCKeyNotFound),
		errors.Is(err, rpctypes.ErrGRPCLeaseNotFound),
		errors.Is(err, rpctypes.ErrGRPCFutureRev),
		errors.Is(err, rpctypes.ErrGRPCCompacted),
		errors.Is(err, rpctypes.ErrGRPCDuplicateKey),
		errors.Is(err, rpctypes.ErrGRPCEmptyKey):
		return false
	}
	return true
}

// learn keeps the revisions the client saw.
func (c *client) learn(resp proto.Message) {
	if h, ok := resp.(interface{ GetHeader() *pb.ResponseHeader }); ok {
		c.rev = max(c.rev, h.GetHeader().GetRevision())
	}
	if g, ok := resp.(*pb.LeaseGrantResponse); ok {
		c.leases = append(c.leases, g.ID)
	}
	var kvsOf func(proto.Message)
	kvsOf = func(m proto.Message) {
		switch r := m.(type) {
		case *pb.RangeResponse:
			for _, kv := range r.Kvs {
				if i := int(kv.Key[len(kv.Key)-1] - '0'); len(kv.Key) == 2 && i >= 0 && i < numKeys {
					c.mods[i] = kv.ModRevision
				}
			}
		case *pb.TxnResponse:
			for _, op := range r.Responses {
				if rr := op.GetResponseRange(); rr != nil {
					kvsOf(rr)
				}
			}
		}
	}
	kvsOf(resp)
}

// request picks the next call: puts, deletes, compare-and-swap, reads at
// the current and past revisions, prefix reads with limits, and
// serializable reads.
func (c *client) request() proto.Message {
	s := c.sim
	k := s.rng.IntN(numKeys)
	val := fmt.Appendf(nil, "c%d-%d", c.id, c.n)
	switch p := s.rng.IntN(100); {
	case p < 22:
		put := &pb.PutRequest{Key: key(k), Value: val, PrevKv: s.rng.IntN(2) == 0}
		if len(c.leases) > 0 && s.rng.IntN(2) == 0 {
			// Mostly the newest lease, so leases collect several keys and
			// a revoke or expiry deletes them at one revision. The lease
			// may have expired already; then the put fails.
			put.Lease = c.leases[len(c.leases)-1]
		}
		return put
	case p < 30:
		return &pb.DeleteRangeRequest{Key: key(k), PrevKv: s.rng.IntN(2) == 0}
	case p < 34:
		// A range delete changes several keys in one revision.
		return &pb.DeleteRangeRequest{Key: key(k), RangeEnd: key(min(k+3, numKeys)), PrevKv: s.rng.IntN(2) == 0}
	case p < 50:
		return &pb.TxnRequest{
			Compare: []*pb.Compare{{
				Key: key(k), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL,
				TargetUnion: &pb.Compare_ModRevision{ModRevision: c.mods[k]},
			}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key(k), Value: val}}}},
			Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key(k)}}}},
		}
	case p < 55:
		// A transaction that writes two keys in one revision.
		k2 := (k + 1 + s.rng.IntN(numKeys-1)) % numKeys
		return &pb.TxnRequest{Success: []*pb.RequestOp{
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key(k), Value: fmt.Appendf(nil, "%sa", val), PrevKv: true}}},
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key(k2), Value: fmt.Appendf(nil, "%sb", val)}}},
		}}
	case p < 80:
		return &pb.RangeRequest{Key: key(k)}
	case p < 90:
		return &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Limit: int64(s.rng.IntN(4))}
	case p < 95:
		return &pb.RangeRequest{Key: key(k), Serializable: true}
	case p < 98 && s.cfg.NoLeases:
		return &pb.RangeRequest{Key: key(k)}
	case p < 98 && len(c.leases) > 0 && s.rng.IntN(4) == 0:
		return &pb.LeaseRevokeRequest{ID: c.leases[s.rng.IntN(len(c.leases))]}
	case p < 98 && len(c.leases) > 0 && s.rng.IntN(2) == 0:
		return &pb.LeaseKeepAliveRequest{ID: c.leases[len(c.leases)-1]}
	case p < 98:
		return &pb.LeaseGrantRequest{TTL: 2 + int64(s.rng.IntN(3))}
	case p < 99:
		// Compact a little behind the newest revision seen, as the
		// Kubernetes compactor does; it may be compacted or ahead already.
		return &pb.CompactionRequest{Revision: max(1, c.rev-int64(s.rng.IntN(30))), Physical: s.rng.IntN(2) == 0}
	default:
		rev := c.mods[k]
		if rev == 0 {
			rev = 1
		}
		return &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: rev}
	}
}
