package sim

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	// v3 is set for a call of a client of etcd's Go client.
	v3 bool
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
	// v3, for a client of etcd's Go client, calls node index node
	// (v3client.go).
	v3   *clientv3.Client
	node int
}

const numKeys = 8

func key(i int) []byte { return fmt.Appendf(nil, "k%d", i) }

// run makes one call after the other, with a think time of up to 20 ms
// between them, until the clients stop.
func (c *client) run() {
	s := c.sim
	if c.node >= 0 {
		err := c.newV3()
		defer func() {
			if err := c.v3.Close(); err != nil && !errors.Is(err, context.Canceled) {
				s.mu.Lock()
				s.fail("client %d: closing clientv3: %w", c.id, err)
				s.mu.Unlock()
			}
		}()
		if err != nil {
			s.mu.Lock()
			s.fail("client %d: %w", c.id, err)
			s.mu.Unlock()
			return
		}
	}
	s.mu.Lock()
	wait := s.rng.Int64N(100 * Millisecond)
	s.mu.Unlock()
	for s.sleep(wait) {
		s.mu.Lock()
		if s.now() >= s.stopAt {
			s.mu.Unlock()
			return
		}
		c.n++
		// The fourth member is a learner for part of the run and is later
		// removed. Ordinary clients keep the original voting endpoints.
		i := c.id % 3
		switch {
		case c.v3 != nil:
			i = c.node
		case !s.cfg.PinClients:
			i = s.rng.IntN(3)
		}
		r := &Record{Client: c.id, Node: i, Call: s.now(), req: c.request(), v3: c.v3 != nil}
		s.mu.Unlock()
		if c.v3 != nil {
			resp, err := c.callV3(r.req)
			s.mu.Lock()
			if errors.Is(err, rpctypes.ErrGRPCStopped) {
				s.nodes[i].reloads = append(s.nodes[i].reloads, s.now())
			}
			c.done(r, resp, err)
			wait = 1 + s.rng.Int64N(20*Millisecond)
			s.mu.Unlock()
			continue
		}
		resp, err := s.call(s.nodes[i], r.req, s.cfg.ClientTimeout)
		// A read that failed with "leader changed" goes again, as
		// clientv3 retries it.
		for errors.Is(err, rpctypes.ErrGRPCLeaderChanged) {
			timeout := s.cfg.ClientTimeout
			if timeout > 0 {
				s.mu.Lock()
				timeout = r.Call + timeout - s.now()
				s.mu.Unlock()
				if timeout <= 0 {
					err = errClientTimeout
					break
				}
			}
			resp, err = s.call(s.nodes[i], r.req, timeout)
		}
		s.mu.Lock()
		c.done(r, resp, err)
		// At least 1 ns, so the next call starts after this one returned.
		wait = 1 + s.rng.Int64N(20*Millisecond)
		s.mu.Unlock()
	}
}

// call sends req to n's process as a client on the other end of a
// connection would, and returns its answer. A timeout of 0 waits for the
// answer for as long as the connection lasts.
func (s *Sim) call(n *node, req proto.Message, timeout int64) (proto.Message, error) {
	s.mu.Lock()
	p, deadline := n.proc, s.now()+timeout
	lat := s.clientLatency()
	s.mu.Unlock()
	if !s.sleep(lat) {
		return nil, errConnLost
	}
	if p == nil {
		return nil, errConnRefused
	}
	ctx, cancel := context.WithCancel(p.ctx)
	if timeout > 0 {
		cancel()
		ctx, cancel = context.WithTimeout(p.ctx, time.Duration(deadline-s.now()))
	}
	resp, err := p.node.Do(ctx, req)
	cancel()
	switch {
	case p.dead():
		resp, err = nil, errConnLost
	case errors.Is(err, context.DeadlineExceeded):
		return nil, errClientTimeout
	case errors.Is(err, rpctypes.ErrGRPCStopped):
		s.mu.Lock()
		n.reloads = append(n.reloads, s.now())
		s.mu.Unlock()
	}
	s.mu.Lock()
	lat = s.clientLatency()
	s.mu.Unlock()
	if !s.sleep(lat) {
		return nil, errConnLost
	}
	if timeout > 0 && s.now() > deadline {
		return nil, errClientTimeout
	}
	return resp, err
}

// done ends a call. The lock is held.
func (c *client) done(r *Record, resp proto.Message, err error) {
	s := c.sim
	r.Return, r.Resp, r.Err = s.now(), resp, err
	r.Ambiguous = err != nil && ambiguous(err)
	s.history = append(s.history, r)
	if r.v3 && err == nil {
		s.stats.V3OK++
	}
	switch {
	case err == nil:
		s.stats.OK++
		if _, ok := resp.(*pb.CompactionResponse); ok {
			s.stats.Compactions++
		}
		c.learn(resp)
	case errors.Is(err, rpctypes.ErrGRPCNoSpace):
		s.stats.NoSpace++
		s.stats.Failed++
	case r.Ambiguous:
		s.stats.Ambiguous++
	default:
		s.stats.Failed++
	}
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
		errors.Is(err, rpctypes.ErrGRPCNoSpace),
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
		// Kubernetes compactor does, or half the time exactly where a
		// disconnected watch will resume, the boundary where etcd's rules
		// change. It may be compacted or ahead already.
		rev := max(1, c.rev-int64(s.rng.IntN(30)))
		var waiting []*watchClient
		for _, w := range s.watchers {
			if !w.open && w.next > 0 {
				waiting = append(waiting, w)
			}
		}
		if len(waiting) > 0 && s.rng.IntN(2) == 0 {
			// A watch that is between streams resumes at next.
			rev = waiting[s.rng.IntN(len(waiting))].next
		}
		return &pb.CompactionRequest{Revision: rev, Physical: s.rng.IntN(2) == 0}
	default:
		rev := c.mods[k]
		if rev == 0 {
			rev = 1
		}
		return &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: rev}
	}
}
