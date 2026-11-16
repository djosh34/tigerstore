package sim

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
)

// checkLive checks that every call that started after the cluster healed
// succeeded, but those a node's own trouble explains.
func (s *Sim) checkLive(res *Result) error {
	live := 0
	liveFrom := s.liveFrom()
	for _, r := range res.History {
		if r.Call < liveFrom {
			continue
		}
		if n := s.nodes[r.Node]; n.stalledAt(r.Call) || r.Err != nil && n.reloadedDuring(r.Call, r.Return) || n.hungOnReload(r) {
			continue
		}
		if r.Err != nil && s.removalDuring(r.Call, r.Return) {
			// The removed member stops, and when it led, the others
			// elect another (seed 50214159).
			continue
		}
		if s.nodes[r.Node].fenced && (errors.Is(r.Err, errConnRefused) || r.v3) {
			// The node was fenced for good; a real client moves on to
			// another endpoint.
			continue
		}
		if r.Err != nil && (r.Ambiguous || !isRejection(r.Err)) {
			return fmt.Errorf("liveness: client %d call at %d ms, %d ms after healing, failed: %w",
				r.Client, r.Call/Millisecond, (r.Call-s.healAt)/Millisecond, r.Err)
		}
		live++
	}
	if live < 50 {
		return fmt.Errorf("liveness: only %d calls completed after healing", live)
	}
	return nil
}

// live returns active members whose disks have not been fenced, but a
// rotted node that stopped too late to restart before the end.
func (s *Sim) live() []*node {
	var out []*node
	for _, n := range s.nodes {
		if !n.out() && !n.rotStopped {
			out = append(out, n)
		}
	}
	return out
}

func at(st *mvcc.Store, key []byte, rev int64) (*mvccpb.KeyValue, error) {
	r, err := st.Range(&pb.RangeRequest{Key: key, Revision: rev})
	if err != nil || len(r.Kvs) == 0 {
		return nil, err
	}
	return r.Kvs[0], nil
}

// sameRead re-runs a read at the revision it saw and compares.
func sameRead(st *mvcc.Store, req *pb.RangeRequest, got *pb.RangeResponse) error {
	q := proto.CloneOf(req)
	if q.Revision == 0 {
		q.Revision = got.Header.Revision
	}
	q.Serializable = false
	want, err := st.Range(q)
	if err != nil {
		return fmt.Errorf("re-reading at revision %d: %w", q.Revision, err)
	}
	g := proto.CloneOf(got)
	g.Header, want.Header = nil, nil
	if !proto.Equal(g, want) {
		return fmt.Errorf("read at revision %d returned %v, the final store has %v", q.Revision, g, want)
	}
	return nil
}

func samePrev(st *mvcc.Store, key []byte, rev int64, got *mvccpb.KeyValue) error {
	want, err := at(st, key, rev)
	if err != nil {
		return err
	}
	if !proto.Equal(got, want) {
		return fmt.Errorf("previous value of %q is %v, the final store has %v at revision %d", key, got, want, rev)
	}
	return nil
}

// checkDelete checks that a delete removed exactly the keys of its range
// that existed at the revision before, and returned them when asked.
func checkDelete(st *mvcc.Store, req *pb.DeleteRangeRequest, resp *pb.DeleteRangeResponse) error {
	rev := resp.Header.Revision
	after, err := st.Range(&pb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd, Revision: rev})
	if err != nil {
		return err
	}
	if len(after.Kvs) != 0 {
		return fmt.Errorf("keys %v exist at revision %d after an acknowledged delete", after.Kvs, rev)
	}
	if resp.Deleted == 0 {
		return nil
	}
	before, err := st.Range(&pb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd, Revision: rev - 1})
	if err != nil {
		return err
	}
	if int64(len(before.Kvs)) != resp.Deleted {
		return fmt.Errorf("delete reported %d keys, the final store had %d at revision %d", resp.Deleted, len(before.Kvs), rev-1)
	}
	if req.PrevKv {
		if len(resp.PrevKvs) != len(before.Kvs) {
			return fmt.Errorf("delete returned %d previous values for %d keys", len(resp.PrevKvs), len(before.Kvs))
		}
		for i := range before.Kvs {
			if !proto.Equal(before.Kvs[i], resp.PrevKvs[i]) {
				return fmt.Errorf("previous value %v, the final store has %v at revision %d", resp.PrevKvs[i], before.Kvs[i], rev-1)
			}
		}
	}
	return nil
}

func checkPut(st *mvcc.Store, p *pb.PutRequest, rev int64, prev *mvccpb.KeyValue) error {
	kv, err := at(st, p.Key, rev)
	if err != nil {
		return err
	}
	if kv == nil || kv.ModRevision != rev || !bytes.Equal(kv.Value, p.Value) {
		return fmt.Errorf("acknowledged put of %q at revision %d is not in the final store: %v", p.Key, rev, kv)
	}
	if p.PrevKv {
		return samePrev(st, p.Key, rev-1, prev)
	}
	return nil
}

func checkRecord(st *mvcc.Store, r *Record) error {
	switch req := r.req.(type) {
	case *pb.RangeRequest:
		resp, ok := r.Resp.(*pb.RangeResponse)
		if !ok {
			return errors.New("wrong response type")
		}
		return sameRead(st, req, resp)
	case *pb.PutRequest:
		resp, ok := r.Resp.(*pb.PutResponse)
		if !ok {
			return errors.New("wrong response type")
		}
		return checkPut(st, req, resp.Header.Revision, resp.PrevKv)
	case *pb.DeleteRangeRequest:
		resp, ok := r.Resp.(*pb.DeleteRangeResponse)
		if !ok {
			return errors.New("wrong response type")
		}
		return checkDelete(st, req, resp)
	case *pb.TxnRequest:
		resp, ok := r.Resp.(*pb.TxnResponse)
		if !ok {
			return errors.New("wrong response type")
		}
		return checkTxn(st, req, resp)
	case *pb.CompactionRequest, *pb.LeaseGrantRequest, *pb.LeaseRevokeRequest, *pb.LeaseKeepAliveRequest:
		// These change no value directly; the model replays their effect
		// on keys from the log, and converged checks the replicas agree.
		return nil
	}
	return fmt.Errorf("unknown request %T", r.req)
}

// checkTxn checks single-key compare-and-swap transactions: the compare
// held at the revision before a successful write and failed at the
// revision a failed one saw.
func checkTxn(st *mvcc.Store, req *pb.TxnRequest, resp *pb.TxnResponse) error {
	rev := resp.Header.Revision
	ops := req.Failure
	evalAt := rev
	if resp.Succeeded {
		ops = req.Success
		if !mvcc.IsTxnReadOnly(req) {
			evalAt = rev - 1
		}
	}
	for _, c := range req.Compare {
		if len(c.RangeEnd) != 0 {
			continue
		}
		kv, err := at(st, c.Key, evalAt)
		if err != nil {
			return err
		}
		if held := compareHolds(c, kv); held != resp.Succeeded {
			return fmt.Errorf("compare on %q gave %v, but at revision %d it gives %v", c.Key, resp.Succeeded, evalAt, held)
		}
	}
	for i, op := range ops {
		switch o := op.Request.(type) {
		case *pb.RequestOp_RequestPut:
			if err := checkPut(st, o.RequestPut, rev, resp.Responses[i].GetResponsePut().GetPrevKv()); err != nil {
				return err
			}
		case *pb.RequestOp_RequestRange:
			if err := sameRead(st, o.RequestRange, resp.Responses[i].GetResponseRange()); err != nil {
				return err
			}
		}
	}
	return nil
}

func compareHolds(c *pb.Compare, kv *mvccpb.KeyValue) bool {
	if kv == nil {
		if c.Target == pb.Compare_VALUE {
			return false
		}
		kv = &mvccpb.KeyValue{}
	}
	var res int
	switch c.Target {
	case pb.Compare_MOD:
		res = cmp.Compare(kv.ModRevision, c.GetModRevision())
	case pb.Compare_VERSION:
		res = cmp.Compare(kv.Version, c.GetVersion())
	case pb.Compare_CREATE:
		res = cmp.Compare(kv.CreateRevision, c.GetCreateRevision())
	case pb.Compare_LEASE:
		res = cmp.Compare(kv.Lease, c.GetLease())
	case pb.Compare_VALUE:
		res = bytes.Compare(kv.Value, c.GetValue())
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return res == 0
	case pb.Compare_NOT_EQUAL:
		return res != 0
	case pb.Compare_GREATER:
		return res > 0
	case pb.Compare_LESS:
		return res < 0
	}
	return false
}

// revision returns the header revision of a successful linearizable call
// and whether the call is a write that must get a new revision.
func revision(r *Record) (rev int64, write, ok bool) {
	if r.Err != nil {
		return 0, false, false
	}
	switch resp := r.Resp.(type) {
	case *pb.RangeResponse:
		req, _ := r.req.(*pb.RangeRequest)
		return resp.Header.Revision, false, req != nil && !req.Serializable
	case *pb.PutResponse:
		return resp.Header.Revision, true, true
	case *pb.DeleteRangeResponse:
		return resp.Header.Revision, resp.Deleted > 0, true
	case *pb.TxnResponse:
		req, _ := r.req.(*pb.TxnRequest)
		if req == nil || mvcc.IsTxnSerializable(req) {
			return 0, false, false
		}
		return resp.Header.Revision, resp.Succeeded && !mvcc.IsTxnReadOnly(req), true
	}
	return 0, false, false
}

// checkRealTime checks that no call sees a revision older than one that a
// call finished before it started, and that a write gets a newer one.
func checkRealTime(h []*Record) error {
	var ops []*Record
	for _, r := range h {
		if _, _, ok := revision(r); ok {
			ops = append(ops, r)
		}
	}
	byReturn := slices.Clone(ops)
	slices.SortStableFunc(byReturn, func(a, b *Record) int { return cmp.Compare(a.Return, b.Return) })
	slices.SortStableFunc(ops, func(a, b *Record) int { return cmp.Compare(a.Call, b.Call) })
	var maxRev int64
	var maxBy *Record
	j := 0
	seen := map[int64]*Record{}
	for _, b := range ops {
		for j < len(byReturn) && byReturn[j].Return < b.Call {
			if rev, _, _ := revision(byReturn[j]); rev > maxRev {
				maxRev, maxBy = rev, byReturn[j]
			}
			j++
		}
		rev, write, _ := revision(b)
		if rev < maxRev || (write && rev == maxRev) {
			return fmt.Errorf("client %d call at %d ms got revision %d, but client %d's call returned revision %d at %d ms, before it started",
				b.Client, b.Call/Millisecond, rev, maxBy.Client, maxRev, maxBy.Return/Millisecond)
		}
		if write {
			if other, dup := seen[rev]; dup {
				return fmt.Errorf("clients %d and %d both wrote at revision %d", other.Client, b.Client, rev)
			}
			seen[rev] = b
		}
	}
	return nil
}

// putValues returns the values that a write request puts. Every value the
// clients write is unique to one call.
func putValues(op proto.Message) []string {
	switch op := op.(type) {
	case *pb.PutRequest:
		return []string{string(op.Value)}
	case *pb.TxnRequest:
		var out []string
		for _, o := range slices.Concat(op.Success, op.Failure) {
			if p := o.GetRequestPut(); p != nil {
				out = append(out, string(p.Value))
			}
		}
		return out
	}
	return nil
}

// checkLog matches the committed log against the calls by their unique
// values.
func checkLog(res *Result) error {
	calls := map[string]*Record{}
	for _, r := range res.History {
		for _, v := range putValues(r.req) {
			calls[v] = r
		}
	}
	committed := map[string]bool{}
	for i, op := range res.Log {
		for _, v := range putValues(op) {
			if committed[v] {
				return fmt.Errorf("the write of %q is in the log twice, again at write %d", v, i)
			}
			committed[v] = true
			r, ok := calls[v]
			if !ok {
				return fmt.Errorf("write %d of the log puts %q, which no call wrote", i, v)
			}
			// A write the state machine rejected when it applied, such as
			// a put whose lease expired meanwhile, is in the log with its
			// answer. Any other certain failure must not be.
			if r.Err != nil && !r.Ambiguous && !isRejection(r.Err) {
				return fmt.Errorf("client %d's call at %d ms failed for certain with %w, but it is in the log",
					r.Client, r.Call/Millisecond, r.Err)
			}
		}
	}
	for _, r := range res.History {
		if r.Err != nil {
			continue
		}
		for _, v := range putValues(r.req) {
			if !committed[v] {
				return fmt.Errorf("client %d's acknowledged call at %d ms putting %q is not in the log",
					r.Client, r.Call/Millisecond, v)
			}
		}
	}
	return nil
}

// isRejection reports whether an error is one of the answers the
// simulated clients can get for a request the cluster handled: a compacted
// or future revision, or a lease that is gone. Anything else, such as "not
// leader", is unavailability.
func isRejection(err error) bool {
	return errors.Is(err, rpctypes.ErrGRPCCompacted) || errors.Is(err, rpctypes.ErrGRPCFutureRev) ||
		errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) || errors.Is(err, rpctypes.ErrGRPCNoSpace)
}

// replay applies the committed log to a fresh state, without compactions,
// so every revision stays readable. It returns the log as the checkers
// read it: an expiry that applied becomes the LeaseRevoke it amounts to,
// and one that no longer applied, because a keepalive came first, is left
// out, as are alarms and the writes the NOSPACE alarm refused. With it
// come the revisions right after each of its entries.
func replay(log []proto.Message) (*mvcc.Store, []proto.Message, []int64, error) {
	st := core.NewState(core.MinLeaseTTL(core.DefaultConfig(1, nil)), &mvcc.Heap{})
	var out []proto.Message
	var revs []int64
	for i, op := range log {
		e, expiry := op.(*pb.LeaseCheckpoint)
		if _, ok := op.(*pb.CompactionRequest); ok {
			out, revs = append(out, op), append(revs, st.Store().Rev())
			continue
		}
		resp, err := st.Apply(op)
		if err != nil && !isRejection(err) {
			return nil, nil, nil, fmt.Errorf("replaying log write %d: %w", i, err)
		}
		_, alarm := op.(*pb.AlarmRequest)
		switch {
		case alarm, errors.Is(err, rpctypes.ErrGRPCNoSpace):
			// The checkers know no alarms: an alarm, and a write the
			// NOSPACE alarm refused, which changed nothing, stay out of
			// their log.
		case !expiry:
			out, revs = append(out, op), append(revs, st.Store().Rev())
		case resp != nil:
			out, revs = append(out, &pb.LeaseRevokeRequest{ID: e.ID}), append(revs, st.Store().Rev())
		}
	}
	return st.Store(), out, revs, nil
}

// revokesKeepPromises checks each lease revoke that took effect, at
// revokedAt (when a node first applied it), against what the clients
// were told:
//   - a keepalive called after it finds no lease: its entry comes after
//     the revoke's in the log;
//   - a grant or keepalive called before it and answered alive with a TTL
//     had that TTL run out by the revoke, less the clock envelope of
//     driftPerMille, unless a client asked for the revoke.
//
// leasesExpire checks when a node decided to expire a lease; this checks
// what the cluster did, whichever node decided it and whenever, so that a
// keepalive answered alive after the decision, whose revoke then commits,
// fails here. A lease granted again after its revoke, with the same ID,
// is left out of the first rule.
func revokesKeepPromises(history []*Record, revokedAt map[int64]int64, driftPerMille int64) error {
	granted := map[int64]int64{}
	asked := map[int64]bool{}
	for _, r := range history {
		if g, ok := r.Resp.(*pb.LeaseGrantResponse); ok && g != nil {
			granted[g.ID] = max(granted[g.ID], r.Call)
		}
		if req, ok := r.req.(*pb.LeaseRevokeRequest); ok {
			asked[req.ID] = true
		}
	}
	for _, r := range history {
		var id, ttl int64
		keepAlive := false
		switch resp := r.Resp.(type) {
		case *pb.LeaseGrantResponse:
			if resp == nil {
				continue
			}
			id, ttl = resp.ID, resp.TTL
		case *pb.LeaseKeepAliveResponse:
			if resp == nil {
				continue
			}
			id, ttl, keepAlive = resp.ID, resp.TTL, true
		default:
			continue
		}
		at, revoked := revokedAt[id]
		if !revoked || ttl <= 0 {
			continue
		}
		if keepAlive && r.Call > at && granted[id] < at {
			return fmt.Errorf("lease %d answered a keepalive called at %d ms with TTL %d s, after a node applied its revoke at %d ms",
				id, r.Call/Millisecond, ttl, at/Millisecond)
		}
		early := ttl*Second*driftPerMille/1000 + TickTime
		if driftPerMille == 0 {
			early = 0
		}
		if r.Call < at && !asked[id] && at < r.Call+ttl*Second-early {
			return fmt.Errorf("lease %d was revoked at %d ms, but a call at %d ms was answered with TTL %d s",
				id, at/Millisecond, r.Call/Millisecond, ttl)
		}
	}
	return nil
}

// leaseGrace is how long after its last grant or keepalive a lease of
// ttl seconds may still live: its TTL, plus a leader change, after which
// the new leader gives the full TTL again.
func leaseGrace(ttl int64) int64 { return 2 * ttl * Second }

// maxLeaseTTL is the longest TTL, in seconds, the clients ask for.
func (c Config) maxLeaseTTL() int64 {
	if c.Objects > 0 {
		return kubeLeaseTTL
	}
	return maxTTL
}

// checkLeasesExpire checks the leases of the run (leasesExpire) and
// counts the revokes in the log, expiries included, and the lease
// checkpoints.
func (s *Sim) checkLeasesExpire(res *Result, alive []int64) error {
	if err := leasesExpire(res.History, s.expiredAt, s.expiredOrder, alive, s.endAt, s.cfg.Faults.DriftPerMille); err != nil {
		return err
	}
	if err := revokesKeepPromises(res.History, s.revokedAt, s.cfg.Faults.DriftPerMille); err != nil {
		return err
	}
	for _, op := range res.Log {
		switch op.(type) {
		case *pb.LeaseRevokeRequest:
			res.Stats.Revokes++
		case *pb.LeaseCheckpointRequest:
			res.Stats.LeaseCheckpoints++
		}
	}
	return nil
}

// leasesExpire requires every lease that still lives at end to have been
// granted or kept alive within the leaseGrace of its TTL, so leases do
// expire, and no lease to expire, at expiredAt, before its TTL ran out
// since its last successful grant or renewal that started before then,
// less the clock envelope of driftPerMille. A renewal after the expiry
// does not count: the revoke a leader proposed may never commit, as when
// it loses its term, and the next leader starts the lease's TTL over.
func leasesExpire(history []*Record, expiredAt map[int64]int64, order, alive []int64, end, driftPerMille int64) error {
	renewed := map[int64]int64{}
	ttl := map[int64]int64{}
	for _, r := range history {
		// A failed call through gRPC (wl's clients) records a nil
		// response of its type.
		switch resp := r.Resp.(type) {
		case *pb.LeaseGrantResponse:
			if resp == nil {
				continue
			}
			ttl[resp.ID] = resp.TTL
			if at, ok := expiredAt[resp.ID]; !ok || r.Call < at {
				renewed[resp.ID] = max(renewed[resp.ID], r.Call)
			}
		case *pb.LeaseKeepAliveResponse:
			if resp == nil || resp.TTL <= 0 {
				continue
			}
			if at, ok := expiredAt[resp.ID]; !ok || r.Call < at {
				renewed[resp.ID] = max(renewed[resp.ID], r.Call)
			}
		}
	}
	for _, id := range order {
		at := expiredAt[id]
		// The declared clock envelope: the leader's clock may run fast by
		// DriftPerMille, and its ticks fall anywhere in a tick of real
		// time. Go tickers never jump several ticks at once.
		early := ttl[id]*Second*driftPerMille/1000 + TickTime
		if driftPerMille == 0 {
			early = 0
		}
		if t, ok := renewed[id]; ok && at < t+ttl[id]*Second-early {
			return fmt.Errorf("lease %d with TTL %d s expired at %d ms, but it was granted or renewed at %d ms",
				id, ttl[id], at/Millisecond, t/Millisecond)
		}
	}
	last := map[int64]int64{}
	for _, r := range history {
		switch req := r.req.(type) {
		case *pb.LeaseGrantRequest:
			if g, ok := r.Resp.(*pb.LeaseGrantResponse); ok && g != nil {
				last[g.ID] = max(last[g.ID], r.Call)
			}
		case *pb.LeaseKeepAliveRequest:
			last[req.ID] = max(last[req.ID], r.Call)
		}
	}
	for _, id := range alive {
		t, known := last[id]
		// A lease from an ambiguous grant has an ID no client learned.
		if known && t < end-leaseGrace(ttl[id]) {
			return fmt.Errorf("lease %d with TTL %d s was last granted or kept alive at %d ms but still lives at %d ms", id, ttl[id], t/Millisecond, end/Millisecond)
		}
	}
	return nil
}

// checkWatchersFreed requires every node to hold exactly the watchers of
// the streams that are still open on it, so closed streams leak nothing.
func (s *Sim) checkWatchersFreed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.live() {
		open := 0
		for _, w := range s.watchers {
			if w.open && w.proc == n.proc {
				open++
			}
		}
		if got := n.proc.wl.turn.Watchers; got != open {
			return fmt.Errorf("node %d holds %d watchers for %d open watch streams", n.id, got, open)
		}
	}
	return nil
}

// liveFrom gives the cluster LiveAfter to recover after the faults end,
// plus 2 s with a node fenced, or uses Config.LiveFrom. A stopped node
// restarts within MaxDown and then gets the same recovery time.
func (s *Sim) liveFrom() int64 {
	if s.cfg.LiveFrom > 0 {
		return s.cfg.LiveFrom
	}
	t := s.recovered()
	if s.lateStopAt > 0 {
		t = max(t, s.lateStopAt+max(s.cfg.Faults.MaxDown, Second)+s.cfg.LiveAfter)
	}
	return t
}

// recovered is when the cluster must serve again after the faults,
// unless a node stopped by then (liveFrom).
func (s *Sim) recovered() int64 {
	// A gray fsync in flight at healing finishes when it finishes, up to
	// 3 s later. Until then its node's term and vote changes and the peer
	// messages behind them are held, so a higher term can reach the new
	// leader late and cost another election (seed 745672193 before #106). Recovery
	// counts from the later of healing and the last such fsync.
	t := max(s.healAt, s.grayHealedAt) + s.cfg.LiveAfter
	if slices.ContainsFunc(s.nodes, (*node).out) {
		// With a node fenced, every election needs both others. One with
		// the longer log and one with the higher term reject each other's
		// pre-votes until the first learns the term from a rejection: one
		// more election timeout, plus one to win (seed 714720575).
		t += 2 * Second
	}
	return t
}

// removalDuring reports whether a call from to to overlaps the
// LiveAfter after the removal of a member.
func (s *Sim) removalDuring(from, to int64) bool {
	at := s.membership.removedAt
	return at > 0 && from <= at+s.cfg.LiveAfter && to >= at
}

// extendLiveness keeps clients running for at least 3 s under the rule.
// Moving its start must not remove the calls that check recovery. The
// lock is held.
func (s *Sim) extendLiveness() {
	if stop := s.liveFrom() + 3*Second; stop > s.stopAt {
		s.endAt += stop - s.stopAt
		s.stopAt = stop
	}
}

// checkRecoverable recovers the durable state of every node that runs at
// the end, as a crash then would leave it: the log must open and reach
// what the node reported durable. A disk that only a later restart would
// read is checked too, so a bad record or root shows in the run that wrote
// it (#182).
func (s *Sim) checkRecoverable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if n.proc == nil || n.fenced {
			continue
		}
		floor := n.floor()
		d, err := s.logOn(n)
		if err != nil && n.rotted && n.corrupted && refusesDamage(err) {
			// Its next start refuses the rotted log, as it must.
			continue
		}
		if err != nil {
			return fmt.Errorf("node %d cannot recover its log at the end: %w", n.id, err)
		}
		if err := d.close(); err != nil {
			return err
		}
		if d.last < floor {
			return fmt.Errorf("node %d recovers entries to %d at the end, but reported %d durable", n.id, d.last, floor)
		}
	}
	return nil
}
