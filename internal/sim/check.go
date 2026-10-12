package sim

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
)

// check runs the simulator's own rules on a finished run. The checkers in
// the history package are the independent oracle; these rules use the
// final state of the cluster and catch most bugs cheaply:
//
//   - every node is up and has applied the same log to the same state;
//   - every successful read returns exactly what the final store holds at
//     the read's revision;
//   - every successful write is in the final store at its revision, and
//     its previous values match the revision before;
//   - revisions follow real time: a call never sees a revision older than
//     one returned before it started, and a write gets a newer one;
//   - every call that starts after the cluster healed succeeds.
func (s *Sim) check(res *Result) error {
	final, err := s.converged()
	if err != nil {
		return err
	}
	ents, err := s.nodes[0].core.AppliedEntries()
	if err != nil {
		return err
	}
	for _, e := range ents {
		if len(e.GetData()) == 0 {
			continue
		}
		if err := core.DecodeBatch(e.GetData(), func(_, _, _ uint64, op proto.Message) error {
			res.Log = append(res.Log, op)
			return nil
		}); err != nil {
			return err
		}
	}
	for _, r := range res.History {
		if r.Err != nil {
			continue
		}
		if err := checkRecord(final, r); err != nil {
			return fmt.Errorf("client %d call at %d ms on node %d: %w\nrequest: %v\nresponse: %v",
				r.Client, r.Call/Millisecond, r.Node+1, err, r.req, r.Resp)
		}
	}
	if err := checkRealTime(res.History); err != nil {
		return err
	}
	live := 0
	for _, r := range res.History {
		if r.Call < s.healAt+s.cfg.LiveAfter {
			continue
		}
		if r.Err != nil {
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

// converged checks that all nodes applied the same log to the same state
// and returns that state.
func (s *Sim) converged() (*mvcc.Store, error) {
	first := s.nodes[0]
	for _, n := range s.nodes {
		if !n.up {
			return nil, fmt.Errorf("node %d is down at the end", n.id)
		}
	}
	want, err := first.core.AppliedEntries()
	if err != nil {
		return nil, err
	}
	for _, n := range s.nodes[1:] {
		if n.core.Applied() != first.core.Applied() {
			return nil, fmt.Errorf("node %d applied %d entries, node %d applied %d", n.id, n.core.Applied(), first.id, first.core.Applied())
		}
		got, err := n.core.AppliedEntries()
		if err != nil {
			return nil, err
		}
		for i := range want {
			if got[i].GetTerm() != want[i].GetTerm() || !bytes.Equal(got[i].GetData(), want[i].GetData()) {
				return nil, fmt.Errorf("node %d and node %d applied different entries at index %d", n.id, first.id, want[i].GetIndex())
			}
		}
		if n.core.Store().Hash() != first.core.Store().Hash() {
			return nil, fmt.Errorf("node %d and node %d applied the same log to different states", n.id, first.id)
		}
	}
	return first.core.Store(), nil
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
		rev := resp.Header.Revision
		if kv, err := at(st, req.Key, rev); err != nil || kv != nil {
			return fmt.Errorf("key %q exists at revision %d after an acknowledged delete: %v %w", req.Key, rev, kv, err)
		}
		if resp.Deleted > 0 && req.PrevKv {
			if len(resp.PrevKvs) != 1 {
				return fmt.Errorf("delete of one key returned %d previous values", len(resp.PrevKvs))
			}
			return samePrev(st, req.Key, rev-1, resp.PrevKvs[0])
		}
		return nil
	case *pb.TxnRequest:
		resp, ok := r.Resp.(*pb.TxnResponse)
		if !ok {
			return errors.New("wrong response type")
		}
		return checkTxn(st, req, resp)
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
