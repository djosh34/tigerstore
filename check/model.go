// Package check holds the checkers that convict a history.History.
//
// The model in this file is a port of etcd v3.7.2's robustness model
// (tests/robustness/model/deterministic.go), extended where the port needs
// more than upstream checks: create revisions, leases on keys, range deletes,
// every compare target and result, count-only and limited ranges, and the
// errors a request can get in a given state.
package check

import (
	"slices"
	"strings"

	"github.com/djosh34/tigerstore/history"
)

// The errors the model produces, with etcd v3.7.2's codes and messages.
var (
	errCompacted     = &history.Error{Code: "OutOfRange", Message: "etcdserver: mvcc: required revision has been compacted"}
	errFutureRev     = &history.Error{Code: "OutOfRange", Message: "etcdserver: mvcc: required revision is a future revision"}
	errLeaseNotFound = &history.Error{Code: "NotFound", Message: "etcdserver: requested lease not found"}
	errLeaseExists   = &history.Error{Code: "FailedPrecondition", Message: "etcdserver: lease already exists"}
)

// errUnsupported marks a request the model can not decide. Linearizable
// reports it as a checker limit, not as a server bug.
var errUnsupported = &history.Error{Code: "Unimplemented", Message: "check: historical range inside a txn is not modelled"}

// modelErrors are the semantic errors the model can decide. A server that
// returns one of them must agree with the model.
var modelErrors = []*history.Error{errCompacted, errFutureRev, errLeaseNotFound, errLeaseExists}

func isModelError(e *history.Error) bool {
	return slices.ContainsFunc(modelErrors, func(m *history.Error) bool { return sameError(m, e) })
}

func sameError(a, b *history.Error) bool {
	return a.Code == b.Code && a.Message == b.Message
}

// state is the store at one revision. Keys is the sorted set of every key the
// checked history can create, shared by all states. KVs is aligned with Keys;
// a zero Version means the key does not exist.
type state struct {
	keys      []string
	kvs       []history.KeyValue
	leases    []int64 // sorted
	rev       int64
	compacted int64
	// sum is a fingerprint of the state, kept only by the Porcupine
	// model, where comparing sets of states is the hot path. It must be
	// recomputed after every change: same and states.equal rely on it,
	// and a stale sum could make two different sets compare equal.
	sum uint64
}

// fingerprint hashes the state FNV-style, one 64-bit word at a time.
// Values count only by length, so equal fingerprints still need equal to
// confirm.
func (s *state) fingerprint() uint64 {
	h := uint64(14695981039346656037)
	mix := func(v int64) {
		h = (h ^ uint64(v)) * 1099511628211
	}
	mix(s.rev)
	mix(s.compacted)
	for _, l := range s.leases {
		mix(l)
	}
	for _, kv := range s.kvs {
		mix(kv.Version)
		mix(kv.ModRevision)
		mix(kv.CreateRevision)
		mix(kv.Lease)
		mix(int64(len(kv.Value)))
	}
	return h
}

func newState(keys []string) *state {
	return &state{
		keys: keys,
		kvs:  make([]history.KeyValue, len(keys)),
		rev:  1,
		// etcd lets a client compact revision 0 once, as upstream's model
		// notes, so nothing is compacted yet.
		compacted: -1,
	}
}

func (s *state) clone() *state {
	c := *s
	c.kvs = slices.Clone(s.kvs)
	c.leases = slices.Clone(s.leases)
	return &c
}

func (s *state) equal(o *state) bool {
	return s.rev == o.rev && s.compacted == o.compacted && slices.Equal(s.kvs, o.kvs) && slices.Equal(s.leases, o.leases)
}

// index returns the position of key in s.keys, or -1.
func (s *state) index(key string) int {
	i, ok := slices.BinarySearch(s.keys, key)
	if !ok {
		return -1
	}
	return i
}

func (s *state) get(key string) (history.KeyValue, bool) {
	i := s.index(key)
	if i < 0 || s.kvs[i].Version == 0 {
		return history.KeyValue{}, false
	}
	return s.kvs[i], true
}

func (s *state) hasLease(id int64) bool {
	_, ok := slices.BinarySearch(s.leases, id)
	return ok
}

// span returns the positions in s.keys that a range selects.
func (s *state) span(key, end string) (int, int) {
	switch end {
	case "":
		i := s.index(key)
		if i < 0 {
			return 0, 0
		}
		return i, i + 1
	case history.AllKeys:
		lo, _ := slices.BinarySearch(s.keys, key)
		return lo, len(s.keys)
	default:
		lo, _ := slices.BinarySearch(s.keys, key)
		hi, _ := slices.BinarySearch(s.keys, end)
		return lo, max(lo, hi)
	}
}

// outcome is what the model says a request returns in some state.
type outcome struct {
	resp *history.Response
	err  *history.Error
	// historical is set for a successful read below the current revision.
	// The model keeps no old versions, so only the header revision of such
	// a response is checked here. The replay checks the rest.
	historical bool
	// events are the watch events the request caused, filled only when the
	// caller asks for them.
	events []history.Event
}

// step applies req to s in place and returns its outcome. A request that
// fails leaves s unchanged. With withEvents the outcome lists the events the
// request caused, with their previous values.
func (s *state) step(req history.Request, withEvents bool) outcome {
	switch {
	case req.Range != nil:
		return s.stepRange(req.Range)
	case req.Put != nil:
		o := s.stepTxn(&history.TxnRequest{Success: []history.TxnOp{{Put: req.Put}}}, withEvents)
		if o.resp != nil {
			o.resp = &history.Response{Revision: o.resp.Revision, Put: o.resp.Txn.Results[0].Put}
		}
		return o
	case req.Delete != nil:
		o := s.stepTxn(&history.TxnRequest{Success: []history.TxnOp{{Delete: req.Delete}}}, withEvents)
		if o.resp != nil {
			o.resp = &history.Response{Revision: o.resp.Revision, Delete: o.resp.Txn.Results[0].Delete}
		}
		return o
	case req.Txn != nil:
		return s.stepTxn(req.Txn, withEvents)
	case req.LeaseGrant != nil:
		return s.stepLeaseGrant(req.LeaseGrant)
	case req.LeaseRevoke != nil:
		return s.stepLeaseRevoke(req.LeaseRevoke, withEvents)
	default:
		return s.stepCompact(req.Compact)
	}
}

func (s *state) stepRange(r *history.RangeRequest) outcome {
	if err := s.checkRevision(r.Revision); err != nil {
		return outcome{err: err}
	}
	if r.Revision > 0 && r.Revision < s.rev {
		return outcome{resp: &history.Response{Revision: s.rev, Range: &history.RangeResponse{}}, historical: true}
	}
	return outcome{resp: &history.Response{Revision: s.rev, Range: s.read(r)}}
}

// checkRevision returns the error of a read at rev. etcd reads at the current
// revision when rev is zero or less.
func (s *state) checkRevision(rev int64) *history.Error {
	switch {
	case rev > s.rev:
		return errFutureRev
	case rev > 0 && rev < s.compacted:
		return errCompacted
	}
	return nil
}

// read answers a range at the current revision.
func (s *state) read(r *history.RangeRequest) *history.RangeResponse {
	resp := &history.RangeResponse{}
	lo, hi := s.span(r.Key, r.End)
	for i := lo; i < hi; i++ {
		kv := s.kvs[i]
		if kv.Version == 0 {
			continue
		}
		resp.Count++
		if r.CountOnly {
			continue
		}
		if r.Limit > 0 && int64(len(resp.KVs)) == r.Limit {
			resp.More = true
			continue
		}
		if r.KeysOnly {
			kv.Value = ""
		}
		resp.KVs = append(resp.KVs, kv)
	}
	return resp
}

func (s *state) compare(c history.Compare) bool {
	kv, ok := s.get(c.Key)
	return compareKV(kv, ok, c)
}

// compareKV evaluates c on kv, which exists if ok.
func compareKV(kv history.KeyValue, ok bool, c history.Compare) bool {
	if c.Target == history.TargetValue {
		// etcd fails every value compare on a missing key, since an
		// empty value and no value look the same on the wire.
		if !ok {
			return false
		}
		return result(strings.Compare(kv.Value, c.Value), c.Result)
	}
	var n int64
	switch c.Target {
	case history.TargetVersion:
		n = kv.Version
	case history.TargetCreate:
		n = kv.CreateRevision
	case history.TargetMod:
		n = kv.ModRevision
	case history.TargetLease:
		n = kv.Lease
	case history.TargetValue:
	}
	switch {
	case n < c.Number:
		return result(-1, c.Result)
	case n > c.Number:
		return result(1, c.Result)
	default:
		return result(0, c.Result)
	}
}

func result(cmp int, r history.Result) bool {
	switch r {
	case history.Equal:
		return cmp == 0
	case history.Greater:
		return cmp > 0
	case history.Less:
		return cmp < 0
	case history.NotEqual:
		return cmp != 0
	}
	return false
}

func (s *state) stepTxn(t *history.TxnRequest, withEvents bool) outcome {
	ok := true
	for _, c := range t.Compare {
		if !s.compare(c) {
			ok = false
			break
		}
	}
	ops := t.Success
	if !ok {
		ops = t.Failure
	}
	// A put with a missing lease fails the whole txn before anything is
	// written, as etcd checks leases before applying.
	for _, op := range ops {
		if op.Put != nil && op.Put.Lease != 0 && !s.hasLease(op.Put.Lease) {
			return outcome{err: errLeaseNotFound}
		}
		if op.Range == nil || op.Range.Revision <= 0 || op.Range.Revision == s.rev {
			continue
		}
		if err := s.checkRevision(op.Range.Revision); err != nil {
			return outcome{err: err}
		}
		// Other historical reads inside a txn are not modelled.
		return outcome{err: errUnsupported}
	}
	// A read at the txn's own start revision sees the store before the
	// txn, as etcd keeps a positive requested revision.
	var before *state
	for _, op := range ops {
		if op.Range != nil && op.Range.Revision > 0 {
			before = s.clone()
			break
		}
	}
	next := s.rev + 1
	changed := false
	var events []history.Event
	// An event's previous value is the key at the revision before the
	// txn, as etcd reads it, not what an earlier op of the txn wrote.
	was := map[string]history.KeyValue{}
	prior := func(key string, cur history.KeyValue) history.KeyValue {
		if kv, ok := was[key]; ok {
			return kv
		}
		was[key] = cur
		return cur
	}
	resp := &history.TxnResponse{Succeeded: ok, Results: make([]history.TxnOpResponse, len(ops))}
	for i, op := range ops {
		switch {
		case op.Range != nil && op.Range.Revision > 0:
			resp.Results[i].Range = before.read(op.Range)
		case op.Range != nil:
			// Reads at the current revision see the txn's earlier
			// writes.
			resp.Results[i].Range = s.read(op.Range)
		case op.Put != nil:
			p := op.Put
			j := s.index(p.Key)
			prev := s.kvs[j]
			kv := history.KeyValue{Key: p.Key, Value: p.Value, CreateRevision: next, ModRevision: next, Version: 1, Lease: p.Lease}
			if prev.Version != 0 {
				kv.CreateRevision = prev.CreateRevision
				kv.Version = prev.Version + 1
			}
			s.kvs[j] = kv
			changed = true
			pr := &history.PutResponse{}
			if p.PrevKV && prev.Version != 0 {
				pr.PrevKV = &prev
			}
			resp.Results[i].Put = pr
			if withEvents {
				events = append(events, putEvent(kv, prior(p.Key, prev)))
			}
		default:
			d := op.Delete
			dr := &history.DeleteResponse{}
			lo, hi := s.span(d.Key, d.End)
			for j := lo; j < hi; j++ {
				prev := s.kvs[j]
				if prev.Version == 0 {
					continue
				}
				s.kvs[j] = history.KeyValue{}
				changed = true
				dr.Deleted++
				if d.PrevKV {
					dr.PrevKVs = append(dr.PrevKVs, prev)
				}
				if withEvents {
					events = append(events, deleteEvent(prev.Key, prior(prev.Key, prev), next))
				}
			}
			resp.Results[i].Delete = dr
		}
	}
	if changed {
		s.rev = next
	}
	return outcome{resp: &history.Response{Revision: s.rev, Txn: resp}, events: events}
}

// putEvent is the put of kv; prev is the key at the revision before. As in
// etcd, a put that creates its key has no previous value.
func putEvent(kv, prev history.KeyValue) history.Event {
	e := history.Event{Type: history.EventPut, KV: kv}
	if prev.Version != 0 && kv.CreateRevision != kv.ModRevision {
		e.PrevKV = &prev
	}
	return e
}

// deleteEvent is the delete of key at rev; prev is the key at the
// revision before, with no previous value if it did not exist then.
func deleteEvent(key string, prev history.KeyValue, rev int64) history.Event {
	e := history.Event{Type: history.EventDelete, KV: history.KeyValue{Key: key, ModRevision: rev}}
	if prev.Version != 0 {
		e.PrevKV = &prev
	}
	return e
}

func (s *state) stepLeaseGrant(g *history.LeaseGrantRequest) outcome {
	if s.hasLease(g.ID) {
		return outcome{err: errLeaseExists}
	}
	// ID zero is a grant whose ID nobody learned. No key can use it, so it
	// changes nothing the model tracks.
	if g.ID != 0 {
		i, _ := slices.BinarySearch(s.leases, g.ID)
		s.leases = slices.Insert(s.leases, i, g.ID)
	}
	return outcome{resp: &history.Response{Revision: s.rev, LeaseGrant: &history.LeaseGrantResponse{ID: g.ID, TTL: g.TTL}}}
}

func (s *state) stepLeaseRevoke(r *history.LeaseRevokeRequest, withEvents bool) outcome {
	i, ok := slices.BinarySearch(s.leases, r.ID)
	if !ok {
		return outcome{err: errLeaseNotFound}
	}
	s.leases = slices.Delete(s.leases, i, i+1)
	next := s.rev + 1
	changed := false
	var events []history.Event
	for j, kv := range s.kvs {
		if kv.Version == 0 || kv.Lease != r.ID {
			continue
		}
		s.kvs[j] = history.KeyValue{}
		changed = true
		if withEvents {
			events = append(events, deleteEvent(kv.Key, kv, next))
		}
	}
	if changed {
		s.rev = next
	}
	return outcome{resp: &history.Response{Revision: s.rev, LeaseRevoke: &history.LeaseRevokeResponse{}}, events: events}
}

func (s *state) stepCompact(c *history.CompactRequest) outcome {
	if c.Revision <= s.compacted {
		return outcome{err: errCompacted}
	}
	if c.Revision > s.rev {
		return outcome{err: errFutureRev}
	}
	s.compacted = c.Revision
	return outcome{resp: &history.Response{Revision: s.rev, Compact: &history.CompactResponse{}}}
}
