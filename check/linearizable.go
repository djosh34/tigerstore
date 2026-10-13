package check

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/djosh34/tigerstore/history"
)

// Linearizable checks h.Ops with Porcupine against the ported etcd model, as
// etcd v3.7.2's robustness tests do (tests/robustness/model and validate).
//
// The store must be empty at the start of the history. Keep checked histories
// small and isolated, about 16 clients and 10k operations: Porcupine's search
// grows fast with concurrency and with bugs, and every other writer to the
// store must be in the history. Not finishing within timeout is a failure,
// never a pass.
//
// A lease that may expire during the history needs an op that models the
// expiry: a LeaseRevoke from its own client, called when the lease was
// granted, with an ambiguous error.
func Linearizable(h *history.History, timeout time.Duration) error {
	if err := h.Validate(); err != nil {
		return err
	}
	if timeout <= 0 {
		return fmt.Errorf("linearizability check needs a positive timeout, got %s", timeout)
	}
	ops := make([]porcupine.Operation, 0, len(h.Ops))
	var reqs []history.Request
	for i := range h.Ops {
		op := &h.Ops[i]
		in := input{req: modelRequest(op)}
		if op.Req.Range != nil && op.Req.Range.Serializable {
			continue
		}
		if op.Err != nil && op.Err.Ambiguous && !op.Req.Mutates() {
			// A read that failed tells nothing.
			continue
		}
		out := output{resp: op.Resp, err: op.Err}
		ret := op.Return
		if op.Err != nil && op.Err.Ambiguous {
			// It may take effect at any time after its call.
			ret = math.MaxInt64
		}
		ops = append(ops, porcupine.Operation{ClientId: op.Client, Input: in, Call: op.Call, Output: out, Return: ret})
		reqs = append(reqs, in.req)
	}
	keys := keysOf(reqs)

	var tripped, unsupported atomic.Bool
	m := porcupine.Model{
		Init: func() any { return states{newState(keys)} },
		Step: func(st, in, out any) (bool, any) {
			if tripped.Load() {
				return false, nil
			}
			next, unsup := st.(states).step(in.(input), out.(output))
			if unsup {
				unsupported.Store(true)
			}
			return len(next) > 0, next
		},
		Equal: func(a, b any) bool { return a.(states).equal(b.(states)) },
		DescribeOperation: func(in, out any) string {
			return fmt.Sprintf("%+v -> %s", in.(input).req, out.(output))
		},
	}
	// Porcupine does not always stop at its timeout (upstream issue 44), so
	// as etcd does, trip the model itself a little later.
	timer := time.AfterFunc(timeout*11/10, func() { tripped.Store(true) })
	res := porcupine.CheckOperationsTimeout(m, ops, timeout)
	timer.Stop()
	if tripped.Load() {
		return fmt.Errorf("linearizability check of %d operations passed its deadline of %s", len(ops), timeout)
	}
	switch res {
	case porcupine.Ok:
		return nil
	case porcupine.Illegal:
		if unsupported.Load() {
			return fmt.Errorf("history of %d operations has a historical range inside a txn, which the model does not support", len(ops))
		}
		return fmt.Errorf("history of %d operations is not linearizable", len(ops))
	case porcupine.Unknown:
		return fmt.Errorf("linearizability check of %d operations timed out after %s", len(ops), timeout)
	}
	return errors.New("porcupine returned an unknown result")
}

type input struct{ req history.Request }

type output struct {
	resp *history.Response
	err  *history.Error
}

func (o output) String() string {
	if o.err != nil {
		return fmt.Sprintf("error %+v", *o.err)
	}
	return fmt.Sprintf("%+v", *o.resp)
}

// modelRequest is the request as the model sees it. A grant whose ID the
// server picked gets the ID from its response.
func modelRequest(op *history.Op) history.Request {
	r := op.Req
	if r.LeaseGrant != nil && r.LeaseGrant.ID == 0 && op.Resp != nil {
		g := *r.LeaseGrant
		g.ID = op.Resp.LeaseGrant.ID
		r.LeaseGrant = &g
	}
	return r
}

// keysOf returns every key a put in reqs can create, sorted.
func keysOf(reqs []history.Request) []string {
	seen := map[string]bool{}
	add := func(ops []history.TxnOp) {
		for _, op := range ops {
			if op.Put != nil {
				seen[op.Put.Key] = true
			}
		}
	}
	for _, r := range reqs {
		switch {
		case r.Put != nil:
			seen[r.Put.Key] = true
		case r.Txn != nil:
			add(r.Txn.Success)
			add(r.Txn.Failure)
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// states is the set of states the store may be in, as in upstream's
// non-deterministic model: an ambiguous mutation forks every state into one
// where it took effect and one where it did not.
type states []*state

// step returns the states after one op, and whether the model met a request
// it does not support.
func (ss states) step(in input, out output) (states, bool) {
	next := make(states, 0, len(ss))
	add := func(s *state) {
		if !slices.ContainsFunc(next, s.equal) {
			next = append(next, s)
		}
	}
	unsupported := false
	for _, s := range ss {
		n := s.clone()
		o := n.step(in.req, false)
		unsupported = unsupported || o.err == errUnsupported
		switch {
		case out.err != nil && out.err.Ambiguous:
			add(s)
			add(n)
		case out.err != nil:
			// The server rejected it, so it had no effect. A rejection
			// the model does not decide (overload, size, bad arguments)
			// is always legal, since etcd checks those first. One the
			// model decides must agree with the model.
			if !isModelError(out.err) || o.err != nil && sameError(o.err, out.err) {
				add(s)
			}
		case o.err == nil && matches(o, out.resp):
			add(n)
		}
	}
	return next, unsupported
}

func (ss states) equal(o states) bool {
	if len(ss) != len(o) {
		return false
	}
	for _, s := range ss {
		if !slices.ContainsFunc(o, s.equal) {
			return false
		}
	}
	return true
}

// matches reports whether a response is the one the model computed.
func matches(o outcome, got *history.Response) bool {
	want := o.resp
	if got.Revision != want.Revision {
		return false
	}
	if o.historical {
		return true
	}
	switch {
	case want.Range != nil:
		return got.Range != nil && sameRange(want.Range, got.Range)
	case want.Put != nil:
		return got.Put != nil && samePut(want.Put, got.Put)
	case want.Delete != nil:
		return got.Delete != nil && sameDelete(want.Delete, got.Delete)
	case want.Txn != nil:
		if got.Txn == nil || got.Txn.Succeeded != want.Txn.Succeeded || len(got.Txn.Results) != len(want.Txn.Results) {
			return false
		}
		for i, w := range want.Txn.Results {
			g := got.Txn.Results[i]
			switch {
			case w.Range != nil:
				if g.Range == nil || !sameRange(w.Range, g.Range) {
					return false
				}
			case w.Put != nil:
				if g.Put == nil || !samePut(w.Put, g.Put) {
					return false
				}
			case w.Delete != nil:
				if g.Delete == nil || !sameDelete(w.Delete, g.Delete) {
					return false
				}
			}
		}
		return true
	case want.LeaseGrant != nil:
		// The server may raise a short TTL to its minimum, so only the
		// ID is checked.
		return got.LeaseGrant != nil && got.LeaseGrant.ID == want.LeaseGrant.ID
	case want.LeaseRevoke != nil:
		return got.LeaseRevoke != nil
	default:
		return got.Compact != nil
	}
}

func sameRange(a, b *history.RangeResponse) bool {
	return a.Count == b.Count && a.More == b.More && slices.Equal(a.KVs, b.KVs)
}

func samePut(a, b *history.PutResponse) bool {
	return samePtr(a.PrevKV, b.PrevKV)
}

func sameDelete(a, b *history.DeleteResponse) bool {
	return a.Deleted == b.Deleted && slices.Equal(a.PrevKVs, b.PrevKVs)
}

func samePtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
