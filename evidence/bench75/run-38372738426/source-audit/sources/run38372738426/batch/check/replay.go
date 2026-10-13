package check

import (
	"errors"
	"fmt"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

// replay is the committed log applied through the model. It is the reference
// that the watch, durability and revision checkers compare client
// observations against. It does not use any tigerstore code.
type replay struct {
	// outcomes has the model's outcome of each log entry.
	outcomes []outcome
	// events are all watch events in commit order.
	events []history.Event
	// versions holds, per key, every put and delete in revision order. A
	// delete is a KeyValue with Version 0.
	versions map[string][]history.KeyValue
	keys     []string
	// rev is the revision after the whole log.
	rev int64
	// compactions are the revisions compacted, in log order.
	compactions []int64
}

var errNoLog = errors.New("history has no committed log")

func newReplay(log []history.Request) (*replay, error) {
	if len(log) == 0 {
		return nil, errNoLog
	}
	keys := keysOf(log)
	s := newState(keys)
	r := &replay{versions: map[string][]history.KeyValue{}, keys: keys}
	for i, req := range log {
		if req.LeaseGrant != nil && req.LeaseGrant.ID == 0 {
			return nil, fmt.Errorf("log entry %d grants a lease without its ID", i)
		}
		o := s.step(req, true)
		r.outcomes = append(r.outcomes, o)
		for _, e := range o.events {
			// A delete event's KV has Version 0, which marks the delete.
			r.versions[e.KV.Key] = append(r.versions[e.KV.Key], e.KV)
		}
		r.events = append(r.events, o.events...)
		if req.Compact != nil && o.err == nil {
			r.compactions = append(r.compactions, req.Compact.Revision)
		}
	}
	r.rev = s.rev
	return r, nil
}

// at returns the key as it was at revision rev.
func (r *replay) at(key string, rev int64) (history.KeyValue, bool) {
	vs := r.versions[key]
	i, _ := slices.BinarySearchFunc(vs, rev+1, func(kv history.KeyValue, rev int64) int {
		switch {
		case kv.ModRevision < rev:
			return -1
		case kv.ModRevision > rev:
			return 1
		}
		return 0
	})
	if i == 0 || vs[i-1].Version == 0 {
		return history.KeyValue{}, false
	}
	return vs[i-1], true
}

// read answers a range at revision rev, which must not be above r.rev.
func (r *replay) read(q *history.RangeRequest, rev int64) *history.RangeResponse {
	s := state{keys: r.keys}
	lo, hi := s.span(q.Key, q.End)
	resp := &history.RangeResponse{}
	for _, key := range r.keys[lo:hi] {
		kv, ok := r.at(key, rev)
		if !ok {
			continue
		}
		resp.Count++
		if q.CountOnly {
			continue
		}
		if q.Limit > 0 && int64(len(resp.KVs)) == q.Limit {
			resp.More = true
			continue
		}
		if q.KeysOnly {
			kv.Value = ""
		}
		resp.KVs = append(resp.KVs, kv)
	}
	return resp
}
