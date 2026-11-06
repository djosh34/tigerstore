package check

import (
	"errors"
	"fmt"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

// replay is the committed log applied through the model. It is the reference
// that the watch, durability and revision checkers compare client
// observations against. It does not use any tigerstore code.
type replay struct {
	// outcomes has the model's outcome of each log entry.
	outcomes []outcome
	// events are all watch events in commit order.
	events   []history.Event
	versions versions
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
	r := &replay{versions: versions{}, keys: keys}
	for i, req := range log {
		if req.LeaseGrant != nil && req.LeaseGrant.ID == 0 {
			return nil, fmt.Errorf("log entry %d grants a lease without its ID", i)
		}
		o := s.step(req, true)
		r.outcomes = append(r.outcomes, o)
		r.versions.add(o.events)
		r.events = append(r.events, o.events...)
		if req.Compact != nil && o.err == nil {
			r.compactions = append(r.compactions, req.Compact.Revision)
		}
	}
	r.rev = s.rev
	return r, nil
}

// at returns the key as it was at revision rev.
func (r *replay) at(key string, rev int64) (history.KeyValue, bool) { return r.versions.at(key, rev) }

// read answers a range at revision rev, which must not be above r.rev.
func (r *replay) read(q *history.RangeRequest, rev int64) *history.RangeResponse {
	return r.versions.read(r.keys, q, rev)
}

// versions holds, per key, every put and delete in revision order. A
// delete is a KeyValue with Version 0.
type versions map[string]*keyVersions

// keyVersions are a key's versions and where its last lookup ended. The
// witness reads at the live revision, and at past ones mostly in their
// order, so a lookup starts there and gallops out: a binary search of
// every version grew with the history's length (#499). revs are the
// versions' revisions, which a lookup searches: 8 bytes each beside the
// versions' 64, as a lookup that reached into the versions themselves
// cost more per op once a busy history's outgrew the cache (#499).
type keyVersions struct {
	kvs  []history.KeyValue
	revs []int64
	at   int
}

// add notes what events changed. A delete event's KV has Version 0, which
// marks the delete.
func (v versions) add(events []history.Event) {
	for _, e := range events {
		vs := v[e.KV.Key]
		if vs == nil {
			vs = &keyVersions{}
			v[e.KV.Key] = vs
		}
		vs.kvs = append(vs.kvs, e.KV)
		vs.revs = append(vs.revs, e.KV.ModRevision)
	}
}

// reserve makes room for n versions of each key of writes, so that adding
// them copies none (#499).
func (v versions) reserve(writes map[string]int) {
	for key, n := range writes {
		if v[key] == nil {
			v[key] = &keyVersions{kvs: make([]history.KeyValue, 0, n), revs: make([]int64, 0, n)}
		}
	}
}

// drop takes the newest version of key off.
func (v versions) drop(key string) {
	vs := v[key]
	vs.kvs = vs.kvs[:len(vs.kvs)-1]
	vs.revs = vs.revs[:len(vs.revs)-1]
}

// at returns the key as it was at revision rev.
func (v versions) at(key string, rev int64) (history.KeyValue, bool) {
	k := v[key]
	if k == nil {
		return history.KeyValue{}, false
	}
	i := len(k.revs)
	if i > 0 && k.revs[i-1] > rev {
		i = firstAbove(k.revs, rev, min(k.at, i))
	}
	k.at = i
	if i == 0 || k.kvs[i-1].Version == 0 {
		return history.KeyValue{}, false
	}
	return k.kvs[i-1], true
}

// firstAbove returns the first place in revs, which are in order, whose
// revision is above rev, searching out from place from by steps that
// double, so that its cost grows with how far the answer is from from,
// not with the length of revs.
func firstAbove(revs []int64, rev int64, from int) int {
	lo, hi := 0, len(revs)
	if from < len(revs) && revs[from] <= rev {
		// Up: the answer is above from.
		lo = from + 1
		for step := 1; lo < hi; step *= 2 {
			p := min(from+step, hi)
			if p == hi || revs[p] > rev {
				hi = p
				break
			}
			lo = p + 1
		}
	} else {
		// Down: the answer is at from or below it.
		hi = from
		for step := 1; lo < hi; step *= 2 {
			p := max(from-step, lo)
			if revs[p] <= rev {
				lo = p + 1
				break
			}
			hi = p
			if p == 0 {
				break
			}
		}
	}
	return lo + sort.Search(hi-lo, func(j int) bool { return revs[lo+j] > rev })
}

// read answers a range over keys at revision rev, which must not be above
// the newest revision added.
func (v versions) read(keys []string, q *history.RangeRequest, rev int64) *history.RangeResponse {
	s := state{keys: keys}
	lo, hi := s.span(q.Key, q.End)
	resp := &history.RangeResponse{}
	for _, key := range keys[lo:hi] {
		kv, ok := v.at(key, rev)
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
