package k8strace

import (
	"fmt"
	"slices"
)

// response takes in what a reply tells: which keys clients now know at
// which revisions, who made each revision, which leases exist, and, for a
// call sent before the window, how the store's keys changed.
func (c *converter) response(r *Record, cl *call) {
	if cl.method == MethodWatch {
		c.watchResponse(r.Resp, c.watches[r.Call])
		return
	}
	resp := r.Resp
	if resp == nil || len(cl.reqs) == 0 {
		return
	}
	cl.resps = append(cl.resps, resp)
	idx := cl.index
	before := idx == notInWindow && cl.start < c.w.Start
	if idx == notInWindow && !before {
		return
	}
	by := delivery{call: idx}
	if before {
		by.call = deliveredEarlier
	}
	if idx != notInWindow && resp.Rev != 0 {
		c.headers[resp.Rev] = idx
	}
	q := cl.reqs[len(cl.reqs)-1]
	switch {
	case q.Range != nil:
		c.saw(resp.KVs, by)
	case q.Put != nil:
		c.wrote(q.Put, resp.Rev, by, before)
	case q.Delete != nil:
		if before {
			c.deleted(q.Delete)
		}
	case q.Txn != nil:
		if idx != notInWindow {
			c.trace[idx].Failed = !resp.Succeeded
		}
		c.txnDone(q.Txn, resp, resp.Rev, by, before)
	case q.LeaseGrant != nil:
		if before {
			c.leases[resp.Lease] = lease{ttl: resp.TTL, at: r.T}
		} else {
			c.grants[resp.Lease] = idx
		}
	case q.Lease != nil && cl.method == MethodLeaseKeepAlive && before:
		if _, ok := c.leases[*q.Lease]; ok && resp.TTL > 0 {
			c.leases[*q.Lease] = lease{ttl: resp.TTL, at: r.T}
		}
	case q.Lease != nil && cl.method == MethodLeaseRevoke && before:
		delete(c.leases, *q.Lease)
		for k, kv := range c.state {
			if kv.lease == *q.Lease {
				delete(c.state, k)
			}
		}
	}
}

func (c *converter) saw(kvs []KV, by delivery) {
	for _, kv := range kvs {
		c.seen[revKey{kv.Key, kv.Mod}] = by
	}
}

func (c *converter) wrote(q *Put, rev int64, by delivery, before bool) {
	k := revKey{q.Key, rev}
	c.seen[k] = by
	if by.call >= 0 {
		c.producers[k] = by.call
	}
	if !before {
		return
	}
	old, ok := c.state[q.Key]
	if ok && old.mod > rev {
		return
	}
	kv := stateKV{mod: rev, create: rev, size: q.Size, lease: q.Lease}
	if ok {
		kv.create = old.create
		if q.IgnoreValue {
			kv.size = old.size
		}
		if q.IgnoreLease {
			kv.lease = old.lease
		}
	}
	c.state[q.Key] = kv
}

func (c *converter) deleted(q *Delete) {
	if q.End == "" {
		delete(c.state, q.Key)
		return
	}
	for k := range c.state {
		if k >= q.Key && (q.End == "\x00" || k < q.End) {
			delete(c.state, k)
		}
	}
}

func (c *converter) txnDone(q *Txn, resp *Response, rev int64, by delivery, before bool) {
	ops := q.Failure
	if resp.Succeeded {
		ops = q.Success
	}
	for i, op := range ops {
		if i >= len(resp.Ops) {
			return
		}
		r := &resp.Ops[i]
		switch {
		case op.Range != nil:
			c.saw(r.KVs, by)
		case op.Put != nil:
			c.wrote(op.Put, rev, by, before)
		case op.Delete != nil:
			if before {
				c.deleted(op.Delete)
			}
		case op.Txn != nil:
			c.txnDone(op.Txn, r, rev, by, before)
		}
	}
}

func (c *converter) watchRequest(r *Record, ws *watchStream) {
	if r.Req == nil || r.Req.Watch == nil {
		return
	}
	w := r.Req.Watch
	switch {
	case w.Create != nil:
		id := int64(-1)
		if w.Create.ID != 0 {
			id = w.Create.ID
		}
		ws.creates = append(ws.creates, watchCreate{WatchCreate: *w.Create, id: id})
	case w.Cancel != nil && r.T < c.w.Start:
		for i := range ws.creates {
			if ws.creates[i].id == *w.Cancel {
				ws.creates[i].canceled = true
			}
		}
	case w.Progress && c.inWindow(r.T):
		c.progress = append(c.progress, progressCall{index: int32(len(c.trace)), stream: r.Call})
		c.trace = append(c.trace, &Call{At: c.at(r.T), Kind: CallProgress})
	}
}

func (c *converter) watchResponse(resp *Response, ws *watchStream) {
	if resp == nil || resp.Watch == nil {
		return
	}
	w := resp.Watch
	if w.Created {
		for i := range ws.creates {
			if ws.creates[i].id == -1 {
				ws.creates[i].id = w.ID
				break
			}
		}
	}
	for _, e := range w.Events {
		if !e.Delete {
			c.seen[revKey{e.KV.Key, e.KV.Mod}] = delivery{call: deliveredByWatch}
		}
	}
}

// finish resolves the revisions the calls named and builds the trace.
func (c *converter) finish() *Trace {
	t := &Trace{Seconds: float64(c.w.End-c.w.Start) / 1e9}
	// The preload, in key order.
	names := make([]string, 0, len(c.state))
	for k := range c.state {
		names = append(names, k)
	}
	slices.Sort(names)
	items := map[revKey]int32{}
	for _, k := range names {
		kv := c.state[k]
		i := int32(len(t.Preload))
		it := Item{Key: c.key(k), Size: int32(kv.size)}
		if _, ok := c.leases[kv.lease]; ok && kv.lease != 0 {
			it.Lease = c.preloadLease(kv.lease)
		}
		t.Preload = append(t.Preload, it)
		items[revKey{k, kv.mod}] = i
		items[revKey{k, kv.create}] = i
	}
	for _, f := range c.fixups {
		k := revKey{f.key, f.rev}
		if p, ok := c.producers[k]; ok && p < f.owner {
			// The value comes from the write: in the replay it may
			// still be in flight when the reply that carried it comes.
			*f.at = Rev(p + 1)
			c.waitOn(f.owner, p)
			continue
		}
		if i, ok := items[k]; ok {
			*f.at = Rev(-i - 1)
			continue
		}
		*f.at = RevStale
		c.dropped["revision no key had when the window opened, compared as stale"]++
	}
	// Every lease alive when the window opened, those the trace names
	// first.
	for id := range c.leases {
		c.preloadLease(id)
	}
	for _, id := range c.leaseOrder {
		t.Leases = append(t.Leases, c.leases[id].ttl)
	}
	ids := make([]int64, 0, len(c.watches))
	for id := range c.watches {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	// A stream that watches what another already does is the same watch,
	// restarted: kube-apiserver cancels its watches every few minutes and
	// opens them again from where they were. It stays one stream, as the
	// apiserver never had both open for long.
	streams := map[int64]int32{}
	same := map[string]int32{}
	for _, id := range ids {
		ws := c.watches[id]
		if !ws.inWindow {
			continue
		}
		var s WatchStream
		for _, w := range ws.creates {
			if w.canceled {
				continue
			}
			s.Creates = append(s.Creates, Watch{Key: c.key(w.Key), End: c.end(w.End), PrevKV: w.PrevKV, Progress: w.Progress, Filters: w.Filters})
		}
		what := fmt.Sprint(s.Creates)
		if i, ok := same[what]; ok {
			streams[id] = i
			continue
		}
		same[what] = int32(len(t.Watches))
		streams[id] = int32(len(t.Watches))
		t.Watches = append(t.Watches, s)
	}
	for _, p := range c.progress {
		c.trace[p.index].Stream = streams[p.stream]
	}
	for _, call := range c.trace {
		slices.Sort(call.Waits)
		t.Calls = append(t.Calls, *call)
	}
	t.Keys = c.keyList
	return t
}

func (c *converter) waitOn(owner, on int32) {
	w := &c.trace[owner].Waits
	if on != owner && !slices.Contains(*w, on) {
		*w = append(*w, on)
	}
}
