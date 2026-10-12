package check

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

// Revisions checks the revision rules on what clients saw. It needs no log:
//
//   - Every key returned has 1 <= version, 2 <= create revision <= mod
//     revision <= the revision it was read at, and version 1 exactly when
//     create and mod revision are equal.
//   - Revisions follow real time. An op that starts after another returned
//     has a header revision at least as high, and higher if it wrote.
//   - Every write gets its own revision, however many keys it changes. A key
//     whose mod revision is the revision of an acked write holds what that
//     write put there, and a delete event at it is a key that write deleted.
//   - The same key at the same mod revision always looks the same. Versions
//     grow with mod revisions and restart at 1 when a key is recreated.
//   - A read below the compaction revision fails as compacted and one above
//     the current revision fails as future, and only then. Compact follows
//     the same rules.
//
// Serializable reads are left out of the real-time rule.
func Revisions(h *history.History) error {
	if err := h.Validate(); err != nil {
		return err
	}
	c := revisionChecker{h: h, byRev: map[int64]int{}, seen: map[string][]history.KeyValue{}, deleted: map[string][]int64{}}
	c.writes()
	c.realTime()
	c.observed()
	c.versions()
	c.compaction()
	return errors.Join(c.errs...)
}

type revisionChecker struct {
	h    *history.History
	errs []error
	// byRev maps the revision of an acked write to its op.
	byRev map[int64]int
	// seen collects every observed key by name.
	seen map[string][]history.KeyValue
	// deleted collects, per key, the revisions at which clients saw it
	// deleted.
	deleted map[string][]int64
}

func (c *revisionChecker) fail(format string, args ...any) {
	if len(c.errs) < maxErrors {
		c.errs = append(c.errs, fmt.Errorf(format, args...))
	}
}

// writes indexes acked writes by revision and checks that no two share one.
func (c *revisionChecker) writes() {
	for i := range c.h.Ops {
		op := &c.h.Ops[i]
		if !bumps(op) {
			continue
		}
		if j, ok := c.byRev[op.Resp.Revision]; ok {
			c.fail("ops %d and %d both wrote at revision %d", j, i, op.Resp.Revision)
			continue
		}
		c.byRev[op.Resp.Revision] = i
	}
}

// bumps reports whether an acked op created a revision. A revoke does so
// only if keys used the lease, which the client can not see, so it is left
// out.
func bumps(op *history.Op) bool {
	if op.Resp == nil {
		return false
	}
	switch {
	case op.Req.Put != nil:
		return true
	case op.Req.Delete != nil, op.Req.Txn != nil:
		return respChanges(op)
	}
	return false
}

// realTime checks that header revisions never go backwards in real time.
func (c *revisionChecker) realTime() {
	var ops []*history.Op
	for i := range c.h.Ops {
		op := &c.h.Ops[i]
		if op.Resp != nil && (op.Req.Range == nil || !op.Req.Range.Serializable) {
			ops = append(ops, op)
		}
	}
	byReturn := slices.Clone(ops)
	slices.SortFunc(byReturn, func(a, b *history.Op) int { return cmp.Compare(a.Return, b.Return) })
	// highest[i] is the highest revision among byReturn[:i+1].
	highest := make([]int64, len(byReturn))
	for i, op := range byReturn {
		highest[i] = op.Resp.Revision
		if i > 0 {
			highest[i] = max(highest[i], highest[i-1])
		}
	}
	// before returns the highest revision of an op that returned before t.
	before := func(t int64) int64 {
		n := sort.Search(len(byReturn), func(i int) bool { return byReturn[i].Return >= t })
		if n == 0 {
			return 0
		}
		return highest[n-1]
	}
	for _, op := range ops {
		prev := before(op.Call)
		switch {
		case bumps(op) && op.Resp.Revision <= prev:
			c.fail("client %d wrote at revision %d, called at %d after revision %d was returned", op.Client, op.Resp.Revision, op.Call, prev)
		case op.Resp.Revision < prev:
			c.fail("client %d got revision %d, called at %d after revision %d was returned", op.Client, op.Resp.Revision, op.Call, prev)
		}
	}
	// Errors that claim a revision is in the future.
	for i := range c.h.Ops {
		op := &c.h.Ops[i]
		if op.Err == nil || !sameError(op.Err, errFutureRev) {
			continue
		}
		rev := requestedRevision(op.Req)
		if prev := before(op.Call); rev <= prev {
			c.fail("op %d (client %d) got a future revision error for revision %d, after revision %d was returned", i, op.Client, rev, prev)
		}
	}
}

// requestedRevision is the revision a range or compact asks for.
func requestedRevision(r history.Request) int64 {
	switch {
	case r.Range != nil:
		return r.Range.Revision
	case r.Compact != nil:
		return r.Compact.Revision
	}
	return 0
}

// observed checks every key a client saw.
func (c *revisionChecker) observed() {
	for i := range c.h.Ops {
		op := &c.h.Ops[i]
		if op.Resp == nil {
			continue
		}
		at := op.Resp.Revision
		if op.Req.Range != nil && op.Req.Range.Revision != 0 {
			at = op.Req.Range.Revision
			if at > op.Resp.Revision {
				c.fail("op %d (client %d) read at revision %d but its header revision is %d", i, op.Client, at, op.Resp.Revision)
			}
		}
		where := fmt.Sprintf("op %d (client %d)", i, op.Client)
		for _, kv := range keysIn(op) {
			c.kv(where, kv, at)
		}
		for _, key := range deletedBy(op) {
			c.deleted[key] = append(c.deleted[key], op.Resp.Revision)
		}
	}
	for i, w := range c.h.Watches {
		for _, resp := range w.Responses {
			for _, e := range resp.Events {
				where := fmt.Sprintf("watch %d (client %d)", i, w.Client)
				if e.PrevKV != nil {
					c.kv(where, *e.PrevKV, e.Revision()-1)
				}
				if e.Type == history.EventPut {
					c.kv(where, e.KV, resp.Revision)
					continue
				}
				kv := e.KV
				c.deleted[kv.Key] = append(c.deleted[kv.Key], kv.ModRevision)
				if kv.Version != 0 || kv.CreateRevision != 0 || kv.Value != "" || kv.ModRevision < 2 {
					c.fail("%s: delete event %+v must have only a key and a mod revision", where, kv)
				}
				if j, ok := c.byRev[kv.ModRevision]; ok && !deletes(&c.h.Ops[j], kv.Key) {
					c.fail("%s: delete event of key %q at revision %d, but op %d wrote that revision and did not delete it", where, kv.Key, kv.ModRevision, j)
				}
			}
		}
	}
}

// kv checks one observed key, read at revision at.
func (c *revisionChecker) kv(where string, kv history.KeyValue, at int64) {
	switch {
	case kv.Version < 1:
		c.fail("%s: key %q has version %d", where, kv.Key, kv.Version)
	case kv.CreateRevision < 2 || kv.CreateRevision > kv.ModRevision:
		c.fail("%s: key %q has create revision %d and mod revision %d", where, kv.Key, kv.CreateRevision, kv.ModRevision)
	case kv.ModRevision > at:
		c.fail("%s: key %q has mod revision %d, above the revision %d it was read at", where, kv.Key, kv.ModRevision, at)
	case (kv.Version == 1) != (kv.CreateRevision == kv.ModRevision):
		c.fail("%s: key %q has version %d, create revision %d and mod revision %d", where, kv.Key, kv.Version, kv.CreateRevision, kv.ModRevision)
	}
	if j, ok := c.byRev[kv.ModRevision]; ok {
		p := puts(&c.h.Ops[j], kv.Key)
		if p == nil {
			c.fail("%s: key %q has mod revision %d, but op %d wrote that revision and did not put it", where, kv.Key, kv.ModRevision, j)
		} else if p.Lease != kv.Lease || p.Value != kv.Value && kv.Value != "" {
			// An empty value may be a keys-only read.
			c.fail("%s: key %q at revision %d is %+v, but op %d put %+v", where, kv.Key, kv.ModRevision, kv, j, *p)
		}
	}
	c.seen[kv.Key] = append(c.seen[kv.Key], kv)
}

// versions checks that each key looks the same at the same mod revision and
// that its versions count up within one life of the key.
func (c *revisionChecker) versions() {
	keys := make([]string, 0, len(c.seen))
	for k := range c.seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, key := range keys {
		kvs := c.seen[key]
		for _, kv := range kvs {
			for _, d := range c.deleted[key] {
				if kv.CreateRevision <= d && d <= kv.ModRevision {
					c.fail("key %q was deleted at revision %d, but %+v was created before and modified after", key, d, kv)
					break
				}
			}
		}
		slices.SortFunc(kvs, func(a, b history.KeyValue) int { return cmp.Compare(a.ModRevision, b.ModRevision) })
		for i := 1; i < len(kvs); i++ {
			a, b := kvs[i-1], kvs[i]
			switch {
			case a.ModRevision == b.ModRevision:
				// A keys-only read has no value; compare the rest.
				if a.Value == "" || b.Value == "" {
					a.Value, b.Value = "", ""
				}
				if a != b {
					c.fail("key %q at mod revision %d was seen as %+v and as %+v", key, a.ModRevision, a, b)
				}
			case a.CreateRevision == b.CreateRevision:
				if b.Version <= a.Version {
					c.fail("key %q has version %d at revision %d and version %d at revision %d", key, a.Version, a.ModRevision, b.Version, b.ModRevision)
				}
			case b.CreateRevision <= a.ModRevision:
				c.fail("key %q was recreated at revision %d, before its earlier life was modified at %d", key, b.CreateRevision, a.ModRevision)
			}
		}
	}
}

// compaction checks compacted errors and reads against the compactions
// clients made.
func (c *revisionChecker) compaction() {
	// Compactions that may have happened: all but those the server
	// rejected.
	var compacts []int
	for i, op := range c.h.Ops {
		if op.Req.Compact != nil && (op.Err == nil || op.Err.Ambiguous) {
			compacts = append(compacts, i)
		}
	}
	for i := range c.h.Ops {
		op := &c.h.Ops[i]
		rev := requestedRevision(op.Req)
		if op.Req.Range == nil && op.Req.Compact == nil || op.Req.Range != nil && rev == 0 {
			continue
		}
		// covers reports whether a compaction at k makes rev compacted
		// for this op: a range needs one above rev, a compact one at or
		// above it.
		covers := func(k int64) bool { return k > rev || op.Req.Compact != nil && k == rev }
		switch {
		case op.Err != nil && sameError(op.Err, errCompacted):
			found := slices.ContainsFunc(compacts, func(j int) bool {
				k := &c.h.Ops[j]
				return j != i && k.Call < op.Return && covers(k.Req.Compact.Revision)
			})
			if !found {
				c.fail("op %d (client %d) got a compacted error for revision %d, but no compaction that could cause it started before it returned", i, op.Client, rev)
			}
		case op.Resp != nil:
			for _, j := range compacts {
				k := &c.h.Ops[j]
				if j != i && k.Resp != nil && k.Return < op.Call && covers(k.Req.Compact.Revision) {
					c.fail("op %d (client %d) succeeded at revision %d, after op %d compacted at %d", i, op.Client, rev, j, k.Req.Compact.Revision)
					break
				}
			}
		}
	}
}

// deletedBy returns the keys an acked op is known to have deleted: single
// key deletes that deleted one key, and range deletes with previous values.
func deletedBy(op *history.Op) []string {
	var keys []string
	add := func(q *history.DeleteRequest, r *history.DeleteResponse) {
		if q.End == "" && r.Deleted == 1 {
			keys = append(keys, q.Key)
			return
		}
		for _, kv := range r.PrevKVs {
			keys = append(keys, kv.Key)
		}
	}
	switch {
	case op.Req.Delete != nil:
		add(op.Req.Delete, op.Resp.Delete)
	case op.Req.Txn != nil:
		for i, o := range branch(op) {
			if o.Delete != nil {
				add(o.Delete, op.Resp.Txn.Results[i].Delete)
			}
		}
	}
	return keys
}

// keysIn returns every key in the response of an acked op.
func keysIn(op *history.Op) []history.KeyValue {
	var kvs []history.KeyValue
	addRange := func(r *history.RangeResponse) { kvs = append(kvs, r.KVs...) }
	addPut := func(p *history.PutResponse) {
		if p.PrevKV != nil {
			kvs = append(kvs, *p.PrevKV)
		}
	}
	addDelete := func(d *history.DeleteResponse) { kvs = append(kvs, d.PrevKVs...) }
	switch r := op.Resp; {
	case r.Range != nil:
		addRange(r.Range)
	case r.Put != nil:
		addPut(r.Put)
	case r.Delete != nil:
		addDelete(r.Delete)
	case r.Txn != nil:
		for _, res := range r.Txn.Results {
			switch {
			case res.Range != nil:
				addRange(res.Range)
			case res.Put != nil:
				addPut(res.Put)
			case res.Delete != nil:
				addDelete(res.Delete)
			}
		}
	}
	return kvs
}

// branch returns the txn operations an acked op ran.
func branch(op *history.Op) []history.TxnOp {
	switch {
	case op.Req.Put != nil:
		return []history.TxnOp{{Put: op.Req.Put}}
	case op.Req.Delete != nil:
		return []history.TxnOp{{Delete: op.Req.Delete}}
	case op.Req.Txn != nil && op.Resp.Txn.Succeeded:
		return op.Req.Txn.Success
	case op.Req.Txn != nil:
		return op.Req.Txn.Failure
	}
	return nil
}

// puts returns the last put of key that an acked op ran, or nil.
func puts(op *history.Op, key string) *history.PutRequest {
	var p *history.PutRequest
	for _, o := range branch(op) {
		switch {
		case o.Put != nil && o.Put.Key == key:
			p = o.Put
		case o.Delete != nil && history.InRange(key, o.Delete.Key, o.Delete.End):
			p = nil
		}
	}
	return p
}

// deletes reports whether an acked op deleted key, and did not put it back.
func deletes(op *history.Op, key string) bool {
	deleted := false
	for _, o := range branch(op) {
		switch {
		case o.Put != nil && o.Put.Key == key:
			deleted = false
		case o.Delete != nil && history.InRange(key, o.Delete.Key, o.Delete.End):
			deleted = true
		}
	}
	return deleted
}
