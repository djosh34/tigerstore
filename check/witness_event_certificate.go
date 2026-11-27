package check

import "github.com/djosh34/tigerstore/history"

// eventCertificate summarizes only negative fits proofs. It owns no candidate
// or observation-sized storage. Its masks use live.keys indexes, not kv.Key
// (an absent key's kv.Key is empty). Unsupported inputs make all false.
// Keep the complete summary within 64 bytes; the tests pin that bound.
type eventCertificate struct {
	all              bool
	present, covered uint16
	reasons          [eventRejectReasons]uint32
}

const (
	eventEmptyDelete = iota
	eventBlockedDelete
	eventBlockedPut
	eventMissingLease
	eventReadOnlyCAS
	eventCompaction
	eventRejectReasons
)

func (r *revisionOrder) newEventCertificate(rev int64) eventCertificate {
	if r.testNoEventCertificate || r.w.slow || rev > r.top || len(r.w.live.keys) > 12 || len(r.byRev(r.revokes, rev)) != 0 {
		return eventCertificate{}
	}
	if _, held := r.writer(rev); held {
		return eventCertificate{}
	}
	if _, resolved := r.resolved[rev]; resolved {
		return eventCertificate{}
	}
	return eventCertificate{all: true}
}

// key is called inside candidates' existing charged live-key traversal.
func (c *eventCertificate) key(r *revisionOrder, rev int64, k int) {
	if !c.all {
		return
	}
	key := r.w.live.keys[k]
	if len(key) > 32 {
		c.all = false
		return
	}
	if r.w.live.kvs[k].Version != 0 {
		c.present |= 1 << k
		if r.unchanged(key, rev) {
			c.covered |= 1 << k
		}
	}
}

// candidate is folded into the existing charged candidate-filter loop. Every
// stencil does bounded constant work: <=12-key binary searches, <=32-byte
// keys/endpoints and direct access to one compare/one branch op. There is no
// extra scan, speculative accounting/refund, count query or txn/grant loop.
func (c *eventCertificate) candidate(r *revisionOrder, i int) {
	if !c.all {
		return
	}
	q := r.w.reqs[i]
	reason := -1
	switch {
	case q.Delete != nil:
		d := q.Delete
		if len(d.Key) > 32 || len(d.End) > 32 {
			break
		}
		lo, hi := r.w.live.span(d.Key, d.End)
		span := uint16((uint32(1)<<hi)-1) &^ uint16((uint32(1)<<lo)-1)
		deleted := c.present & span
		switch {
		case deleted == 0:
			reason = eventEmptyDelete
		case deleted&c.covered != 0:
			reason = eventBlockedDelete
		}
	case q.Compact != nil:
		reason = eventCompaction
	case q.Put != nil:
		reason = c.put(r, q.Put)
	case q.Txn != nil:
		t := q.Txn
		if len(t.Compare) != 1 || len(t.Success) != 1 || len(t.Failure) != 1 || t.Success[0].Put == nil || t.Failure[0].Range == nil {
			break
		}
		cmp, put, read := t.Compare[0], t.Success[0].Put, t.Failure[0].Range
		if cmp.Target != history.TargetMod || cmp.Result != history.Equal || put.Lease != 0 || len(cmp.Key) > 32 || len(put.Key) > 32 || len(read.Key) > 32 || len(read.End) > 32 {
			break
		}
		kv, _ := r.w.live.get(cmp.Key)
		// Missing keys have mod revision zero; image keys keep their actual
		// mod revision, not their creation revision or version.
		if kv.ModRevision != cmp.Number {
			reason = eventReadOnlyCAS
		} else {
			reason = c.put(r, put)
		}
	}
	if reason < 0 {
		c.all = false
		return
	}
	c.reasons[reason]++
}

func (c *eventCertificate) put(r *revisionOrder, p *history.PutRequest) int {
	if len(p.Key) > 32 {
		return -1
	}
	k := r.w.live.index(p.Key)
	if k < 0 {
		return -1
	}
	if p.Lease != 0 && !r.w.live.hasLease(p.Lease) {
		// Check this BEFORE covered: a possible trial grant would create
		// replay generations even if the eventual write were blocked.
		if len(r.grantsOf[p.Lease]) != 0 {
			return -1
		}
		return eventMissingLease
	}
	if c.covered&(1<<k) != 0 {
		return eventBlockedPut
	}
	return -1
}

// Why skipping a certified child preserves the parent continuation:
// finish(parent) has already built due and eventBounds. Certified probes fail
// before reachable, so they cannot initialize mayRevoke. No stencil can enter
// a trial grant; readsFit uses placeSeen(..., false), setting neither placements
// nor first answers. Trials undo their key changes; replay generations/events
// therefore stay fixed. Parent restore resets readiness/availability but retains
// due's timeAt/revAt/fromAt: omitting intermediate threshold moves cannot cost
// more than traversing them. Scope buffers are caches (at most deferred first
// initialization). Construction uses the same accounting ticks even on unknown
// fallback; a hit only removes probes, with the same exhaustion/dead ordering.
