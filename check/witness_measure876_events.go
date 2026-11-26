package check

// Scratch-only, read-only measurement of a bounded event certificate. A hit
// is recorded; the real search still executes every original candidate.
import "github.com/djosh34/tigerstore/history"

type measure876EventCert struct {
	Certified                                 bool
	Why                                       string
	Items                                     int
	NoWrite, BlockedEvent, MissingLease, Used int
}

func measure876EventCertificate(r *revisionOrder, rev int64, candidates []int) measure876EventCert {
	c := measure876EventCert{}
	if rev > r.top || r.w.slow {
		c.Why = "off"
		return c
	}
	if _, ok := r.writer(rev); ok {
		c.Why = "acked"
		return c
	}
	if _, ok := r.resolved[rev]; ok {
		c.Why = "resolved"
		return c
	}
	s := r.w.live
	if len(s.keys) > 12 {
		c.Why = "key-cap"
		return c
	}
	item := func() bool { c.Items++; return c.Items <= 65536 }
	var present, covered uint16
	for k, key := range s.keys {
		if !item() || len(key) > 32 {
			c.Why = "key/work-cap"
			return c
		}
		if s.kvs[k].Version != 0 {
			present |= 1 << k
		}
		if r.unchanged(key, rev) {
			covered |= 1 << k
		}
	}
	// Optional due work cannot add leases. Trial grants may add any unused
	// eligible grant of the ID, so this is an over-approximation, not just
	// the lease set the current trial happened to choose.
	leasePossible := func(id int64) bool {
		if id == 0 || s.hasLease(id) {
			return true
		}
		for _, g := range r.grantsOf[id] {
			if !item() {
				return true
			} // Caller sees the cap and returns unknown.
			if !r.used[g] && r.lb[g] <= rev-1 {
				return true
			}
		}
		return false
	}
	// Each span/index lookup has at most four binary-search levels at the
	// 12-key cap. Request key strings are limited to 32 bytes.
	spanMask := func(d *history.DeleteRequest) (uint16, bool) {
		if len(d.Key) > 32 || len(d.End) > 32 {
			return 0, false
		}
		lo, hi := s.span(d.Key, d.End)
		return uint16((uint32(1)<<hi)-1) &^ uint16((uint32(1)<<lo)-1), true
	}
	for _, i := range candidates {
		if !item() {
			c.Why = "work-cap"
			return c
		}
		if r.used[i] {
			c.Used++
			continue
		}
		q := r.w.reqs[i]
		var ops []history.TxnOp
		switch {
		case q.Compact != nil || q.LeaseGrant != nil:
			c.NoWrite++
			continue
		case q.LeaseRevoke != nil:
			var deleted uint16
			for k, kv := range s.kvs {
				if !item() {
					c.Why = "work-cap"
					return c
				}
				if kv.Version != 0 && kv.Lease == q.LeaseRevoke.ID {
					deleted |= 1 << k
				}
			}
			if deleted == 0 {
				c.NoWrite++
				continue
			}
			if deleted&covered != 0 {
				c.BlockedEvent++
				continue
			}
			c.Why = "possible-revoke-event"
			return c
		case q.Put != nil:
			if !leasePossible(q.Put.Lease) {
				c.MissingLease++
				continue
			}
			if c.Items > 65536 {
				c.Why = "work-cap"
				return c
			}
			if len(q.Put.Key) > 32 {
				c.Why = "key-cap"
				return c
			}
			k := s.index(q.Put.Key)
			if k < 0 {
				c.Why = "unknown-key"
				return c
			}
			if covered&(1<<k) != 0 {
				c.BlockedEvent++
				continue
			}
			c.Why = "possible-put-event"
			return c
		case q.Delete != nil:
			mask, ok := spanMask(q.Delete)
			if !ok {
				c.Why = "key-cap"
				return c
			}
			deleted := mask & present
			if deleted == 0 {
				c.NoWrite++
				continue
			}
			if deleted&covered != 0 {
				c.BlockedEvent++
				continue
			}
			c.Why = "possible-delete-event"
			return c
		case q.Txn != nil:
			success := true
			for _, cmp := range q.Txn.Compare {
				if !item() || len(cmp.Key) > 32 || len(cmp.Value) > 256 {
					c.Why = "compare/work-cap"
					return c
				}
				kv, exists := s.get(cmp.Key)
				if cmp.Target == history.TargetValue && len(kv.Value) > 256 {
					c.Why = "value-cap"
					return c
				}
				if !compareKV(kv, exists, cmp) {
					success = false
					break
				}
			}
			ops = q.Txn.Success
			if !success {
				ops = q.Txn.Failure
			}
		default:
			c.Why = "unsupported-request"
			return c
		}
		// Missing leases abort the selected txn before any writes. Relax
		// historical range errors: permitting their execution can only add
		// possible events; it cannot hide a real writer.
		missing := false
		for _, op := range ops {
			if !item() {
				c.Why = "work-cap"
				return c
			}
			if op.Put != nil && !leasePossible(op.Put.Lease) {
				missing = true
				break
			}
			if c.Items > 65536 {
				c.Why = "work-cap"
				return c
			}
		}
		if missing {
			c.MissingLease++
			continue
		}
		p := present
		wrote, blocked := false, false
		for _, op := range ops {
			if !item() {
				c.Why = "work-cap"
				return c
			}
			switch {
			case op.Put != nil:
				if len(op.Put.Key) > 32 {
					c.Why = "key-cap"
					return c
				}
				k := s.index(op.Put.Key)
				if k < 0 {
					c.Why = "unknown-key"
					return c
				}
				wrote = true
				blocked = blocked || covered&(1<<k) != 0
				p |= 1 << k
			case op.Delete != nil:
				mask, ok := spanMask(op.Delete)
				if !ok {
					c.Why = "key-cap"
					return c
				}
				deleted := p & mask
				wrote = wrote || deleted != 0
				blocked = blocked || deleted&covered != 0
				p &^= mask
			case op.Range != nil:
			default:
				c.Why = "unsupported-txn-op"
				return c
			}
		}
		switch {
		case blocked:
			c.BlockedEvent++
		case !wrote:
			c.NoWrite++
		default:
			c.Why = "possible-txn-event"
			return c
		}
	}
	c.Certified, c.Why = true, "every-candidate-event-blocked-or-none"
	return c
}
