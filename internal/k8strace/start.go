package k8strace

// Start is the store a replay starts from when it begins at call K of a
// trace instead of the first: the preload with the writes of the calls
// before K applied, as they went when they were recorded.
type Start struct {
	K      int
	Items  []StartItem
	Leases []int64 // TTLs
	// itemOf maps a key to its item; leaseOf a lease of the trace to its
	// index in Leases.
	itemOf  map[int32]int32
	leaseOf map[LeaseRef]int32
}

// StartItem is a key alive at the start. Lease is an index into
// Start.Leases plus one, 0 for none. Writer is the write that left it, as
// a Rev: the call, or the preload item.
type StartItem struct {
	Key    int32
	Size   int32
	Lease  int32
	Writer Rev
}

// From returns the store before call k.
func (t *Trace) From(k int) *Start {
	s := &Start{K: k, itemOf: map[int32]int32{}, leaseOf: map[LeaseRef]int32{}}
	live := map[int32]StartItem{} // by key
	leases := map[LeaseRef]int64{}
	for i, ttl := range t.Leases {
		leases[LeaseRef(-i-1)] = ttl
	}
	for i, it := range t.Preload {
		live[it.Key] = StartItem{Key: it.Key, Size: it.Size, Lease: int32(it.Lease), Writer: Rev(-i - 1)}
	}
	var apply func(op *Op, writer Rev)
	apply = func(op *Op, writer Rev) {
		switch op.Kind {
		case OpPut:
			it := StartItem{Key: op.Key, Size: op.Size, Lease: int32(op.Lease), Writer: writer}
			if old, ok := live[op.Key]; ok {
				if op.Flags&FlagIgnoreValue != 0 {
					it.Size = old.Size
				}
				if op.Flags&FlagIgnoreLease != 0 {
					it.Lease = old.Lease
				}
			}
			live[op.Key] = it
		case OpDelete:
			if op.End < 0 {
				delete(live, op.Key)
				return
			}
			from, to := t.Keys[op.Key], t.Keys[op.End]
			for k := range live {
				if name := t.Keys[k]; name >= from && (to == "\x00" || name < to) {
					delete(live, k)
				}
			}
		case OpTxn:
			// A nested Txn's outcome was not recorded; its success
			// branch is the common one.
			ops := op.Success
			if writer > 0 && t.Calls[writer-1].Failed && op == &t.Calls[writer-1].Op {
				ops = op.Failure
			}
			for i := range ops {
				apply(&ops[i], writer)
			}
		}
	}
	for j := range k {
		c := &t.Calls[j]
		switch c.Kind {
		case CallPut, CallDelete, CallTxn:
			apply(&c.Op, Rev(j+1))
		case CallLeaseGrant:
			leases[LeaseRef(j+1)] = c.TTL
		case CallLeaseRevoke:
			delete(leases, c.Lease)
			for key, it := range live {
				if LeaseRef(it.Lease) == c.Lease {
					delete(live, key)
				}
			}
		}
	}
	// Number the leases and items in the trace's order, so the same k
	// gives the same start.
	for i := range t.Leases {
		if ttl, ok := leases[LeaseRef(-i-1)]; ok {
			s.leaseOf[LeaseRef(-i-1)] = int32(len(s.Leases))
			s.Leases = append(s.Leases, ttl)
		}
	}
	for j := range k {
		if ttl, ok := leases[LeaseRef(j+1)]; ok && t.Calls[j].Kind == CallLeaseGrant {
			s.leaseOf[LeaseRef(j+1)] = int32(len(s.Leases))
			s.Leases = append(s.Leases, ttl)
		}
	}
	for key := range t.Keys {
		it, ok := live[int32(key)]
		if !ok {
			continue
		}
		if l, ok := s.leaseOf[LeaseRef(it.Lease)]; ok && it.Lease != 0 {
			it.Lease = l + 1
		} else {
			it.Lease = 0
		}
		s.itemOf[it.Key] = int32(len(s.Items))
		s.Items = append(s.Items, it)
	}
	return s
}

// Item returns the start's item of key if the write r left it, for a
// compare that names r: a revision from before the start.
func (s *Start) Item(key int32, r Rev) (int32, bool) {
	i, ok := s.itemOf[key]
	if !ok || s.Items[i].Writer != r {
		return 0, false
	}
	return i, true
}

// Lease returns the index in Leases of a lease from before the start, if
// it is still alive.
func (s *Start) Lease(l LeaseRef) (int32, bool) {
	i, ok := s.leaseOf[l]
	return i, ok
}

// Before reports whether r names a write from before the start.
func (s *Start) Before(r Rev) bool { return r < 0 || r > 0 && r != RevStale && int(r-1) < s.K }
