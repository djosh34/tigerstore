package mvcc

// The store holds the value of each key's newest version in memory, in
// its slabs, as far as its limit allows (part 4 of docs/wire-loop.md). A
// put copies its value in once, as it is applied, and a read or a watch
// event copies it out: it needs no lookup in a map, no Raft entry and no
// disk. Older versions name their values in the log (Ref), where a helper
// reads them (Fill).
//
// A value that a put or a delete replaces stays readable until the end
// of the turn (Release): the turn's watch events carry it as their
// previous value, as Kubernetes asks for every event (#602: reading those
// back from the log took 8% of the leader's CPU on crd-heavy).
//
// The limit is the memory budget less what the rest of the process uses
// (Hold). Past it, the store lets go of values of keys in the order of
// their slots, at most stepEvictions a step, and a value read back from
// the log is held again when there is room (Keep). What gives memory back
// takes bounded steps (Step), so that no turn holds the node's heartbeats
// (#647).
type heldValues struct {
	slabs slabs
	// on is set once the store has a limit; until then it holds nothing.
	on bool
	// bytes counts every held value by its room in the slabs, limit is
	// what they may take, and low is the smallest bytes was since the
	// last Low.
	limit, bytes, low int64
	// retired are the values that the turn's writes replaced, by Ref,
	// until Release frees them.
	retired map[Ref]slot
	// hand is the slot of the key that eviction looks at next, and
	// evictions how many more values fit may drop before the next step.
	hand      uint32
	evictions int
	// compacting is set while the slabs' free slots take more room than
	// they should, and each step moves values out of chunks it can empty.
	compacting bool
	// hits counts the values read from memory.
	hits int64
}

const (
	// stepBytes is the most a step copies in the slabs: a few
	// milliseconds of work.
	stepBytes = 2 << 20
	// stepEvictions is the most values fit drops between two steps.
	stepEvictions = 1 << 13
	// minSlack is the least room of free slots that makes the slabs
	// compact, however few values the store holds.
	minSlack = 4 << 20
)

// SetValueLimit makes the store hold values in memory, up to limit bytes
// of them. A lower limit than before takes effect over the next steps.
func (s *Store) SetValueLimit(limit int64) {
	if !s.vals.on {
		s.vals.on, s.vals.evictions = true, stepEvictions
	}
	s.vals.limit = limit
	s.fit()
}

// ValueBytes is the room of the values the store holds, and ValueLimit
// what they may take.
func (s *Store) ValueBytes() int64 { return s.vals.bytes }

// ValueLimit is what the held values may take.
func (s *Store) ValueLimit() int64 { return max(s.vals.limit, 0) }

// ValueHits counts the values read from memory.
func (s *Store) ValueHits() int64 { return s.vals.hits }

// ValueLow is the smallest the held values took since the last call, and
// the next span starts from what they take now. A collection of the
// garbage collector in between found at least that much of them live.
func (s *Store) ValueLow() int64 {
	low := s.vals.low
	s.vals.low = s.vals.bytes
	return low
}

// slotOf returns the slot of k's held value.
func (k *keyIndex) slotOf() slot { return slot{n: k.valN, class: k.valClass} }

// hold copies value, the value of k's newest version, into the slabs, if
// it fits in the limit after fit.
func (s *Store) hold(k *keyIndex, value []byte) {
	v := &s.vals
	if !v.on || len(value) == 0 || k.held {
		return
	}
	room := int64(len(value))
	if ci := classOf(len(value)); ci != bigClass {
		room = int64(classSizes[ci])
	}
	if room > v.limit {
		// It would not fit with nothing else held.
		return
	}
	if v.bytes+room > v.limit {
		s.evict(v.bytes + room - v.limit)
		if v.bytes+room > v.limit {
			return
		}
	}
	sl := v.slabs.put(value, int32(k.id))
	k.valN, k.valClass, k.held = sl.n, sl.class, true
	v.bytes += room
}

// retire takes k's held value, which names r, out of k: it stays
// readable by r until Release.
func (s *Store) retire(k *keyIndex, r Ref) {
	if !k.held {
		return
	}
	k.held = false
	if s.vals.retired == nil {
		s.vals.retired = map[Ref]slot{}
	}
	s.vals.retired[r] = k.slotOf()
}

// dropValue frees k's held value, of n bytes, at once.
func (s *Store) dropValue(k *keyIndex, n uint32) {
	if !k.held {
		return
	}
	k.held = false
	s.freeValue(k.slotOf(), n)
}

// freeValue gives back slot sl of a value of n bytes.
func (s *Store) freeValue(sl slot, n uint32) {
	v := &s.vals
	v.bytes -= v.slabs.room(sl, int(n))
	v.slabs.free(sl)
	v.low = min(v.low, v.bytes)
}

// Release frees the values that the writes since the last call replaced:
// the caller has built every answer and event that reads them.
func (s *Store) Release() {
	if len(s.vals.retired) == 0 {
		// gosim's clear fails on a nil map.
		return
	}
	for r, sl := range s.vals.retired {
		s.freeValue(sl, r.Len)
	}
	clear(s.vals.retired)
}

// heldValue returns the bytes of the value r of the key in slot id, if the
// store holds them: as the key's newest value, or as one replaced in this
// turn. Nobody may keep or change them.
func (s *Store) heldValue(id uint32, r Ref) ([]byte, bool) {
	v := &s.vals
	if id != 0 && id < s.used {
		if k := s.rec(id); k.held && k.current(r) {
			return v.slabs.bytes(k.slotOf(), int(r.Len)), true
		}
	}
	if sl, ok := v.retired[r]; ok {
		return v.slabs.bytes(sl, int(r.Len)), true
	}
	return nil, false
}

// Keep holds b, the value r that a helper read from the log for key, if r
// is still the key's newest value and there is room.
func (s *Store) Keep(key []byte, r Ref, b []byte) {
	if k, ok := s.get(key); ok && k.current(r) && !k.held && uint32(len(b)) == r.Len {
		s.hold(k, b)
	}
}

// evict lets go of held values until need bytes are free, or until it
// dropped what a step allows: the values of the keys after the hand, in
// the order of their slots. It looks at each slot at most once a call.
func (s *Store) evict(need int64) {
	v := &s.vals
	freed := int64(0)
	for scanned := uint32(0); freed < need && v.evictions > 0 && v.bytes > 0 && scanned < s.used; scanned++ {
		if v.hand++; v.hand >= s.used {
			v.hand = 1
		}
		k := s.rec(v.hand)
		if !k.held {
			continue
		}
		before := v.bytes
		s.dropValue(k, k.vers()[len(k.vers())-1].value.Len)
		freed += before - v.bytes
		v.evictions--
	}
}

// fit drops held values until they are within the limit, or until it
// dropped what a step allows; the steps drop the rest.
func (s *Store) fit() {
	if v := &s.vals; v.bytes > max(v.limit, 0) {
		s.evict(v.bytes - max(v.limit, 0))
	}
}

// ValuesBusy reports whether the held values have steps to take.
func (s *Store) ValuesBusy() bool {
	v := &s.vals
	return v.compacting || v.bytes > max(v.limit, 0) && v.slabs.used > 0 || v.loose()
}

// loose reports whether the slabs' free slots take enough room to
// compact them.
func (v *heldValues) loose() bool {
	return v.slabs.slack() > max(v.slabs.used/2, minSlack) && v.slabs.anyLoose()
}

// Step takes the next bounded step of the held values: it drops values
// past the limit, up to stepEvictions since the last step, and compacts
// the slabs by up to stepBytes. It runs after Release.
func (s *Store) Step() {
	v := &s.vals
	v.evictions = stepEvictions
	s.fit()
	if !v.compacting && v.loose() {
		v.compacting = true
	}
	if v.compacting {
		more := v.slabs.compact(stepBytes, func(owner int32, to slot) {
			k := s.rec(uint32(owner))
			k.valN, k.valClass = to.n, to.class
		})
		v.compacting = more && v.slabs.slack() > max(v.slabs.used/4, minSlack/2)
	}
}
