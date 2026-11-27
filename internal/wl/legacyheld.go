package wl

import "encoding/binary"

const legacyRecordReady = uint32(1) << 31

// Only main's unchanged capture debit is outside #897's proof (option A,
// #904). All materialization, work and finished output still count.
func (n *node) mainCaptureCount() int64 {
	held := n.heldReplies
	if n.heldScanning != nil {
		held = n.heldScanning
	}
	var count int64
	for _, hr := range held {
		count += int64(hr.counted)
	}
	return count
}

func (n *node) queueLegacyHeld(hr *heldReply) {
	hr.fetching = true
	if n.legacyCollect != nil || n.legacyPending != nil || n.legacyRunning != nil || n.legacyMemo != nil && n.legacyMemo.needed {
		n.legacyAgain = true
		return
	}
	n.beginLegacyHeld(hr)
	lease := n.fillBook.reserve(stageUnit)
	if lease == nil && n.legacyMemo != nil && !n.legacyMemo.needed {
		n.dropLegacyBatch(n.legacyMemo)
		lease = n.fillBook.reserve(stageUnit)
	}
	if lease == nil {
		n.legacyAgain = true
		return
	}
	n.fillSequence++
	b := &legacyBatch{st: n.st, owner: n.legacyReading, lease: lease, op: n.fillSequence, needed: true, held: true}
	hr.c.fillOwners++
	n.legacyCollect = b
	n.freezeLegacy()
}

func (b *legacyBatch) freezeHeld() {
	b.n = len(b.owner.hr.refs)
	b.payload = 0
	for _, r := range b.owner.hr.refs {
		if r.rec != nil {
			b.payload += 4 + int64(len(r.rec))
			continue
		}
		ks, ok := b.st.putStateAt(r.id, r.mod)
		if !ok {
			b.err = errLogCut
			continue
		}
		b.payload += 4 + int64(kvSize(int(b.st.keys[r.id].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease))
	}
	b.frozen = true
}

// Lookups traverse the already captured references and W-owned canonical
// records, with a scalar hint. There is no duplicate per-record index.
func (b *legacyBatch) heldRecord(id uint32, mod int64) ([]byte, error, bool) {
	if b.owner.hr == nil || !b.ready {
		return nil, nil, false
	}
	if b.err != nil {
		return nil, b.err, true
	}
	refs := b.owner.hr.refs
	for pass := range 2 {
		idx, at := b.lookupIndex, b.lookupOffset
		if pass == 1 {
			idx, at = 0, 0
		}
		for ; idx < len(refs); idx++ {
			size := int64(binary.LittleEndian.Uint32(b.data[at:at+4]) &^ legacyRecordReady)
			start := at + 4
			at = start + size
			if refs[idx].id == id && refs[idx].mod == mod {
				b.lookupIndex, b.lookupOffset = idx+1, at
				return b.data[start:at], nil, true
			}
		}
		if b.lookupIndex == 0 {
			break
		}
	}
	return nil, nil, false
}

func (b *legacyBatch) initHeldData() {
	at := 0
	for _, r := range b.owner.hr.refs {
		rec := r.rec
		if rec == nil {
			k := &b.st.keys[r.id]
			if k.mod == r.mod && k.rec != 0 {
				rec = b.st.slab.bytes(k.rec, int(k.rlen))
			}
		}
		if rec != nil {
			binary.LittleEndian.PutUint32(b.data[at:], uint32(len(rec))|legacyRecordReady)
			at += 4
			at += copy(b.data[at:], rec)
			continue
		}
		ks, ok := b.st.putStateAt(r.id, r.mod)
		if !ok {
			b.err = errLogCut
			return
		}
		key := b.st.key(r.id)
		size := kvSize(len(key), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)
		begin := at
		binary.LittleEndian.PutUint32(b.data[at:], uint32(size))
		at += 4
		p := appendFillLen(b.data[at:at], 1, len(key))
		at += len(p)
		at += copy(b.data[at:], key)
		for _, kv := range [...]struct {
			field int
			value int64
		}{{2, ks.create}, {3, ks.mod}, {4, ks.ver}} {
			if kv.value != 0 {
				p = appendVarintField(b.data[at:at], kv.field, uint64(kv.value))
				at += len(p)
			}
		}
		if ks.vlen > 0 {
			p = appendFillLen(b.data[at:at], 5, int(ks.vlen))
			at += len(p) + int(ks.vlen)
		}
		if ks.lease != 0 {
			p = appendVarintField(b.data[at:at], 6, uint64(ks.lease))
			at += len(p)
		}
		if old := b.st.legacyView; old != nil && old != b {
			if value, err, ok := old.lookup(r.id, r.mod); ok && err == nil {
				copy(parseKV(b.data[begin+4:at]).value, value)
				binary.LittleEndian.PutUint32(b.data[begin:], uint32(size)|legacyRecordReady)
			}
		}
	}
}

func (n *node) planLegacyHeld(b *legacyBatch) error {
	b.planN = 0
	at := b.recordOffset
	prefix := int64(4)
	if b.material {
		prefix = 0
	}
	idx, pos := b.cursor, b.valuePos
	for idx < b.n && b.planN < len(b.plan) {
		var word uint32
		var size int64
		if b.material {
			size = int64(len(b.materialRecord(idx)))
			if *b.materialWord(idx)&materialReady != 0 {
				word = legacyRecordReady
			}
		} else {
			word = binary.LittleEndian.Uint32(b.data[at : at+4])
			size = int64(word &^ legacyRecordReady)
		}
		start := at + prefix
		p := parseKV(b.data[start : start+size])
		var r recRef
		if b.material {
			id, ok := b.st.find(p.key)
			if !ok {
				return errLogCut
			}
			r = recRef{id: id, mod: p.mod}
		} else {
			r = b.owner.hr.refs[idx]
		}
		total := len(p.value)
		if total == 0 {
			idx++
			at = start + size
			pos = 0
			continue
		}
		valueAt := start
		for m := b.data[start : start+size]; len(m) > 0; {
			num, _, _, value, rest, err := field(m)
			if err != nil {
				return err
			}
			if num == 5 {
				valueAt = start + size - int64(len(rest)+len(value))
				break
			}
			m = rest
		}
		source, take := -2, total-pos
		off := int64(0)
		var expected uint32
		if word&legacyRecordReady != 0 {
			if r.rec != nil {
				expected = valueSum(p.value)
			} else {
				ks, ok := b.st.putStateAt(r.id, r.mod)
				if !ok {
					return errLogCut
				}
				expected = ks.vsum
			}
		} else {
			ks, ok := b.st.putStateAt(r.id, r.mod)
			if !ok {
				return errLogCut
			}
			off = ks.voff + int64(pos)
			expected = ks.vsum
			q := replyFill{st: b.st}
			var err error
			source, take, err = n.fillSource(&q, &b.fetchSources, off, take)
			if err != nil {
				return err
			}
			if source == -1 {
				break
			}
			if source == -2 {
				w := b.st.log.(*wal)
				if !w.copyPlannedStage(b.data[valueAt+int64(pos):valueAt+int64(pos+take)], off) {
					return errLogCut
				}
			}
		}
		b.plan[b.planN] = legacySpan{off: off, dst: valueAt + int64(pos), entry: uint32(idx), pos: uint32(pos), len: uint32(take), source: uint32(source), expected: expected, total: uint32(total)}
		b.planN++
		pos += take
		if pos == total {
			idx++
			at = start + size
			pos = 0
		}
	}
	b.plannedCursor, b.plannedPos = idx, pos
	return nil
}
