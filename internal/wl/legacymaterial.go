package wl

import (
	"bytes"
	"encoding/binary"
)

const materialPrefix = 8

// walkMaterial follows the unchanged range input/size policy. It holds
// no record plan; the caller retains only a scalar cursor and immutable
// canonical materialization. Value-sort admission itself is unchanged.
func (b *legacyBatch) walkMaterial(visit func(uint32, ckptState) bool, before func() bool) {
	r := &b.owner.req
	st := b.st
	taken := int64(0)
	fn := func(id uint32) bool {
		if before != nil && !before() {
			return false
		}
		if taken >= r.readLimit() {
			return false
		}
		ks, ok := st.stateAt(id, b.owner.view)
		if !ok {
			return true
		}
		taken++
		return visit(id, ks)
	}
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			fn(id)
		}
		return
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	st.tree.scan(r.key, end, fn)
}

// convertLegacyMaterial uses the consumer's canonical materialization,
// charged in its existing client allocation. No payload is also retained
// in W and no per-record cache/index is built alongside that storage.
func (n *node) convertLegacyMaterial(b *legacyBatch) bool {
	payload, canonical := int64(0), int64(0)
	count := 0
	big := false
	b.walkMaterial(func(id uint32, ks ckptState) bool {
		size := kvSize(int(b.st.keys[id].klen), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)
		canonical += int64(size)
		payload += materialPrefix + int64(size)
		count++
		return true
	}, func() bool {
		if canonical > n.oneShot() {
			big = true
			return false
		}
		return true
	})
	if big {
		owner := b.owner
		if c, s := n.clientOf(owner.h); c != nil {
			n.startBig(c, s, owner.msg)
		}
		n.dropLegacyBatch(b)
		return false
	}
	for index := &b.index; index != nil; {
		clear(index.pages[:])
		next := index.next
		index.next = nil
		index = next
	}
	credit := b.lease.bytes - stageUnit
	b.lease.bytes -= credit
	b.lease.book.used -= credit
	b.material = true
	b.full = false
	b.n = count
	b.payload = payload
	b.capacity, b.pages = 0, 0
	return true
}

func (b *legacyBatch) canonicalRecord(id uint32, mod int64) ([]byte, error, bool) {
	if !b.material {
		return b.heldRecord(id, mod)
	}
	if !b.ready || !b.needed || b.st == nil {
		return nil, nil, false
	}
	if b.err != nil {
		return nil, b.err, true
	}
	key := b.st.key(id)
	for pass := range 2 {
		idx, at := b.lookupIndex, b.lookupOffset
		if pass == 1 {
			idx, at = 0, 0
		}
		for ; idx < b.n; idx++ {
			size := int64(binary.LittleEndian.Uint32(b.data[at:at+4]) &^ legacyRecordReady)
			start := at + materialPrefix
			rec := b.data[start : start+size]
			p := parseKV(rec)
			at = start + size
			if p.mod == mod && bytes.Equal(p.key, key) {
				b.lookupIndex, b.lookupOffset = idx+1, at
				return rec, nil, true
			}
		}
		if b.lookupIndex == 0 {
			break
		}
	}
	return nil, nil, false
}

func (st *store) promoteCanonical(rec []byte) {
	p := parseKV(rec)
	id, ok := st.find(p.key)
	if !ok {
		return
	}
	k := &st.keys[id]
	if k.rec != 0 || k.create == 0 || k.mod != p.mod || int(k.vlen) != len(p.value) {
		return
	}
	size := len(rec)
	st.loads++
	if _, cell := classOf(size); st.budget != 0 && (st.slab.used+int64(cell) > st.topMax() || !st.slab.reuses(size) && int64(st.slab.top+cell) > st.topMax()) {
		st.unkept++
		return
	}
	off, err := st.slab.alloc(size)
	if err != nil {
		st.unkept++
		return
	}
	copy(st.slab.bytes(off, size), rec)
	k.rec, k.rlen = off, uint32(size)
}

func (b *legacyBatch) initMaterialData() {
	at := 0
	b.walkMaterial(func(id uint32, ks ckptState) bool {
		key := b.st.key(id)
		size := kvSize(len(key), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)
		begin := at
		binary.LittleEndian.PutUint32(b.data[at:], uint32(size))
		at += materialPrefix
		k := &b.st.keys[id]
		if k.mod == ks.mod && k.rec != 0 {
			copy(b.data[at:at+size], b.st.slab.bytes(k.rec, int(k.rlen)))
			at += size
			binary.LittleEndian.PutUint32(b.data[begin:], uint32(size)|legacyRecordReady)
			return true
		}
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
		return true
	}, nil)
}
