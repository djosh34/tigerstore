package wl

import (
	"errors"
	"hash/crc32"
)

// Records split into parts (#919). A large reply's piece is built only as
// the client's share and windows take it now, so a record or event past
// that goes out in parts, one a piece. A part writes its record's fields
// from the key's state, as appendKV builds them, and reads only the bytes
// of the value it sends: from the slab where the record is resident,
// else from the log or the values fetched. Built whole for every part, a
// past revision's 2 MiB record was read and copied whole for each window's
// worth of it. The value's sum is checked as its parts go, in order
// (partSum), as readValue checks a value read whole.

// partSum is the sum of the bytes of the value at off sent so far, at of
// them.
type partSum struct {
	off int64
	at  int
	sum uint32
}

// valuePart is bytes a to b of key id's value of state s, unchecked: from
// its record in the slab if resident, else as readValue reads a value.
func (st *store) valuePart(id uint32, s *ckptState, a, b int) ([]byte, error) {
	if k := &st.keys[id]; k.rec != 0 && k.voff == s.voff && k.vlen == s.vlen {
		rec := st.slab.bytes(k.rec, int(k.rlen))
		at := kvSize(int(k.klen), int(s.vlen), s.create, s.mod, s.ver, s.lease) - sizeVarintField(6, uint64(s.lease)) - int(s.vlen)
		return rec[at+a : at+b], nil
	}
	if f, ok := st.fetched[s.voff]; ok && len(f.val) == int(s.vlen) {
		if f.err != nil {
			return nil, f.err
		}
		return f.val[a:b], nil
	}
	p, err := st.readLog(s.voff+int64(a), b-a)
	if errors.Is(err, errAway) {
		st.away = append(st.away, logValue{off: s.voff, len: s.vlen, sum: s.vsum})
	}
	return p, err
}

// readPart reads bytes a to b of key id's value of state s for a part, and
// sums them on from those before (ps). The last part of a value that does
// not match its sum fails, and stops the node as readValue does.
func (st *store) readPart(id uint32, s *ckptState, a, b int, ps *partSum) ([]byte, error) {
	if ps.off != s.voff || ps.at != a {
		// Another value's sum, or a part this value's sum missed: sum its
		// bytes before a first.
		*ps = partSum{off: s.voff}
		if a > 0 {
			p, err := st.valuePart(id, s, 0, a)
			if err != nil {
				return nil, err
			}
			ps.sum, ps.at = crc32.Update(0, castagnoli, p), a
		}
	}
	p, err := st.valuePart(id, s, a, b)
	if err != nil {
		return nil, err
	}
	ps.sum, ps.at = crc32.Update(ps.sum, castagnoli, p), b
	if b == int(s.vlen) && ps.sum != s.vsum {
		err := &ReadBackError{Err: ErrBadValue, Off: s.voff, Len: int(s.vlen)}
		if st.bad == nil {
			st.bad = err
		}
		return nil, err
	}
	return p, nil
}

// partGet reads the value bytes a part writes.
type partGet func(id uint32, s *ckptState, a, b int) []byte

// writeRec writes record field num of key id at state s through w, as
// appendKV builds it; get reads the bytes of its value that w keeps.
func writeRec(w *msgW, st *store, num int, id uint32, s *ckptState, get partGet) {
	key := st.key(id)
	w.lenField(num, kvSize(len(key), int(s.vlen), s.create, s.mod, s.ver, s.lease))
	w.bytesField(1, key)
	writeVarintNZ(w, 2, uint64(s.create))
	writeVarintNZ(w, 3, uint64(s.mod))
	writeVarintNZ(w, 4, uint64(s.ver))
	if s.vlen > 0 {
		w.lenField(5, int(s.vlen))
		w.value(int(s.vlen), func(a, b int) []byte { return get(id, s, a, b) })
	}
	writeVarintNZ(w, 6, uint64(s.lease))
}

// writeEventPart writes event ev of a revision of st through w, as
// writeEvent does; get reads the bytes of its values that w keeps.
func writeEventPart(w *msgW, st *store, ev *bigEv, get partGet) {
	ch := ev.ch
	w.lenField(11, ev.size)
	if ch.del {
		key := st.key(ch.key)
		w.varintField(1, 1)
		w.lenField(2, sizeBytesField(1, len(key))+sizeVarintField(3, uint64(ch.rev)))
		w.bytesField(1, key)
		w.varintField(3, uint64(ch.rev))
	} else {
		s, _ := st.putStateAt(ch.key, ch.rev)
		writeRec(w, st, 2, ch.key, &s, get)
	}
	if ev.prev {
		s, _ := st.stateAt(ch.key, ch.rev-1)
		writeRec(w, st, 3, ch.key, &s, get)
	}
}

// probePart reads, with the store's noWait, the value bytes that a part
// of room bytes from skip of what write writes takes, so that the part
// is written only once they are all in memory: errAway if one is not.
// It sums them on a copy of ps.
func (st *store) probePart(ps partSum, skip, room int, write func(*msgW, partGet)) error {
	var err error
	w := msgW{view: true, dry: true, skip: skip, room: room}
	write(&w, func(id uint32, s *ckptState, a, b int) []byte {
		if err == nil {
			_, err = st.readPart(id, s, a, b, &ps)
		}
		return nil
	})
	return err
}

// writePart writes what write writes through w, its value bytes read and
// summed on ps; it reports the first error a read met.
func (st *store) writePart(w *msgW, ps *partSum, write func(*msgW, partGet)) error {
	var err error
	write(w, func(id uint32, s *ckptState, a, b int) []byte {
		p, e := st.readPart(id, s, a, b, ps)
		if e != nil && err == nil {
			err = e
		}
		return p
	})
	return err
}
