package wl

import (
	"fmt"
	"math"
	"sort"
)

// A reply built at once is bounded by maxMsg (#721's second ruling): a
// Txn's, and a DeleteRange's with prev_kv. Their records' sizes are known
// from their keys' states, so the reply is sized before any value is
// read. One past maxMsg is refused with ResourceExhausted, as grpc-go
// refuses a message past its server's limit, and its writes still apply,
// as in etcd, where the refusal comes after the apply. Kubernetes' Txns
// range over one key, far below it.

// replyRoom is what the records of a reply built at once may take: maxMsg
// less room for the reply's other fields, its header and each
// operation's (a Txn has at most etcd's --max-txn-ops of them, 128 by
// default, a few dozen bytes each).
const replyRoom = maxMsg - 64<<10

// replyTooLarge refuses the reply of stream s, past maxMsg by its
// records' size alone.
func (n *node) replyTooLarge(c *client, s *stream, size int) {
	n.replyError(c, s.id, codeResourceExhausted, fmt.Sprintf("grpc: trying to send message larger than max (%d vs. %d)", size, maxMsg))
}

// recSize is the size of the record field of key id in state s as range
// r returns it: without its value if r is keys only, and then without its
// lease too unless r sorts by value (keysOnly).
func (n *node) recSize(r *rangeReq, id uint32, s *ckptState) int {
	klen := int(n.st.keys[id].klen)
	if r.keysOnly {
		lease := int64(0)
		if r.sortTarget == sortByValue {
			lease = s.lease
		}
		return sizeBytesField(2, kvSize(klen, 0, s.create, s.mod, s.ver, lease))
	}
	return sizeBytesField(2, kvSize(klen, int(s.vlen), s.create, s.mod, s.ver, s.lease))
}

// txnRange reads the records of Txn t's range r into t.kvs, once their
// sizes say t's reply takes them, and returns the range's count and
// whether its limit left some out. Its caller makes the records keys only
// for a range of keys only (keysOnly). Only the node that answers reads
// them, none once t's reply is refused.
//
// A range of keys picks its records from their states before any value
// is read, filtered, sorted and cut to its limit as rangeKVs does (etcd's
// executeRange), and reads only theirs (#731's ruling, B5): read first,
// a sort by mod revision with a limit of one loaded every candidate's
// value. One sorted by value reads its candidates, as a Range does, while
// they are one piece (oneShot), and is sized by the records it returns
// (B8); past one piece it is refused as a Range is. Kubernetes' Txns
// range over one key, which is read as before.
func (n *node) txnRange(t *txnRun, r *rangeReq) (count int64, more bool, err error) {
	if !t.reply {
		return 0, false, nil
	}
	st := n.st
	k0 := len(t.kvs)
	rev := max(r.rev, 0)
	if len(r.end) == 0 {
		size := 0
		if !r.countOnly {
			if id, ok := st.find(r.key); ok {
				if s, ok := st.stateAt(id, rev); ok && r.passes(&s) {
					size = n.recSize(r, id, &s)
				}
			}
		}
		if !t.sized(size) {
			return 0, false, nil
		}
		t.kvs, count, more, err = n.rangeKVs(r, t.kvs, 0)
		return count, more, err
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	if r.sortTarget == sortByValue && !r.countOnly {
		// Its candidates' records, as rangeKVs reads them all to sort,
		// past one piece as serveRange finds them: past it before the
		// next key.
		cand, past := int64(0), false
		st.tree.scan(r.key, end, func(id uint32) bool {
			s, ok := st.stateAt(id, rev)
			if !ok {
				return true
			}
			if cand > n.oneShot() {
				past = true
				return false
			}
			cand += int64(kvSize(int(st.keys[id].klen), int(s.vlen), s.create, s.mod, s.ver, s.lease))
			return true
		})
		if t.big {
			return 0, false, nil
		}
		if past {
			t.big, t.byValue = true, true
			return 0, false, nil
		}
		if t.kvs, count, more, err = n.rangeKVs(r, t.kvs, 0); err != nil {
			return 0, false, err
		}
		size := 0
		for _, kv := range t.kvs[k0:] {
			if r.keysOnly {
				kv = keysOnly(kv, r)
			}
			size += sizeBytesField(2, len(kv))
		}
		if !t.sized(size) {
			clear(t.kvs[k0:])
			t.kvs = t.kvs[:k0]
			return 0, false, nil
		}
		return count, more, nil
	}
	limit := int64(math.MaxInt64)
	if r.limit > 0 {
		limit = r.limit
	}
	sorts := r.sortsOther()
	var matched, taken int64
	keys := n.selKeys[:0]
	defer func() {
		clear(keys)
		n.selKeys = keys[:0]
		if cap(keys) > selKeep {
			n.selKeys = nil
		}
	}()
	read := r.readLimit()
	st.tree.scan(r.key, end, func(id uint32) bool {
		s, ok := st.stateAt(id, rev)
		if !ok {
			return true
		}
		count++
		if taken >= read {
			return true // past what the range reads, it only counts
		}
		taken++
		if r.countOnly || !r.passes(&s) {
			return true
		}
		if matched++; !sorts && matched > limit {
			return true // past its limit, it only counts
		}
		v := int64(0)
		switch r.sortTarget {
		case sortByVersion:
			v = s.ver
		case sortByCreate:
			v = s.create
		case sortByMod:
			v = s.mod
		}
		keys = append(keys, sortedKey{id: id, v: v})
		return true
	})
	if sorts {
		// As shapeKVs sorts the records, so ties come out the same.
		var x sort.Interface = sortedKeys{keys, st, r.sortTarget}
		if r.order() == sortDescend {
			x = sort.Reverse(x)
		}
		sort.Sort(x)
	}
	keys = keys[:min(int64(len(keys)), limit)]
	size := 0
	for _, k := range keys {
		s, _ := st.stateAt(k.id, rev)
		size += n.recSize(r, k.id, &s)
	}
	if !t.sized(size) {
		return 0, false, nil
	}
	for _, k := range keys {
		var rec []byte
		if r.keysOnly {
			rec = st.keyAt(k.id, rev)
		} else if rec, err = st.at(k.id, rev); err != nil {
			return 0, false, err
		}
		if rec != nil {
			t.kvs = append(t.kvs, rec)
		}
	}
	return count, r.limit > 0 && matched > r.limit, nil
}

// selKeep is the most keys txnRange's scratch keeps between Txns.
const selKeep = 4096

// delRecsSize is the size of the record fields a DeleteRange with
// prev_kv returns: its live keys' records before it, from their states.
func (n *node) delRecsSize(r *delReq) int {
	st := n.st
	size := func(id uint32) int {
		s, ok := st.stateAt(id, 0)
		if !ok {
			return 0
		}
		return sizeBytesField(3, kvSize(int(st.keys[id].klen), int(s.vlen), s.create, s.mod, s.ver, s.lease))
	}
	if len(r.end) == 0 {
		if id, ok := st.find(r.key); ok {
			return size(id)
		}
		return 0
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	total := 0
	st.tree.scan(r.key, end, func(id uint32) bool {
		total += size(id)
		return true
	})
	return total
}

// putPrevSize is the size of the record field a Put with prev_kv returns.
func (n *node) putPrevSize(key []byte) int {
	st := n.st
	id, ok := st.find(key)
	if !ok {
		return 0
	}
	s, ok := st.stateAt(id, 0)
	if !ok {
		return 0
	}
	return sizeBytesField(2, kvSize(int(st.keys[id].klen), int(s.vlen), s.create, s.mod, s.ver, s.lease))
}
