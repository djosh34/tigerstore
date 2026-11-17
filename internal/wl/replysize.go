package wl

import (
	"fmt"
	"math"
	"slices"
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

// rangeRecsSize is the size of the record fields range r returns, from
// its keys' states alone: no value is read. It follows rangeKVs: the
// range's keys at its revision, filtered, sorted and cut to its limit.
// Which records a range sorted by value keeps under a limit depends on
// the values, so for one it is the most that limit many of them take.
func (n *node) rangeRecsSize(r *rangeReq) int {
	st := n.st
	if r.countOnly {
		return 0
	}
	rev := max(r.rev, 0)
	size := func(id uint32, s *ckptState) int {
		klen := int(st.keys[id].klen)
		if r.keysOnly {
			lease := int64(0)
			if r.sortTarget == sortByValue {
				lease = s.lease
			}
			return sizeBytesField(2, kvSize(klen, 0, s.create, s.mod, s.ver, lease))
		}
		return sizeBytesField(2, kvSize(klen, int(s.vlen), s.create, s.mod, s.ver, s.lease))
	}
	if len(r.end) == 0 {
		id, ok := st.find(r.key)
		if !ok {
			return 0
		}
		if s, ok := st.stateAt(id, rev); ok && r.passes(&s) {
			return size(id, &s)
		}
		return 0
	}
	end := r.end
	if len(end) == 1 && end[0] == 0 {
		end = nil
	}
	limit := int64(math.MaxInt64)
	if r.limit > 0 {
		limit = r.limit
	}
	sorts := r.sortsOther()
	total, taken := 0, int64(0)
	keys := n.sizeKeys[:0]
	st.tree.scan(r.key, end, func(id uint32) bool {
		s, ok := st.stateAt(id, rev)
		if !ok || !r.passes(&s) {
			return true
		}
		if sorts {
			v := int64(size(id, &s))
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
		}
		if taken == limit {
			return false
		}
		taken++
		total += size(id, &s)
		return true
	})
	if sorts {
		switch {
		case int64(len(keys)) <= limit:
		case r.sortTarget == sortByValue:
			// The largest limit many, whichever the values pick.
			slices.SortFunc(keys, func(a, b sortedKey) int { return int(b.v - a.v) })
		default:
			// As planBig sorts them, so ties come out the same.
			var x sort.Interface = sortedKeys{keys, st, r.sortTarget}
			if r.order() == sortDescend {
				x = sort.Reverse(x)
			}
			sort.Sort(x)
		}
		for _, k := range keys[:min(int64(len(keys)), limit)] {
			s, _ := st.stateAt(k.id, rev)
			total += size(k.id, &s)
		}
	}
	clear(keys)
	n.sizeKeys = keys[:0]
	return total
}

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
