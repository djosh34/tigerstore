package wl

import "hash/crc32"

// Hash and HashKV. etcd hashes its backend and its revisions since the
// last compaction; a node here hashes the records of every key live at a
// revision, in name order: the same on every node, whatever its key ids,
// and the same for a revision before and after it is compacted, as
// etcd's answers are.

// hashKV is the hash of the store at rev.
func (n *node) hashKV(rev int64) (uint32, error) {
	st := n.st
	h := crc32.New(castagnoli)
	var err error
	st.tree.scan(nil, nil, func(id uint32) bool {
		var rec []byte
		if rec, err = st.at(id, rev); err != nil {
			return false
		}
		if rec != nil {
			_, _ = h.Write(rec)
		}
		return true
	})
	return h.Sum32(), err
}

// hashRequest answers a Hash, the hash of the store as it is, or a
// HashKV, of the store at its revision, with etcd's errors for one
// compacted or still to come.
func (n *node) hashRequest(c *client, s *stream, msg []byte) error {
	st := n.st
	rev := int64(0)
	if s.kind == kHashKV {
		for m := msg; len(m) > 0; {
			num, _, v, _, rest, err := field(m)
			if err != nil {
				n.reject(c, s, badRequest(err))
				return nil
			}
			if num == 1 {
				rev = int64(v)
			}
			m = rest
		}
	}
	switch {
	case rev > st.rev:
		n.reject(c, s, requestErr(codeOutOfRange, msgFuture))
		return nil
	case rev > 0 && rev < st.compactRev:
		n.reject(c, s, requestErr(codeOutOfRange, msgCompacted))
		return nil
	case rev <= 0:
		rev = st.rev
	}
	sum, err := n.hashKV(rev)
	if err != nil {
		return err
	}
	size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(sum))
	// The compaction a HashKV's hash starts from: at the compacted
	// revision, the one before it; etcd names -1 for none.
	compact := st.compactRev
	if rev == st.compactRev {
		compact = st.prevCompactRev
	}
	if compact == 0 {
		compact = -1
	}
	if s.kind == kHashKV {
		size += sizeVarintField(3, uint64(compact)) + sizeVarintField(4, uint64(rev))
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	writeVarintNZ(&w, 2, uint64(sum))
	if s.kind == kHashKV {
		writeVarintNZ(&w, 3, uint64(compact))
		writeVarintNZ(&w, 4, uint64(rev))
	}
	n.endReply(c, s, &w)
	return nil
}
