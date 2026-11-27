package wl

import "errors"

// watchWire includes every fragment's own prefix and repeated header. The
// original fragment boundaries/representation are unchanged.
func (n *node) watchWire(x *watcher, size int, each func(func(int))) int {
	if !x.fragment || size < maxRequestBytes {
		return 5 + size
	}
	hdr := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
	part, count, wire, events := hdr+2, 0, 0, 0
	each(func(add int) {
		if count > 0 && part+add >= maxRequestBytes {
			wire += 5 + part
			part = hdr + 2
			count = 0
		}
		part += add
		count++
		events++
	})
	if events < 2 {
		return 5 + size
	}
	return wire + 5 + part - 2
}

func (n *node) linkedWatchWire(x *watcher, first int32, size int) int {
	return n.watchWire(x, size, func(add func(int)) {
		for e := first; e >= 0; e = n.w.events[e].next {
			add(sizeBytesField(11, n.eventSize(x, &n.w.events[e])))
		}
	})
}

func (n *node) tryResidentWatch(x *watcher, c *client, s *stream, b *syncBudget) (handled, done bool, err error) {
	return n.residentWatchRead(x, c, s, b, n.st.residentAt, n.st.residentPutAt)
}

// Stage only main's selected event descriptors in the node-global scratch.
// A cold value discards the whole unsent batch, including provisional budget.
// eachRevEvent preserves the exact planted live sequence as on the cursor path.
func (n *node) residentWatchRead(x *watcher, c *client, s *stream, b *syncBudget, at, putAt func(uint32, int64) ([]byte, error)) (handled, done bool, err error) {
	if x.fetchWait || !b.left() || c.slow() {
		return true, false, nil
	}
	room := n.connOut - c.waiting()
	if room <= 0 {
		return true, false, nil
	}
	before := *b
	all := b.bytes
	b.bytes = min(all, room)
	st := n.st
	to := x.syncRev
	j := revStart(st, to)
	evs := n.w.syncBuf[:0]
	defer func() { clear(evs); n.w.syncBuf = evs[:0] }()
	hdr := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(x.id))
	size := hdr
	for j < len(st.changes) && b.left() {
		rev := st.changes[j].rev
		revSize, e := n.revSize(x, j)
		if e != nil {
			*b = before
			return true, false, e
		}
		if revSize > replyRoom {
			if len(evs) == 0 {
				*b = before
				return false, false, nil
			}
			break
		}
		var readErr error
		e = n.eachRevEvent(st, x, j, 0, func(_ int, wanted bigEv) bool {
			ev := event{key: wanted.ch.key, rev: wanted.ch.rev, del: wanted.ch.del}
			if !ev.del {
				ev.kv, readErr = putAt(ev.key, ev.rev)
				if readErr != nil {
					return false
				}
			}
			if wanted.prev {
				ev.prev, readErr = at(ev.key, ev.rev-1)
				if readErr != nil {
					return false
				}
			}
			evs = append(evs, ev)
			size += sizeBytesField(11, n.eventSize(x, &ev))
			b.events--
			b.bytes -= int64(len(ev.kv) + len(ev.prev))
			if ev.del {
				b.bytes -= int64(st.keys[ev.key].klen)
			}
			return true
		})
		if e != nil {
			*b = before
			return true, false, e
		}
		if errors.Is(readErr, errResidentCold) {
			*b = before
			return false, false, nil
		}
		if readErr != nil {
			*b = before
			return true, false, readErr
		}
		next := revStart(st, rev+1)
		b.changes -= next - j
		j = next
		to = rev + 1
	}
	done = j == len(st.changes)
	if done {
		to = st.rev + 1
	}
	b.bytes = all - (min(all, room) - b.bytes)
	if len(evs) > 0 {
		wire := n.watchWire(x, size, func(add func(int)) {
			for i := range evs {
				add(sizeBytesField(11, n.eventSize(x, &evs[i])))
			}
		})
		if !n.replyRoom(c, s, int64(wire), int64(5+size)) {
			return true, false, nil
		}
		n.writeEvents(c, s, x, evs)
		n.commitReplyStage(c, s)
	}
	x.syncRev = to
	x.deliveryPlant.retire(to)
	return true, done, nil
}
