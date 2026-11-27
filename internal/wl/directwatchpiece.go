package wl

func (n *node) directWatchValue(b *bigWatch, ev bigEv, prev bool) ([]byte, error) {
	// Borrow main's immutable apply event ONLY in flushEvents' non-yielding
	// turn. No live value pointer survives that call or enters a source plan.
	for i := b.liveFirst; len(b.liveEvents) > 0 && i >= 0; i = b.liveEvents[i].next {
		live := &b.liveEvents[i]
		if live.key == ev.ch.key && live.rev == ev.ch.rev && live.del == ev.ch.del {
			if prev {
				return live.prev, nil
			}
			return live.kv, nil
		}
	}
	if prev {
		return b.st.residentAt(ev.ch.key, ev.ch.rev-1)
	}
	return b.st.residentPutAt(ev.ch.key, ev.ch.rev)
}

func (n *node) buildDirectWatch(b *bigWatch, c *client, s *stream) bool {
	if b.directHead > 0 {
		stage := n.directStage(c, s, b.directHead, b.pos, true, func(w *msgW) {
			directPrefix(w, b.directBody)
			n.writeHdrTerm(w, b.hdrRev, b.hdrTerm)
			writeVarintNZ(w, 2, uint64(b.id))
			if b.frag {
				w.varintField(7, 1)
			}
		})
		if stage == nil {
			b.waitP = n.directCapacity(c, s, true) > 0
			return true
		}
		b.directHead -= stage.size
		b.pos += stage.size
		b.msgLeft -= stage.size
		b.stage = stage
		if b.directHead == 0 {
			b.pos = 0
		}
		return true
	}
	if b.pieceLeft > 0 && b.resume.taken >= 0 {
		return n.startRawWatch(b)
	}
	var wanted bigEv
	off := -1
	err := n.eachRevEvent(b.st, &b.x, revStart(b.st, b.rev), b.next, func(i int, ev bigEv) bool {
		if i < b.end {
			wanted = ev
			off = i
		}
		return false
	})
	if err != nil || off < 0 {
		n.abortBigWatch(c, s)
		return false
	}
	full := sizeBytesField(11, wanted.size)
	if b.pieceLeft == 0 {
		b.pieceLeft = full
		b.resume = fillResume{taken: -1, watch: fillWatch{wi: b.wi, inc: b.inc, from: b.rev, to: b.rev + 1, rev: b.rev, compact: b.st.compactRev, offset: b.next, stopOffset: off + 1}}
	}
	ev := event{key: wanted.ch.key, rev: wanted.ch.rev, del: wanted.ch.del}
	if !ev.del {
		ev.kv, err = n.directWatchValue(b, wanted, false)
		if err != nil {
			return n.startRawWatch(b)
		}
	}
	offset := full - b.pieceLeft
	left := b.pieceLeft
	outer := full - wanted.size
	var current int
	if ev.del {
		current = 2 + sizeBytesField(2, sizeBytesField(1, len(b.st.key(ev.key)))+sizeVarintField(3, uint64(ev.rev)))
	} else {
		current = sizeBytesField(2, len(ev.kv))
	}
	if wanted.prev {
		ev.prev, err = n.directWatchValue(b, wanted, true)
		if err != nil {
			// Emit the still-resident current value through MAIN's serializer;
			// the cold previous value starts the continuation, not another message.
			if offset >= outer+current {
				return n.startRawWatch(b)
			}
			left = min(left, outer+current-offset)
		}
	}
	stage := n.directStage(c, s, left, offset, true, func(w *msgW) { n.writeEventSized(w, &b.x, &ev, wanted.size) })
	if stage == nil {
		b.waitP = n.directCapacity(c, s, true) > 0
		return true
	}
	seed := b.resume.record.crc
	if !ev.del {
		ks, _ := b.st.putStateAt(ev.key, ev.rev)
		seed = directValueCRC(seed, offset, stage.size, outer+directValueStart(2, b.st.key(ev.key), &ks), parseKV(ev.kv).value)
	}
	if ev.prev != nil {
		ks, _ := b.st.stateAt(ev.key, ev.rev-1)
		seed = directValueCRC(seed, offset, stage.size, outer+current+directValueStart(3, b.st.key(ev.key), &ks), parseKV(ev.prev).value)
	}
	b.resume.record.crc = seed
	b.msgLeft -= stage.size
	b.pieceLeft -= stage.size
	b.stage = stage
	if b.pieceLeft == 0 {
		b.next = off + 1
		b.resume = fillResume{}
	}
	return true
}
