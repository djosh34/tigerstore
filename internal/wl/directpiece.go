package wl

import (
	"encoding/binary"
	"hash/crc32"
)

// Direct output uses main's serializer, viewed through a bounded destination.
// No source cursor/plan is made for resident records, even for a partial KV.
func (s *replyStage) appendViewport(p []byte) {
	skip := min(s.skip, len(p))
	s.skip -= skip
	p = p[skip:]
	s.append(p[:min(len(p), s.size-s.written)])
}

// Never park resident payload behind a closed HTTP/2 window. A direct piece
// fits both windows AND the funded socket copy guard, and drains this turn.
// In particular maxClients blocked readers cannot pin one 2-MiB copy each.
func (n *node) directCapacity(c *client, s *stream, watch bool) int {
	win := s.win
	if watch {
		win = c.streamWin(s)
	}
	return int(max(0, min(int64(stagePayload), c.flow.conn, win, n.connOut-c.waiting(), int64(rawSocketRoom(c)))))
}

func (n *node) directStage(c *client, s *stream, left, skip int, watch bool, write func(*msgW)) *replyStage {
	size := min(left, n.directCapacity(c, s, watch))
	if size <= 0 {
		return nil
	}
	stage := n.admitStage(c, size)
	if stage == nil {
		n.roomShort = true
		return nil
	}
	stage.flags |= stageViewport
	stage.skip = skip
	w := msgW{stage: stage, left: skip + left}
	write(&w)
	stage.flags &^= stageViewport
	if stage.skip != 0 || !stage.commitPiece() {
		stage.discard()
		c.broken = true
		return nil
	}
	c.stageSocket()
	return stage
}

func directPrefix(w *msgW, body int) {
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(body))
	w.write(p[:])
}

// CRC is only a scalar continuation seed, not a retained source plan. It is
// needed if a resident value is evicted after some of its bytes reached DATA.
func directValueCRC(seed uint32, offset, size, valueStart int, value []byte) uint32 {
	start, end := max(offset, valueStart), min(offset+size, valueStart+len(value))
	if end <= start {
		return seed
	}
	if start == valueStart {
		seed = 0
	}
	return crc32.Update(seed, castagnoli, value[start-valueStart:end-valueStart])
}

func directValueStart(field int, key []byte, ks *ckptState) int {
	size := kvSize(len(key), int(ks.vlen), ks.create, ks.mod, ks.ver, ks.lease)
	return sizeBytesField(field, size) - size + sizeBytesField(1, len(key)) + sizeVarintField(2, uint64(ks.create)) + sizeVarintField(3, uint64(ks.mod)) + sizeVarintField(4, uint64(ks.ver)) + sizeBytesField(5, int(ks.vlen)) - int(ks.vlen)
}

// A large logical owner retains only P-funded metadata, never pointers into
// receive buffers. Unlike a private fill/worker it need not pin those buffers
// while its peer advertises a zero window. Orphan accounting still uses the
// full fillOwners count until its metadata is retired.
func (c *client) hasPrivateFill() bool {
	owners := c.fillOwners
	if owners == 0 {
		return false
	}
	for _, b := range c.node.bigs {
		if b.c == c && b.directMeta > 0 {
			owners--
		}
	}
	for _, b := range c.node.bigWatches {
		if b.c == c && b.directMeta > 0 {
			owners--
		}
	}
	return owners > 0
}

// Fund fixed owner/control and the unchanged request copy before allocation.
// The inherited sort/key scratch remains in the existing P debits.
func (n *node) takeDirectOwner(c *client, request int) int {
	meta := ((1024 + request + stageUnit - 1) / stageUnit) * stageUnit
	n.stageCredits.limit = n.sizes.clientPool
	if !n.stageCredits.take(int64(meta), n.stageOther(0)) {
		return 0
	}
	c.growOut(meta)
	c.heldCounted += int64(meta)
	c.fillOwners++
	return meta
}

func (n *node) freeDirectOwner(c *client, meta int) {
	if meta == 0 {
		return
	}
	c.growOut(-meta)
	c.heldCounted -= int64(meta)
	c.fillOwners--
	if c.fillOwners == 0 && c.stages == 0 && c.stageOrphan {
		c.stageOrphan = false
		n.stageOrphans--
	}
	n.stageCredits.give(int64(meta))
	n.fillCreditWake = true
}

func (n *node) buildDirectRange(b *bigRange, c *client, s *stream) bool {
	if b.directHead > 0 {
		stage := n.directStage(c, s, b.directHead, b.pos, false, func(w *msgW) {
			size := b.directBody
			if b.stream {
				size = sizeBytesField(1, size)
			}
			directPrefix(w, size)
			if b.stream {
				w.lenField(1, b.directBody)
			}
			if !b.stream || b.msgClose {
				n.writeHdrTerm(w, b.hdrRev, b.hdrTerm)
			}
		})
		if stage == nil {
			b.waitP = n.directCapacity(c, s, false) > 0
			return true
		}
		n.openStream(c, s)
		b.directHead -= stage.size
		b.pos += stage.size
		b.msgLeft -= stage.size
		b.stage = stage
		if b.directHead == 0 {
			b.pos = 0
		}
		return true
	}
	if b.msgRecs == 0 && b.msgLeft == 0 {
		n.beginChunk(b, s)
		return true
	}
	if b.msgRecs == 0 {
		stage := n.directStage(c, s, b.msgLeft, b.pos, false, func(w *msgW) {
			if b.msgClose {
				if b.msgMore {
					w.varintField(3, 1)
				}
				writeVarintNZ(w, 4, uint64(b.msgCount))
			}
		})
		if stage == nil {
			b.waitP = n.directCapacity(c, s, false) > 0
			return true
		}
		b.msgLeft -= stage.size
		b.pos += stage.size
		b.stage = stage
		if b.msgLeft == 0 {
			b.pos = 0
		}
		return true
	}
	if b.pieceLeft > 0 && b.resume.taken >= 0 {
		return n.startRawRange(b)
	}
	if b.pieceLeft == 0 {
		b.piece = b.piece[:0]
		n.walk(b, func(id uint32, _ int) bool { b.piece = append(b.piece, id); return false })
		b.recount()
		if len(b.piece) == 0 {
			n.abortBig(c, s)
			return false
		}
	}
	return n.directRangeRecord(b, c, s)
}

func (n *node) directRangeRecord(b *bigRange, c *client, s *stream) bool {
	id := b.piece[0]
	ks, ok := b.st.stateAt(id, b.rev)
	if !ok {
		n.abortBig(c, s)
		return false
	}
	var rec []byte
	if !b.req.keysOnly {
		var err error
		rec, err = b.st.residentAt(id, b.rev)
		if err != nil {
			return n.startRawRange(b)
		}
	} else {
		rec = keysOnly(b.st.keyAt(id, b.rev), &b.req)
	}
	full := sizeBytesField(2, len(rec))
	if b.pieceLeft == 0 {
		b.pieceLeft = full
		b.resume = fillResume{taken: -1}
	}
	offset := full - b.pieceLeft
	stage := n.directStage(c, s, b.pieceLeft, offset, false, func(w *msgW) { w.bytesField(2, rec) })
	if stage == nil {
		b.waitP = n.directCapacity(c, s, false) > 0
		return true
	}
	if !b.req.keysOnly {
		b.resume.record.crc = directValueCRC(b.resume.record.crc, offset, stage.size, directValueStart(2, b.st.key(id), &ks), parseKV(rec).value)
	}
	b.msgLeft -= stage.size
	b.pieceLeft -= stage.size
	b.stage = stage
	if b.pieceLeft == 0 {
		b.msgRecs--
		b.taken++
		if b.sorts {
			b.next++
		} else {
			b.after = append(b.after[:0], b.st.key(id)...)
			b.begun = true
		}
		b.resume = fillResume{}
	}
	b.recount()
	return true
}

// Actual cold handoff only: reconstitute the source cursor's metadata at the
// already-published byte offset, without reading/copying any preceding value.
func (n *node) seekColdFill(q *replyFill, skip int, seed uint32) error {
	consume := func(size int) int { take := min(skip, size); skip -= take; return take }
	for skip > 0 {
		if !q.record.active {
			ok, err := n.nextFillRecord(q)
			if err != nil {
				return err
			}
			if !ok {
				return errLogCut
			}
		}
		q.outerPos += consume(q.outerSize - q.outerPos)
		if skip == 0 {
			return nil
		}
		r := &q.record
		s := &r.state
		key := q.st.key(r.id)
		switch r.phase {
		case 0:
			p := appendFillLen(r.atom[:0], r.field, r.size)
			p = appendFillLen(p, 1, len(key))
			r.atomPos, r.atomSize = 0, len(p)
			r.phase = 1
		case 1:
			r.atomPos += consume(r.atomSize - r.atomPos)
			if r.atomPos == r.atomSize {
				r.phase = 2
			}
		case 2:
			r.keyPos += consume(len(key) - r.keyPos)
			if r.keyPos == len(key) {
				p := r.atom[:0]
				for _, kv := range [...]struct {
					field int
					value int64
				}{{2, s.create}, {3, s.mod}, {4, s.ver}} {
					if kv.value != 0 {
						p = appendVarintField(p, kv.field, uint64(kv.value))
					}
				}
				if s.vlen > 0 {
					p = appendFillLen(p, 5, int(s.vlen))
				}
				r.atomPos, r.atomSize = 0, len(p)
				r.phase = 3
			}
		case 3:
			r.atomPos += consume(r.atomSize - r.atomPos)
			if r.atomPos == r.atomSize {
				r.phase = 4
			}
		case 4:
			r.valuePos += consume(int(s.vlen) - r.valuePos)
			r.crc = seed
			if r.valuePos == int(s.vlen) {
				r.leaseAtom()
				r.phase = 5
			}
		case 5:
			r.atomPos += consume(r.atomSize - r.atomPos)
			if r.atomPos == r.atomSize {
				r.active = false
				q.taken++
			}
		}
	}
	return nil
}
