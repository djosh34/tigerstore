package wl

// admitStage funds physical destination capacity in the client pool.
// Logical output-reserve quota remains charged, but it cannot encumber
// the physical input turn: its backing here is independent and irrevocable.
func (n *node) admitStage(c *client, size int) *replyStage {
	before := c.roomHeld
	if !n.msgRoom(c, int64(size)) {
		return nil
	}
	promise := int64(0)
	if c.stages == 0 {
		promise = stageUnit // protected copy/control guard
		if cap(c.out) < 64<<10 {
			// Old and new socket arrays can coexist during a growth.
			promise += 64 << 10
		}
	}
	n.stageCredits.limit = n.sizes.clientPool
	other := n.cpool + n.cout - n.stageCredits.used + int64(max(n.clients, n.active)+n.stageOrphans)*idleConn + n.stagePromises + promise
	if n.inTurn != nil {
		base := n.turnLimit - n.turnRoom + n.looseGrown - n.turnLoose
		other -= min(n.turnRoom, max(0, n.cpool-base))
	}
	stage := newReplyStage(&n.stageCredits, size, other)
	if stage == nil {
		n.giveRoom(c, false)
		return nil
	}
	stage.owner = c
	c.growOut(int(stage.charged))
	// Physical capacity belongs to outPool, not heldCounted (parked
	// requests/captures). An unpublished fill must not manufacture a
	// busy refusal before ordinary finished output exists.
	c.stagePending += int64(size)
	c.stages++
	c.stagePromise += promise
	n.stagePromises += promise
	covered := c.roomHeld - before
	c.coveredRoom += covered
	n.coveredRoom += covered
	n.reserved -= covered
	return stage
}

// holdStage is the complete-small-message commit point. Publication and
// its caller's cursor update occur on the loop, before any other event.
// A closed window does not leave a committed response tied to the store.
func (c *client) holdStage(s *stream, stage *replyStage) bool {
	if stage.owner != c || !stage.commit() {
		return false
	}
	f := &c.flow
	if !s.holds {
		s.holds = true
		if f.streams == nil {
			f.streams, f.counts = map[uint32]int64{}, map[uint32]int{}
		}
		f.streams[s.id] = s.win
	}
	f.held = append(f.held, held{sid: s.id, stage: stage})
	f.bytes += int64(stage.size)
	f.counts[s.id]++
	if f.counts[s.id] == 1 && f.conn > 0 {
		c.drain()
	}
	return true
}

func (c *client) hasStageOutput() bool {
	if c.stages == 0 {
		return false
	}
	for _, h := range c.flow.held {
		if h.stage != nil {
			return true
		}
	}
	return false
}

// stageSocket prepares a bounded socket array out of the finish credit
// funded before the stage was admitted. A legacy larger socket remains
// charged at its existing capacity; new stage bytes do not grow it.
func (c *client) stageSocket() {
	if c.ow > 0 {
		if c.rec >= 0 {
			c.rec -= c.ow
		}
		c.out = c.out[:copy(c.out, c.out[c.ow:])]
		c.ow = 0
	}
	if cap(c.out) < 64<<10 {
		b := make([]byte, len(c.out), 64<<10)
		copy(b, c.out)
		c.growOut(cap(b) - cap(c.out))
		c.out = b
	}
}

func (c *client) drainStage(h *held) {
	c.stageSocket()
	for p := h.stage.front(); len(p) > 0; p = h.stage.front() {
		// Leave a complete record's growth room so openRec never has to
		// double this socket for a stage. Framing/crypto stay in c.out.
		space := min(64<<10-len(c.out), cap(c.out)-len(c.out)) - (maxPlain + 2*recOverhead + 64)
		room := c.node.connOut - c.waiting()
		m := min(int64(len(p)), c.flow.conn, c.flow.streams[h.sid], int64(space), room)
		if m <= 0 {
			return
		}
		w := msgW{s: &c.sock, sid: h.sid, left: int(m)}
		w.write(p[:m])
		c.flow.bytes -= m
		c.flow.conn -= m
		c.flow.streams[h.sid] -= m
		h.stage.consume(int(m))
		c.node.markDirty(&c.sock)
	}
}

// dropStageOutput removes the stage part of a closing client's output
// before legacy dropAll accounts its flat buffers. No byte is freed twice.
func (c *client) dropStageOutput() {
	for i := range c.flow.held {
		h := &c.flow.held[i]
		if h.stage != nil {
			c.flow.bytes -= int64(h.stage.size - h.stage.read)
			h.stage.discard()
			h.stage = nil
		}
	}
}
