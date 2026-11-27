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
	stage := newReplyStage(&n.stageCredits, size, n.stageOther(promise))
	if stage == nil {
		n.giveRoom(c, false)
		return nil
	}
	stage.owner = c
	if c.stages == 0 && cap(c.out) < 64<<10 {
		c.socketPromise = 64 << 10
	}
	c.dropOut = true
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

// stageOther excludes only actual input debt backed by the independent
// input-turn reserve. Its logical unused grant is never spare output P.
func (n *node) stageOther(promise int64) int64 {
	input := n.cpool
	if n.inTurn != nil {
		base := n.turnLimit - n.turnRoom + n.looseGrown - n.turnLoose
		input -= min(n.turnRoom, max(0, n.cpool-base))
	}
	if n.cursorFetch {
		// Normal input admission remains entitled to the full read half.
		// Only actual active-turn debt belongs to the separate reserve.
		read := n.sizes.clientInBufs()
		future := max(0, int(read/int64(clientInBuf+clientPtBuf+slotBytes))-n.active)
		input = max(input, read) + n.headerSpare + int64(future)*maxHeaderBlock
	}
	other := input + n.cout - n.stageCredits.used + int64(max(n.clients, n.active)+n.stageOrphans)*idleConn + n.stagePromises + n.legacyPromises + promise
	if n.cursorFetch {
		other -= n.mainCaptureCount()
	}
	return other
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
		if h.stage != nil || h.flatCharge > 0 || h.material != nil {
			return true
		}
	}
	return false
}

func (c *client) releaseFlatOutput(h *held) {
	c.growOut(-int(h.flatCharge))
	h.flatCharge = 0
	c.stages--
	if c.stages == 0 {
		c.node.stagePromises -= c.stagePromise
		c.stagePromise = 0
		c.socketPromise = 0
	}
	if c.stages == 0 && c.fillOwners == 0 && c.stageOrphan {
		c.stageOrphan = false
		c.node.stageOrphans--
	}
	h.data = nil
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
		local, total := c.promiseLocal, c.promiseTotal
		if c.socketPromise > 0 {
			c.promiseLocal, c.promiseTotal = nil, nil
		}
		c.growOut(cap(b) - cap(c.out))
		c.promiseLocal, c.promiseTotal = local, total
		c.out = b
	}
	if c.socketPromise > 0 {
		c.stagePromise -= c.socketPromise
		c.node.stagePromises -= c.socketPromise
		c.socketPromise = 0
	}
	c.dropOut = true
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
		if h.material != nil {
			c.flow.bytes -= h.material.remaining
			c.releaseMaterial(h)
		}
		if h.flatCharge > 0 {
			c.flow.bytes -= int64(len(h.data))
			c.releaseFlatOutput(h)
		}
		if h.stage != nil {
			c.flow.bytes -= int64(h.stage.size - h.stage.read)
			h.stage.discard()
			h.stage = nil
		}
	}
}
