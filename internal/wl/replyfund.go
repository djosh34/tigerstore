package wl

// replyRoom keeps the existing whole-or-wait outcome and capture format.
// A ready ordinary reply cannot spend another fill's P or the input turn.
func (n *node) replyRoom(c *client, s *stream, wire int64, logical ...int64) bool {
	quota := wire
	if len(logical) > 0 {
		quota = logical[0]
	}
	if !n.cursorFetch || n.legacyRetry != nil {
		return n.msgRoom(c, quota)
	}
	protected := n.stageCredits.used > 0 || n.stagePromises > 0 || n.legacyPromises > 0
	if !protected && n.roomNeeded(c, quota) == 0 {
		return n.msgRoom(c, quota)
	}
	// A fully funded existing socket needs no new storage. Keep the hot
	// within-share path allocation-free, even while another fill runs.
	room := c.room()
	if c.rec < 0 && cap(c.out)-len(c.out) >= maxPlain+recOverhead+8 {
		room = maxPlain
	}
	need := 3*int(wire) + len(respHeaders) + len(respTrailers) + 128
	if !s.holds && !s.big && c.flow.conn >= wire && s.win >= wire && room >= need && cap(c.out)-len(c.out) >= need+2*recOverhead {
		before := c.roomHeld
		if !n.msgRoom(c, quota) {
			return false
		}
		covered := c.roomHeld - before
		c.coveredRoom += covered
		n.coveredRoom += covered
		n.reserved -= covered
		if cap(c.out) > 2*c.outBase {
			c.dropOut = true
		}
		return true
	}
	if c.replyStage != nil {
		return false
	}
	c.replyStage = n.admitStageQuota(c, int(wire), quota)
	return c.replyStage != nil
}

func (n *node) dropReplyStage(c *client) {
	if c.replyStage != nil {
		c.replyStage.discard()
		c.replyStage = nil
	}
}

func (n *node) commitReplyStage(c *client, s *stream) {
	stage := c.replyStage
	if stage == nil {
		return
	}
	c.replyStage = nil
	stage.responsePending = !s.streaming
	s.streaming = true
	if !c.holdStage(s, stage) {
		stage.discard()
		c.broken = true
		return
	}
	n.markDirty(&c.sock)
}
