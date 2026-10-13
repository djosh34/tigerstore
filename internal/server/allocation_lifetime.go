package server

// All lifetime changes below run under c.mu. A stopped timer may still have
// an active callback: only Stop()==true transfers its reference to us.
func (c *conn) stopDeadlineLocked(st *stream) {
	if st.deadline != nil && st.deadline.Stop() && st.slot != nil {
		st.slot.timerRef = false
	}
}

func (c *conn) dropManagedCompletions(head *allocSlot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for s := head; s != nil; {
		next := s.compNext
		s.compNext = nil
		s.nodeRef = false
		s.st.atNode = false
		c.releaseLocked(&s.st)
		c.retireSlotLocked(s)
		s = next
	}
}

// readerDone is the only cleanup that may release queued-but-unsubmitted
// batches. close can race the reader, and must not recycle those links.
func (c *conn) readerDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for st := c.managedHead; st != nil; {
		next := st.slot.submitNext
		st.slot.queueRef = false
		c.retireSlotLocked(st.slot)
		st = next
	}
	c.managedHead, c.managedTail, c.managedCount, c.managedBytes = nil, nil, 0, 0
	c.sealArenaLocked()
}

func (c *conn) frameOwner(st *stream) *allocSlot {
	if st.slot != nil {
		st.slot.frameRefs++
	}
	return st.slot
}

// Writer frames retain response aliases through the ACTUAL flush, including
// its error/abort path, not merely endStream's removal from the stream map.
func (c *conn) retireFrames(frames []wframe) {
	if c.s.alloc == nil {
		clear(frames)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range frames {
		if s := frames[i].owner; s != nil {
			s.frameRefs--
			c.retireSlotLocked(s)
		}
		frames[i] = wframe{}
	}
}
