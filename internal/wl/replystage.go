package wl

// A fetch destination is private until validated and committed. A worker
// loan keeps its storage quarantined even after the stream is cancelled.
const (
	stageUnit    = 8 << 10
	stageHead    = 64
	stagePayload = stageUnit - stageHead
	stageSlots   = 512
)

type stagePage [stageUnit]byte

type stageRoot struct {
	next  *stageRoot
	pages [stageSlots]*stagePage
}

// stageCredit is the physical client-pool ledger. Other client owners
// and protected finish commitments are supplied on admission. The input
// turn's reserve is not available for these allocations.
type stageCredit struct {
	limit int64
	used  int64
}

func (b *stageCredit) take(n, other int64) bool {
	if n < 0 || other < 0 || n > b.limit || other > b.limit-n || b.used > b.limit-n-other {
		return false
	}
	b.used += n
	return true
}

func (b *stageCredit) give(n int64) bool {
	if n < 0 || n > b.used {
		return false
	}
	b.used -= n
	return true
}

// The first root and the stage's control fit in one charged index unit.
// No page has an exterior slice header. read is a message offset, not
// a position in a resliced allocation whose prefix still stays live.
type replyStage struct {
	root            stageRoot
	credit          *stageCredit
	owner           *client
	size            int
	written         int
	read            int
	charged         int64
	loans           uint32
	flags           uint32
	responsePending bool
}

const (
	stageDiscard = 1 << iota
	stageFreed
	stageCommitted
	stageBroken
	stageStreaming
)

func stageCapacity(n int) int64 {
	if n < 0 {
		return -1
	}
	pages := n / stagePayload
	if n%stagePayload != 0 {
		pages++
	}
	roots := max(1, (pages+stageSlots-1)/stageSlots)
	return int64(pages+roots) * stageUnit
}

// newReplyStage reserves every allocation before creating any payload.
// A failed admission neither changes the ledger nor allocates a stage.
func newReplyStage(b *stageCredit, n int, other int64) *replyStage {
	charge := stageCapacity(n)
	if charge < 0 || !b.take(charge, other) {
		return nil
	}
	s := &replyStage{credit: b, size: n, charged: charge}
	r := &s.root
	pages := n / stagePayload
	if n%stagePayload != 0 {
		pages++
	}
	for i := range pages {
		if i > 0 && i%stageSlots == 0 {
			r.next = &stageRoot{}
			r = r.next
		}
		r.pages[i%stageSlots] = &stagePage{}
	}
	return s
}

func (s *replyStage) page(at int) *stagePage {
	r := &s.root
	i := at / stagePayload
	for i >= stageSlots {
		r = r.next
		i -= stageSlots
	}
	return r.pages[i]
}

// span is an exclusive, unpublished destination. It cannot outlive the
// owning worker loan or be used after the message enters ordered output.
func (s *replyStage) span(at, n int) []byte {
	if s.flags&(stageDiscard|stageFreed|stageCommitted) != 0 || at < 0 || n < 0 || at > s.size || n > s.size-at {
		s.flags |= stageBroken
		return nil
	}
	if n == 0 {
		return nil
	}
	p := s.page(at)
	off := at % stagePayload
	n = min(n, stagePayload-off)
	return p[stageHead+off : stageHead+off+n]
}

func (s *replyStage) append(p []byte) {
	for len(p) > 0 {
		to := s.span(s.written, len(p))
		if len(to) == 0 {
			return
		}
		n := copy(to, p)
		s.written += n
		p = p[n:]
	}
}

// commit transfers a complete small message to ordered output. It does
// not require either a positive HTTP/2 window or a successful TCP write.
func (s *replyStage) commit() bool {
	if s.flags != 0 || s.loans != 0 || s.written != s.size {
		return false
	}
	s.flags |= stageCommitted
	if s.owner != nil {
		s.owner.stagePending -= int64(s.size)
	}
	return true
}

// commitPiece publishes a validated part of an already-large message.
// Its remaining bytes stay logically pending until copied to the socket.
func (s *replyStage) commitPiece() bool {
	if s.flags != 0 || s.loans != 0 || s.written != s.size {
		return false
	}
	s.flags |= stageCommitted | stageStreaming
	return true
}

func (s *replyStage) borrow() bool {
	if s.flags != 0 {
		return false
	}
	s.loans++
	return true
}

func (s *replyStage) acknowledge() bool {
	if s.loans == 0 {
		return false
	}
	s.loans--
	if s.loans == 0 && s.flags&stageDiscard != 0 {
		s.free()
	}
	return true
}

// discard is idempotent; it cannot lend an in-flight worker's pages or
// credit to another request. Completion, not cancellation, releases them.
func (s *replyStage) discard() {
	if s.flags&stageFreed != 0 {
		return
	}
	s.flags |= stageDiscard
	if s.loans == 0 {
		s.free()
	}
}

func (s *replyStage) free() {
	for r := &s.root; r != nil; {
		clear(r.pages[:])
		next := r.next
		r.next = nil
		r = next
	}
	if s.owner != nil {
		c := s.owner
		c.growOut(-int(s.charged))
		if s.flags&stageCommitted == 0 {
			c.stagePending -= int64(s.size)
		} else if s.flags&stageStreaming != 0 {
			c.stagePending -= int64(s.size - s.read)
		}
		c.stages--
		if c.stages == 0 {
			c.node.stagePromises -= c.stagePromise
			c.stagePromise = 0
			c.socketPromise = 0
			if c.stageOrphan && c.fillOwners == 0 {
				c.stageOrphan = false
				c.node.stageOrphans--
			}
		}
	}
	if !s.credit.give(s.charged) {
		s.flags |= stageBroken
		return
	}
	s.charged = 0
	s.flags |= stageFreed
	if s.owner != nil {
		c := s.owner
		c.node.giveRoom(c, c.dead || c.slots == nil)
	}
	s.owner, s.credit = nil, nil
}

func (s *replyStage) front() []byte {
	if s.flags&stageCommitted == 0 || s.flags&stageFreed != 0 || s.read == s.size {
		return nil
	}
	p := s.page(s.read)
	off := s.read % stagePayload
	n := min(s.size-s.read, stagePayload-off)
	return p[stageHead+off : stageHead+off+n]
}

// consume releases independent whole pages, never just their visible
// suffix. Index units remain charged until the complete response drains.
func (s *replyStage) consume(n int) {
	if n < 0 || n > len(s.front()) {
		s.flags |= stageBroken
		return
	}
	old := s.read
	s.read += n
	if s.flags&stageStreaming != 0 && s.owner != nil {
		s.owner.stagePending -= int64(n)
	}
	if s.read == s.size {
		s.discard()
		return
	}
	if s.read/stagePayload != old/stagePayload {
		r := &s.root
		i := old / stagePayload
		for i >= stageSlots {
			r = r.next
			i -= stageSlots
		}
		r.pages[i] = nil
		s.charged -= stageUnit
		s.credit.give(stageUnit)
		if s.owner != nil {
			s.owner.growOut(-stageUnit)
		}
	}
}
