package server

// Throwaway experiment: can bounded startup storage replace transient wire-Put
// allocations without reusing retained Raft/MVCC bytes? Only TIGER_ALLOC_STRATEGY
// ring|arena opts in. Non-Put RPCs and transport/timer/error infrastructure are
// outside the managed scope; there is NO managed heap fallback.

import (
	"encoding/binary"
	"os"
	"sync"
	"unsafe"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

const (
	allocationSlots     = 4096
	allocationQuota     = 256
	allocationByteQuota = 8 << 20
	allocationBudget    = 64 << 20
	arenaSlots          = 8
)

type allocSlot struct {
	st                                  stream
	workspace                           core.Workspace
	req                                 core.Request
	reply                               core.Reply
	response                            [5 + putResponseSize]byte
	output                              [3]outItem
	index                               int
	group                               int
	block                               int
	prev, next                          *allocSlot // connection's ownership list, not the ready list
	readerRefs, frameRefs               int
	mapRef, queueRef, nodeRef, timerRef bool
	submitNext                          *stream
	compNext                            *allocSlot
}

// Worst-case startup capacity (ring has the most block descriptors). This
// zero-sized array type fails compilation if descriptors outgrow the budget.
const (
	allocationDriverReserve = 8 << 20
	allocationStartupBytes  = (48 << 20) + allocationDriverReserve + allocationSlots*int(unsafe.Sizeof(allocSlot{})) + (allocationSlots/arenaSlots)*int(unsafe.Sizeof(allocGroup{})) + (allocationSlots+64+16)*int(unsafe.Sizeof(allocBlock{})) + (2*allocationSlots+allocationSlots/arenaSlots+allocationSlots+64+16)*int(unsafe.Sizeof(int(0))) + int(unsafe.Sizeof(allocationPool{}))
)

type allocationBudgetCheck [allocationBudget - allocationStartupBytes]struct{}

var _ allocationBudgetCheck

type allocGroup struct {
	used, live int
	sealed     bool
	blocks     int
	c          *conn
}

type allocBlock struct {
	b     []byte
	used  int
	next  int
	class int
}

type allocationPool struct {
	mu                                                                                 sync.Mutex
	mode                                                                               uint64
	slots                                                                              []allocSlot
	groups                                                                             []allocGroup
	freeSlots, freeGroups                                                              []int
	freeSlotHead, freeSlotCount                                                        int // fixed circular free-slot queue (ring mode)
	blocks                                                                             []allocBlock
	freeBlocks                                                                         [3][]int
	heldSlots, heldBytes, peakSlots, peakBytes, denied, timerAllocations, unmanagedRPC uint64
}

func newAllocationPool() *allocationPool {
	var mode uint64
	switch os.Getenv("TIGER_ALLOC_STRATEGY") {
	case "ring":
		mode = 1
	case "arena":
		mode = 2
	default:
		return nil
	}
	p := &allocationPool{mode: mode, slots: make([]allocSlot, allocationSlots), groups: make([]allocGroup, allocationSlots/arenaSlots)}
	p.freeSlots = make([]int, allocationSlots)
	p.freeSlotCount = allocationSlots
	p.freeGroups = make([]int, 0, len(p.groups))
	for i := range p.slots {
		p.slots[i].index = i
		p.freeSlots[i] = i
	}
	for i := range p.groups {
		p.freeGroups = append(p.freeGroups, i)
	}
	// 48MiB body capacity, not 2MiB per slot. The remaining budget covers
	// all slot, group, block and free-list descriptors allocated here.
	smallSize, smallCount := 2048, allocationSlots
	if mode == 2 {
		smallSize, smallCount = 16<<10, allocationSlots/arenaSlots
	}
	sizes := [3]int{smallSize, 128 << 10, 2 << 20}
	counts := [3]int{smallCount, 64, 16}
	totalBlocks := counts[0] + counts[1] + counts[2]
	p.blocks = make([]allocBlock, 0, totalBlocks)
	for class := range sizes {
		data := make([]byte, sizes[class]*counts[class])
		p.freeBlocks[class] = make([]int, 0, counts[class])
		for i := 0; i < counts[class]; i++ {
			p.freeBlocks[class] = append(p.freeBlocks[class], len(p.blocks))
			p.blocks = append(p.blocks, allocBlock{b: data[i*sizes[class] : (i+1)*sizes[class]], next: -1, class: class})
		}
	}
	// The compile-time allocationBudgetCheck below guards startup capacity,
	// including a conservative 8MiB reserve for core/node list and map backing.
	return p
}

// AllocationStats snapshots actual managed reservations. Arena slots/bytes are
// charged for the WHOLE region until its last reader/node/timer/flush owner
// retires; unused capacity is not mislabeled free. Snapshot allocation itself
// is observer overhead, not managed fallback.
func (s *Server) AllocationStats() map[string]uint64 {
	m := map[string]uint64{"mode": 0, "slot_budget": 0, "byte_budget": 0, "per_connection_slots": 0, "held_slots": 0, "held_bytes": 0, "peak_slots": 0, "peak_bytes": 0, "denied_count": 0, "fallback_allocations": 0}
	if s.alloc == nil {
		return m
	}
	p := s.alloc
	p.mu.Lock()
	defer p.mu.Unlock()
	m["mode"], m["slot_budget"], m["byte_budget"], m["per_connection_slots"] = p.mode, allocationSlots, allocationBudget, allocationQuota
	m["held_slots"], m["held_bytes"], m["peak_slots"], m["peak_bytes"] = p.heldSlots, p.heldBytes+allocationDriverReserve, p.peakSlots, p.peakBytes+allocationDriverReserve
	m["driver_reserve_bytes"] = allocationDriverReserve
	m["denied_count"] = p.denied
	m["timer_allocations"], m["unmanaged_rpc_count"] = p.timerAllocations, p.unmanagedRPC
	m["per_connection_bytes"] = allocationByteQuota
	return m
}

func (p *allocationPool) peaks() {
	p.peakSlots = max(p.peakSlots, p.heldSlots)
	p.peakBytes = max(p.peakBytes, p.heldBytes)
}

// acquireSlot runs with c.mu. Allocation mutex never calls back into a conn.
func (c *conn) acquireSlot() *stream {
	p := c.s.alloc
	p.mu.Lock()
	defer p.mu.Unlock()
	var slot *allocSlot
	if p.mode == 1 {
		if c.allocSlots >= allocationQuota || p.freeSlotCount == 0 {
			p.denied++
			return nil
		}
		i := p.freeSlots[p.freeSlotHead]
		p.freeSlotHead = (p.freeSlotHead + 1) % allocationSlots
		p.freeSlotCount--
		slot = &p.slots[i]
		slot.group = -1
		c.allocSlots++
		p.heldSlots++
		p.heldBytes += uint64(unsafe.Sizeof(allocSlot{}))
	} else {
		if c.arena < 0 {
			if c.allocSlots+arenaSlots > allocationQuota || len(p.freeGroups) == 0 {
				p.denied++
				return nil
			}
			i := p.freeGroups[len(p.freeGroups)-1]
			p.freeGroups = p.freeGroups[:len(p.freeGroups)-1]
			p.groups[i] = allocGroup{blocks: -1, c: c}
			c.arena = i
			c.allocSlots += arenaSlots
			p.heldSlots += arenaSlots
			p.heldBytes += uint64(arenaSlots)*uint64(unsafe.Sizeof(allocSlot{})) + uint64(unsafe.Sizeof(allocGroup{}))
		}
		g := &p.groups[c.arena]
		slot = &p.slots[c.arena*arenaSlots+g.used]
		slot.group = c.arena
		g.used++
		g.live++
		if g.used == arenaSlots {
			g.sealed = true
			c.arena = -1
		}
	}
	slot.block = -1
	slot.readerRefs = 1 // onHeaders owns the pointer through onEnd/admit
	slot.mapRef = true
	slot.st = stream{c: c, slot: slot, out: slot.output[:0]}
	slot.next = c.allocHead
	if slot.next != nil {
		slot.next.prev = slot
	}
	c.allocHead = slot
	p.peaks()
	return &slot.st
}

func (c *conn) sealArenaLocked() {
	if c.s.alloc == nil || c.arena < 0 {
		return
	}
	p := c.s.alloc
	p.mu.Lock()
	i := c.arena
	p.groups[i].sealed = true
	c.arena = -1
	p.recycleGroup(i)
	p.mu.Unlock()
}

func (p *allocationPool) recycleGroup(i int) {
	g := &p.groups[i]
	if !g.sealed || g.live != 0 {
		return
	}
	c := g.c
	for block := g.blocks; block >= 0; {
		next := p.blocks[block].next
		p.releaseBlock(c, block)
		block = next
	}
	for j := 0; j < g.used; j++ {
		s := &p.slots[i*arenaSlots+j]
		index := s.index
		*s = allocSlot{index: index}
	}
	c.allocSlots -= arenaSlots
	p.heldSlots -= arenaSlots
	p.heldBytes -= uint64(arenaSlots)*uint64(unsafe.Sizeof(allocSlot{})) + uint64(unsafe.Sizeof(allocGroup{}))
	*g = allocGroup{}
	p.freeGroups = append(p.freeGroups, i)
}

// releaseReader pins even a pointer removed from streams by an early timer.
func (c *conn) releaseReader(st *stream) {
	if st == nil || st.slot == nil {
		return
	}
	c.mu.Lock()
	st.slot.readerRefs--
	c.retireSlotLocked(st.slot)
	c.mu.Unlock()
}

func (c *conn) retireSlotLocked(s *allocSlot) {
	if s.mapRef || s.queueRef || s.nodeRef || s.timerRef || s.readerRefs != 0 || s.frameRefs != 0 || s.st.listed {
		return
	}
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		c.allocHead = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	p := c.s.alloc
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == 2 {
		g := &p.groups[s.group]
		g.live--
		// No slot in an arena is reused independently, even after retirement.
		s.st.body, s.st.msg, s.st.out, s.req = nil, nil, nil, core.Request{}
		p.recycleGroup(s.group)
	} else {
		for block := s.block; block >= 0; {
			next := p.blocks[block].next
			p.releaseBlock(c, block)
			block = next
		}
		i := s.index
		*s = allocSlot{index: i}
		p.freeSlots[(p.freeSlotHead+p.freeSlotCount)%allocationSlots] = i
		p.freeSlotCount++
		c.allocSlots--
		p.heldSlots--
		p.heldBytes -= uint64(unsafe.Sizeof(allocSlot{}))
	}
}

func (p *allocationPool) releaseBlock(c *conn, i int) {
	b := &p.blocks[i]
	p.heldBytes -= uint64(len(b.b))
	c.allocBytes -= len(b.b)
	b.used, b.next = 0, -1
	p.freeBlocks[b.class] = append(p.freeBlocks[b.class], i)
}

func (st *stream) managedBody(size int) ([]byte, error) { return st.managedBytes(size, false) }

func (st *stream) managedReply(size int) ([]byte, error) {
	b, err := st.managedBytes(size, true)
	if err != nil {
		return nil, err
	}
	return b[:5], nil
}

func (st *stream) managedBytes(size int, reply bool) ([]byte, error) {
	c, s := st.c, st.slot
	p := c.s.alloc
	c.mu.Lock()
	defer c.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if !reply && st.msgs != 0 {
		return nil, status.Error(codes.Internal, "grpc: multiple unary messages")
	}
	if size > 2<<20 {
		p.denied++
		return nil, errAllocationFull
	}
	if size == 0 {
		return s.response[:0:0], nil
	}
	block := -1
	if p.mode == 2 {
		g := &p.groups[s.group]
		for i := g.blocks; i >= 0; i = p.blocks[i].next {
			if len(p.blocks[i].b)-p.blocks[i].used >= size {
				block = i
				break
			}
		}
	}
	if block < 0 {
		class := 0
		for class < 2 && size > len(p.blocks[classStart(p, class)].b) {
			class++
		}
		if len(p.freeBlocks[class]) == 0 {
			p.denied++
			return nil, errAllocationFull
		}
		block = p.freeBlocks[class][len(p.freeBlocks[class])-1]
		if c.allocBytes+len(p.blocks[block].b) > allocationByteQuota {
			p.denied++
			return nil, errAllocationFull
		}
		p.freeBlocks[class] = p.freeBlocks[class][:len(p.freeBlocks[class])-1]
		b := &p.blocks[block]
		c.allocBytes += len(b.b)
		p.heldBytes += uint64(len(b.b))
		if p.mode == 2 {
			g := &p.groups[s.group]
			b.next = g.blocks
			g.blocks = block
		} else {
			b.next = s.block
			s.block = block
		}
		p.peaks()
	}
	b := &p.blocks[block]
	start := b.used
	b.used += size
	// Exact capacity is mandatory: receive uses cap to recognize message end.
	return b.b[start : start : start+size], nil
}

func classStart(p *allocationPool, class int) int {
	if class == 0 {
		return 0
	}
	if class == 1 {
		if p.mode == 2 {
			return allocationSlots / arenaSlots
		}
		return allocationSlots
	}
	if p.mode == 2 {
		return allocationSlots/arenaSlots + 64
	}
	return allocationSlots + 64
}

var errAllocationFull = status.Error(codes.ResourceExhausted, "allocation experiment: bounded workspace full")

func (st *stream) ManagedRequest() (core.Request, any) { return st.slot.req, st }
func (st *stream) NextManagedRequest() node.ManagedBatch {
	if st.slot.submitNext == nil {
		return nil
	}
	return st.slot.submitNext
}

// queueManaged establishes ownership BEFORE flush, reset, or deadline can run.
func (c *conn) queueManaged(st *stream, req core.Request) {
	c.mu.Lock()
	req.Workspace = &st.slot.workspace
	st.slot.req = req
	st.slot.queueRef = true
	if c.managedTail == nil {
		c.managedHead = st
	} else {
		c.managedTail.slot.submitNext = st
	}
	c.managedTail = st
	c.managedCount++
	c.managedBytes += st.reqSize
	c.mu.Unlock()
}

func (c *conn) flushManaged() error {
	c.mu.Lock()
	head, count := c.managedHead, c.managedCount
	c.managedHead, c.managedTail, c.managedCount, c.managedBytes = nil, nil, 0, 0
	c.sealArenaLocked()
	for st := head; st != nil; st = st.slot.submitNext {
		st.slot.queueRef = false
		st.slot.nodeRef = true
		st.atNode = true
		c.holdLocked(st, st.reqSize+callCost)
	}
	c.mu.Unlock()
	if head == nil {
		return nil
	}
	err := c.s.n.SubmitManagedBatch(c.ctx, head, count, c)
	if err == nil {
		return nil
	}
	for st := head; st != nil; {
		next := st.slot.submitNext // callback may retire st
		c.CompleteOne(st, core.Reply{Err: err})
		st = next
	}
	if c.ctx.Err() != nil {
		return err
	}
	return nil
}

// CompleteOne is an O(1) bounded linked queue copy under compMu; the owner
// does NOT encode or acquire c.mu unless this connection is already closed.
// This intentionally pays one short mutex operation per completion.
func (c *conn) CompleteOne(tag any, reply core.Reply) {
	st := tag.(*stream)
	s := st.slot
	c.compMu.Lock()
	if c.completionClosed {
		c.compMu.Unlock()
		c.mu.Lock()
		s.nodeRef = false
		st.atNode = false
		c.releaseLocked(st)
		c.retireSlotLocked(s)
		c.mu.Unlock()
		return
	}
	s.reply = reply
	if c.compTail == nil {
		c.compHead = s
	} else {
		c.compTail.compNext = s
	}
	c.compTail = s
	c.compMu.Unlock()
	c.signal()
}

func (c *conn) takeManagedCompletions() *allocSlot {
	c.compMu.Lock()
	head := c.compHead
	c.compHead, c.compTail = nil, nil
	c.compMu.Unlock()
	return head
}

func (c *conn) completeManaged(head *allocSlot) {
	for s := head; s != nil; {
		next := s.compNext
		st := &s.st
		r := s.reply
		var b []byte
		err := r.Err
		if err == nil {
			b = s.response[:5]
			if r.PrevKv != nil {
				// Previous-key responses are variable-sized but remain bounded
				// managed storage, not a silently allocating append fallback.
				size := 5 + putResponseSize + 2 + binary.MaxVarintLen64 + proto.Size(r.PrevKv)
				b, err = st.managedReply(size)
			}
			if err == nil {
				b, err = appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)
				if err == nil {
					binary.BigEndian.PutUint32(b[1:5], uint32(len(b)-5))
				}
			}
		}
		c.mu.Lock()
		s.compNext = nil
		s.reply = core.Reply{}
		s.req = core.Request{}
		s.nodeRef = false
		st.atNode = false
		if st.reset || c.closed {
			c.releaseLocked(st)
		} else if !st.done.Swap(true) {
			c.holdLocked(st, len(b))
			var statusValue *status.Status
			if err != nil {
				statusValue = status.Convert(toStatus(err))
			}
			st.pushLocked(b, true, statusValue)
		}
		c.retireSlotLocked(s)
		c.mu.Unlock()
		s = next
	}
}
