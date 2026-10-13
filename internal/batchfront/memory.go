package batchfront

import (
	"errors"
	"os"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/batchcmd"
	"github.com/djosh34/tigerstore/internal/batchmem"
	"github.com/djosh34/tigerstore/internal/node"
)

const memorySlots = 256

type admissionSlot struct {
	commands         [maxBatchCommands]batchcmd.Command
	ops              [maxBatchCommands]proto.Message
	tags, legacyTags [maxBatchCommands]uint32
}
type replySlot struct{ encoded [MaxStreams]encoded }

type memory struct {
	input, output, admissions, replies *batchmem.Bank
	admissionSlots                     []admissionSlot
	descriptorBytes                    int64
	replySlots                         []replySlot
}

// MemoryStats distinguishes the configured byte cap from materialized chunks
// and pinned chunks. Typed descriptor arrays are additional startup metadata.
type MemoryStats struct {
	Mode                             string
	DescriptorBytes                  int64
	Input, Output                    batchmem.Stats
	AdmissionRejected, ReplyRejected uint64
}

func newMemory() (*memory, error) {
	mode := batchmem.Mode(os.Getenv("TIGERSTORE_BATCH_MEMORY"))
	if mode == "" {
		return nil, nil
	}
	if mode != batchmem.Ring && mode != batchmem.Pool {
		return nil, errors.New("TIGERSTORE_BATCH_MEMORY must be ring or pool, or unset")
	}
	capacities := make([]int, 256, 272)
	for i := range capacities {
		capacities[i] = 64 << 10
	}
	for range 16 {
		capacities = append(capacities, batchmem.ChunkBytes)
	}
	input, err := batchmem.New(mode, capacities)
	if err != nil {
		return nil, err
	}
	// Small batches do not each pin 4 MiB. Both policies have the same size
	// classes and prefer the smallest available class. Large chunks preserve
	// the existing 16 MiB response cap without an unbounded fallback.
	outputCapacities := make([]int, 256, 266)
	for i := range outputCapacities {
		outputCapacities[i] = 64 << 10
	}
	for range 8 {
		outputCapacities = append(outputCapacities, batchmem.ChunkBytes)
	}
	output, err := batchmem.New(mode, append(outputCapacities, MaxResponseBytes, MaxResponseBytes))
	if err != nil {
		return nil, err
	}
	admissions, err := batchmem.New(mode, make([]int, memorySlots))
	if err != nil {
		return nil, err
	}
	replies, err := batchmem.New(mode, make([]int, memorySlots))
	if err != nil {
		return nil, err
	}
	return &memory{input: input, output: output, admissions: admissions, replies: replies, admissionSlots: make([]admissionSlot, memorySlots), replySlots: make([]replySlot, memorySlots), descriptorBytes: int64(memorySlots) * (int64(reflect.TypeFor[admissionSlot]().Size()) + int64(reflect.TypeFor[replySlot]().Size()))}, nil
}

func (s *Server) MemoryStats() MemoryStats {
	if s.memory == nil {
		return MemoryStats{Mode: "default"}
	}
	return MemoryStats{Mode: s.memoryMode, DescriptorBytes: s.memory.descriptorBytes, Input: s.memory.input.Stats(), Output: s.memory.output.Stats(), AdmissionRejected: s.memory.admissions.Stats().Rejected, ReplyRejected: s.memory.replies.Stats().Rejected}
}

func (c *conn) prepareAdmission(a *admission, id uint32) bool {
	if c.s.memory == nil {
		return true
	}
	if a.arena == nil {
		a.arena = c.s.memory.admissions.Acquire(0)
		if a.arena == nil {
			c.close()
			return false
		}
		s := &c.s.memory.admissionSlots[a.arena.Index()]
		*s = admissionSlot{}
		a.commands, a.ops = s.commands[:0], s.ops[:0]
		a.tags, a.legacyTags = s.tags[:0], s.legacyTags[:0]
	}
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		a.arena.Retain()
		b.descriptors = a.arena
	}
	c.mu.Unlock()
	return true
}

// Input has a separate lifetime from immutable GC-owned Raft entries. Core
// encoding copies command bytes into a new entry; only that entry backs MVCC.
func (c *conn) receiveBuffer(id uint32, old []byte, capacity int) ([]byte, bool) {
	if c.s.memory == nil {
		owned := make([]byte, len(old), capacity)
		copy(owned, old)
		return owned, true
	}
	var owned []byte
	if c.inputArena != nil {
		owned = c.inputArena.Alloc(capacity)
	}
	if owned == nil {
		if c.inputArena != nil {
			c.inputArena.Release()
			c.inputArena = nil
		}
		c.inputArena = c.s.memory.input.Acquire(capacity)
		if c.inputArena == nil {
			return nil, false
		}
		owned = c.inputArena.Alloc(capacity)
	}
	copy(owned, old)
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		c.inputArena.Retain()
		if b.receiveArena != nil {
			b.receiveArena.Release()
		}
		b.receiveArena = c.inputArena
	}
	c.mu.Unlock()
	return owned[:len(old)], true
}

// Called under the connection ledger lock, after completion or for an input
// that was never submitted. Submitted input is not recycled on client reset.
func releaseInput(b *budget) {
	if b.receiveArena != nil {
		b.receiveArena.Release()
		b.receiveArena = nil
	}
	if b.descriptors != nil {
		b.descriptors.Release()
		b.descriptors = nil
	}
}

func (c *conn) unsubmit(tags []uint32) {
	c.mu.Lock()
	for _, id := range tags {
		if b := c.budgets[id]; b != nil {
			b.admitted = false
		}
	}
	c.mu.Unlock()
}

func (c *conn) readerCleanup(a *admission) {
	// A parse/read failure can leave a prepared batch that was never sent.
	// Successful flushes clear their local tags before this cleanup runs.
	c.unsubmit(a.tags)
	c.unsubmit(a.legacyTags)
	if a.arena != nil {
		a.arena.Release()
		a.arena = nil
	}
	if c.inputArena != nil {
		c.inputArena.Release()
		c.inputArena = nil
	}
	c.mu.Lock()
	for id, b := range c.budgets {
		if !b.admitted {
			releaseInput(b)
			c.inputBytes -= b.input
			b.slotID = 0
			delete(c.budgets, id)
		}
	}
	c.mu.Unlock()
}

func (c *conn) completeClosed(replies []node.Completion) {
	c.mu.Lock()
	for _, r := range replies {
		if b := c.budgets[r.Tag]; b != nil {
			releaseInput(b)
			c.inputBytes -= b.input
			b.slotID = 0
			delete(c.budgets, r.Tag)
		}
	}
	c.mu.Unlock()
}

func (c *conn) writerQueueCleanup() {
	c.close()
	// Complete's publication check and send are protected by this lock.
	// It cannot publish after this cancellation barrier, even on startup I/O failure.
	c.mu.Lock()
	var first []encoded
	select {
	case first = <-c.out:
	default:
	}
	c.mu.Unlock()
	for _, e := range first {
		c.releaseEncoded(e)
	}
	for {
		select {
		case batch := <-c.out:
			for _, e := range batch {
				c.releaseEncoded(e)
			}
		default:
			return
		}
	}
}

func (c *conn) releaseEncoded(e encoded) {
	if e.arena != nil {
		e.arena.Release()
	}
	if e.descriptors != nil {
		e.descriptors.Release()
	}
	c.release(e.reserved)
}

func (c *conn) wireBuffer(n int, active **batchmem.Arena) ([]byte, *batchmem.Arena) {
	if c.s.memory == nil {
		return make([]byte, n), nil
	}
	var wire []byte
	if *active != nil {
		wire = (*active).Alloc(n)
	}
	if wire == nil {
		if *active != nil {
			(*active).Release()
			*active = nil
		}
		*active = c.s.memory.output.Acquire(n)
		if *active == nil {
			return nil, nil
		}
		wire = (*active).Alloc(n)
	}
	(*active).Retain()
	return wire, *active
}
