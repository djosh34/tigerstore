package core

import (
	"encoding/binary"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
)

// The fast path (#602). A client's put in wire form, or its short
// transaction, that needs nothing rare goes from its record straight into
// the open batch, encoded by appendWrite as the slow path encodes it, and
// its waiter into a slot of a table instead of the waiting map, the
// driver's waiters and their maps. Its entry applies through the same
// code as every other (applyEntry), which finds the waiter in its slot;
// the answer goes to the call itself (FastCall), not through
// Effects.Replies.
//
// So there is one log, one store and one apply: a follower, or a node
// that replays its log, cannot tell which path proposed an entry.
//
// The slow path guards the fast one. SubmitFast takes a request only if
// every check of submit passes for it, and without auth, leases, alarms
// or a format older than originVersion; anything else goes to Submit,
// which answers it as before. A write in the open batch that cannot go
// out as it is (no leader, the NOSPACE alarm, the unapplied budget, a
// format change, its deadline, a cancel while no leader is known) moves
// to the slow path's queue (demote) and goes on there.

// FastCall is a call on the fast path. Answer gets its reply, as
// Effects.Replies would carry it: a whole reply, or a piece of one
// (MorePieces), whose next piece comes after Next. It runs on the
// driver's thread inside a call into the node, so it must not call the
// node, and r is the node's until Answer returns.
type FastCall interface {
	Answer(r *Reply)
}

// fastSlots is the size of the slot table: the fast calls that may wait
// at once. A request whose slot is taken goes the slow way.
const fastSlots = 1 << 13

// openHead is the room the open batch keeps for its header, written once
// the batch is sealed: the format, the count, the origin and the boot.
const openHead = 2 + 3*binary.MaxVarintLen64

// fastSlot is the waiter of a call on the fast path.
type fastSlot struct {
	// p is the request while the fast path has it. A demoted request
	// goes on as a copy on the slow path.
	p    pending
	id   uint64
	call FastCall
	// live says that the call waits for its answer or the rest of it,
	// open that its write is in the open batch, and slow that it was
	// demoted.
	live, open, slow bool
}

// fastPath is the fast path's state on a node.
type fastPath struct {
	slots []fastSlot
	// order holds the IDs of the slots taken, oldest first, from head on:
	// their deadlines come in the same order (expireFast).
	order []uint64
	head  int
	// open is the batch being built, from openHead on, in format; ids
	// are its writes.
	open   []byte
	format uint32
	ids    []uint64
	// last is the size of the last batch sealed, which the next one
	// starts with room for.
	last int
	// reply is the reply Answer gets, kept here so that it does not
	// escape to the heap each time.
	reply Reply
}

// slot returns the slot of a fast request ID, once the table exists.
func (f *fastPath) slot(id uint64) *fastSlot {
	return &f.slots[id&(fastSlots-1)]
}

// live returns the slot of request id if it waits on the fast path's
// table, or nil.
func (f *fastPath) live(id uint64) *fastSlot {
	if f.slots == nil {
		return nil
	}
	if s := f.slot(id); s.id == id && s.live {
		return s
	}
	return nil
}

// SubmitFast takes r, a put in wire form or a short transaction, on the
// fast path, and reports whether it did. Its ID is one the driver never
// used, as for Submit, so the entry is the same on either path. A request
// it does not take, the driver hands to Submit. call gets the answer.
func (n *Node) SubmitFast(r *Request, call FastCall) bool {
	f := &n.fast
	if n.cfg.Slow || n.err != nil || n.removeAt != 0 || n.leader == 0 || r.ID == 0 {
		return false
	}
	if st := n.state; st.noSpace || st.corrupt || st.auth.enabled || st.version < originVersion {
		return false
	}
	if f.slots == nil {
		f.slots = make([]fastSlot, fastSlots)
	}
	s := f.slot(r.ID)
	if s.live {
		return false
	}
	ai, hasAI, err := n.identify(r.Caller)
	if err != nil {
		return false
	}
	extra := identityExtra(r.Caller, ai, hasAI)
	var kind byte
	if r.Op == nil {
		put := &r.Put.PutOp
		if put.Lease != 0 || !n.quotaLeft(putCost(put)) || mvcc.CheckPutOp(put) != nil ||
			n.tooManyRequests(false) || checkPutSize(&r.Put, extra, n.cfg.MaxRequestBytes) != nil {
			return false
		}
		kind = kindPut
	} else {
		t, ok := r.Op.(*pb.TxnRequest)
		if !ok || !isShort(t) || shortLease(t) || mvcc.IsTxnReadOnly(t) {
			return false
		}
		if !n.quotaLeft(txnCost(t)) || mvcc.CheckTxn(t, n.cfg.MaxTxnOps) != nil || n.tooManyRequests(false) ||
			txnRequestBytes+protowire.SizeBytes(txnSize(t))+extra > n.cfg.MaxRequestBytes {
			return false
		}
		kind = kindShortTxn
	}
	*s = fastSlot{p: pending{req: *r, deadline: n.clock + int64(n.cfg.RequestTicks), fast: true}, id: r.ID, call: call, live: true, open: true}
	size := writeSize(&s.p)
	if size > maxWriteBytes {
		*s = fastSlot{}
		return false
	}
	if len(f.ids) > 0 && len(f.open)-openHead+size > n.cfg.MaxBatchBytes {
		// As queueWrite: a write that would take the entry past its cap
		// sends off what waits first.
		n.propose()
	}
	if len(f.ids) == 0 {
		f.open = make([]byte, openHead, openHead+max(f.last, size))
		f.format = n.state.version
	}
	open, err := appendKind(f.open, &s.p, kind, true, n.cfg.ID, n.cfg.Boot)
	if err != nil {
		// appendKind fails only to marshal, which these writes never do.
		*s = fastSlot{}
		return false
	}
	f.open = open
	f.ids = append(f.ids, r.ID)
	f.order = append(f.order, r.ID)
	return true
}

// shortLease reports whether a short transaction puts with a lease.
func shortLease(t *pb.TxnRequest) bool {
	put := t.Success[0].GetRequestPut()
	return put != nil && put.Lease != 0
}

// fastWaiter returns the request with ID id that waits on this node, on
// either path, or nil.
func (n *Node) fastWaiter(id uint64) *pending {
	s := n.fast.live(id)
	switch {
	case s == nil:
		return nil
	case s.slow:
		return n.waiting[id]
	}
	return &s.p
}

// waiter returns the request with ID id that waits on this node, or nil.
func (n *Node) waiter(id uint64) *pending {
	if p := n.fastWaiter(id); p != nil {
		return p
	}
	return n.waiting[id]
}

// emit hands out a reply: a fast call's to the call, any other to the
// driver.
func (n *Node) emit(r Reply) {
	s := n.fast.live(r.ID)
	if s == nil {
		n.out.Replies = append(n.out.Replies, r)
		return
	}
	call := s.call
	if !r.MorePieces {
		*s = fastSlot{}
	}
	if planted.On("B34") && r.Err == nil && r.Rev > 0 {
		// Planted: the fast path answers a put with the revision before
		// its own.
		r.Rev--
	}
	f := &n.fast
	f.reply = r
	call.Answer(&f.reply)
	f.reply = Reply{}
}

// sealOpen proposes the open batch as one entry.
func (n *Node) sealOpen() {
	f := &n.fast
	if len(f.ids) == 0 {
		return
	}
	at := openHead
	var head [openHead]byte
	h := append(head[:0], 0, byte(f.format))
	h = binary.AppendUvarint(h, uint64(len(f.ids)))
	h = binary.AppendUvarint(h, n.cfg.ID)
	h = binary.AppendUvarint(h, n.cfg.Boot)
	at -= len(h)
	copy(f.open[at:], h)
	data := f.open[at:len(f.open):len(f.open)]
	ids := f.ids
	f.last = len(data)
	f.open, f.ids = nil, ids[:0]
	for _, id := range ids {
		f.slot(id).open = false
	}
	if !n.proposeData(data) {
		for _, id := range ids {
			if s := f.slot(id); s.id == id && s.live {
				n.finish(&s.p, errDropped)
			}
		}
	}
	clear(ids)
}

// openFits reports whether the open batch may go out as it is now.
func (n *Node) openFits() bool {
	f := &n.fast
	if n.state.noSpace || n.state.version != f.format || n.rn.BasicStatus().Lead == raft.None {
		return false
	}
	unapplied := n.unapplied()
	return unapplied == 0 || unapplied+int64(len(f.open)-openHead) <= n.cfg.UnappliedBytes
}

// demote moves the writes of the open batch to the slow path's queue,
// where they go on as if Submit had taken them.
func (n *Node) demote() {
	f := &n.fast
	ids := f.ids
	f.open, f.ids = nil, nil
	for _, id := range ids {
		s := f.slot(id)
		if s.id != id || !s.live || !s.open {
			continue
		}
		s.open, s.slow = false, true
		p := new(pending)
		*p = s.p
		p.fast, p.queued = false, true
		n.waiting[id] = p
		n.proposals = append(n.proposals, p)
		n.propBytes += writeSize(p)
		i, _ := slices.BinarySearchFunc(n.timeouts, p.deadline, func(q *pending, d int64) int {
			if q.deadline > d {
				return 1
			}
			return -1
		})
		n.timeouts = slices.Insert(n.timeouts, i, p)
	}
	clear(ids)
	f.ids = ids[:0]
}

// expireFast fails fast requests whose deadline passed, as expire does
// the others. One still in the open batch first goes to the slow path,
// whose expire then fails it as a write never proposed.
func (n *Node) expireFast() {
	f := &n.fast
	for f.head < len(f.order) {
		id := f.order[f.head]
		s := f.slot(id)
		if s.id != id || !s.live || s.slow || s.p.done {
			f.head++
			continue
		}
		if s.p.deadline > n.clock {
			break
		}
		if s.open {
			n.demote()
			continue
		}
		n.counters.ProposalsFailed++
		n.finish(&s.p, n.timeoutCause(s.p.deadline-int64(n.cfg.RequestTicks)))
		f.head++
	}
	if f.head > 1024 && f.head > len(f.order)/2 {
		f.order = f.order[:copy(f.order, f.order[f.head:])]
		f.head = 0
	}
}

// stopFast fails every fast request the slow path does not hold, as stop
// does the others.
func (n *Node) stopFast() {
	f := &n.fast
	for _, id := range f.order[f.head:] {
		if s := f.slot(id); s.id == id && s.live && !s.slow {
			n.finish(&s.p, rpctypes.ErrGRPCStopped)
			if s.id == id && s.live {
				// An answer in pieces whose next piece never comes.
				n.emit(Reply{ID: id, Err: rpctypes.ErrGRPCStopped})
			}
		}
	}
	f.order, f.head, f.ids, f.open = nil, 0, nil, nil
}
