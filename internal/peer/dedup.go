package peer

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.etcd.io/raft/v3/raftpb"
)

// Prototype 602-5: the leader orders writes, the followers spread their
// bytes.
//
// A follower that forwards a proposal to the leader keeps its entries'
// bytes by their 64-bit hash, puts the hashes in the proposal's Context,
// and sends the same entries to every other peer as a side copy, which
// keeps them too. The leader learns the hashes from the proposal. When it
// sends an append, an entry whose hash it learned goes out as the 8-byte
// hash, and the Context marks which entries those are. The follower's
// transport puts the bytes back before Raft sees the append. Raft, the
// log and the WAL are as they were: only the peer links change.
//
// Not safe (prototype): a follower that does not hold a hash waits up to
// missWait for the side copy, then drops the append and tells the leader
// to send those entries in full (a nack); Raft sends again after the gap.
// The leader assumes that every follower holds every forwarded entry.
// Hash collisions are not checked.

// Context tags, 4 bytes, of the messages of the dedup.
const (
	propTag = "TS5P" // a forwarded MsgProp: the tag and its entries' hashes
	sideTag = "TS5S" // a side copy, a MsgProp to a follower: the same
	nackTag = "TS5N" // to the leader: the hashes a follower lacked
	appTag  = "TS5A" // an MsgApp: the tag and a bitmap of the hashed entries
)

const (
	// heldAge and heldCap bound the bytes a node keeps by hash; knownAge
	// is how long the leader sends a learned hash, well within heldAge.
	heldAge  = 2 * time.Second
	heldCap  = 256 << 20
	knownAge = time.Second
	// missWait is how long a follower waits for a side copy.
	missWait = 100 * time.Millisecond
	// minHashed is the smallest entry sent as a hash.
	minHashed = 32
)

type heldEntry struct {
	data []byte
	at   int64
}

type queuedHash struct {
	h  uint64
	at int64
	n  int64
}

// dedup is a node's bytes by hash, as a follower, and the hashes it
// learned, as the leader.
type dedup struct {
	mu        sync.Mutex
	held      map[uint64]heldEntry
	heldQ     []queuedHash
	heldBytes int64
	waiters   map[uint64]chan struct{}
	known     map[uint64]int64
	knownQ    []queuedHash
}

func newDedup() *dedup {
	return &dedup{held: map[uint64]heldEntry{}, waiters: map[uint64]chan struct{}{}, known: map[uint64]int64{}}
}

// hold keeps the entries' bytes by the hashes hs.
func (d *dedup) hold(hs []uint64, es []*raftpb.Entry) {
	now := time.Now().UnixNano()
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, h := range hs {
		if h == 0 {
			continue
		}
		data := es[i].Data
		d.held[h] = heldEntry{data, now}
		d.heldQ = append(d.heldQ, queuedHash{h, now, int64(len(data))})
		d.heldBytes += int64(len(data))
		if len(d.waiters) > 0 {
			if ch, ok := d.waiters[h]; ok {
				close(ch)
				delete(d.waiters, h)
			}
		}
	}
	i := 0
	for i < len(d.heldQ) && (now-d.heldQ[i].at > int64(heldAge) || d.heldBytes > heldCap) {
		q := d.heldQ[i]
		if e, ok := d.held[q.h]; ok && e.at == q.at {
			delete(d.held, q.h)
		}
		d.heldBytes -= q.n
		i++
	}
	if i > 0 {
		d.heldQ = append(d.heldQ[:0], d.heldQ[i:]...)
	}
}

// get returns the bytes of hash h, or a channel that is closed when they
// come.
func (d *dedup) get(h uint64) ([]byte, chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.held[h]; ok {
		return e.data, nil
	}
	ch, ok := d.waiters[h]
	if !ok {
		ch = make(chan struct{})
		d.waiters[h] = ch
	}
	return nil, ch
}

// learn notes, on the leader, that the followers hold the hashes hs.
func (d *dedup) learn(hs []uint64) {
	now := time.Now().UnixNano()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, h := range hs {
		if h == 0 {
			continue
		}
		d.known[h] = now
		d.knownQ = append(d.knownQ, queuedHash{h: h, at: now})
	}
	i := 0
	for i < len(d.knownQ) && now-d.knownQ[i].at > int64(knownAge) {
		q := d.knownQ[i]
		if at, ok := d.known[q.h]; ok && at == q.at {
			delete(d.known, q.h)
		}
		i++
	}
	if i > 0 {
		d.knownQ = append(d.knownQ[:0], d.knownQ[i:]...)
	}
}

// forget drops hashes a follower lacked.
func (d *dedup) forget(hs []uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, h := range hs {
		delete(d.known, h)
	}
}

// hashable reports whether entry e may go as a hash.
func hashable(e *raftpb.Entry) bool {
	return e.GetType() == raftpb.EntryNormal && len(e.Data) >= minHashed
}

func appendHashes(ctx []byte, hs []uint64) []byte {
	for _, h := range hs {
		ctx = binary.BigEndian.AppendUint64(ctx, h)
	}
	return ctx
}

func readHashes(ctx []byte) []uint64 {
	hs := make([]uint64, len(ctx)/8)
	for i := range hs {
		hs[i] = binary.BigEndian.Uint64(ctx[8*i:])
	}
	return hs
}

func tagged(m *raftpb.Message, tag string) bool {
	return len(m.Context) >= 4 && string(m.Context[:4]) == tag
}

// forward, on a follower, keeps the bytes of a proposal it forwards,
// marks it with their hashes, and sends the side copies.
func (t *TCP) forward(m *raftpb.Message) {
	if len(m.Context) != 0 || len(m.Entries) == 0 {
		return
	}
	hs := make([]uint64, len(m.Entries))
	some := false
	for i, e := range m.Entries {
		if hashable(e) {
			hs[i] = xxhash.Sum64(e.Data)
			some = true
		}
	}
	if !some {
		return
	}
	t.dd.hold(hs, m.Entries)
	ctx := appendHashes([]byte(propTag), hs)
	m.Context = ctx
	side := append([]byte(sideTag), ctx[4:]...)
	for id, s := range *t.senders.Load() {
		if id == m.GetTo() || id == t.cfg.ID {
			continue
		}
		sm := &raftpb.Message{Type: raftpb.MsgProp.Enum(), To: new(id), From: new(t.cfg.ID), Entries: m.Entries, Context: side}
		var n int64
		for _, e := range m.Entries {
			n += int64(len(e.Data))
		}
		t.pc.side.Add(1)
		t.pc.sideData.Add(n)
		s.send(sm)
	}
}

// received handles the dedup's part of a received message. It returns
// false for one that Raft does not get.
func (t *TCP) received(m *raftpb.Message, from uint64) bool {
	switch m.GetType() {
	case raftpb.MsgProp:
		switch {
		case tagged(m, sideTag):
			t.dd.hold(readHashes(m.Context[4:]), m.Entries)
			return false
		case tagged(m, nackTag):
			t.dd.forget(readHashes(m.Context[4:]))
			return false
		case tagged(m, propTag):
			t.dd.learn(readHashes(m.Context[4:]))
			m.Context = nil
		}
	case raftpb.MsgApp:
		if tagged(m, appTag) {
			return t.resolve(m, from)
		}
	}
	return true
}

// resolve puts the bytes of an append's hashed entries back. An entry
// whose bytes do not come within missWait drops the append, and the
// leader hears to send it in full.
func (t *TCP) resolve(m *raftpb.Message, from uint64) bool {
	bits := m.Context[4:]
	m.Context = nil
	var lacked []uint64
	for i, e := range m.Entries {
		if i/8 >= len(bits) || bits[i/8]&(1<<(i%8)) == 0 {
			continue
		}
		h := binary.BigEndian.Uint64(e.Data)
		data, ch := t.dd.get(h)
		if ch != nil {
			start := time.Now()
			timer := time.NewTimer(missWait)
			select {
			case <-ch:
			case <-timer.C:
			case <-t.ctx.Done():
			}
			timer.Stop()
			w := time.Since(start).Microseconds()
			t.pc.miss.Add(1)
			t.pc.missWait.Add(w)
			for {
				mx := t.pc.missMax.Load()
				if w <= mx || t.pc.missMax.CompareAndSwap(mx, w) {
					break
				}
			}
			data, _ = t.dd.get(h)
			if data == nil {
				lacked = append(lacked, h)
				continue
			}
		} else {
			t.pc.resolved.Add(1)
		}
		e.Data = data
	}
	if len(lacked) > 0 {
		t.pc.unresolved.Add(int64(len(lacked)))
		t.Send(&raftpb.Message{Type: raftpb.MsgProp.Enum(), To: new(from), From: new(t.cfg.ID), Context: appendHashes([]byte(nackTag), lacked)})
		return false
	}
	return true
}

// dedupApp, on the leader, returns the append m with every entry whose
// hash the followers hold sent as that hash.
func (t *TCP) dedupApp(m *raftpb.Message) *raftpb.Message {
	if len(m.Entries) == 0 || len(m.Context) != 0 {
		return m
	}
	hs := make([]uint64, len(m.Entries))
	for i, e := range m.Entries {
		if hashable(e) {
			hs[i] = xxhash.Sum64(e.Data)
		}
	}
	now := time.Now().UnixNano()
	var bits []byte
	k := 0
	t.dd.mu.Lock()
	for i, h := range hs {
		if h == 0 {
			continue
		}
		if at, ok := t.dd.known[h]; ok && now-at <= int64(knownAge) {
			if bits == nil {
				bits = make([]byte, (len(hs)+7)/8)
			}
			bits[i/8] |= 1 << (i % 8)
			k++
		} else {
			hs[i] = 0
		}
	}
	t.dd.mu.Unlock()
	t.pc.hashed.Add(int64(k))
	t.pc.full.Add(int64(len(hs) - k))
	if k == 0 {
		return m
	}
	es := make([]*raftpb.Entry, len(m.Entries))
	for i, e := range m.Entries {
		if hs[i] == 0 {
			es[i] = e
			continue
		}
		es[i] = &raftpb.Entry{Term: e.Term, Index: e.Index, Type: e.Type, Data: binary.BigEndian.AppendUint64(nil, hs[i])}
	}
	return &raftpb.Message{
		Type: m.Type, To: m.To, From: m.From, Term: m.Term, LogTerm: m.LogTerm, Index: m.Index,
		Commit: m.Commit, Entries: es, Context: append([]byte(appTag), bits...),
	}
}
