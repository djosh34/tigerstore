package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// A checkpoint is every piece of replicated state at one applied index,
// and never a value: values stay in the log, named by their Raft
// position. Every node cuts one at the same applied index, from the log
// alone, and writes the same bytes. Its file is
//
//	header | keys (mvcc.Store.CopyFrozen) | u32 CRC-32C of everything before
//
// and the header is
//
//	magic "tigerckp" | u32 format version | u64 index | u64 term |
//	uvarint voter count | uvarint voters |
//	per voter: uvarint version it supports, 0 before it announced one |
//	uvarint cluster version | varint revision |
//	varint compaction revision | varint lease stamps | varint base |
//	uvarint lease count | per lease: varint ID | varint TTL | varint stamp
//
// where base is the size of the newest checkpoint finished when this one
// was cut, which the next cut is measured against.
//
// The leases' keys are not in it: they are the keys whose newest version
// has the lease, rebuilt on load.

const ckptMagic = "tigerckp"

// ckptVersion is the format version of checkpoint files. Version 2 added
// the versions.
const ckptVersion = 2

// ckptHeader is the part of a checkpoint before the keys.
type ckptHeader struct {
	index, term          uint64
	voters               []uint64
	supports             []uint32
	version              uint32
	rev, compact, stamps int64
	base                 int64
	leases               []*lease
}

func (h *ckptHeader) encode() []byte {
	b := append([]byte(ckptMagic), 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(b[len(ckptMagic):], ckptVersion)
	b = binary.LittleEndian.AppendUint64(b, h.index)
	b = binary.LittleEndian.AppendUint64(b, h.term)
	b = binary.AppendUvarint(b, uint64(len(h.voters)))
	for _, v := range h.voters {
		b = binary.AppendUvarint(b, v)
	}
	for _, v := range h.supports {
		b = binary.AppendUvarint(b, uint64(v))
	}
	b = binary.AppendUvarint(b, uint64(h.version))
	b = binary.AppendVarint(b, h.rev)
	b = binary.AppendVarint(b, h.compact)
	b = binary.AppendVarint(b, h.stamps)
	b = binary.AppendVarint(b, h.base)
	b = binary.AppendUvarint(b, uint64(len(h.leases)))
	for _, l := range h.leases {
		b = binary.AppendVarint(b, l.id)
		b = binary.AppendVarint(b, l.ttl)
		b = binary.AppendVarint(b, l.stamp)
	}
	return b
}

var errBadCheckpoint = fmt.Errorf("%w: bad checkpoint", wal.ErrCorrupt)

// readHeader reads a checkpoint header.
func readHeader(r *bufio.Reader) (*ckptHeader, error) {
	var fixed [len(ckptMagic) + 4 + 16]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, fmt.Errorf("%w: %w", errBadCheckpoint, err)
	}
	if string(fixed[:len(ckptMagic)]) != ckptMagic {
		return nil, fmt.Errorf("%w: no magic", errBadCheckpoint)
	}
	if v := binary.LittleEndian.Uint32(fixed[len(ckptMagic):]); v != ckptVersion {
		// A root of this node names it, so it is damage or a newer build's.
		return nil, fmt.Errorf("%w: format version %d; this build reads version %d", errBadCheckpoint, v, ckptVersion)
	}
	h := &ckptHeader{
		index: binary.LittleEndian.Uint64(fixed[len(ckptMagic)+4:]),
		term:  binary.LittleEndian.Uint64(fixed[len(ckptMagic)+12:]),
	}
	var err error
	uv := func() uint64 {
		var x uint64
		if err == nil {
			x, err = binary.ReadUvarint(r)
		}
		return x
	}
	sv := func() int64 {
		var x int64
		if err == nil {
			x, err = binary.ReadVarint(r)
		}
		return x
	}
	n := uv()
	if n > 7 {
		return nil, fmt.Errorf("%w: %d voters", errBadCheckpoint, n)
	}
	for range n {
		h.voters = append(h.voters, uv())
	}
	for range n {
		h.supports = append(h.supports, uint32(uv()))
	}
	h.version = uint32(uv())
	h.rev, h.compact, h.stamps, h.base = sv(), sv(), sv(), sv()
	n = uv()
	for i := uint64(0); i < n && err == nil; i++ {
		l := &lease{id: sv(), ttl: sv(), stamp: sv(), keys: map[string]struct{}{}}
		h.leases = append(h.leases, l)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: header: %w", errBadCheckpoint, err)
	}
	return h, nil
}

// copying is a checkpoint being copied out of the index.
type copying struct {
	index, term uint64
	crc         uint32
	size        int64
	minRef      uint64
}

// lastCkpt is the newest checkpoint finished: its index, size, the lowest
// entry a value of it is in, and its header's base.
type lastCkpt struct {
	index  uint64
	size   int64
	minRef uint64
	base   int64
}

// maybeCheckpoint cuts a checkpoint once the entries applied since the
// last one pass CheckpointBytes and CheckpointRatio times the base: the
// size of the newest checkpoint finished when the last one was cut. Every
// node knows that size, however far its copy got, so every node cuts at
// the same index; it runs after each applied entry. A copy still under
// way then is finished at once.
func (n *Node) maybeCheckpoint(index, term uint64) {
	if n.sinceCkpt < max(n.cfg.CheckpointBytes, n.cfg.CheckpointRatio*n.base) {
		return
	}
	if n.copy != nil {
		n.copyChunk(-1)
	}
	if planted.On("B21") && n.rn != nil {
		// Planted: the checkpoint names the committed index, not the
		// applied one.
		index = max(index, n.rn.BasicStatus().GetCommit())
	}
	n.base = n.last.size
	supports := make([]uint32, len(n.cfg.Peers))
	for i, id := range n.cfg.Peers {
		supports[i] = n.state.supports[id]
	}
	h := &ckptHeader{
		index: index, term: term, voters: n.cfg.Peers,
		supports: supports, version: n.state.version,
		rev: n.kv.Rev(), compact: n.kv.CompactRev(), stamps: n.state.stamps,
		base: n.base, leases: n.state.sortedLeases(),
	}
	data := h.encode()
	n.copy = &copying{index: index, term: term, crc: crc32.Update(0, castagnoli, data), size: int64(len(data))}
	n.kv.Freeze()
	n.sinceCkpt = 0
	n.out.Checkpoint = append(n.out.Checkpoint, CheckpointChunk{Index: index, Data: data})
}

// copyChunk copies about limit bytes more of the checkpoint under way, or
// all of it if limit is negative.
func (n *Node) copyChunk(limit int) {
	c := n.copy
	for {
		max := limit
		if limit < 0 {
			max = 1 << 20
		}
		data, minRef, done := n.kv.CopyFrozen(make([]byte, 0, max+1024), max)
		c.crc = crc32.Update(c.crc, castagnoli, data)
		c.size += int64(len(data))
		if minRef != 0 && (c.minRef == 0 || minRef < c.minRef) {
			c.minRef = minRef
		}
		if done {
			data = binary.LittleEndian.AppendUint32(data, c.crc)
			c.size += 4
			n.last = lastCkpt{index: c.index, size: c.size, minRef: c.minRef}
			n.copy = nil
		}
		n.out.Checkpoint = append(n.out.Checkpoint, CheckpointChunk{Index: c.index, Data: data, Done: done})
		if done || limit >= 0 {
			return
		}
	}
}

// Copying reports whether a checkpoint is being copied out of the index.
func (n *Node) Copying() bool { return n.copy != nil }

// CopyCheckpoint copies the next part of the checkpoint under way, about
// CheckpointChunk bytes, for the next Effects. The driver paces it: the
// copy runs on the committer and the parts go to the disk.
func (n *Node) CopyCheckpoint() {
	if n.err == nil && n.copy != nil {
		n.copyChunk(n.cfg.CheckpointChunk)
	}
}

// CheckpointWritten tells the node that the driver has written and synced
// the checkpoint at index, and synced its directory, or failed to, which
// stops the node like any failed disk write. The node then asks for the
// root that makes it count.
func (n *Node) CheckpointWritten(index uint64, err error) {
	if err != nil {
		n.fail(fmt.Errorf("core: write the checkpoint at %d: %w", index, err))
		return
	}
	if n.err != nil || index != n.last.index || index <= n.storage.ckpt.Index {
		// An older checkpoint than one a snapshot installed; it never
		// counts.
		return
	}
	term, err := n.termAt(index)
	if err != nil {
		n.fail(err)
		return
	}
	mark := index + 1
	if n.last.minRef != 0 {
		mark = min(mark, n.last.minRef)
	}
	// A value read under way may need a value that a compaction since
	// dropped from the index.
	mark = n.readMark(mark)
	if len(n.snapWait) > 0 {
		// A follower may be fetching a snapshot from what the removal
		// would take; the next root removes it.
		mark = 0
	}
	n.out.Persist = append(n.out.Persist, PersistJob{Root: &wal.Root{Index: index, Term: term}, RemoveBelow: mark})
}

// termAt is the term of an entry, from memory or from the log.
func (n *Node) termAt(index uint64) (uint64, error) {
	if t, err := n.storage.Term(index); err == nil {
		return t, nil
	}
	if index == n.storage.ckpt.Index {
		return n.storage.ckpt.Term, nil
	}
	e, err := n.log.Entry(index)
	if err != nil {
		return 0, err
	}
	return e.GetTerm(), nil
}

// loadCheckpoint reads the checkpoint at root into a new state and
// checks it against its checksum and the root.
func loadCheckpoint(cfg Config, disk Log, root wal.Root, v mvcc.Values) (*State, lastCkpt, error) {
	f, err := disk.Checkpoint(root.Index)
	if err != nil {
		return nil, lastCkpt{}, err
	}
	st, last, err := readCheckpoint(cfg, f, root, v)
	return st, last, errors.Join(err, f.Close())
}

// readCheckpoint reads a checkpoint from f.
func readCheckpoint(cfg Config, f io.Reader, root wal.Root, v mvcc.Values) (*State, lastCkpt, error) {
	tr := &trailerReader{r: f}
	r := bufio.NewReaderSize(tr, 64<<10)
	h, err := readHeader(r)
	if err != nil {
		return nil, lastCkpt{}, err
	}
	if h.index != root.Index || h.term != root.Term || !slices.Equal(h.voters, cfg.Peers) {
		return nil, lastCkpt{}, fmt.Errorf("%w: checkpoint at %d term %d with voters %v, the root names %d term %d",
			errBadCheckpoint, h.index, h.term, h.voters, root.Index, root.Term)
	}
	if own := ownVersion(cfg); h.version > own {
		return nil, lastCkpt{}, tooOld(h.version, own)
	}
	st := NewState(MinLeaseTTL(cfg), v)
	if cfg.Record {
		st.kv.TrackHash()
	}
	st.version, st.stamps = h.version, h.stamps
	for i, id := range h.voters {
		if h.supports[i] != 0 {
			st.supports[id] = h.supports[i]
		}
	}
	for _, l := range h.leases {
		st.leases[l.id] = l
	}
	minRef, err := st.kv.Load(r, h.rev, h.compact)
	if err != nil {
		return nil, lastCkpt{}, fmt.Errorf("%w: %w", errBadCheckpoint, err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		return nil, lastCkpt{}, err
	}
	if len(rest) != 4 || len(tr.tail) != 4 || binary.LittleEndian.Uint32(tr.tail) != tr.crc {
		return nil, lastCkpt{}, fmt.Errorf("%w: checksum", errBadCheckpoint)
	}
	st.kv.EachLeased(func(key []byte, id int64) {
		if l, ok := st.leases[id]; ok {
			l.keys[string(key)] = struct{}{}
		}
	})
	return st, lastCkpt{index: root.Index, size: tr.n, minRef: minRef, base: h.base}, nil
}

// trailerReader computes the CRC-32C of everything it reads but the last
// 4 bytes, which it keeps: the trailer of a checkpoint.
type trailerReader struct {
	r    io.Reader
	crc  uint32
	n    int64
	tail []byte
}

func (t *trailerReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.add(p[:n])
	return n, err
}

// add takes the next bytes of the file.
func (t *trailerReader) add(p []byte) {
	t.n += int64(len(p))
	t.tail = append(t.tail, p...)
	if k := len(t.tail) - 4; k > 0 {
		t.crc = crc32.Update(t.crc, castagnoli, t.tail[:k])
		t.tail = append(t.tail[:0], t.tail[k:]...)
	}
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)
