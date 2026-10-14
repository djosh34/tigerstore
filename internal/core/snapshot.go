package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// A follower that is behind the leader's window gets a snapshot from
// Raft: only an index and a term, the newest checkpoint's or the window's
// start, with no data. Its driver fetches what it lacks from the leader
// (FetchSnapshot), writes it to its own log, and reports the persist job;
// the core then brings its state to the snapshot's index (install).
//
// The leader sends the raw entries the follower lacks, read from its
// segments. When those are gone, it sends its checkpoint, the values of
// the entries after the follower's applied index that the checkpoint
// names, and the entries after the checkpoint. The root names the
// checkpoint; the entries after it, in the log, bring the state to the
// snapshot's index, where Raft's snapshot is.

// Install is a snapshot that Raft hands a follower: the state at Index,
// whose entry has Term. The driver brings its log there with
// ReceiveSnapshot, fetching from From what follows Applied.
type Install struct {
	From, Index, Term, Applied uint64
	// Checkpoint is the root of the checkpoint that came with the
	// snapshot, if any; ReceiveSnapshot sets it.
	Checkpoint wal.Root
}

// install brings the state to the snapshot: it loads the checkpoint that
// came with it, if any, and applies the entries after that from the log.
// Memory is emptied first: the entries it held after the applied index
// can be ones the snapshot replaced, and values must come from the log.
func (n *Node) install(in *Install) error {
	if err := n.storage.reset(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(in.Index), Term: new(in.Term), ConfState: n.storage.conf}}); err != nil {
		return err
	}
	if c := in.Checkpoint; c.Index > 0 {
		if n.copy != nil {
			// The checkpoint under way is of the state being replaced;
			// it is finished, written, and never counts.
			n.copyChunk(-1)
		}
		st, last, err := loadCheckpoint(n.cfg, n.log, c, n.values)
		if err != nil {
			return fmt.Errorf("core: load the snapshot's checkpoint at %d: %w", c.Index, err)
		}
		n.setState(st)
		n.applied, n.last, n.base, n.sinceCkpt, n.storage.ckpt = c.Index, last, last.base, 0, c
		n.record(c.Index, c.Term)
		// The state jumped; watchers would miss the changes in between.
		// Their clients watch again from where they were.
		n.closeWatchStreams()
	}
	to := in.Index
	if planted.On("B23") {
		// Planted: the state stops one entry short of Raft's snapshot.
		to--
	}
	term := in.Checkpoint.Term
	for i := n.applied + 1; i <= to; i++ {
		e, err := n.log.Entry(i)
		if err != nil {
			return fmt.Errorf("core: read entry %d of the snapshot: %w", i, err)
		}
		if err := n.apply([]*raftpb.Entry{e}); err != nil {
			return err
		}
		term = e.GetTerm()
	}
	if (n.applied != in.Index || term != in.Term) && !planted.On("B23") {
		return fmt.Errorf("core: the snapshot at %d term %d left the state at %d term %d", in.Index, in.Term, n.applied, term)
	}
	n.applied = in.Index
	return nil
}

// record notes, with Config.Record, that the state at index came from a
// checkpoint instead of from applying the entry.
func (n *Node) record(index, term uint64) {
	if n.cfg.Record {
		n.out.Applied = append(n.out.Applied, Applied{Index: index, Term: term, Hash: n.state.Hash(), Checkpoint: true})
	}
}

// closeWatchStreams ends every watch stream, in order.
func (n *Node) closeWatchStreams() {
	ids := make([]uint64, 0, len(n.streams))
	for id := range n.streams {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		n.CloseWatchStream(id)
		n.out.Closed = append(n.out.Closed, id)
	}
}

// unapplied is the entry bytes in memory that are not applied yet.
func (n *Node) unapplied() int64 {
	return n.storage.total() - n.storage.bytes(n.applied) + n.unpersisted + n.unready
}

// trimWindow lets applied entries leave memory once they pass the window,
// by an eighth of it at a time. On the leader the window grows up to
// WindowMaxBytes to keep what a follower still needs.
func (n *Node) trimWindow() {
	have := n.storage.bytes(n.applied)
	limit := n.cfg.WindowBytes
	if n.isLeader {
		match := n.applied
		n.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
			if id != n.cfg.ID {
				match = min(match, pr.Match)
			}
		})
		limit = max(limit, min(n.cfg.WindowMaxBytes, have-n.storage.bytes(match)))
	}
	if have <= limit+n.cfg.WindowBytes/8 {
		return
	}
	index := n.storage.keep(n.applied, limit)
	if planted.On("B20") {
		// Planted: entries that are not applied yet leave memory too.
		last, err := n.storage.LastIndex()
		if err != nil {
			n.fail(err)
			return
		}
		index = max(index, min(n.applied+1, last))
	}
	if err := n.storage.trim(index); err != nil {
		n.fail(err)
	}
}

// snapshotProgress gives up, on the leader, on a snapshot that a follower
// has not started to fetch within SnapshotTicks, so Raft sends another,
// as etcd's leader reports a failed snapshot.
func (n *Node) snapshotProgress() {
	if !n.isLeader {
		clear(n.snapWait)
		return
	}
	var failed []uint64
	n.rn.WithProgress(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		if pr.State != tracker.StateSnapshot {
			delete(n.snapWait, id)
			return
		}
		at, ok := n.snapWait[id]
		switch {
		case !ok:
			n.snapWait[id] = n.now
		case at != 0 && n.now-at >= int64(n.cfg.SnapshotTicks):
			failed = append(failed, id)
		}
	})
	slices.Sort(failed)
	for _, id := range failed {
		delete(n.snapWait, id)
		n.rn.ReportSnapshot(id, raft.SnapshotFailure)
	}
}

// SnapshotStarted tells the leader that a follower started to fetch a
// snapshot from it.
func (n *Node) SnapshotStarted(to uint64) {
	if n.err == nil && n.isLeader {
		n.snapWait[to] = 0
	}
}

// SnapshotSent tells the leader that a follower's fetch ended, and
// whether it got everything.
func (n *Node) SnapshotSent(to uint64, ok bool) {
	if n.err != nil || !n.isLeader {
		return
	}
	delete(n.snapWait, to)
	status := raft.SnapshotFinish
	if !ok {
		status = raft.SnapshotFailure
	}
	n.rn.ReportSnapshot(to, status)
}

// DurableCheckpoint is the root of the newest durable checkpoint, which
// a snapshot that this node serves may need.
func (n *Node) DurableCheckpoint() wal.Root { return n.storage.ckpt }

// The snapshot stream is a sequence of items
//
//	u8 kind | uvarint length | payload | u32 CRC-32C of kind and payload
//
// starting with a header (from, to, checkpoint index and term, 0 for none)
// and ending with an end item.
const (
	snapHeader     = 1
	snapCheckpoint = 2
	snapValue      = 3
	snapEntry      = 4
	snapEnd        = 5

	// snapChunk is how much of a checkpoint file one item carries.
	snapChunk = 1 << 20
	// maxSnapItem bounds an item a receiver takes.
	maxSnapItem = wal.MaxEntryBytes + 64
)

type snapWriter struct {
	w   *bufio.Writer
	buf []byte
}

func (s *snapWriter) item(kind byte, payload ...[]byte) error {
	n := 0
	for _, p := range payload {
		n += len(p)
	}
	s.buf = append(s.buf[:0], kind)
	s.buf = binary.AppendUvarint(s.buf, uint64(n))
	crc := crc32.Update(0, castagnoli, s.buf[:1])
	for _, p := range payload {
		crc = crc32.Update(crc, castagnoli, p)
	}
	if _, err := s.w.Write(s.buf); err != nil {
		return err
	}
	for _, p := range payload {
		if _, err := s.w.Write(p); err != nil {
			return err
		}
	}
	return binary.Write(s.w, binary.LittleEndian, crc)
}

func (s *snapWriter) entry(e *raftpb.Entry) error {
	h := binary.AppendUvarint(nil, e.GetTerm())
	h = binary.AppendUvarint(h, e.GetIndex())
	h = append(h, byte(e.GetType()))
	return s.item(snapEntry, h, e.GetData())
}

// errNoSnapshot means that a node cannot serve the snapshot asked for: its
// log lacks the entries and it has no checkpoint that fits.
var errNoSnapshot = errors.New("core: the log cannot serve this snapshot")

// ServeSnapshot writes to w what a follower that applied up to from-1
// needs to reach index to: the entries from..to from log, or, when some
// are gone, the checkpoint ckpt, the values it names in entries from from
// on, and the entries after it.
func ServeSnapshot(disk Log, ckpt wal.Root, from, to uint64, w io.Writer) error {
	s := &snapWriter{w: bufio.NewWriterSize(w, 256<<10)}
	// Entries after the checkpoint are always on disk.
	raw := true
	for i := from; i <= min(to, ckpt.Index) && raw; i++ {
		raw = disk.Has(i)
	}
	if !raw && (ckpt.Index < from || ckpt.Index > to) {
		return fmt.Errorf("%w: entries %d to %d, the checkpoint is at %d", errNoSnapshot, from, to, ckpt.Index)
	}
	var hdr []byte
	hdr = binary.AppendUvarint(hdr, from)
	hdr = binary.AppendUvarint(hdr, to)
	next := from
	if raw {
		hdr = binary.AppendUvarint(hdr, 0)
		hdr = binary.AppendUvarint(hdr, 0)
	} else {
		hdr = binary.AppendUvarint(hdr, ckpt.Index)
		hdr = binary.AppendUvarint(hdr, ckpt.Term)
		next = ckpt.Index + 1
	}
	if err := s.item(snapHeader, hdr); err != nil {
		return err
	}
	if !raw {
		if err := serveCheckpoint(s, disk, ckpt, from); err != nil {
			return err
		}
	}
	for i := next; i <= to; i++ {
		e, err := disk.Entry(i)
		if err != nil {
			return err
		}
		if err := s.entry(e); err != nil {
			return err
		}
	}
	if err := s.item(snapEnd); err != nil {
		return err
	}
	return s.w.Flush()
}

// serveCheckpoint writes the checkpoint file, then every value it names
// in entries from from on.
func serveCheckpoint(s *snapWriter, disk Log, ckpt wal.Root, from uint64) error {
	f, err := disk.Checkpoint(ckpt.Index)
	if err != nil {
		return err
	}
	buf := make([]byte, snapChunk)
	for {
		n, err := io.ReadFull(f, buf)
		if n > 0 {
			if err := s.item(snapCheckpoint, buf[:n]); err != nil {
				return errors.Join(err, f.Close())
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return errors.Join(err, f.Close())
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	// A second pass, for the values; the file is read back, not held.
	if f, err = disk.Checkpoint(ckpt.Index); err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, 64<<10)
	if _, err := readHeader(r); err != nil {
		return errors.Join(err, f.Close())
	}
	var hdr []byte
	err = mvcc.WalkRefs(r, func(ref mvcc.Ref) error {
		if ref.Index < from {
			return nil
		}
		v, err := disk.Value(ref.Index, ref.Off, ref.Len)
		if err != nil {
			return err
		}
		hdr = binary.AppendUvarint(hdr[:0], ref.Index)
		hdr = binary.AppendUvarint(hdr, uint64(ref.Off))
		hdr = binary.LittleEndian.AppendUint32(hdr, ref.Sum)
		return s.item(snapValue, hdr, v)
	})
	return errors.Join(err, f.Close())
}

// SnapshotSink is where a follower's driver puts a snapshot it receives:
// its own log.
type SnapshotSink interface {
	// Entries appends entries, which replace those at and after their
	// index.
	Entries(ents []*raftpb.Entry) error
	// Value appends a value that starts at offset off of the data of
	// entry index, an entry the follower does not hold.
	Value(index uint64, off uint32, value []byte) error
	// CheckpointChunk appends to the checkpoint file at index, which it
	// creates on the first chunk.
	CheckpointChunk(index uint64, data []byte) error
	// CheckpointDone syncs the checkpoint file at index and its
	// directory.
	CheckpointDone(index uint64) error
	// Root appends a root.
	Root(r wal.Root) error
}

type snapReader struct {
	r   *bufio.Reader
	buf []byte
}

// next reads an item and checks its CRC.
func (s *snapReader) next() (byte, []byte, error) {
	kind, err := s.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := binary.ReadUvarint(s.r)
	if err != nil {
		return 0, nil, err
	}
	if n > maxSnapItem {
		return 0, nil, fmt.Errorf("core: snapshot item of %d bytes", n)
	}
	if uint64(cap(s.buf)) < n+4 {
		s.buf = make([]byte, n+4)
	}
	s.buf = s.buf[:n+4]
	if _, err := io.ReadFull(s.r, s.buf); err != nil {
		return 0, nil, err
	}
	payload := s.buf[:n]
	crc := crc32.Update(crc32.Update(0, castagnoli, []byte{kind}), castagnoli, payload)
	if crc != binary.LittleEndian.Uint32(s.buf[n:]) && !planted.On("B22") {
		return 0, nil, errors.New("core: a snapshot item does not match its checksum")
	}
	return kind, payload, nil
}

var errBadSnapshot = errors.New("core: bad snapshot")

// ReceiveSnapshot reads a snapshot stream from r for in, checks it, and
// writes it to sink: the entries from in.Applied+1 to in.Index, or a
// checkpoint, its values and the entries after it. It checks every item's
// CRC, that the entries follow each other up to the snapshot's index and
// term, that each value matches its checksum, and the checkpoint's CRC
// and header, and only then writes the root that makes the checkpoint
// count. log reads the checkpoint back. It sets in.Checkpoint.
//
// The entries the follower already holds with the leader's term are the
// leader's (Raft's log matching), and it may have acknowledged them: it
// skips them, so that they stay durable through the install, and the
// root keeps them. Only the first entry it lacks replaces those after it.
func ReceiveSnapshot(r io.Reader, sink SnapshotSink, disk Log, peers []uint64, in *Install) error {
	s := &snapReader{r: bufio.NewReaderSize(r, 256<<10)}
	kind, p, err := s.next()
	if err != nil {
		return err
	}
	var from, to, ckpt, ckptTerm uint64
	for _, x := range []*uint64{&from, &to, &ckpt, &ckptTerm} {
		v, k := binary.Uvarint(p)
		if k <= 0 {
			return fmt.Errorf("%w: header", errBadSnapshot)
		}
		*x, p = v, p[k:]
	}
	check := !planted.On("B22")
	if check && (kind != snapHeader || from != in.Applied+1 || to != in.Index || (ckpt != 0 && (ckpt < from || ckpt > to))) {
		return fmt.Errorf("%w: header for %d to %d with the checkpoint at %d, want %d to %d", errBadSnapshot, from, to, ckpt, in.Applied+1, in.Index)
	}
	next, term := from, uint64(0)
	// root waits for the first entry the follower lacks.
	var root *wal.Root
	if ckpt != 0 {
		next, term = ckpt+1, ckptTerm
		if err := receiveCheckpoint(s, sink, disk, peers, from, ckpt, ckptTerm); err != nil {
			return err
		}
		in.Checkpoint = wal.Root{Index: ckpt, Term: ckptTerm}
		root = &wal.Root{Index: ckpt, Term: ckptTerm, Reset: true, Keep: in.Applied}
		if holds(disk, ckpt, ckptTerm) {
			root.Keep = ckpt
		}
	}
	// Past a checkpoint, the follower's entries are the leader's only if
	// it holds the checkpoint's own entry.
	held := root == nil || root.Keep == ckpt
	var ents []*raftpb.Entry
	size := 0
	flush := func() error {
		err := sink.Entries(ents)
		ents, size = ents[:0], 0
		return err
	}
	for {
		kind, p, err := s.next()
		if err != nil {
			return err
		}
		if kind == snapEnd {
			break
		}
		if kind != snapEntry {
			return fmt.Errorf("%w: item of kind %d among the entries", errBadSnapshot, kind)
		}
		e, err := decodeSnapEntry(p)
		if err != nil {
			return err
		}
		if check && (e.GetIndex() != next || e.GetTerm() < term) {
			return fmt.Errorf("%w: entry %d term %d where entry %d after term %d was due", errBadSnapshot, e.GetIndex(), e.GetTerm(), next, term)
		}
		next, term = e.GetIndex()+1, e.GetTerm()
		if held = held && holds(disk, e.GetIndex(), e.GetTerm()); held {
			if root != nil {
				root.Keep = e.GetIndex()
			}
			continue
		}
		if root != nil {
			if err := sink.Root(*root); err != nil {
				return err
			}
			root = nil
		}
		ents = append(ents, e)
		// The batch stays small next to the entry budget.
		if size += len(e.GetData()); size >= snapChunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if check && (next != to+1 || term != in.Term) {
		return fmt.Errorf("%w: it ends at entry %d term %d, Raft's snapshot is at %d term %d", errBadSnapshot, next-1, term, to, in.Term)
	}
	if root != nil {
		if err := sink.Root(*root); err != nil {
			return err
		}
	}
	return flush()
}

// holds reports whether the log holds entry index with term.
func holds(disk Log, index, term uint64) bool {
	if !disk.Has(index) {
		return false
	}
	e, err := disk.Entry(index)
	return err == nil && e.GetTerm() == term
}

func decodeSnapEntry(p []byte) (*raftpb.Entry, error) {
	term, k := binary.Uvarint(p)
	if k <= 0 {
		return nil, fmt.Errorf("%w: entry", errBadSnapshot)
	}
	p = p[k:]
	index, k := binary.Uvarint(p)
	if k <= 0 || len(p) < k+1 {
		return nil, fmt.Errorf("%w: entry", errBadSnapshot)
	}
	etype := raftpb.EntryType(p[k])
	return &raftpb.Entry{Term: new(term), Index: new(index), Type: &etype, Data: slices.Clone(p[k+1:])}, nil
}

// receiveCheckpoint writes the checkpoint file and the values, and checks
// them.
func receiveCheckpoint(s *snapReader, sink SnapshotSink, disk Log, peers []uint64, from, ckpt, ckptTerm uint64) error {
	root := wal.Root{Index: ckpt, Term: ckptTerm}
	check := !planted.On("B22")
	tr := &trailerReader{}
	for {
		b, err := s.r.Peek(1)
		if err != nil {
			return err
		}
		if b[0] != snapCheckpoint {
			break
		}
		_, p, err := s.next()
		if err != nil {
			return err
		}
		tr.add(p)
		if err := sink.CheckpointChunk(root.Index, p); err != nil {
			return err
		}
	}
	if check && (len(tr.tail) != 4 || binary.LittleEndian.Uint32(tr.tail) != tr.crc) {
		return fmt.Errorf("%w: the checkpoint at %d does not match its checksum", errBadSnapshot, root.Index)
	}
	if err := sink.CheckpointDone(root.Index); err != nil {
		return err
	}
	f, err := disk.Checkpoint(root.Index)
	if err != nil {
		return err
	}
	h, err := readHeader(bufio.NewReader(f))
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if check && (h.index != root.Index || h.term != root.Term || !slices.Equal(h.voters, peers)) {
		return fmt.Errorf("%w: the checkpoint says index %d term %d, the stream %d term %d", errBadSnapshot, h.index, h.term, root.Index, root.Term)
	}
	for {
		b, err := s.r.Peek(1)
		if err != nil {
			return err
		}
		if b[0] != snapValue {
			break
		}
		_, p, err := s.next()
		if err != nil {
			return err
		}
		index, k := binary.Uvarint(p)
		if k <= 0 {
			return fmt.Errorf("%w: value", errBadSnapshot)
		}
		p = p[k:]
		off, k := binary.Uvarint(p)
		if k <= 0 || len(p) < k+4 || off > 1<<32-1 {
			return fmt.Errorf("%w: value", errBadSnapshot)
		}
		sum, v := binary.LittleEndian.Uint32(p[k:]), p[k+4:]
		if check && (index < from || index > root.Index || crc32.Checksum(v, castagnoli) != sum) {
			return fmt.Errorf("%w: value of entry %d at %d", errBadSnapshot, index, off)
		}
		if err := sink.Value(index, uint32(off), v); err != nil {
			return err
		}
	}
	return nil
}
