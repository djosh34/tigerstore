package core

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

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
//	uvarint member count | per member: uvarint ID | string name |
//	uvarint count | strings peer URLs | uvarint count | strings client URLs |
//	u8 learner | uvarint version it supports, 0 before it announced one |
//	uvarint removed member count | uvarint removed IDs |
//	uvarint cluster version | varint revision |
//	varint compaction revision | varint lease stamps | varint base |
//	uvarint lease count | per lease: varint ID | varint TTL | varint stamp |
//	varint checkpointed remaining TTL |
//	uvarint alarm count | per alarm: uvarint member ID | varint type |
//	u8 auth enabled | uvarint auth revision |
//	uvarint user count | per user: bytes authpb.User |
//	uvarint role count | per role: bytes authpb.Role |
//	string etcd's cluster version | u8 downgrade enabled | string target
//
// where a string or bytes are a uvarint length and the bytes, and base is the size
// of the newest checkpoint finished when this one was cut, which the next
// cut is measured against.
//
// The leases' keys are not in it: they are the keys whose newest version
// has the lease, rebuilt on load.

const ckptMagic = "tigerckp"

// ckptVersion is the format version of checkpoint files. Version 2 added
// the versions, version 3 the members and auth.
const ckptVersion = 3

// ckptHeader is the part of a checkpoint before the keys.
type ckptHeader struct {
	index, term          uint64
	members              []*pb.Member
	supports             []uint32
	removed              []uint64
	version              uint32
	rev, compact, stamps int64
	base                 int64
	leases               []*lease
	alarms               []*pb.AlarmMember
	auth                 *authState
	etcdVersion          string
	downgradeEnabled     bool
	downgradeTarget      string
}

func (h *ckptHeader) encode() ([]byte, error) {
	b := append([]byte(ckptMagic), 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(b[len(ckptMagic):], ckptVersion)
	b = binary.LittleEndian.AppendUint64(b, h.index)
	b = binary.LittleEndian.AppendUint64(b, h.term)
	str := func(v string) {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	strs := func(vs []string) {
		b = binary.AppendUvarint(b, uint64(len(vs)))
		for _, v := range vs {
			str(v)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(h.members)))
	for i, m := range h.members {
		b = binary.AppendUvarint(b, m.ID)
		str(m.Name)
		strs(m.PeerURLs)
		strs(m.ClientURLs)
		learner := byte(0)
		if m.IsLearner {
			learner = 1
		}
		b = append(b, learner)
		b = binary.AppendUvarint(b, uint64(h.supports[i]))
	}
	b = binary.AppendUvarint(b, uint64(len(h.removed)))
	for _, id := range h.removed {
		b = binary.AppendUvarint(b, id)
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
		b = binary.AppendVarint(b, l.remaining)
	}
	b = binary.AppendUvarint(b, uint64(len(h.alarms)))
	for _, a := range h.alarms {
		b = binary.AppendUvarint(b, a.MemberID)
		b = binary.AppendVarint(b, int64(a.Alarm))
	}
	enabled := byte(0)
	if h.auth.enabled {
		enabled = 1
	}
	b = append(b, enabled)
	b = binary.AppendUvarint(b, h.auth.revision)
	var err error
	msg := func(m proto.Message) {
		var data []byte
		if err == nil {
			data, err = proto.MarshalOptions{Deterministic: true}.Marshal(m)
		}
		b = binary.AppendUvarint(b, uint64(len(data)))
		b = append(b, data...)
	}
	b = binary.AppendUvarint(b, uint64(len(h.auth.users)))
	for _, name := range sortedKeys(h.auth.users) {
		msg(h.auth.users[name])
	}
	b = binary.AppendUvarint(b, uint64(len(h.auth.roles)))
	for _, name := range sortedKeys(h.auth.roles) {
		msg(h.auth.roles[name])
	}
	str(h.etcdVersion)
	downgrade := byte(0)
	if h.downgradeEnabled {
		downgrade = 1
	}
	b = append(b, downgrade)
	str(h.downgradeTarget)
	return b, err
}

var errBadCheckpoint = fmt.Errorf("%w: bad checkpoint", wal.ErrCorrupt)

// maxCheckpointString bounds a name or URL in a checkpoint header.
const maxCheckpointString = 64 << 10

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
	str := func() string {
		n := uv()
		if err != nil || n > maxCheckpointString {
			err = cmp.Or(err, fmt.Errorf("%w: a string of %d bytes", errBadCheckpoint, n))
			return ""
		}
		b := make([]byte, n)
		_, err = io.ReadFull(r, b)
		return string(b)
	}
	strs := func() []string {
		var out []string
		for i, n := uint64(0), uv(); i < n && err == nil; i++ {
			out = append(out, str())
		}
		return out
	}
	n := uv()
	for i := uint64(0); i < n && err == nil; i++ {
		m := &pb.Member{ID: uv(), Name: str(), PeerURLs: strs(), ClientURLs: strs()}
		if err == nil {
			var learner byte
			learner, err = r.ReadByte()
			m.IsLearner = learner == 1
		}
		h.members = append(h.members, m)
		h.supports = append(h.supports, uint32(uv()))
	}
	n = uv()
	for i := uint64(0); i < n && err == nil; i++ {
		h.removed = append(h.removed, uv())
	}
	h.version = uint32(uv())
	h.rev, h.compact, h.stamps, h.base = sv(), sv(), sv(), sv()
	n = uv()
	for i := uint64(0); i < n && err == nil; i++ {
		l := &lease{id: sv(), ttl: sv(), stamp: sv(), remaining: sv(), keys: map[string]struct{}{}}
		h.leases = append(h.leases, l)
	}
	n = uv()
	for i := uint64(0); i < n && err == nil; i++ {
		h.alarms = append(h.alarms, &pb.AlarmMember{MemberID: uv(), Alarm: pb.AlarmType(sv())})
	}
	h.auth = newAuthState()
	if err == nil {
		var enabled byte
		enabled, err = r.ReadByte()
		h.auth.enabled = enabled == 1
	}
	h.auth.revision = uv()
	// A role grows with each permission granted, so a user or role has
	// no size bound. ReadAll grows with the bytes actually there, so a
	// damaged length cannot ask for a huge buffer.
	msg := func(m proto.Message) {
		n := uv()
		if err != nil {
			return
		}
		var b []byte
		// A length past MaxInt64 reads nothing and fails the check below.
		b, err = io.ReadAll(io.LimitReader(r, int64(n)))
		if err == nil && uint64(len(b)) != n {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			err = proto.Unmarshal(b, m)
		}
	}
	for i, n := uint64(0), uv(); i < n && err == nil; i++ {
		u := &authpb.User{}
		msg(u)
		h.auth.users[string(u.Name)] = u
	}
	for i, n := uint64(0), uv(); i < n && err == nil; i++ {
		ro := &authpb.Role{}
		msg(ro)
		h.auth.roles[string(ro.Name)] = ro
	}
	h.etcdVersion = str()
	if err == nil {
		var downgrade byte
		downgrade, err = r.ReadByte()
		h.downgradeEnabled = downgrade == 1
	}
	h.downgradeTarget = str()
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
	h := n.state.header(index, term)
	h.base = n.base
	data, err := h.encode()
	if err != nil {
		n.fail(err)
		return
	}
	n.copy = &copying{index: index, term: term, crc: crc32.Update(0, castagnoli, data), size: int64(len(data))}
	n.kv.Freeze()
	n.sinceCkpt = 0
	n.out.Checkpoint = append(n.out.Checkpoint, CheckpointChunk{Index: index, Data: data})
}

// header is the checkpoint header of the state, but for its base.
func (st *State) header(index, term uint64) *ckptHeader {
	h := &ckptHeader{
		index: index, term: term, members: st.memberList(), version: st.version,
		rev: st.kv.Rev(), compact: st.kv.CompactRev(), stamps: st.stamps,
		leases: st.sortedLeases(), alarms: st.alarms, auth: st.auth,
		etcdVersion: st.etcdVersion, downgradeEnabled: st.downgradeEnabled, downgradeTarget: st.downgradeTarget,
	}
	for _, m := range h.members {
		h.supports = append(h.supports, st.supports[m.ID])
	}
	for id := range st.removed {
		h.removed = append(h.removed, id)
	}
	slices.Sort(h.removed)
	return h
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

// Installing reports whether a snapshot install is on its way to disk.
func (n *Node) Installing() bool { return n.installs > 0 }

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
	if n.err != nil || index != n.last.index || index <= n.storage.ckpt.Index || n.installs > 0 && !planted.On("B27") {
		// An older checkpoint than one a snapshot installed, or one of
		// the state an install on its way to disk replaces; it never
		// counts. Its root would land after the install's.
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
	if h.index != root.Index || h.term != root.Term {
		return nil, lastCkpt{}, fmt.Errorf("%w: checkpoint at %d term %d, the root names %d term %d",
			errBadCheckpoint, h.index, h.term, root.Index, root.Term)
	}
	st := NewState(MinLeaseTTL(cfg), v)
	if cfg.Record {
		st.kv.TrackHash()
	}
	st.version, st.stamps = h.version, h.stamps
	for i, m := range h.members {
		st.members[m.ID] = m
		if h.supports[i] != 0 {
			st.supports[m.ID] = h.supports[i]
		}
	}
	for _, id := range h.removed {
		st.removed[id] = true
	}
	for _, l := range h.leases {
		st.leases[l.id] = l
	}
	st.alarms = h.alarms
	st.alarmsChanged()
	st.auth = h.auth
	st.auth.rebuild()
	st.etcdVersion, st.downgradeEnabled, st.downgradeTarget = h.etcdVersion, h.downgradeEnabled, h.downgradeTarget
	minRef, err := st.kv.Load(r, h.rev, h.compact)
	if err != nil {
		return nil, lastCkpt{}, fmt.Errorf("%w: %w", errBadCheckpoint, err)
	}
	if err := tr.check(r); err != nil {
		return nil, lastCkpt{}, err
	}
	// Only a checkpoint that matches its checksum says the cluster runs
	// a newer version; a damaged one is damage.
	if own := ownVersion(cfg); h.version > own {
		return nil, lastCkpt{}, tooOld(h.version, own)
	}
	st.kv.EachLeased(func(key []byte, id int64) {
		if l, ok := st.leases[id]; ok {
			l.keys[string(key)] = struct{}{}
		}
	})
	return st, lastCkpt{index: root.Index, size: tr.n, minRef: minRef, base: h.base}, nil
}

// WalkCheckpoint calls fn for the value of every version that the
// checkpoint at index names, read from f, the start of its file, then
// checks the file's trailer. The node's cleaner keeps the values it names.
func WalkCheckpoint(f io.Reader, index uint64, fn func(mvcc.Ref) error) error {
	tr := &trailerReader{r: f}
	r := bufio.NewReaderSize(tr, 64<<10)
	h, err := readHeader(r)
	if err == nil && h.index != index {
		err = fmt.Errorf("%w: the file of the checkpoint at %d holds the one at %d", errBadCheckpoint, index, h.index)
	}
	if err == nil {
		if err = mvcc.WalkRefs(r, fn); errors.Is(err, mvcc.ErrBadCheckpoint) {
			err = fmt.Errorf("%w: %w", errBadCheckpoint, err)
		}
	}
	if err == nil {
		err = tr.check(r)
	}
	return err
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

// check reads the rest of r, which reads from t, and checks that it is
// the trailer and matches.
func (t *trailerReader) check(r io.Reader) error {
	rest, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(rest) != 4 || len(t.tail) != 4 || binary.LittleEndian.Uint32(t.tail) != t.crc {
		return fmt.Errorf("%w: checksum", errBadCheckpoint)
	}
	return nil
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

// RestoredCheckpoint gives the state the members of a restored cluster,
// as etcdutl snapshot restore does, and returns a whole checkpoint of it
// at the applied index, for the new log to start from. Each member
// supports the cluster version.
func (n *Node) RestoredCheckpoint(members []*pb.Member) (wal.Root, []byte, error) {
	if n.copy != nil {
		n.copyChunk(-1)
		n.out.Checkpoint = nil
	}
	st := n.state
	st.members, st.removed, st.supports = map[uint64]*pb.Member{}, map[uint64]bool{}, map[uint64]uint32{}
	for _, m := range members {
		st.members[m.ID] = &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, IsLearner: m.IsLearner}
		st.supports[m.ID] = st.version
	}
	term, err := n.termAt(n.applied)
	if err != nil {
		return wal.Root{}, nil, err
	}
	data, err := st.header(n.applied, term).encode()
	if err != nil {
		return wal.Root{}, nil, err
	}
	n.kv.Freeze()
	for {
		b, _, done := n.kv.CopyFrozen(nil, 1<<20)
		data = append(data, b...)
		if done {
			break
		}
	}
	data = binary.LittleEndian.AppendUint32(data, crc32.Checksum(data, castagnoli))
	return wal.Root{Index: n.applied, Term: term}, data, nil
}
