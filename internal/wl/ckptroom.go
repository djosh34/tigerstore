package wl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/bits"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"
)

// A checkpoint needs room on the disk beside the checkpoint it replaces,
// which stays until the new one is whole. A node whose disk lacks it does
// as etcd does with a database past its quota: it raises its NOSPACE
// alarm and stays up, serving reads, deletes, compactions, defrags and
// the alarm's disarm, and writes no checkpoint until there is room again.
// Before #769 it wrote the checkpoint anyway, stopped on ENOSPC halfway,
// and stopped again at every start, its half-written file filling the
// disk. The room is asked for once the checkpoint's scan knows which
// values it copies in, before it writes anything (ckptFits). A checkpoint
// that meets ENOSPC partway, the room taken meanwhile, is dropped the same
// way (ckptNoSpace).

// ckptRoomRetry is how long a node that found no room for a checkpoint
// waits before it looks again. It scans the store again only once the
// disk has the room the checkpoint asked for or the store got smaller
// (ckptRetry): a full log ring needs the checkpoint as soon as there is
// room.
const ckptRoomRetry = time.Second

// ckptRetry reports whether the checkpoint that found no room may be
// tried again now: once ckptRoomRetry passed, and the disk has the room
// it asked for, or the store is smaller. One dropped on ENOSPC partway,
// which the room it asked for had seemed to fit, tries again then: a
// short write, or a disk full for a while, leaves the room as it was,
// and waiting for more than that left the node without a checkpoint for
// good, its log's ring full (TestScenarios seed 448 on #846).
func (n *node) ckptRetry() bool {
	if n.now.Before(n.ckptRoomAt) {
		return false
	}
	if !n.ckptShort {
		return true
	}
	free, err := n.sys.freeSpace(n.ckptDir)
	if err == nil && free < n.ckptShortNeed && n.st.quotaSize() >= n.ckptShortSize {
		n.ckptRoomAt = n.now.Add(ckptRoomRetry)
		return false
	}
	n.ckptShort = false
	return true
}

// CheckpointRoomRetry is ckptRoomRetry, for the simulator's operator, who
// waits for a node to try again before it disarms its NOSPACE.
const CheckpointRoomRetry = ckptRoomRetry

// ckptFits reports whether the disk has room for checkpoint r, scanned,
// beside the current one, with a margin. If not, it drops r: NOSPACE is
// raised and the next try comes ckptRoomRetry later. A disk whose free
// space the system does not say has room.
func (n *node) ckptFits(r *ckptRun) bool {
	free, err := n.sys.freeSpace(n.ckptDir)
	if err != nil {
		return true
	}
	need := n.ckptNeed(r)
	if free >= need {
		return true
	}
	n.dropCkpt(fmt.Errorf("it needs about %d bytes and the disk has %d free", need, free), need)
	return false
}

// ckptNeed bounds the room checkpoint r, scanned, takes, with a margin:
// its header with the members and leases; the values it copies in, each
// once; each key it holds, with its bytes and its states' fields; the
// changes back to the compacted revision, those at it with their key's
// bytes; its end and checksum. Each field counts as the longest varint
// its values make.
func (n *node) ckptNeed(r *ckptRun) int64 {
	st := n.st
	const (
		page = 4 << 10
		u32  = binary.MaxVarintLen32
		u64  = binary.MaxVarintLen64
	)
	rev := int64(uvarintLen(uint64(max(r.meta.rev, 0))))
	// The values it copies are known to the byte; the rest is bounded by
	// each field's longest varint, and only that bound gets the margin. A
	// margin on the values too refused, on a disk that held it, a
	// checkpoint whose ring ran low and which took in 1.6 MB of values,
	// and the ring then stayed full (TestScenarios seed 309 on #846).
	var values int64
	for _, v := range r.rels {
		values += int64(v.n)
	}
	need := int64(len(ckptMagic)+5*u64+u64+proto.Size(r.meta.cs)) + int64(len(r.members)+len(r.leases)) + u64
	need += r.nKeys*(1+3*u32) + r.keyBytes + // its tag, id, key length and state count; the key
		r.nStates*(1+3*rev+u64+2*u32+u64) // flags, create, mod, version, lease, value length and sum, place
	from, to := searchChanges(st.changes, r.meta.compactRev), searchChanges(st.changes, r.meta.rev+1)
	need += int64(to-from) * (1 + rev + u32 + 1 + u32) // tag, revision, key id, deletion, key length
	for _, ch := range st.changes[from:to] {
		if ch.rev != r.meta.compactRev {
			break
		}
		need += int64(st.keys[ch.key].klen) // a change at the compacted revision names its key
	}
	need += 1 + 4 // the end and the checksum
	return values + need + need/8 + page
}

// uvarintLen is the length of v as a uvarint.
func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// cleanCkptDirAtStart does loadNewest's cleanup of the checkpoint dir of
// dataDir before the start writes anything: CURRENT made durable, then
// every checkpoint it does not name and every file being written
// removed. A checkpoint half written, or written whole but not yet named
// by CURRENT, when the disk filled took the room the start's own small
// files need, and the node stopped again at every start (#769).
func cleanCkptDirAtStart(sys sysEnv, dataDir string) error {
	dir := filepath.Join(dataDir, "ckpt")
	if _, err := sys.readDir(dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	p, err := readPointer(sys, dir)
	if err != nil {
		return err
	}
	keep := ""
	if p != nil {
		keep = p.name
		if err := syncDir(sys, dir); err != nil {
			return err
		}
	}
	return cleanCkptDir(sys, dir, keep)
}

// isNoSpace reports whether err is the disk's ENOSPC.
func isNoSpace(err error) bool { return errors.Is(err, syscall.ENOSPC) }

// ckptNoSpace drops the checkpoint being written, which met ENOSPC.
func (n *node) ckptNoSpace(err error) { n.dropCkpt(err, 0) }

// dropCkpt drops the checkpoint being written, for lack of room (why): it
// removes what it wrote, raises NOSPACE, and leaves the next to try
// ckptRoomRetry later, and, if it asked for need bytes the disk did not
// have, once the disk has them or the store is smaller (ckptRetry). The
// checkpoint stays wanted, so that the clients waiting for it get the
// next.
func (n *node) dropCkpt(why error, need int64) {
	r := n.ckpt
	if r == nil {
		return
	}
	// abortCkpt requeues the live client waiters exactly once.
	n.abortCkpt()
	// The writer may have named the file before CURRENT's write failed,
	// and the file takes the room the next needs. A CURRENT that names it
	// already, its rename done and only the dir's sync failed, keeps it: a
	// start loads it.
	if r.w.name != n.ckptName {
		if p, err := readPointer(n.sys, n.ckptDir); err == nil && (p == nil || p.name != r.w.name) {
			_ = n.sys.remove(filepath.Join(n.ckptDir, r.w.name))
		}
	}
	// A client that has gone must not become independent demand.
	// Periodic and ring-pressure checkpoints stay due; Raft's request
	// belongs to the run until a successful checkpoint answers it.
	n.storage.wantCkpt = n.storage.wantCkpt || r.raftWanted
	n.ckptRoomAt = n.now.Add(ckptRoomRetry)
	n.ckptShort, n.ckptShortNeed, n.ckptShortSize = need > 0, need, n.st.quotaSize()
	n.logf("checkpoint at index %d: %v: dropped, NOSPACE, and no checkpoint until there is room", r.meta.index, why)
	n.raiseNoSpace()
	n.simNoRoom()
}
