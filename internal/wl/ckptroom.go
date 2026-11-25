package wl

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"math/bits"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// A checkpoint needs room on the disk beside the checkpoint it replaces,
// which stays until the new one is whole. A node whose disk lacks it does
// as etcd does with a database past its quota: it raises its NOSPACE
// alarm and stays up, serving reads, deletes, compactions, defrags and
// the alarm's disarm, and writes no checkpoint until there is room again.
// Before #769 it wrote the checkpoint anyway, stopped on ENOSPC halfway,
// and stopped again at every start, its half-written file filling the
// disk. A checkpoint that meets ENOSPC partway, the room counted wrong or
// taken meanwhile, is dropped the same way.

// ckptRoomRetry is how long a node that found no room for a checkpoint
// waits before it asks again.
const ckptRoomRetry = time.Second

// ckptRoom reports whether the disk has room for the next checkpoint
// beside the current one, with a margin. If not, it raises NOSPACE and
// asks again ckptRoomRetry later. A disk whose free space the system does
// not say has room.
func (n *node) ckptRoom() bool {
	if n.now.Before(n.ckptRoomAt) {
		return false
	}
	free, err := n.sys.freeSpace(n.ckptDir)
	if err != nil {
		return true
	}
	need := n.ckptNeed()
	if free >= need {
		return true
	}
	n.ckptRoomAt = n.now.Add(ckptRoomRetry)
	n.logf("checkpoint: it needs about %d bytes and the disk has %d free: NOSPACE, and no checkpoint until there is room", need, free)
	n.raiseNoSpace()
	return false
}

// ckptNeed bounds the room a checkpoint of the store takes, with a margin.
// The file holds at most the values it copies in, which the store's value
// bytes, live and in the history, bound; each key, its bytes and its
// states' fields; the changes back to the compacted revision, some of
// which name their key; the leases and the members. Each field counts as
// the longest varint its values make. The quota's size is no bound: it
// counts 32 bytes a state, which a leased key's state passes, and no
// changes.
func (n *node) ckptNeed() int64 {
	st := n.st
	const (
		page = 4 << 10
		u32  = binary.MaxVarintLen32
		u64  = binary.MaxVarintLen64
	)
	rev := int64(uvarintLen(uint64(max(st.rev, 0))))
	keys := int64(st.nkeys) - int64(len(st.keyFree))
	// Each key's state now, live or deleted, and those in the history.
	states := keys + int64(st.nhist) - int64(st.nfree)
	need := st.liveBytes + st.histBytes +
		keys*(1+3*u32) + st.kmem.used + // its tag, id, key length and state count; the key
		states*(1+3*rev+u64+2*u32+u64) + // flags, create, mod, version, lease, value length and sum, place
		int64(len(st.changes))*(1+rev+u32+1+u32) + st.kmem.used + // tag, revision, key id, deletion, key length; the keys named
		int64(len(st.leases))*2*u64 + int64(len(n.encodeMembers(nil))) + page
	return need + need/8 + page
}

// uvarintLen is the length of v as a uvarint.
func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// dropCkptTemps removes the files a node that stopped was writing in the
// checkpoint dir of dataDir, before the start writes anything: a
// checkpoint half written when the disk filled took the room the start's
// own small files need, and the node stopped again at every start (#769).
// No start needs one; loadNewest removes them too, later.
func dropCkptTemps(sys sysEnv, dataDir string) error {
	dir := filepath.Join(dataDir, "ckpt")
	names, err := sys.readDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasSuffix(name, ".tmp") {
			if err := sys.remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// isNoSpace reports whether err is the disk's ENOSPC.
func isNoSpace(err error) bool { return errors.Is(err, syscall.ENOSPC) }

// ckptNoSpace drops the checkpoint being written, which met ENOSPC: it
// removes what it wrote, raises NOSPACE, and leaves the next checkpoint
// to ask for room ckptRoomRetry later. The clients waiting for the
// checkpoint wait for the next.
func (n *node) ckptNoSpace(err error) {
	r := n.ckpt
	if r == nil {
		return
	}
	waiters := r.snapWaiters
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
	n.snapWaiters = append(waiters, n.snapWaiters...)
	n.ckptRoomAt = n.now.Add(ckptRoomRetry)
	n.logf("checkpoint at index %d: %v: dropped, NOSPACE, and no checkpoint until there is room", r.meta.index, err)
	n.raiseNoSpace()
}
