package wl

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// The log ring's space (wal.go's head, #602). A log made with a ring
// formats it when its directory is made: spare segment files, each written
// in full with zeros, and then the meta file (ringMeta) that says the
// ring is there. Making segment k renames a spare into place, and a cut,
// or a start, renames the files it frees back into the pool, so the log
// only ever writes into space written before.
//
// Reuse changes what a crash leaves. A write the disk tore keeps, in the
// sectors it lost, what the space held before, not the zeros a fresh
// extent reads back as, so a lost sector no longer tells a torn write from
// damage. Two rules follow:
//   - The torn end. One sync is under way at a time, and each sync's last
//     record (recSynced, appendSynced) says the log is durable up to where
//     the sync started: what syncs before it wrote. A record that does not
//     check is damage if a record further on that checks at its offset
//     says the log is durable past it; else it is where a crash tore the
//     last sync, and the log ends there (ringDamaged). Records an earlier
//     lap left do not check where the log reads them (wal.go's rule 1), so
//     they claim nothing. A log that goes idle syncs a mark alone at its
//     next tick (markIdle), so that the last sync's records are claimed
//     too; damage to them before that mark is durable, within a tick of
//     the last sync, reads as the torn last sync, and the log ends before
//     them. The simulator damages only records a mark claims.
//   - No record a crash left past the end is read again. A start writes
//     zeros over the last segment from the log's end on, and over every
//     segment after it before it goes back to the pool: those records lie
//     at offsets the log reaches again, where they would check
//     (wal.reopen). What a cut frees lies before the log's start, which
//     it never reaches again.

// ringMeta is the ring's meta file in the log's directory; its presence
// says the ring is formatted.
const ringMeta = "RING"

// ringInfo is what the meta file holds: the ring's segments, and the
// segment size it was formatted with.
type ringInfo struct {
	segs, segSize int64
}

// readRing returns the ring's meta of the log in dir, or nil when the log
// has no ring.
func readRing(sys sysEnv, dir string) (*ringInfo, error) {
	b, err := sys.readFile(filepath.Join(dir, ringMeta))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) != 2*8+4 || crc32.Checksum(b[:16], castagnoli) != binary.LittleEndian.Uint32(b[16:]) {
		return nil, fmt.Errorf("%w: the ring's meta file %s does not check", ErrCorruptLog, filepath.Join(dir, ringMeta))
	}
	return &ringInfo{
		segs:    int64(binary.LittleEndian.Uint64(b)),
		segSize: int64(binary.LittleEndian.Uint64(b[8:])),
	}, nil
}

// writeRing writes the ring's meta: a file of its own, renamed into
// place, and the directory synced.
func writeRing(sys sysEnv, dir string, r *ringInfo) error {
	b := binary.LittleEndian.AppendUint64(nil, uint64(r.segs))
	b = binary.LittleEndian.AppendUint64(b, uint64(r.segSize))
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	tmp := filepath.Join(dir, ringMeta+".tmp")
	if err := writeFileSynced(sys, tmp, b); err != nil {
		return err
	}
	if err := sys.rename(tmp, filepath.Join(dir, ringMeta)); err != nil {
		return err
	}
	return syncDir(sys, dir)
}

// spareExt names the ring's spare files, each by a number no other spare
// of the log ever had: the format numbers its from 0, and a spare made
// later takes the next past the highest in the directory (spareSeq). A
// segment's number does not do: one that a crash cut off is made again,
// and freed twice.
const spareExt = ".spare"

func sparePath(dir string, i int64) string {
	return filepath.Join(dir, fmt.Sprintf("%016x%s", i, spareExt))
}

// spareSeq is the number past the highest of spares.
func spareSeq(spares []string) int64 {
	var next int64
	for _, p := range spares {
		var i int64
		if _, err := fmt.Sscanf(filepath.Base(p), "%016x"+spareExt, &i); err == nil {
			next = max(next, i+1)
		}
	}
	return next
}

// listSpares returns the spare files of the log in dir.
func listSpares(sys sysEnv, dir string) ([]string, error) {
	names, err := sys.readDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		if strings.HasSuffix(name, spareExt) {
			out = append(out, filepath.Join(dir, name))
		}
	}
	return out, nil
}

// hasSegments reports whether the log in dir holds any segment.
func hasSegments(sys sysEnv, dir string) (bool, error) {
	names, err := sys.readDir(dir)
	if err != nil {
		return false, err
	}
	for _, name := range names {
		if strings.HasSuffix(name, ".seg") {
			return true, nil
		}
	}
	return false, nil
}

// formatRing formats a ring of segs segments in dir, a log with no
// segment: spare files written in full with zeros, synced, and then the
// meta file. A crash before the meta leaves no ring; the next start
// formats again, over what it left.
func formatRing(sys sysEnv, dir string, segs int64) error {
	old, err := listSpares(sys, dir)
	if err != nil {
		return err
	}
	for _, p := range old {
		if err := sys.remove(p); err != nil {
			return err
		}
	}
	zeros, err := mmapBytes(int(min(segSize, 4<<20)))
	if err != nil {
		return err
	}
	for i := range segs {
		if err := writeZeros(sys, sparePath(dir, i), 0, zeros, true); err != nil {
			return err
		}
	}
	if err := syncDir(sys, dir); err != nil {
		return err
	}
	return writeRing(sys, dir, &ringInfo{segs: segs, segSize: segSize})
}

// writeZeros writes zeros into the file at path from offset from to
// segSize, through zeros, a block-aligned buffer of them, durably;
// create makes the file.
func writeZeros(sys sysEnv, path string, from int64, zeros []byte, create bool) error {
	if create {
		fd, err := sys.open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		_ = sysClose(fd)
	}
	fd, err := sys.openDirect(path)
	if err != nil {
		return err
	}
	defer func() { _ = sysClose(fd) }()
	for off := from; off < segSize; {
		n, err := sysPwrite(fd, zeros[:min(int64(len(zeros)), segSize-off)], off)
		if err != nil {
			return fmt.Errorf("write zeros to %s at %d: %w", path, off, err)
		}
		off += int64(n)
	}
	return sysFsync(fd)
}

// ringMinSegs is the fewest segments a ring sized from a budget or the
// quota holds: one written, two the maker holds ahead, the reserve
// (ringReserve) and room to go low (ringLow) before it is short. A ring
// of fewer would be low from its first write (#751's review). The log
// itself works with four (ringSegs), as the model's rings do.
const ringMinSegs = 8

// ringSegs is how many segments a ring of size bytes holds, at least
// ringMinSegs. Only a ring sized from the quota is ever rounded up to
// them: the quota bounds the store's values, not the disk. A disk budget
// or a ring size too small for them is refused at start (Config.check),
// so that the ring never takes more than the budget (#751's review).
func ringSegs(size int64) int64 {
	if size <= 0 {
		return 0
	}
	return max(size/segSize, 4)
}

// appendSynced appends the sync's last record: the log is durable before
// the sync's start, done (the head of this file). A stage too full for it
// leaves it out, and the sync claims nothing; it reports whether it
// appended it. Only the loop calls it.
func (w *wal) appendSynced() bool {
	var b [recHead + 8]byte
	done := w.done.Load()
	binary.LittleEndian.PutUint32(b[4:], 8)
	b[8] = recSynced
	binary.LittleEndian.PutUint64(b[recHead:], uint64(done))
	binary.LittleEndian.PutUint32(b[:], crc32.Update(recSeed(w.off), castagnoli, b[4:]))
	if w.off+int64(len(b))-done > w.stageLen-block {
		return false
	}
	w.claimed = done
	for v := b[:]; len(v) > 0; {
		pos := w.off & (w.stageLen - 1)
		k := copy(w.stage[pos:], v)
		v = v[k:]
		w.off += int64(k)
	}
	if tail := w.off % block; tail != 0 {
		pos := w.off & (w.stageLen - 1)
		clear(w.stage[pos : pos+block-tail])
	}
	return true
}

// markIdle syncs a mark alone once a ring's log has gone idle: nothing
// waits for a sync, none started for half a tick, and the last sync's
// records are durable but no mark claims them. Until one does, damage to
// them reads as a crash's torn sync, and a start would cut them, though
// they were durable: on seed 7 of TestSeeds a flipped hard state, the
// last sync's, was cut, and the node's vote with it (#751's CI). A node
// that syncs more often than that, a busy follower's too, has the next
// sync's mark claim them, and syncs no mark alone; the mark leaves the
// sync gap's measures (lastKick, lastBytes) as they were (#751's
// re-check). The loop calls it at its tick.
func (w *wal) markIdle(now time.Time) {
	if w.ring == nil || w.inflight || len(w.pending) > 0 || now.Sub(w.lastKick) < tickEvery/2 ||
		w.claimed >= w.dataEnd || w.done.Load() < w.dataEnd || !w.appendSynced() {
		return
	}
	w.stats.syncs++
	w.want.Store(w.off)
	w.inflight = true
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = sysWrite(w.kickFD, b[:])
}

// claimsPast reports whether b, the log's bytes at offset at, holds a
// sync's mark (recSynced) that checks at its offset and says the log is
// durable past off.
func claimsPast(b []byte, at, off int64) bool {
	for i := 0; i+recHead+8 <= len(b); i++ {
		r := b[i:]
		if r[8] != recSynced || !checks(r, at+int64(i)) {
			continue
		}
		if int64(binary.LittleEndian.Uint64(r[recHead:])) > off {
			return true
		}
	}
	return false
}

// ringDamaged is damaged for a ring's log: the error of b, the log's bytes
// at offset off on, in segment k on (none past it when k is
// math.MaxInt64-1), whose first record does not check. It is nil unless
// a sync's mark further on says the log is durable past off: then a
// crash cannot have torn it, and the log is damaged (the head of this
// file).
func ringDamaged(sys sysEnv, dir string, k, off int64, b []byte) error {
	if claimsPast(b, off, off) {
		return fmt.Errorf("%w: the record at log offset %d does not check, and a later sync says the log is durable past it", ErrCorruptLog, off)
	}
	if k == math.MaxInt64-1 {
		return nil
	}
	// A mark may lie across the end of a segment: each segment is read
	// with the last bytes before it, as many as a mark's but one (#751's
	// review).
	const carry = recHead + 8 - 1
	prev, end := b, off+int64(len(b))
	for k++; ; k++ {
		seg, err := sys.readFile(segPath(dir, k))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		at, buf := k*segSize, seg
		if end == at {
			tail := prev[max(0, len(prev)-carry):]
			at, buf = at-int64(len(tail)), append(slices.Clip(tail), seg...)
		}
		if claimsPast(buf, at, off) {
			return fmt.Errorf("%w: the record at log offset %d does not check, and a later segment's sync says the log is durable past it", ErrCorruptLog, off)
		}
		prev, end = seg, k*segSize+int64(len(seg))
	}
}

// logRing is the size of the log's ring for cfg: LogRing if set, else
// half the disk budget, else the quota (#602's ruling); 0, a log with no
// ring, when the quota is off.
func logRing(cfg *Config) int64 {
	switch {
	case cfg.LogRing != 0:
		return max(cfg.LogRing, 0)
	case cfg.DiskBudget > 0:
		return cfg.DiskBudget / 2
	case cfg.QuotaBackendBytes < 0:
		return 0
	}
	return max(cmp.Or(cfg.QuotaBackendBytes, defaultQuota), ringMinSegs*segSize)
}

// ringReserve is how many spare segments a ring keeps for the writes that
// free space, deletes, compactions and alarms, once it refuses the rest
// (ringShort): two, and a sixteenth of a large ring.
func ringReserve(segs int64) int64 { return max(2, segs/16) }

// ringFree is how many of the ring's segments the log does not hold: all
// but those from the cut (rbase) to the one its end is in. The segments
// the maker made ahead, and those cut and not yet back in the pool, are
// free: the log has not reached them, or let them go (#751's review).
// Only the loop calls it.
func (w *wal) ringFree() int64 {
	return w.ring.segs - (w.off/segSize - w.rbase + 1)
}

// ringLow reports whether the ring's free segments ran low: a checkpoint
// is due (ringDue), and its cut takes in what the store names before it,
// so that the segments go back to the pool (cutPoint). Only the loop
// calls it; a node with no log, as checkpoint tests make, has no ring.
func (w *wal) ringLow() bool {
	return w != nil && w.ring != nil && w.ringFree() <= ringReserve(w.ring.segs)+max(1, w.ring.segs/8)
}

// ringShort reports whether the ring holds no more than its reserve: the
// node refuses writes as over the quota, and raises NOSPACE, as etcd does
// (alarm.go).
func (w *wal) ringShort() bool {
	return w.ring != nil && w.ringFree() < ringReserve(w.ring.segs)
}

// ringDue reports whether a checkpoint is due for the ring's sake: it ran
// low, and a checkpoint would let the cut reach further. A checkpoint's
// cut reaches the start of the checkpoint before it (cutPoint), so one
// helps if the newest checkpoint started in a segment past the cut, or if
// the node applied entries past it, so that the one after can (#751's
// review). Both are the checkpoints' own places, not the log's: the cut
// may stay behind them for Raft's storage. Without them, a ring that
// stays low would checkpoint on end for nothing, and a full one would
// never answer NOSPACE.
func (n *node) ringDue() bool {
	if !n.wal.ringLow() {
		return false
	}
	snap := n.storage.snap
	return snap == nil || n.applied > snap.GetMetadata().GetIndex() || n.ckptFrom/segSize > n.ckptCut/segSize
}

// cutLog lets the log's tail go once a checkpoint is durable or an entry
// applied: while a ring is low, Raft's storage drops what the newest
// checkpoint holds, and the log is cut up to the checkpoint's cut and what
// Raft's storage still holds. A checkpoint that finishes while writes are
// refused frees its segments at once, not at the next apply (#751's
// review).
func (n *node) cutLog() {
	s := n.storage
	if s.snap == nil {
		return
	}
	if n.wal.ringLow() {
		// The ring needs the space more than a follower that lags needs
		// the entries: that one gets the checkpoint as a snapshot, and
		// the cut may reach the checkpoint (logring.go).
		s.compact(min(n.applied, s.snap.GetMetadata().GetIndex()))
	}
	if !ringHoldCut.Load() {
		n.removeAll(n.wal.cut(min(n.ckptCut, s.firstRecord(n.wal.off))))
	}
}

// SetSegmentSize sets the size of the log's segments for the process, as
// --unsafe-segment-bytes asks: for tests only, such as real-process chaos,
// whose small disks hold no ring of 64 MiB segments. It is called before
// any node runs; a data directory keeps the size it was made with.
func SetSegmentSize(n int64) error {
	if n <= 0 || n%block != 0 {
		return fmt.Errorf("%w: a segment of %d bytes: it must be a positive multiple of %d", errConfig, n, block)
	}
	segSize = n
	return nil
}

// ringHoldCut keeps the log's cut where it is, for the test of a full ring
// (TestAFullLogRingRaisesNOSPACE): the state of a ring whose log no
// checkpoint can free.
var ringHoldCut atomic.Bool

// ckptHold keeps a running checkpoint from going on, for the test of a
// short ring (TestAShortLogRingFreesWhenACheckpointEnds): the ring fills
// while every write is refused, until it ends.
var ckptHold atomic.Bool

// ringRefuses refuses a write of stream s while the log's ring is short
// (ringShort), and reports whether it did: a put, a lease grant, or a Txn
// that puts. While a checkpoint runs or is due, whose cut gives segments
// back, the node is too busy for it, as etcd answers past its apply gap;
// with nothing left to free, it raises NOSPACE, as a full disk does to
// etcd (#602's ruling), and answers NOSPACE while the alarm is up, as
// etcd does. Deletes and compactions, which free space, pass,
// within the ring's reserve.
func (n *node) ringRefuses(c *client, s *stream, msg []byte) bool {
	if !n.wal.ringShort() {
		return false
	}
	switch s.kind {
	case kPut, kLeaseGrant:
	case kTxn:
		if txnCost(msg) == 0 {
			return false
		}
	default:
		return false
	}
	if (n.ckpt != nil || n.ringDue()) && !n.noSpaceRaised() {
		n.reject(c, s, errTooManyRequests)
		return true
	}
	n.refuseNoSpace(c, s)
	return true
}

// ckptEvery is the log written after the newest checkpoint before the
// next: ckptLog, and at most a quarter of the log's ring. A cut reaches
// only the checkpoint before's place in the log, so a ring holds about
// two checkpoints' log; checkpoints a quarter of it apart keep its tail
// going back to the pool as the log grows.
func (n *node) ckptEvery() int64 {
	if r := n.wal.ring; r != nil {
		return min(n.ckptLog, r.segs*r.segSize/4)
	}
	return n.ckptLog
}
