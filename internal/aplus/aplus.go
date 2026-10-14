// Package aplus is a throwaway prototype of storage design A+ for
// djosh34/tigerstore#133.
//
// Question: built crudely and measured, what does A+ cost against main
// 76952348? A+ keeps values in the Raft log, written once, and names each
// by a local file, offset, length and checksum. Checkpoints write only the
// dirty pages of a copy-on-write metadata tree under a durable root.
// Cleaning relocates live values and the committer swaps references. Raft
// entries stay in RAM only in a bounded window. Cold values are read off
// the committer.
//
// Shortcuts (crude on purpose): value files are mmapped and the index keeps
// a slice into the mapping as the descriptor (file, offset, length), plus
// a CRC32C per value. A cold read is a page fault in whichever goroutine
// copies the bytes, which is the connection writer, never the committer
// (except a VALUE compare). Global state lives in this package.
package aplus

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/metrics"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// Dir is the directory of the log, value and metadata files.
var Dir string

// Env knobs.
var (
	// Window is the bytes of applied Raft entries kept in RAM.
	Window = envInt("APLUS_WINDOW", 64<<20)
	// CkptEvery is the log bytes between checkpoints.
	CkptEvery = envInt("APLUS_CKPT_EVERY", 64<<20)
	// CkptRatio cuts a checkpoint once the log since the last one is this
	// many times the size of all leaf pages; 0 turns it off.
	CkptRatio = envFloat("APLUS_CKPT_RATIO", 32)
	// CleanU is the live fraction at or below which a sealed file is cleaned.
	CleanU = envFloat("APLUS_CLEAN_U", 0.5)
	// SpaceAmp: cleaning runs only while the log and value files hold more
	// than this many times the live value bytes.
	SpaceAmp = envFloat("APLUS_SPACE_AMP", 2)
	// PaceMicros is the checkpoint writer's sleep per 256 KiB of page
	// work and per 1 MiB written; 0 turns pacing off.
	PaceMicros = envInt("APLUS_PACE_US", 1000)
	// Off turns the whole prototype off, for a main-like control run.
	Off = os.Getenv("APLUS_OFF") != ""
)

func envInt(name string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return def
}

func envFloat(name string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil {
		return v
	}
	return def
}

// Bytes written to disk, by component.
var (
	LogBytes        atomic.Int64 // every byte the WAL wrote
	LogValueBytes   atomic.Int64 // value bytes inside applied entries (from the committer)
	LogEntryData    atomic.Int64 // entry payload bytes
	CkptLeaf        atomic.Int64
	CkptLeafHist    atomic.Int64 // of leaf bytes, the versions that are not a key's newest
	CkptLeafOther   atomic.Int64 // of leaf bytes, unchanged keys that share a page with a changed one
	CkptInterior    atomic.Int64
	CkptRoot        atomic.Int64
	CkptMetaMove    atomic.Int64 // leaf pages rewritten only to empty an old metadata file
	CleanBytes      atomic.Int64 // relocated value bytes
	SnapSent        atomic.Int64 // snapshot bytes handed to the peer layer
	SnapsSent       atomic.Int64
	SnapBuilt       atomic.Int64 // snapshot bytes built
	Checkpoints     atomic.Int64
	Cleanings       atomic.Int64
	FilesDeleted    atomic.Int64
	PauseCaptureNs  atomic.Int64 // committer time capturing dirty pages
	PauseCaptureMax atomic.Int64
	PauseCleanNs    atomic.Int64 // committer time finding and swapping relocated values
	PauseCleanMax   atomic.Int64
	Puts            atomic.Int64
)

// MaxPause records a committer pause.
func MaxPause(total, max *atomic.Int64, d time.Duration) {
	total.Add(int64(d))
	for {
		m := max.Load()
		if int64(d) <= m || max.CompareAndSwap(m, int64(d)) {
			return
		}
	}
}

// ValueFileBase is the first ID of a value file made by cleaning or by a
// snapshot install; IDs below it are WAL segment numbers.
const ValueFileBase = 1 << 40

// MetaFileBase is the first ID of a metadata file.
const MetaFileBase = 2 << 40

// FileName names a file by ID.
func FileName(fid uint64) string {
	switch {
	case fid >= MetaFileBase:
		return fmt.Sprintf("%016x.meta", fid-MetaFileBase)
	case fid >= ValueFileBase:
		return fmt.Sprintf("%016x.val", fid-ValueFileBase)
	}
	return fmt.Sprintf("%016x.wal", fid)
}

type region struct {
	base uintptr
	size int
	fid  uint64
	data []byte
}

var (
	mu      sync.Mutex
	regions atomic.Pointer[[]region] // sorted by base, copy on write
)

// mapBytes is the address space reserved per file. Pages past the end of
// the file are never touched: values are read only after they are written.
const mapBytes = 1 << 30

// Map maps file fid read-only and returns the mapping.
func Map(fid uint64) []byte {
	mu.Lock()
	defer mu.Unlock()
	if r := find(fid); r != nil {
		return r.data
	}
	f, err := os.Open(filepath.Join(Dir, FileName(fid)))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	data, err := syscall.Mmap(int(f.Fd()), 0, mapBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	// A cold value read faults in its own pages only, no read-around:
	// the design reads a value's exact range with pread.
	if err := syscall.Madvise(data, syscall.MADV_RANDOM); err != nil {
		panic(err)
	}
	var rs []region
	if p := regions.Load(); p != nil {
		rs = slices.Clone(*p)
	}
	rs = append(rs, region{base: uintptr(unsafe.Pointer(&data[0])), size: len(data), fid: fid, data: data})
	slices.SortFunc(rs, func(a, b region) int {
		if a.base < b.base {
			return -1
		}
		return 1
	})
	regions.Store(&rs)
	return data
}

func find(fid uint64) *region {
	p := regions.Load()
	if p == nil {
		return nil
	}
	for i := range *p {
		if (*p)[i].fid == fid {
			return &(*p)[i]
		}
	}
	return nil
}

// Get returns the mapping of fid, mapping it if needed.
func Get(fid uint64) []byte { return Map(fid) }

// Locate returns the file and offset of a value slice.
func Locate(v []byte) (fid uint64, off int, ok bool) {
	if len(v) == 0 {
		return 0, 0, false
	}
	p := regions.Load()
	if p == nil {
		return 0, 0, false
	}
	a := uintptr(unsafe.Pointer(unsafe.SliceData(v)))
	rs := *p
	i, _ := slices.BinarySearchFunc(rs, a, func(r region, a uintptr) int {
		if r.base+uintptr(r.size) <= a {
			return -1
		}
		if r.base > a {
			return 1
		}
		return 0
	})
	if i < len(rs) && rs[i].base <= a && a < rs[i].base+uintptr(rs[i].size) {
		return rs[i].fid, int(a - rs[i].base), true
	}
	return 0, 0, false
}

// Remove unlinks a file now and unmaps it after a grace period, so a
// reader that still holds a slice of it finishes first (crude: no pins).
func Remove(fid uint64) {
	os.Remove(filepath.Join(Dir, FileName(fid)))
	FilesDeleted.Add(1)
	time.AfterFunc(30*time.Second, func() {
		mu.Lock()
		defer mu.Unlock()
		p := regions.Load()
		if p == nil {
			return
		}
		rs := slices.Clone(*p)
		for i := range rs {
			if rs[i].fid == fid {
				syscall.Munmap(rs[i].data)
				rs = slices.Delete(rs, i, i+1)
				break
			}
		}
		regions.Store(&rs)
	})
}

var heapSample = []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/total:bytes"}}

// Stats is one line of counters for the node log.
func Stats() string {
	metrics.Read(heapSample)
	return fmt.Sprintf("aplus-stats heap_live=%d go_total=%d log=%d logdata=%d logvalue=%d ckpt_leaf=%d ckpt_leaf_hist=%d ckpt_leaf_other=%d ckpt_interior=%d ckpt_root=%d ckpt_metamove=%d clean=%d snapsent=%d snaps=%d snapbuilt=%d ckpts=%d cleanings=%d deleted=%d capture_ns=%d capture_max_ns=%d clean_pause_ns=%d clean_pause_max_ns=%d puts=%d",
		heapSample[0].Value.Uint64(), heapSample[1].Value.Uint64(), LogBytes.Load(), LogEntryData.Load(), LogValueBytes.Load(), CkptLeaf.Load(), CkptLeafHist.Load(), CkptLeafOther.Load(), CkptInterior.Load(), CkptRoot.Load(), CkptMetaMove.Load(),
		CleanBytes.Load(), SnapSent.Load(), SnapsSent.Load(), SnapBuilt.Load(), Checkpoints.Load(), Cleanings.Load(), FilesDeleted.Load(),
		PauseCaptureNs.Load(), PauseCaptureMax.Load(), PauseCleanNs.Load(), PauseCleanMax.Load(), Puts.Load())
}
