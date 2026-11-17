package wl

import (
	"bytes"
	"os"
	"strconv"
	"unsafe"
)

// memStats is where the node's memory goes, for the loop's log: what the
// kernel holds for it, Go's share, and the store's tables outside Go's
// heap.
type memStats struct {
	rss                         int64 // resident, as /proc/self/statm says
	goTotal, goLive, goGoal     int64 // Go's mapped memory, its live heap after the last collection, and the next collection's goal
	keys, hash, hist, arena     int64
	slabTop, slabUsed, tree, ch int64
	// The sockets' buffers: sealed and open records out, and the bytes
	// read and opened in; and the peer messages held behind a missing
	// batch.
	sockOut, sockIn, held int64
	// clientHeld is what client streams hold for want of window, and
	// clientPartial what they hold of requests sent in part.
	clientHeld, clientPartial int64
}

func (n *node) memStats() memStats {
	st := n.st
	goTotal, goLive, goGoal := n.sys.goMemory()
	m := memStats{
		rss:      residentBytes(),
		goTotal:  goTotal,
		goLive:   goLive,
		goGoal:   goGoal,
		keys:     int64(st.nkeys) * int64(unsafe.Sizeof(keyEnt{})),
		hash:     int64(len(st.hash)) * 8,
		hist:     int64(st.nhist) * int64(unsafe.Sizeof(histEnt{})),
		arena:    int64(st.kmem.top),
		slabTop:  int64(st.slab.top),
		slabUsed: st.slab.used,
		tree:     st.tree.bytes(),
		ch:       int64(cap(st.changes)) * int64(unsafe.Sizeof(change{})),
	}
	for _, x := range n.socks {
		if s := sockOf(x); s != nil {
			m.sockOut += int64(cap(s.out))
			m.sockIn += int64(cap(s.in) + cap(s.pt))
		}
		if c, ok := x.(*client); ok {
			m.clientHeld += c.flow.bytes
			m.clientPartial += c.partial
		}
		if p, ok := x.(*peerIn); ok {
			for _, h := range p.held {
				m.held += int64(cap(h.f))
			}
		}
	}
	return m
}

// residentBytes reads the process's resident memory, or -1 where /proc
// does not tell.
func residentBytes() int64 {
	b, err := procFile("/proc/self/statm")
	if err != nil {
		return -1
	}
	f := bytes.Fields(b)
	if len(f) < 2 {
		return -1
	}
	pages, err := strconv.ParseInt(string(f[1]), 10, 64)
	if err != nil {
		return -1
	}
	return pages * int64(os.Getpagesize())
}

// cpuStats is where the node's time goes, for the loop's log: the CPU of
// the process and of the loop's thread, the log's helper in its writes,
// a checkpoint's encoding on the loop and its writer's reads and writes,
// and the kernel's dirty pages waiting to be written back. Times are
// totals in nanoseconds; a checkpoint's are its own.
type cpuStats struct {
	process, loop, walWrite         int64
	ckptEncode, ckptRead, ckptWrite int64
	dirty, writeback                int64
}

// cpuStats reads the totals; it runs on the loop's thread.
func (n *node) cpuStats() cpuStats {
	var c cpuStats
	c.process, c.loop = sysCPU()
	if n.wal != nil {
		c.walWrite = n.wal.writeNs.Load()
	}
	if r := n.ckpt; r != nil {
		c.ckptEncode, c.ckptRead, c.ckptWrite = r.encodeNs, r.w.readNs.Load(), r.w.writeNs.Load()
	}
	c.dirty, c.writeback = dirtyPages()
	return c
}

// dirtyPages reads /proc/meminfo's dirty and writeback bytes, or -1 where
// /proc does not tell.
func dirtyPages() (dirty, writeback int64) {
	dirty, writeback = -1, -1
	b, err := procFile("/proc/meminfo")
	if err != nil {
		return dirty, writeback
	}
	for line := range bytes.Lines(b) {
		f := bytes.Fields(line)
		if len(f) < 2 {
			continue
		}
		kb, err := strconv.ParseInt(string(f[1]), 10, 64)
		if err != nil {
			continue
		}
		switch string(f[0]) {
		case "Dirty:":
			dirty = kb << 10
		case "Writeback:":
			writeback = kb << 10
		}
	}
	return dirty, writeback
}
